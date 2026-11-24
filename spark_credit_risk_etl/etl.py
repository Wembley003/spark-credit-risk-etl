"""
Capa: Reglas de negocio — validación, enriquecimiento y agregados de la cartera. Por dentro, el flujo de datos lleva sus propias capas Bronce → Plata → Oro.
ETL de riesgo de crédito con PySpark, corriendo en modo local.

La idea de fondo es la misma que en física estadística: una cartera no
se entiende crédito por crédito, sino por los promedios del conjunto.
Cada crédito trae su exposición (EAD, cuánto dinero hay en juego), su
probabilidad de impago (PD) y lo que se perdería si impaga (LGD). La
pérdida esperada de toda la cartera es el valor esperado de esa
distribución — la suma de EAD x PD x LGD — y la morosidad es qué fracción
de la exposición ya está vencida. Este módulo calcula esos "momentos" de
la cartera con Spark, los valida antes de creerse nada y los deja
guardados en Parquet (para máquinas) y en CSV (para humanos).

Todo corre con operaciones del lado de la JVM — lectura de CSV, SQL y
escritura de Parquet — a propósito: en la máquina virtual donde se
desarrolló esto, el puente Python-JVM de PySpark (el que usan
``createDataFrame`` y ``toPandas``) se cae por el sandbox, así que el ETL
está diseñado para no necesitarlo. En una máquina normal ese puente
funciona, pero aquí no hace falta.

El pipeline está organizado en tres capas, como un yacimiento
estratificado — cada capa es un estado de los datos y solo se puede
llegar a la siguiente pasando por la anterior:

- **Bronce** (``to_bronze``): los datos tal como llegan, sin tocar.
  Se guardan en ``outputs/bronze`` para que siempre se pueda volver
  al origen y reproducir todo desde ahí.
- **Plata** (``to_silver``): los datos limpios y validados. Aquí vive
  la compuerta de calidad: si la cartera no pasa la validación, el
  pipeline se detiene y nada de lo que sigue se escribe. También se
  enriquece: pérdida esperada y buckets de PD.
- **Oro** (``to_gold``): los agregados listos para el negocio — el
  resumen de la cartera y sus aperturas por segmento, estatus y
  bucket de PD. Es lo que leen el tablero, el Excel y Access.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

__all__ = [
    "PORTFOLIO_SCHEMA",
    "ESTATUS_VALIDOS",
    "PD_BUCKETS",
    "build_spark",
    "read_portfolio",
    "validate_portfolio",
    "add_expected_loss",
    "add_pd_bucket",
    "portfolio_summary_df",
    "segment_summary_df",
    "status_summary_df",
    "pd_bucket_summary_df",
    "row_to_dict",
    "to_bronze",
    "to_silver",
    "to_gold",
    "write_outputs",
    "run_etl",
]

# El esquema va explícito, no se deja que Spark lo adivine. Si una columna
# llega como texto por un dato raro, inferSchema le pondría tipo string y
# las multiplicaciones fallarían a la mitad del pipeline, sin avisar
# dónde. Con el esquema fijo, el archivo que no cumple el contrato se
# rechaza en la puerta, que es donde debe rechazarse.
PORTFOLIO_SCHEMA = StructType(
    [
        StructField("id_credito", StringType(), nullable=False),
        StructField("segmento", StringType(), nullable=False),
        StructField("ead", DoubleType(), nullable=False),
        StructField("pd", DoubleType(), nullable=False),
        StructField("lgd", DoubleType(), nullable=False),
        StructField("estatus", StringType(), nullable=False),
    ]
)

ESTATUS_VALIDOS = ("vigente", "vencido")

# Los buckets de PD son los que un analista de riesgo miraría primero:
# casi nada de riesgo, riesgo medio y la cola donde se concentra el
# peligro. Los cortes (2%, 5%, 10%) son convención de trabajo, no ley.
PD_BUCKETS = ["<2%", "2-5%", "5-10%", ">=10%"]


def build_spark(app_name="spark-credit-risk-etl", master="local[*]",
                spark_tmp=None):
    """Levanta la sesión de Spark en modo local.

    Los dos ajustes de red (driver en 127.0.0.1) son necesarios en la
    máquina virtual donde nació este proyecto: ahí el nombre del host
    resuelve a una IP que el ejecutor no alcanza y las tareas se caen
    con "broken pipe". En cualquier otra máquina son inocuos.
    El directorio temporal se pone dentro del proyecto y no en /tmp,
    porque en esta VM /tmp es un tmpfs de 512 MB y un shuffle grande
    lo llenaría.
    """
    if spark_tmp is None:
        spark_tmp = Path(__file__).resolve().parent.parent / "spark-tmp"
    spark_tmp = Path(spark_tmp)
    spark_tmp.mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.local.dir", str(spark_tmp))
        .getOrCreate()
    )


def read_portfolio(spark, path):
    """Lee el CSV de la cartera con el esquema fijo."""
    return (
        spark.read.option("header", True)
        .schema(PORTFOLIO_SCHEMA)
        .csv(str(path))
    )


def validate_portfolio(df):
    """Revisa que la cartera tenga sentido antes de calcular nada.

    Devuelve una lista de problemas (vacía = todo bien). Esto es lo
    primero que se hace en un ETL serio: si los datos de entrada están
    rotos, cualquier número que salga después es mentira con formato.
    Se revisan nulos, exposiciones y probabilidades fuera de rango,
    estatus desconocidos e ids duplicados.
    """
    issues = []
    n = df.count()
    if n == 0:
        return ["la cartera está vacía (0 filas)"]
    for col in ("id_credito", "segmento", "ead", "pd", "lgd", "estatus"):
        n_null = df.filter(F.col(col).isNull()).count()
        if n_null:
            issues.append(f"columna {col}: {n_null} valores nulos")
    n_bad_ead = df.filter(F.col("ead") <= 0).count()
    if n_bad_ead:
        issues.append(f"ead: {n_bad_ead} filas con exposición <= 0")
    for col in ("pd", "lgd"):
        n_bad = df.filter((F.col(col) < 0) | (F.col(col) > 1)).count()
        if n_bad:
            issues.append(f"{col}: {n_bad} filas fuera del rango [0, 1]")
    bad_status = (
        df.filter(~F.col("estatus").isin(*ESTATUS_VALIDOS))
        .select("estatus").distinct().collect()
    )
    for row in bad_status:
        issues.append(f"estatus desconocido: {row['estatus']!r}")
    dups = (
        df.groupBy("id_credito").count()
        .filter(F.col("count") > 1).select("id_credito").collect()
    )
    for row in dups:
        issues.append(f"id_credito duplicado: {row['id_credito']}")
    return issues


def add_expected_loss(df):
    """Agrega la columna perdida_esperada = EAD x PD x LGD.

    Es el valor esperado de la pérdida de cada crédito: si impaga
    (probabilidad PD), se pierde la fracción LGD de lo expuesto (EAD).
    Como en física, el valor esperado es lo que se observaría en
    promedio si la cartera se "repitiera" muchas veces.
    """
    return df.withColumn(
        "perdida_esperada",
        F.col("ead") * F.col("pd") * F.col("lgd"),
    )


def add_pd_bucket(df):
    """Agrega el bucket de PD al que pertenece cada crédito."""
    return df.withColumn(
        "pd_bucket",
        F.when(F.col("pd") < 0.02, "<2%")
        .when(F.col("pd") < 0.05, "2-5%")
        .when(F.col("pd") < 0.10, "5-10%")
        .otherwise(">=10%"),
    )


def _morosidad_expr(df):
    """Expresión de morosidad: exposición vencida / exposición total."""
    return (
        100.0
        * F.sum(F.when(F.col("estatus") == "vencido", F.col("ead")).otherwise(0.0))
        / F.sum("ead")
    )


def portfolio_summary_df(df):
    """Una sola fila con las métricas de toda la cartera.

    La PD ponderada no es el promedio de las PD: es la suma de EAD x PD
    entre el EAD total, o sea, cada crédito pesa lo que pesa su
    exposición. Un crédito de 2.5 millones con PD 6% mueve la aguja
    mucho más que uno de 60 mil con PD 20%.
    """
    return df.agg(
        F.count("*").alias("numero_creditos"),
        F.sum("ead").alias("ead_total"),
        F.sum("perdida_esperada").alias("perdida_esperada_total"),
        (F.sum(F.col("ead") * F.col("pd")) / F.sum("ead")).alias("pd_ponderada"),
        F.sum(
            F.when(F.col("estatus") == "vencido", F.col("ead")).otherwise(0.0)
        ).alias("cartera_vencida"),
        _morosidad_expr(df).alias("morosidad_pct"),
    )


def segment_summary_df(df):
    """Las mismas métricas, abiertas por segmento (Hipotecario, PyME...).

    Aquí es donde se ve la concentración del riesgo: normalmente uno o
    dos segmentos se llevan casi toda la pérdida esperada aunque no
    sean los más grandes en exposición.
    """
    return (
        df.groupBy("segmento")
        .agg(
            F.count("*").alias("numero_creditos"),
            F.sum("ead").alias("ead_total"),
            F.sum("perdida_esperada").alias("perdida_esperada_total"),
            (F.sum(F.col("ead") * F.col("pd")) / F.sum("ead")).alias("pd_ponderada"),
            F.sum(
                F.when(F.col("estatus") == "vencido", F.col("ead")).otherwise(0.0)
            ).alias("cartera_vencida"),
            _morosidad_expr(df).alias("morosidad_pct"),
        )
        .orderBy("segmento")
    )


def status_summary_df(df):
    """Exposición y pérdida esperada por estatus (vigente / vencido)."""
    return (
        df.groupBy("estatus")
        .agg(
            F.count("*").alias("numero_creditos"),
            F.sum("ead").alias("ead_total"),
            F.sum("perdida_esperada").alias("perdida_esperada_total"),
        )
        .orderBy("estatus")
    )


def pd_bucket_summary_df(df):
    """Exposición y pérdida esperada por bucket de PD.

    La lectura que importa: si la cola (>=10%) concentra mucha
    exposición, la cartera es frágil aunque el promedio se vea bien.
    """
    return (
        df.groupBy("pd_bucket")
        .agg(
            F.count("*").alias("numero_creditos"),
            F.sum("ead").alias("ead_total"),
            F.sum("perdida_esperada").alias("perdida_esperada_total"),
        )
        .orderBy("pd_bucket")
    )


def row_to_dict(row):
    """Convierte una fila de Spark en un dict plano de Python."""
    return {k: row[k] for k in row.__fields__}


def _write_dataset(df, layer_dir, name):
    """Escribe un dataset en una capa: Parquet para máquinas, CSV para humanos.

    El Parquet es columnar y tipado: es lo que leería el siguiente paso
    del pipeline (un tablero, otro job de Spark, un modelo). El CSV es
    un resumen chico con una sola parte (coalesce(1)) para que se pueda
    abrir directo en Excel sin andar buscando qué part-00000 es.
    Devuelve las rutas que se escribieron.
    """
    layer_dir = Path(layer_dir)
    pq = layer_dir / "parquet" / name
    csv = layer_dir / "csv" / name
    df.write.mode("overwrite").parquet(str(pq))
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv))
    return {"parquet": str(pq), "csv": str(csv)}


def to_bronze(spark, input_path, out_dir):
    """Capa Bronce: los datos tal como llegan, sin tocar.

    Se lee el CSV con el esquema fijo y se guarda una copia exacta en
    ``outputs/bronze`` (Parquet + CSV). No se valida nada aquí y no se
    enriquece nada: el Bronce es el fósil, y su trabajo es preservar
    lo que llegó — incluida una fila rota, si la hay — para que el
    problema se pueda diagnosticar después en vez de perderse en el
    camino. Devuelve (DataFrame crudo, rutas escritas).
    """
    df = read_portfolio(spark, input_path)
    written = _write_dataset(df, Path(out_dir) / "bronze", "cartera_cruda")
    return df, written


def to_silver(df, out_dir):
    """Capa Plata: los datos limpios y validados.

    Aquí vive la compuerta de calidad del pipeline: si la cartera no
    pasa la validación, se levanta ValueError y NO se escribe nada en
    Plata ni en Oro — mejor no tener salida que tener una salida que
    miente. Si pasa, se enriquece con la pérdida esperada y el bucket
    de PD y se guarda en ``outputs/silver``. Devuelve (DataFrame
    limpio y enriquecido, rutas escritas).
    """
    issues = validate_portfolio(df)
    if issues:
        raise ValueError("cartera inválida: " + "; ".join(issues))
    df = add_expected_loss(df)
    df = add_pd_bucket(df)
    written = _write_dataset(df, Path(out_dir) / "silver", "cartera_limpia")
    return df, written


def to_gold(df, out_dir):
    """Capa Oro: los agregados listos para el negocio.

    El resumen de toda la cartera y sus aperturas por segmento, por
    estatus y por bucket de PD — las cifras que un analista miraría
    cada mañana y las que leen el tablero, el Excel y Access. Se
    guardan en ``outputs/gold``. Devuelve (resúmenes, rutas escritas).
    """
    summaries = {
        "resumen_cartera": portfolio_summary_df(df),
        "resumen_segmento": segment_summary_df(df),
        "resumen_estatus": status_summary_df(df),
        "resumen_buckets_pd": pd_bucket_summary_df(df),
    }
    written = {}
    for name, summary_df in summaries.items():
        written[name] = _write_dataset(summary_df, Path(out_dir) / "gold", name)
    return summaries, written


def write_outputs(df_clean, summaries, out_dir):
    """Escribe la cartera limpia en Plata y los resúmenes en Oro.

    Es la entrada vieja para escribir salidas (la usan quienes ya
    tenían el DataFrame limpio y los resúmenes calculados); por dentro
    solo delega en las capas. Para un pipeline completo se usa
    ``run_etl``, que pasa por Bronce, Plata y Oro en orden.
    """
    out_dir = Path(out_dir)
    silver = _write_dataset(df_clean, out_dir / "silver", "cartera_limpia")
    gold = {}
    for name, summary_df in summaries.items():
        gold[name] = _write_dataset(summary_df, out_dir / "gold", name)
    return {"silver": {"cartera_limpia": silver}, "gold": gold}


def run_etl(input_path, output_dir, master="local[*]", spark=None):
    """Corre el ETL completo: Bronce -> Plata -> Oro.

    El orden importa y no es capricho: primero se lee y se preserva
    lo crudo (Bronce), luego se valida y se enriquece (Plata — si los
    datos están rotos se aborta con ValueError antes de calcular nada)
    y al final se agrega (Oro). Si algo falla a la mitad, no queda
    ninguna salida a medias haciéndose pasar por buena: la capa que
    falló simplemente no se escribe. Devuelve el resumen y las rutas
    escritas, agrupadas por capa.
    """
    own_spark = spark is None
    if own_spark:
        spark = build_spark(master=master)
    try:
        df_raw, bronze = to_bronze(spark, input_path, output_dir)
        df, silver = to_silver(df_raw, output_dir)
        summaries, gold = to_gold(df, output_dir)
        summary = row_to_dict(summaries["resumen_cartera"].collect()[0])
        return {
            "summary": summary,
            "outputs": {
                "bronze": bronze,
                "silver": silver,
                "gold": gold,
            },
        }
    finally:
        if own_spark:
            spark.stop()
