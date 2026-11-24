"""ETL de riesgo de crédito con PySpark, corriendo en modo local.

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


def write_outputs(df_clean, summaries, out_dir):
    """Escribe las salidas: Parquet para máquinas, CSV para humanos.

    El Parquet es columnar y tipado: es lo que leería el siguiente paso
    del pipeline (un tablero, otro job de Spark, un modelo). El CSV es
    un resumen chico con una sola parte (coalesce(1)) para que se pueda
    abrir directo en Excel sin andar buscando qué part-00000 es.
    Devuelve las rutas que se escribieron.
    """
    out_dir = Path(out_dir)
    written = {}

    def _write(df, name):
        pq = out_dir / "parquet" / name
        csv = out_dir / "csv" / name
        df.write.mode("overwrite").parquet(str(pq))
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv))
        written[name] = {"parquet": str(pq), "csv": str(csv)}

    df_clean.write.mode("overwrite").parquet(str(out_dir / "parquet" / "cartera_limpia"))
    written["cartera_limpia"] = {"parquet": str(out_dir / "parquet" / "cartera_limpia")}
    for name, summary_df in summaries.items():
        _write(summary_df, name)
    return written


def run_etl(input_path, output_dir, master="local[*]", spark=None):
    """Corre el ETL completo y devuelve el resumen y las rutas.

    El orden importa y no es capricho: primero se lee, luego se valida
    (si los datos están rotos se aborta con ValueError antes de
    calcular nada), luego se enriquece (pérdida esperada, buckets),
    luego se agrega y al final se escribe. Si algo falla a la mitad,
    no queda ninguna salida a medias haciéndose pasar por buena.
    """
    own_spark = spark is None
    if own_spark:
        spark = build_spark(master=master)
    try:
        df = read_portfolio(spark, input_path)
        issues = validate_portfolio(df)
        if issues:
            raise ValueError("cartera inválida: " + "; ".join(issues))
        df = add_expected_loss(df)
        df = add_pd_bucket(df)
        summaries = {
            "resumen_cartera": portfolio_summary_df(df),
            "resumen_segmento": segment_summary_df(df),
            "resumen_estatus": status_summary_df(df),
            "resumen_buckets_pd": pd_bucket_summary_df(df),
        }
        written = write_outputs(df, summaries, output_dir)
        summary = row_to_dict(summaries["resumen_cartera"].collect()[0])
        return {"summary": summary, "outputs": written}
    finally:
        if own_spark:
            spark.stop()
