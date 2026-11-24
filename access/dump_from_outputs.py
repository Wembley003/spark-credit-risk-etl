"""Vuelca la cartera y sus resúmenes a CSV + esquema, para Access.

Access no se puede fabricar desde Python en Linux, así que este
script prepara lo que el constructor de Java necesita: un CSV por
tabla y los dos archivos de esquema (``dump_access.dump_tables``).
Las cifras salen de ``reference_impl`` (pandas) sobre la misma
cartera de ejemplo del ETL, así que son las mismas que las del
pipeline de Spark: 24 créditos, EAD $17,955,000, pérdida esperada
$358,595.

Las 4 tablas que van al .accdb:

- cartera: la cartera limpia, crédito por crédito, con su pérdida
  esperada y su bucket de PD.
- por_segmento: las métricas abiertas por segmento.
- por_estatus: exposición y pérdida esperada por vigente/vencido.
- buckets_pd: exposición y pérdida esperada por tramo de PD.

Uso:

    python3 access/dump_from_outputs.py

Escribe access/csv/*.csv, access/schema.txt y access/schema.json.
Después, ``access/build_access.sh`` construye el .accdb real con
Jackcess (Java) y lo verifica con mdbtools.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ACCESS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = ACCESS_DIR.parent
sys.path.insert(0, str(ACCESS_DIR))

from dump_access import dump_tables  # noqa: E402

# reference_impl se carga por ruta, no con "from spark_credit_risk_etl
# import ...": el __init__ del paquete importa el módulo de Spark
# (etl.py -> pyspark), y este vuelco tiene que correr también con el
# python3 del sistema, que no tiene PySpark. reference_impl.py es
# pandas puro y no necesita nada de eso.
_spec = importlib.util.spec_from_file_location(
    "reference_impl",
    PROJECT_ROOT / "spark_credit_risk_etl" / "reference_impl.py",
)
reference_impl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reference_impl)

DATA = PROJECT_ROOT / "data" / "portfolio_sample.csv"


def build_tables():
    """Carga la cartera, la enriquece y arma las 4 tablas de Access.

    Los tipos son los que Access entiende: TEXT para los textos,
    LONG para los conteos y DOUBLE para el dinero y las tasas.
    ``dump_tables`` calcula sola la longitud de los TEXT.
    """
    df = reference_impl.load_portfolio(DATA)
    issues = reference_impl.validate(df)
    if issues:
        raise ValueError("cartera inválida: " + "; ".join(issues))
    df = reference_impl.add_pd_bucket(reference_impl.add_expected_loss(df))

    cartera_rows = [
        (
            row.id_credito,
            row.segmento,
            float(row.ead),
            float(row.pd),
            float(row.lgd),
            row.estatus,
            float(row.perdida_esperada),
            row.pd_bucket,
        )
        for row in df.itertuples(index=False)
    ]
    tables = [
        {
            "name": "cartera",
            "columns": [
                {"name": "id_credito", "type": "TEXT"},
                {"name": "segmento", "type": "TEXT"},
                {"name": "ead", "type": "DOUBLE"},
                {"name": "pd", "type": "DOUBLE"},
                {"name": "lgd", "type": "DOUBLE"},
                {"name": "estatus", "type": "TEXT"},
                {"name": "perdida_esperada", "type": "DOUBLE"},
                {"name": "pd_bucket", "type": "TEXT"},
            ],
            "rows": cartera_rows,
        },
    ]
    # Los resúmenes: mismo orden de columnas que en los Parquet del ETL.
    for name, summary_df in (
        ("por_segmento", reference_impl.segment_summary(df)),
        ("por_estatus", reference_impl.status_summary(df)),
        ("buckets_pd", reference_impl.pd_bucket_summary(df)),
    ):
        columns = []
        for col in summary_df.columns:
            if col in ("numero_creditos",):
                columns.append({"name": col, "type": "LONG"})
            elif col in ("segmento", "estatus", "pd_bucket"):
                columns.append({"name": col, "type": "TEXT"})
            else:
                columns.append({"name": col, "type": "DOUBLE"})
        rows = [
            tuple(
                int(v) if spec["type"] == "LONG" else
                (str(v) if spec["type"] == "TEXT" else float(v))
                for v, spec in zip(row, columns)
            )
            for row in summary_df.itertuples(index=False)
        ]
        tables.append({"name": name, "columns": columns, "rows": rows})
    return tables


def main():
    summary = dump_tables(build_tables(), ACCESS_DIR)
    for table in summary["tables"]:
        print(f"{table['name']}: {table['rows']} filas -> {table['csv']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
