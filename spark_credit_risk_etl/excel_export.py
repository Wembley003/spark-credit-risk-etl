"""
Capa: Consultas o análisis — lo que sale de Oro, preguntado y exportado a Excel.
Exportación a Excel con tablas dinámicas NATIVAS.

¿Por qué no basta con dejar los CSV? Porque el analista que abre la
cartera en Excel no quiere solo los números: quiere *preguntarle*
cosas — ¿dónde está la exposición por segmento?, ¿cuánto pesa lo
vencido?, ¿qué parte de la cartera vive en la cola de PD alta? — y
una tabla dinámica es justo eso: una pregunta guardada, que se
reagrupa sola cuando cambian los datos.

El problema es que openpyxl no sabe escribir tablas dinámicas por
sí solo: una tabla dinámica no es una hoja más, es un objeto con su
propia definición XML, su caché de datos y sus registros, todo
relacionado dentro del zip. Si falta una pieza, Excel abre el archivo
y dice que está dañado. Ese trabajo lo hace ``pivot_table.py`` (una
copia verbatim de la receta probada en ``_recipes/``); este módulo
solo prepara las hojas con los datos y le pide tres tablas.

Las tres preguntas que se guardan en el libro, todas sobre la hoja
"Cartera":

- Dinámica por segmento: EAD, pérdida esperada y número de créditos
  por segmento. Aquí se ve la concentración del riesgo: normalmente
  uno o dos segmentos se llevan casi toda la pérdida esperada aunque
  no sean los más grandes en exposición.
- Dinámica por estatus: EAD y pérdida esperada por vigente/vencido.
- Dinámica por bucket PD: EAD y pérdida esperada por tramo de PD.
  La lectura que importa: si la cola (>=10%) concentra mucha
  exposición, la cartera es frágil aunque el promedio se vea bien.

Uso:

    python -m spark_credit_risk_etl.excel_export
    python -m spark_credit_risk_etl.excel_export --input-dir outputs --output outputs/portfolio.xlsx

La cartera limpia se lee de la capa Plata (``outputs/silver``) y los
resúmenes de la capa Oro (``outputs/gold``): el libro se construye
sobre las salidas ya validadas y agregadas del ETL, no sobre lo crudo.
Si no hay salidas del ETL todavía, el programa lo dice claro y sale
con código 2: primero hay que correr el ETL (``./run_etl.sh`` o
``make run``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

from .pivot_table import add_pivot_table

__all__ = ["build_workbook", "main"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# La raíz de salidas del ETL: la cartera limpia vive en Plata y los
# resúmenes en Oro (ver etl.py: to_bronze / to_silver / to_gold).
DEFAULT_OUTPUTS_DIR = PROJECT_ROOT / "outputs"
DEFAULT_XLSX = PROJECT_ROOT / "outputs" / "portfolio.xlsx"

# Las hojas del libro y las columnas que lleva cada una, en orden.
CARTERA_COLUMNS = [
    "id_credito",
    "segmento",
    "estatus",
    "ead",
    "pd",
    "lgd",
    "perdida_esperada",
    "bucket_pd",
]
SEGMENTO_COLUMNS = [
    "segmento",
    "numero_creditos",
    "ead_total",
    "perdida_esperada_total",
    "pd_ponderada",
    "cartera_vencida",
    "morosidad_pct",
]
ESTATUS_COLUMNS = [
    "estatus",
    "numero_creditos",
    "ead_total",
    "perdida_esperada_total",
]
BUCKETS_COLUMNS = [
    "pd_bucket",
    "numero_creditos",
    "ead_total",
    "perdida_esperada_total",
]

# Los cortes de bucket de PD son LOS MISMOS que usa el ETL
# (etl.add_pd_bucket / reference_impl.add_pd_bucket): 2%, 5% y 10%.
# No se inventa un corte nuevo aquí: si el ETL cambia de cortes,
# este módulo tiene que cambiar con él, o el libro mentiría.
def _bucket_pd(pd):
    if pd < 0.02:
        return "<2%"
    if pd < 0.05:
        return "2-5%"
    if pd < 0.10:
        return "5-10%"
    return ">=10%"


def _py(value):
    """Un valor de pandas a un escalar de Python que openpyxl entienda.

    pandas devuelve enteros y flotantes de numpy (``numpy.int64``,
    ``numpy.float64``), y openpyxl los escribe, pero un ``NaN`` se
    cuela como flotante y en el caché de la tabla dinámica aparece
    como número raro. Aquí un NaN es un hueco, o sea ``None``.
    """
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None
    if hasattr(value, "item"):  # escalar de numpy -> escalar de Python
        return value.item()
    return value


def _write_sheet(wb, title, df, columns):
    """Escribe un DataFrame en una hoja nueva: encabezados en la fila 1.

    Si el DataFrame viene vacío se escribe solo el encabezado: la hoja
    existe y se ve qué columnas llevaría, pero no hay filas que
    resumir. Si faltan columnas (por ejemplo, la cartera sin
    ``perdida_esperada``), se calculan antes de escribir.
    """
    ws = wb.create_sheet(title)
    for j, col in enumerate(columns, start=1):
        ws.cell(row=1, column=j, value=col)
    for i, (_, row) in enumerate(df.iterrows(), start=2):
        for j, col in enumerate(columns, start=1):
            ws.cell(row=i, column=j, value=_py(row[col]))
    return ws


def _prepare_cartera(portfolio_df):
    """La cartera limpia, con las columnas del libro y en ese orden.

    El bucket se calcula a partir de la PD con los mismos cortes del
    ETL (no se copia la columna ``pd_bucket`` del Parquet: la fuente
    de verdad es la PD misma, y así el libro no depende de que el
    Parquet traiga esa columna). Si falta ``perdida_esperada`` se
    calcula como EAD x PD x LGD, que es su definición.
    """
    df = portfolio_df.copy()
    if df.empty and not set(CARTERA_COLUMNS) <= set(df.columns):
        # Cartera vacía sin columnas: solo queda el encabezado.
        return pd.DataFrame(columns=CARTERA_COLUMNS)
    if "perdida_esperada" not in df.columns:
        df["perdida_esperada"] = df["ead"] * df["pd"] * df["lgd"]
    df["bucket_pd"] = df["pd"].map(_bucket_pd)
    return df[[c for c in CARTERA_COLUMNS if c in df.columns]]


def build_workbook(portfolio_df, segmento_df, estatus_df, buckets_df,
                   output_path):
    """Construye el libro y lo escribe en ``output_path``.

    Las hojas de datos se escriben siempre, aunque un resumen venga
    vacío. Las tablas dinámicas solo se crean si la cartera tiene
    filas: una tabla dinámica sin datos no tiene nada que resumir y
    ``add_pivot_table`` lo rechaza con ValueError.
    """
    wb = Workbook()
    wb.remove(wb.active)  # la hoja vacía que trae el libro nuevo

    cartera = _prepare_cartera(portfolio_df)
    ws_cartera = _write_sheet(wb, "Cartera", cartera, CARTERA_COLUMNS)
    _write_sheet(wb, "Por segmento", segmento_df, SEGMENTO_COLUMNS)
    _write_sheet(wb, "Por estatus", estatus_df, ESTATUS_COLUMNS)
    _write_sheet(wb, "Buckets PD", buckets_df, BUCKETS_COLUMNS)

    if not cartera.empty:
        # Cada tabla lleva su propio caché (ver pivot_table.py): dos
        # cachés idénticos se funden al escribir y la segunda tabla
        # queda apuntando a un caché que no existe.
        add_pivot_table(
            wb,
            source_sheet=ws_cartera,
            pivot_sheet_title="Dinámica por segmento",
            pivot_name="PivotSegmento",
            row_fields=["segmento"],
            data_fields=[
                ("ead", "EAD", "sum"),
                ("perdida_esperada", "Pérdida esperada", "sum"),
                ("ead", "Número de créditos", "count"),
            ],
        )
        add_pivot_table(
            wb,
            source_sheet=ws_cartera,
            pivot_sheet_title="Dinámica por estatus",
            pivot_name="PivotEstatus",
            row_fields=["estatus"],
            data_fields=[
                ("ead", "EAD", "sum"),
                ("perdida_esperada", "Pérdida esperada", "sum"),
            ],
        )
        add_pivot_table(
            wb,
            source_sheet=ws_cartera,
            pivot_sheet_title="Dinámica por bucket PD",
            pivot_name="PivotBucketPD",
            row_fields=["bucket_pd"],
            data_fields=[
                ("ead", "EAD", "sum"),
                ("perdida_esperada", "Pérdida esperada", "sum"),
            ],
        )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    return output_path


def _read_parquet(parquet_dir, name):
    """Lee un Parquet de salida del ETL (o explica cuál falta)."""
    path = Path(parquet_dir) / name
    if not path.is_dir():
        raise FileNotFoundError(
            f"falta la salida {name} del ETL en {parquet_dir}"
        )
    return pd.read_parquet(path)


def main(argv=None):
    """CLI: de outputs/silver y outputs/gold a outputs/portfolio.xlsx."""
    parser = argparse.ArgumentParser(
        description="Exporta las salidas del ETL a Excel con tablas "
                    "dinámicas nativas."
    )
    parser.add_argument(
        "--input-dir", default=str(DEFAULT_OUTPUTS_DIR),
        help="directorio raíz de salidas del ETL (por defecto: "
             "outputs; la cartera limpia se lee de silver/parquet y "
             "los resúmenes de gold/parquet)",
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_XLSX),
        help="archivo .xlsx a escribir (por defecto: outputs/portfolio.xlsx)",
    )
    args = parser.parse_args(argv)

    outputs_dir = Path(args.input_dir)
    if not outputs_dir.is_dir():
        print(
            "error: no existe el directorio de salidas del ETL "
            f"({outputs_dir}). Primero corre el ETL para generarlas: "
            "./run_etl.sh  o  make run  (o ./run_etl.sh --reference, "
            "sin Java).",
            file=sys.stderr,
        )
        return 2
    try:
        portfolio_df = _read_parquet(
            outputs_dir / "silver" / "parquet", "cartera_limpia"
        )
        segmento_df = _read_parquet(
            outputs_dir / "gold" / "parquet", "resumen_segmento"
        )
        estatus_df = _read_parquet(
            outputs_dir / "gold" / "parquet", "resumen_estatus"
        )
        buckets_df = _read_parquet(
            outputs_dir / "gold" / "parquet", "resumen_buckets_pd"
        )
    except FileNotFoundError as exc:
        print(
            f"error: {exc}. Vuelve a correr el ETL para regenerar las "
            "salidas: ./run_etl.sh  o  make run.",
            file=sys.stderr,
        )
        return 2

    output = build_workbook(
        portfolio_df, segmento_df, estatus_df, buckets_df, args.output
    )
    print(f"escrito {output} ({len(portfolio_df)} créditos)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
