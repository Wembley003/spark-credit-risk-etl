"""Línea de comandos del ETL.

Dos formas de correrlo, según lo que tenga la máquina:

    python -m spark_credit_risk_etl.cli --input data/portfolio_sample.csv
    spark-submit run_etl.py --input data/portfolio_sample.csv

Y una tercera para cuando no hay Java a la mano:

    python -m spark_credit_risk_etl.cli --reference

que corre la implementación de pandas (``reference_impl``) y escribe
las mismas salidas. Los códigos de salida son parte del contrato:
0 = todo bien, 2 = argumentos mal, 3 = la cartera no pasó la
validación, 1 = cualquier otro error.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from . import etl, reference_impl

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_BAD_ARGS = 2
EXIT_INVALID_DATA = 3

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = PROJECT_ROOT / "data" / "portfolio_sample.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs"


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="ETL de riesgo de crédito con PySpark (modo local)."
    )
    parser.add_argument(
        "--input", default=str(DEFAULT_INPUT),
        help="CSV de la cartera (por defecto: data/portfolio_sample.csv)",
    )
    parser.add_argument(
        "--output-dir", default=str(DEFAULT_OUTPUT),
        help="directorio de salidas (por defecto: outputs/)",
    )
    parser.add_argument(
        "--master", default="local[*]",
        help="master de Spark (por defecto: local[*])",
    )
    parser.add_argument(
        "--reference", action="store_true",
        help="corre la implementación de pandas en vez de Spark (sin Java)",
    )
    return parser.parse_args(argv)


def _print_summary(summary):
    print(f"Créditos:              {summary['numero_creditos']}")
    print(f"EAD total:            ${summary['ead_total']:,.2f}")
    print(f"Pérdida esperada:     ${summary['perdida_esperada_total']:,.2f}")
    print(f"PD ponderada:         {summary['pd_ponderada']:.4f}")
    print(f"Cartera vencida:      ${summary['cartera_vencida']:,.2f}")
    print(f"Morosidad:            {summary['morosidad_pct']:.2f}%")


def _run_reference(input_path, output_dir):
    """La versión de pandas: mismo cálculo, sin JVM de por medio."""
    df = reference_impl.load_portfolio(input_path)
    issues = reference_impl.validate(df)
    if issues:
        print("Cartera inválida:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        return EXIT_INVALID_DATA
    df = reference_impl.add_expected_loss(df)
    df = reference_impl.add_pd_bucket(df)
    summary = reference_impl.portfolio_summary(df)
    out = Path(output_dir)
    (out / "csv").mkdir(parents=True, exist_ok=True)
    (out / "parquet").mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "parquet" / "cartera_limpia.parquet", index=False)
    pd.DataFrame([summary]).to_csv(out / "csv" / "resumen_cartera.csv", index=False)
    reference_impl.segment_summary(df).to_csv(
        out / "csv" / "resumen_segmento.csv", index=False
    )
    reference_impl.status_summary(df).to_csv(
        out / "csv" / "resumen_estatus.csv", index=False
    )
    reference_impl.pd_bucket_summary(df).to_csv(
        out / "csv" / "resumen_buckets_pd.csv", index=False
    )
    _print_summary(summary)
    print(f"Salidas en: {out}")
    return EXIT_OK


def main(argv=None):
    args = _parse_args(argv)
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"No existe el archivo de entrada: {input_path}", file=sys.stderr)
        return EXIT_BAD_ARGS
    if args.reference:
        return _run_reference(input_path, args.output_dir)
    try:
        result = etl.run_etl(
            input_path, args.output_dir, master=args.master,
        )
    except ValueError as exc:
        # La validación falló: los datos están rotos y el ETL se niega
        # a inventar números con ellos. Eso es un 3, no un 1.
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_INVALID_DATA
    except Exception as exc:  # noqa: BLE001 - el CLI reporta y sale con 1
        print(f"Error inesperado: {exc}", file=sys.stderr)
        return EXIT_ERROR
    _print_summary(result["summary"])
    print(f"Salidas en: {args.output_dir}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
