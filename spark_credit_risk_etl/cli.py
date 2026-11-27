"""
Capa: Cliente (presentación) — la línea de comandos que el usuario corre.
Línea de comandos del ETL.

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
import os
import sys
from pathlib import Path

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
    parser.add_argument(
        "--upload-adls", action="store_true",
        help="sube los CSV de zona a ADLS Gen2 después del ETL "
        "(contenedor credit-risk por defecto; necesita "
        "AZURE_STORAGE_CONNECTION_STRING o AZURE_STORAGE_ACCOUNT + "
        "AZURE_STORAGE_KEY)",
    )
    parser.add_argument(
        "--adls-container", default="credit-risk",
        help="contenedor de ADLS Gen2 (por defecto: credit-risk)",
    )
    parser.add_argument(
        "--adls-account", default=None,
        help="cuenta de almacenamiento de Azure (por defecto: "
        "AZURE_STORAGE_ACCOUNT)",
    )
    return parser.parse_args(argv)


def _upload_to_adls(output_dir, container, account):
    """Espejo opcional: sube los CSV de zona al lago de Azure.

    Devuelve la lista de blobs subidos. Si algo falla, levanta
    ``LakeError`` con el motivo; las salidas locales ya existen
    para entonces, así que el fallo es solo de la subida.
    """
    from . import adls

    service_client = adls.build_service_client(
        account=account or os.environ.get("AZURE_STORAGE_ACCOUNT"),
        connection_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
        key=os.environ.get("AZURE_STORAGE_KEY"),
    )
    uploader = adls.LakeUploader(service_client, container=container)
    return uploader.upload_outputs(output_dir)


def _print_summary(summary):
    print(f"Créditos:              {summary['numero_creditos']}")
    print(f"EAD total:            ${summary['ead_total']:,.2f}")
    print(f"Pérdida esperada:     ${summary['perdida_esperada_total']:,.2f}")
    print(f"PD ponderada:         {summary['pd_ponderada']:.4f}")
    print(f"Cartera vencida:      ${summary['cartera_vencida']:,.2f}")
    print(f"Morosidad:            {summary['morosidad_pct']:.2f}%")


def _run_reference(input_path, output_dir):
    """La versión de pandas: mismo cálculo, sin JVM de por medio.

    Pasa por las mismas tres capas que la versión de Spark — Bronce
    (lo crudo), Plata (validado y enriquecido) y Oro (agregados) — así
    que las salidas quedan en los mismos lugares y con las mismas
    cifras. Si la cartera no pasa la validación, la Plata se niega y
    el programa sale con código 3, igual que con Spark.
    """
    try:
        df_raw = reference_impl.to_bronze(input_path, output_dir)[0]
        df = reference_impl.to_silver(df_raw, output_dir)[0]
        reference_impl.to_gold(df, output_dir)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_INVALID_DATA
    summary = reference_impl.portfolio_summary(df)
    _print_summary(summary)
    print(f"Salidas en: {output_dir}")
    return EXIT_OK


def main(argv=None):
    args = _parse_args(argv)
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"No existe el archivo de entrada: {input_path}", file=sys.stderr)
        return EXIT_BAD_ARGS
    if args.reference:
        codigo = _run_reference(input_path, args.output_dir)
    else:
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
        codigo = EXIT_OK
    if codigo != EXIT_OK:
        return codigo
    if args.upload_adls:
        try:
            subidos = _upload_to_adls(
                args.output_dir, args.adls_container, args.adls_account,
            )
        except Exception as exc:  # noqa: BLE001 - el CLI reporta y sale con 1
            print(
                "Error subiendo a ADLS: las salidas locales ya existen en "
                f"{args.output_dir}, solo falló el espejo: {exc}",
                file=sys.stderr,
            )
            return EXIT_ERROR
        print(f"Subidos a ADLS ({args.adls_container}):")
        for blob in subidos:
            print(f"  {blob}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
