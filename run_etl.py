"""
Capa: Cliente (presentación) — punto de entrada para spark-submit.
Punto de entrada para spark-submit.

Uso:

    spark-submit run_etl.py --input data/portfolio_sample.csv
    spark-submit run_etl.py --input data/portfolio_sample.csv --master local[2]

También se puede correr como módulo (sin spark-submit):

    python -m spark_credit_risk_etl.cli --input data/portfolio_sample.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from spark_credit_risk_etl.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
