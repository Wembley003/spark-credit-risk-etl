"""ETL de riesgo de crédito con PySpark.

La sesión de Spark se levanta aquí, las transformaciones viven en
``etl`` y la comprobación independiente en ``reference_impl``.
"""

from .etl import (
    build_spark,
    portfolio_summary_df,
    read_portfolio,
    run_etl,
    segment_summary_df,
    validate_portfolio,
)
from .reference_impl import portfolio_summary as reference_summary

__all__ = [
    "build_spark",
    "read_portfolio",
    "run_etl",
    "validate_portfolio",
    "portfolio_summary_df",
    "segment_summary_df",
    "reference_summary",
]
