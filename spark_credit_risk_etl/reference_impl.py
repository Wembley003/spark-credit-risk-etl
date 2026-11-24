"""Implementación de referencia en Python puro (pandas) del mismo ETL.

Dos motivos para que exista. El primero es práctico: en una máquina sin
Java no hay Spark, y esta versión corre en cualquier lado con solo
pandas. El segundo es de verificación: si dos implementaciones
independientes — Spark y pandas — dan los mismos números sobre los
mismos datos, es mucho más difícil que un bug se cuele callado en
ambas. Los tests comparan las dos, cifra por cifra.
"""

from __future__ import annotations

import pandas as pd

__all__ = [
    "ESTATUS_VALIDOS",
    "load_portfolio",
    "validate",
    "add_expected_loss",
    "add_pd_bucket",
    "portfolio_summary",
    "segment_summary",
    "status_summary",
    "pd_bucket_summary",
]

ESTATUS_VALIDOS = ("vigente", "vencido")


def load_portfolio(path):
    """Lee el CSV de la cartera."""
    return pd.read_csv(
        path,
        dtype={
            "id_credito": str,
            "segmento": str,
            "ead": float,
            "pd": float,
            "lgd": float,
            "estatus": str,
        },
    )


def validate(df):
    """Mismas reglas que en la versión de Spark; misma lista de problemas."""
    issues = []
    if len(df) == 0:
        return ["la cartera está vacía (0 filas)"]
    for col in ("id_credito", "segmento", "ead", "pd", "lgd", "estatus"):
        n_null = int(df[col].isna().sum())
        if n_null:
            issues.append(f"columna {col}: {n_null} valores nulos")
    n_bad_ead = int((df["ead"] <= 0).sum())
    if n_bad_ead:
        issues.append(f"ead: {n_bad_ead} filas con exposición <= 0")
    for col in ("pd", "lgd"):
        n_bad = int(((df[col] < 0) | (df[col] > 1)).sum())
        if n_bad:
            issues.append(f"{col}: {n_bad} filas fuera del rango [0, 1]")
    for estatus in sorted(set(df["estatus"]) - set(ESTATUS_VALIDOS)):
        issues.append(f"estatus desconocido: {estatus!r}")
    dups = df["id_credito"][df["id_credito"].duplicated()].unique()
    for dup in dups:
        issues.append(f"id_credito duplicado: {dup}")
    return issues


def add_expected_loss(df):
    """Agrega perdida_esperada = EAD x PD x LGD (ver etl.py para el porqué)."""
    out = df.copy()
    out["perdida_esperada"] = out["ead"] * out["pd"] * out["lgd"]
    return out


def add_pd_bucket(df):
    """Agrega el bucket de PD al que pertenece cada crédito."""
    out = df.copy()

    def bucket(pd):
        if pd < 0.02:
            return "<2%"
        if pd < 0.05:
            return "2-5%"
        if pd < 0.10:
            return "5-10%"
        return ">=10%"

    out["pd_bucket"] = out["pd"].map(bucket)
    return out


def portfolio_summary(df):
    """Las métricas de toda la cartera en un dict."""
    ead_total = float(df["ead"].sum())
    el_total = float(df["perdida_esperada"].sum())
    vencida = float(df.loc[df["estatus"] == "vencido", "ead"].sum())
    return {
        "numero_creditos": int(len(df)),
        "ead_total": ead_total,
        "perdida_esperada_total": el_total,
        "pd_ponderada": float((df["ead"] * df["pd"]).sum() / ead_total),
        "cartera_vencida": vencida,
        "morosidad_pct": 100.0 * vencida / ead_total,
    }


def _group_metrics(group):
    """Métricas de un grupo: las mismas que en el resumen, en pequeño."""
    ead_total = float(group["ead"].sum())
    vencida = float(group.loc[group["estatus"] == "vencido", "ead"].sum())
    return {
        "numero_creditos": int(len(group)),
        "ead_total": ead_total,
        "perdida_esperada_total": float(group["perdida_esperada"].sum()),
        "pd_ponderada": float((group["ead"] * group["pd"]).sum() / ead_total),
        "cartera_vencida": vencida,
        "morosidad_pct": 100.0 * vencida / ead_total,
    }


def segment_summary(df):
    """Las métricas abiertas por segmento, como DataFrame."""
    rows = []
    for segmento, group in sorted(df.groupby("segmento")):
        row = {"segmento": segmento}
        row.update(_group_metrics(group))
        rows.append(row)
    return pd.DataFrame(rows)


def status_summary(df):
    """Exposición y pérdida esperada por estatus."""
    rows = []
    for estatus, group in sorted(df.groupby("estatus")):
        rows.append(
            {
                "estatus": estatus,
                "numero_creditos": int(len(group)),
                "ead_total": float(group["ead"].sum()),
                "perdida_esperada_total": float(group["perdida_esperada"].sum()),
            }
        )
    return pd.DataFrame(rows)


def pd_bucket_summary(df):
    """Exposición y pérdida esperada por bucket de PD."""
    rows = []
    for bucket, group in sorted(df.groupby("pd_bucket")):
        rows.append(
            {
                "pd_bucket": bucket,
                "numero_creditos": int(len(group)),
                "ead_total": float(group["ead"].sum()),
                "perdida_esperada_total": float(group["perdida_esperada"].sum()),
            }
        )
    return pd.DataFrame(rows)
