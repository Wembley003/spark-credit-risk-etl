"""
Capa: Reglas de negocio — el gemelo en pandas de las mismas reglas del ETL, con las mismas capas Bronce → Plata → Oro.
Implementación de referencia en Python puro (pandas) del mismo ETL.

Dos motivos para que exista. El primero es práctico: en una máquina sin
Java no hay Spark, y esta versión corre en cualquier lado con solo
pandas. El segundo es de verificación: si dos implementaciones
independientes — Spark y pandas — dan los mismos números sobre los
mismos datos, es mucho más difícil que un bug se cuele callado en
ambas. Los tests comparan las dos, cifra por cifra.

Refleja las mismas tres capas del ETL de Spark (Bronce → Plata →
Oro), con las mismas reglas y los mismos archivos de salida: el
Bronce preserva lo crudo, la Plata valida y enriquece, y el Oro
agrega. Si una capa da distinto en pandas que en Spark, el bug está
en una de las dos y los tests lo cazan.
"""

from __future__ import annotations

from pathlib import Path

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
    "to_bronze",
    "to_silver",
    "to_gold",
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


def _write_layer(df, layer_dir, name):
    """Escribe un DataFrame de una capa: Parquet + CSV, un solo archivo cada uno."""
    layer_dir = Path(layer_dir)
    (layer_dir / "parquet").mkdir(parents=True, exist_ok=True)
    (layer_dir / "csv").mkdir(parents=True, exist_ok=True)
    pq = layer_dir / "parquet" / f"{name}.parquet"
    csv = layer_dir / "csv" / f"{name}.csv"
    df.to_parquet(pq, index=False)
    df.to_csv(csv, index=False)
    return {"parquet": str(pq), "csv": str(csv)}


def to_bronze(path, out_dir):
    """Capa Bronce: la cartera tal como llega, sin tocar.

    Se lee el CSV y se guarda una copia exacta en ``outputs/bronze``.
    No se valida ni se enriquece nada: el Bronce preserva lo crudo,
    incluida una fila rota, para poder diagnosticarla después.
    Devuelve (DataFrame crudo, rutas escritas).
    """
    df = load_portfolio(path)
    written = _write_layer(df, Path(out_dir) / "bronze", "cartera_cruda")
    return df, written


def to_silver(df, out_dir):
    """Capa Plata: validación (la compuerta) y enriquecimiento.

    Si la cartera no pasa la validación se levanta ValueError y no se
    escribe nada en Plata ni en Oro. Si pasa, se agregan la pérdida
    esperada y el bucket de PD y se guarda en ``outputs/silver``.
    Devuelve (DataFrame limpio y enriquecido, rutas escritas).
    """
    issues = validate(df)
    if issues:
        raise ValueError("cartera inválida: " + "; ".join(issues))
    df = add_expected_loss(df)
    df = add_pd_bucket(df)
    written = _write_layer(df, Path(out_dir) / "silver", "cartera_limpia")
    return df, written


def to_gold(df, out_dir):
    """Capa Oro: los agregados listos para el negocio.

    El resumen de toda la cartera y sus aperturas por segmento, por
    estatus y por bucket de PD, en ``outputs/gold``. Devuelve
    (resúmenes como DataFrames, rutas escritas).
    """
    summaries = {
        "resumen_cartera": pd.DataFrame([portfolio_summary(df)]),
        "resumen_segmento": segment_summary(df),
        "resumen_estatus": status_summary(df),
        "resumen_buckets_pd": pd_bucket_summary(df),
    }
    written = {}
    for name, summary_df in summaries.items():
        written[name] = _write_layer(summary_df, Path(out_dir) / "gold", name)
    return summaries, written
