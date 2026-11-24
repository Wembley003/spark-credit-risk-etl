"""Pruebas de la implementación de referencia (pandas, sin Spark).

Estas son las que corren en cualquier máquina, con o sin Java: si
Spark no está disponible, al menos esta versión deja verificar que el
cálculo está bien.
"""

from pathlib import Path

import pytest

from spark_credit_risk_etl import reference_impl

DATA = Path(__file__).resolve().parent.parent / "data" / "portfolio_sample.csv"


def test_carga_24_creditos():
    df = reference_impl.load_portfolio(DATA)
    assert len(df) == 24
    assert list(df.columns) == ["id_credito", "segmento", "ead", "pd", "lgd", "estatus"]


def test_validacion_pasa_con_la_muestra():
    df = reference_impl.load_portfolio(DATA)
    assert reference_impl.validate(df) == []


def test_validacion_atrapa_lgd_negativa():
    df = reference_impl.load_portfolio(DATA).copy()
    df.loc[0, "lgd"] = -0.1
    issues = reference_impl.validate(df)
    assert any("lgd" in i for i in issues)


def test_resumen_cifras_exactas():
    df = reference_impl.add_expected_loss(reference_impl.load_portfolio(DATA))
    resumen = reference_impl.portfolio_summary(df)
    assert resumen["numero_creditos"] == 24
    assert resumen["ead_total"] == pytest.approx(17_955_000.0)
    assert resumen["perdida_esperada_total"] == pytest.approx(358_595.0)
    assert resumen["pd_ponderada"] == pytest.approx(0.0430, abs=1e-4)
    assert resumen["morosidad_pct"] == pytest.approx(31.30, abs=0.01)


def test_segmentos_suman_al_total():
    df = reference_impl.add_expected_loss(reference_impl.load_portfolio(DATA))
    resumen = reference_impl.portfolio_summary(df)
    segmentos = reference_impl.segment_summary(df)
    assert len(segmentos) == 4
    assert segmentos["ead_total"].sum() == pytest.approx(resumen["ead_total"])
    assert segmentos["perdida_esperada_total"].sum() == pytest.approx(
        resumen["perdida_esperada_total"]
    )


def test_buckets_pd_cubren_toda_la_cartera():
    df = reference_impl.add_pd_bucket(
        reference_impl.add_expected_loss(reference_impl.load_portfolio(DATA))
    )
    buckets = reference_impl.pd_bucket_summary(df)
    assert buckets["numero_creditos"].sum() == 24
    assert buckets["ead_total"].sum() == pytest.approx(17_955_000.0)
    # La cola de más riesgo (>=10%) existe y es menos del 10% de la exposición.
    cola = buckets.loc[buckets["pd_bucket"] == ">=10%", "ead_total"].iloc[0]
    assert cola == pytest.approx(1_750_000.0)
