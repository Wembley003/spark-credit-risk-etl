"""Pruebas del ETL de Spark.

Ojo con lo que se está probando aquí: que Spark lea bien el CSV, que
la validación atrape datos rotos, que los números salgan exactos (se
recalculan por un camino independiente, con el módulo csv estándar) y
que la versión de pandas dé lo mismo que la de Spark. Si las dos
implementaciones coinciden, el cálculo es el que es, no un accidente
de una librería.
"""

import csv
from pathlib import Path

import pytest

from spark_credit_risk_etl import etl, reference_impl

DATA = Path(__file__).resolve().parent.parent / "data" / "portfolio_sample.csv"


@pytest.fixture(scope="module")
def spark():
    session = etl.build_spark(app_name="test-spark-credit-risk-etl")
    yield session
    session.stop()


def _leer_csv_independiente():
    """Lee el CSV con el módulo csv, sin tocar el código del ETL."""
    with open(DATA, newline="", encoding="utf-8") as f:
        return [
            {
                "id_credito": fila["id_credito"],
                "segmento": fila["segmento"],
                "ead": float(fila["ead"]),
                "pd": float(fila["pd"]),
                "lgd": float(fila["lgd"]),
                "estatus": fila["estatus"],
            }
            for fila in csv.DictReader(f)
        ]


def test_lectura_24_creditos(spark):
    df = etl.read_portfolio(spark, DATA)
    assert df.count() == 24
    assert df.columns == ["id_credito", "segmento", "ead", "pd", "lgd", "estatus"]


def test_validacion_pasa_con_la_muestra(spark):
    df = etl.read_portfolio(spark, DATA)
    assert etl.validate_portfolio(df) == []


def test_validacion_atrapa_pd_fuera_de_rango(spark, tmp_path):
    # Un solo crédito con PD = 1.5 (imposible: una probabilidad no pasa
    # de 1) tiene que bastar para que la validación grite.
    malo = tmp_path / "malo.csv"
    malo.write_text(
        "id_credito,segmento,ead,pd,lgd,estatus\nX-001,PyME,100000,1.5,0.40,vigente\n",
        encoding="utf-8",
    )
    df = etl.read_portfolio(spark, malo)
    issues = etl.validate_portfolio(df)
    assert issues
    assert any("pd" in i and "fuera del rango" in i for i in issues)


def test_validacion_atrapa_id_duplicado(spark, tmp_path):
    duplicado = tmp_path / "dup.csv"
    duplicado.write_text(
        "id_credito,segmento,ead,pd,lgd,estatus\n"
        "X-001,PyME,100000,0.05,0.40,vigente\n"
        "X-001,PyME,200000,0.05,0.40,vigente\n",
        encoding="utf-8",
    )
    df = etl.read_portfolio(spark, duplicado)
    issues = etl.validate_portfolio(df)
    assert any("duplicado" in i for i in issues)


def test_resumen_cifras_exactas(spark):
    # Las cifras de referencia se calculan aquí mismo desde el CSV,
    # sin usar ni una función del ETL: si el ETL miente, el test lo caza.
    cartera = _leer_csv_independiente()
    ead = sum(c["ead"] for c in cartera)
    el = sum(c["ead"] * c["pd"] * c["lgd"] for c in cartera)
    vencida = sum(c["ead"] for c in cartera if c["estatus"] == "vencido")
    pd_pond = sum(c["ead"] * c["pd"] for c in cartera) / ead

    df = etl.add_expected_loss(etl.read_portfolio(spark, DATA))
    resumen = etl.row_to_dict(etl.portfolio_summary_df(df).collect()[0])
    assert resumen["numero_creditos"] == 24
    assert resumen["ead_total"] == pytest.approx(ead)
    assert resumen["perdida_esperada_total"] == pytest.approx(el)
    assert resumen["pd_ponderada"] == pytest.approx(pd_pond)
    assert resumen["cartera_vencida"] == pytest.approx(vencida)
    assert resumen["morosidad_pct"] == pytest.approx(100 * vencida / ead)
    # Y las cifras que ya conocíamos del agente: tienen que salir iguales.
    assert resumen["ead_total"] == pytest.approx(17_955_000.0)
    assert resumen["perdida_esperada_total"] == pytest.approx(358_595.0)


def test_segmentos_suman_al_total(spark):
    df = etl.add_expected_loss(etl.read_portfolio(spark, DATA))
    resumen = etl.row_to_dict(etl.portfolio_summary_df(df).collect()[0])
    segmentos = etl.segment_summary_df(df).collect()
    assert {r["segmento"] for r in segmentos} == {
        "Consumo", "Hipotecario", "PyME", "Tarjeta",
    }
    assert sum(r["ead_total"] for r in segmentos) == pytest.approx(
        resumen["ead_total"]
    )
    assert sum(r["perdida_esperada_total"] for r in segmentos) == pytest.approx(
        resumen["perdida_esperada_total"]
    )
    assert sum(r["numero_creditos"] for r in segmentos) == 24


def test_spark_y_pandas_dan_lo_mismo(spark):
    # La prueba que más vale: dos motores distintos, mismos números.
    df = etl.add_expected_loss(etl.read_portfolio(spark, DATA))
    resumen_spark = etl.row_to_dict(etl.portfolio_summary_df(df).collect()[0])

    ref = reference_impl.add_expected_loss(reference_impl.load_portfolio(DATA))
    resumen_ref = reference_impl.portfolio_summary(ref)
    for key in resumen_spark:
        assert resumen_spark[key] == pytest.approx(resumen_ref[key], rel=1e-9)


def test_run_etl_escribe_parquet_y_csv(spark, tmp_path):
    out = tmp_path / "outputs"
    result = etl.run_etl(DATA, out, spark=spark)
    assert result["summary"]["ead_total"] == pytest.approx(17_955_000.0)
    # Bronce: lo crudo tal como llegó (24 filas, sin columnas calculadas).
    cruda = spark.read.parquet(str(out / "bronze" / "parquet" / "cartera_cruda"))
    assert cruda.count() == 24
    assert cruda.columns == [
        "id_credito", "segmento", "ead", "pd", "lgd", "estatus",
    ]
    # Plata: la cartera limpia y enriquecida.
    cartera = spark.read.parquet(str(out / "silver" / "parquet" / "cartera_limpia"))
    assert cartera.count() == 24
    assert "perdida_esperada" in cartera.columns
    assert "pd_bucket" in cartera.columns
    # Oro: los resúmenes, en Parquet y en CSV.
    resumen = spark.read.parquet(str(out / "gold" / "parquet" / "resumen_cartera"))
    assert resumen.count() == 1
    # Los CSV quedan como un solo archivo part-*.csv dentro de su carpeta.
    csv_files = list((out / "gold" / "csv" / "resumen_segmento").glob("*.csv"))
    assert len(csv_files) == 1
    with open(csv_files[0], encoding="utf-8") as f:
        filas = list(csv.DictReader(f))
    assert len(filas) == 4
    assert {r["segmento"] for r in filas} == {
        "Consumo", "Hipotecario", "PyME", "Tarjeta",
    }


def test_run_etl_aborta_con_datos_rotos(spark, tmp_path):
    malo = tmp_path / "malo.csv"
    malo.write_text(
        "id_credito,segmento,ead,pd,lgd,estatus\nX-001,PyME,-5000,0.05,0.40,vigente\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="cartera inválida"):
        etl.run_etl(malo, tmp_path / "out", spark=spark)
