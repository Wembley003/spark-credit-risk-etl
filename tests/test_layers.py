"""Pruebas de la estructura por capas (Bronce -> Plata -> Oro).

Lo que se verifica aquí es el contrato entre capas, no los números
en sí (esos ya los cubren test_etl y test_reference_impl):

- Bronce preserva lo crudo tal como llegó, sin juzgarlo: una fila
  rota se guarda igual, porque el Bronce es el fósil y su trabajo es
  que nada se pierda en el camino.
- Plata es la compuerta: si los datos no pasan la validación, no se
  escribe nada en Plata ni en Oro, y lo que sí se escribe ya no tiene
  filas inválidas.
- Oro reproduce las cifras conocidas de la cartera (las del agente
  ADK), en Spark y en pandas por igual.

Si una capa se salta o se reordena, alguna de estas pruebas se cae:
esa es la gracia de probar la estructura y no solo el resultado.
"""

import csv
from pathlib import Path

import pandas as pd
import pytest

from spark_credit_risk_etl import etl, reference_impl

DATA = Path(__file__).resolve().parent.parent / "data" / "portfolio_sample.csv"


@pytest.fixture(scope="module")
def spark():
    session = etl.build_spark(app_name="test-spark-layers")
    yield session
    session.stop()


def _csv_malo(tmp_path, fila="X-001,PyME,100000,1.5,0.40,vigente\n"):
    malo = tmp_path / "malo.csv"
    malo.write_text(
        "id_credito,segmento,ead,pd,lgd,estatus\n" + fila,
        encoding="utf-8",
    )
    return malo


def _suma_ead_csv():
    with open(DATA, newline="", encoding="utf-8") as f:
        return sum(float(r["ead"]) for r in csv.DictReader(f))


def test_bronce_preserva_filas_crudas(spark, tmp_path):
    out = tmp_path / "outputs"
    df, written = etl.to_bronze(spark, DATA, out)
    assert df.count() == 24
    # El Bronce no agrega nada: solo las 6 columnas que traía el CSV.
    assert df.columns == ["id_credito", "segmento", "ead", "pd", "lgd", "estatus"]
    assert "perdida_esperada" not in df.columns
    assert "pd_bucket" not in df.columns
    # Y lo que se guardó es lo mismo que llegó, peso por peso.
    guardada = spark.read.parquet(written["parquet"])
    assert guardada.count() == 24
    assert guardada.agg({"ead": "sum"}).collect()[0][0] == pytest.approx(
        _suma_ead_csv()
    )
    assert (out / "bronze" / "csv" / "cartera_cruda").is_dir()


def test_bronce_guarda_fila_rota_sin_juzgarla(spark, tmp_path):
    # Una PD de 1.5 es imposible (una probabilidad no pasa de 1), pero
    # el Bronce no es quien para decidirlo: la guarda igual, con su
    # valor crudo, para que el problema se vea en Plata.
    malo = _csv_malo(tmp_path)
    out = tmp_path / "outputs"
    df, _ = etl.to_bronze(spark, malo, out)
    assert df.count() == 1
    assert df.collect()[0]["pd"] == pytest.approx(1.5)


def test_plata_rechaza_datos_rotos(spark, tmp_path):
    malo = _csv_malo(tmp_path)
    out = tmp_path / "outputs"
    df, _ = etl.to_bronze(spark, malo, out)
    with pytest.raises(ValueError, match="cartera inválida"):
        etl.to_silver(df, out)
    # La compuerta funcionó: no quedó nada escrito en Plata ni en Oro.
    assert not (out / "silver").exists()
    assert not (out / "gold").exists()


def test_plata_sin_filas_invalidas(spark, tmp_path):
    out = tmp_path / "outputs"
    df_raw, _ = etl.to_bronze(spark, DATA, out)
    df, _ = etl.to_silver(df_raw, out)
    assert etl.validate_portfolio(df) == []
    assert df.filter("pd < 0 OR pd > 1").count() == 0
    assert df.filter("lgd < 0 OR lgd > 1").count() == 0
    assert df.filter("ead <= 0").count() == 0
    assert "perdida_esperada" in df.columns
    assert "pd_bucket" in df.columns


def test_oro_reproduce_cifras_conocidas(spark, tmp_path):
    out = tmp_path / "outputs"
    result = etl.run_etl(DATA, out, spark=spark)
    assert set(result["outputs"]) == {"bronze", "silver", "gold"}
    resumen = result["summary"]
    assert resumen["numero_creditos"] == 24
    assert resumen["ead_total"] == pytest.approx(17_955_000.0)
    assert resumen["perdida_esperada_total"] == pytest.approx(358_595.0)
    assert resumen["pd_ponderada"] == pytest.approx(0.0430, abs=1e-4)
    assert resumen["morosidad_pct"] == pytest.approx(31.30, abs=0.01)
    # El Oro quedó guardado: los 4 resúmenes, en Parquet y en CSV.
    for nombre in (
        "resumen_cartera",
        "resumen_segmento",
        "resumen_estatus",
        "resumen_buckets_pd",
    ):
        assert (out / "gold" / "parquet" / nombre).is_dir()
        assert (out / "gold" / "csv" / nombre).is_dir()


def test_referencia_capas_en_orden(tmp_path):
    out = tmp_path / "outputs"
    df_raw, _ = reference_impl.to_bronze(DATA, out)
    assert len(df_raw) == 24
    df, _ = reference_impl.to_silver(df_raw, out)
    assert reference_impl.validate(df) == []
    summaries, _ = reference_impl.to_gold(df, out)
    resumen = summaries["resumen_cartera"].iloc[0]
    assert int(resumen["numero_creditos"]) == 24
    assert float(resumen["ead_total"]) == pytest.approx(17_955_000.0)
    assert float(resumen["perdida_esperada_total"]) == pytest.approx(358_595.0)
    assert float(resumen["pd_ponderada"]) == pytest.approx(0.0430, abs=1e-4)
    assert float(resumen["morosidad_pct"]) == pytest.approx(31.30, abs=0.01)
    # Los archivos de cada capa están donde toca.
    assert (out / "bronze" / "parquet" / "cartera_cruda.parquet").is_file()
    assert (out / "silver" / "parquet" / "cartera_limpia.parquet").is_file()
    assert (out / "gold" / "csv" / "resumen_segmento.csv").is_file()


def test_referencia_bronce_guarda_fila_rota_y_plata_la_rechaza(tmp_path):
    malo = _csv_malo(tmp_path)
    out = tmp_path / "outputs"
    df_raw, _ = reference_impl.to_bronze(malo, out)
    assert len(df_raw) == 1
    assert float(df_raw.iloc[0]["pd"]) == pytest.approx(1.5)
    with pytest.raises(ValueError, match="cartera inválida"):
        reference_impl.to_silver(df_raw, out)
    assert not (out / "silver").exists()
    assert not (out / "gold").exists()


def test_referencia_y_spark_dan_las_mismas_cifras_en_oro(spark, tmp_path):
    out_spark = tmp_path / "spark"
    out_ref = tmp_path / "ref"
    resumen_spark = etl.run_etl(DATA, out_spark, spark=spark)["summary"]
    df_raw, _ = reference_impl.to_bronze(DATA, out_ref)
    df, _ = reference_impl.to_silver(df_raw, out_ref)
    summaries, _ = reference_impl.to_gold(df, out_ref)
    resumen_ref = summaries["resumen_cartera"].iloc[0].to_dict()
    for key in resumen_spark:
        assert resumen_spark[key] == pytest.approx(float(resumen_ref[key]), rel=1e-9)
    # Y el Oro de pandas coincide fila por fila con el de Spark.
    oro_spark = pd.read_parquet(out_spark / "gold" / "parquet" / "resumen_segmento")
    oro_ref = pd.read_parquet(out_ref / "gold" / "parquet" / "resumen_segmento.parquet")
    assert list(oro_spark["segmento"]) == list(oro_ref["segmento"])
    assert list(oro_spark["ead_total"]) == pytest.approx(list(oro_ref["ead_total"]))


# ---------------------------------------------------------------------------
# Las tres capas arquitectónicas: Cliente / Reglas de negocio /
# Consultas o análisis. Bronce → Plata → Oro vive DENTRO del flujo de
# datos (en etl.py y reference_impl.py); estas tres capas son la
# arquitectura del proyecto:
#   Cliente:            cli.py, run_etl.py (lo que el usuario corre)
#   Reglas de negocio:  etl.py, reference_impl.py (validación, EL, PD, morosidad)
#   Consultas/análisis: excel_export.py, pivot_table.py (lo de Oro, preguntado
#                       y exportado; la tecnología aquí es Spark SQL + Excel)
# La regla es de dirección: el Cliente no toca el almacenamiento
# directo (nada de openpyxl ni de módulos de consultas), y las Reglas
# no dependen del formato de salida (nada de openpyxl ni sqlite3).
# ---------------------------------------------------------------------------

import ast
import subprocess
import sys
import textwrap

_PROJECT = Path(__file__).resolve().parent.parent
_PACKAGE = _PROJECT / "spark_credit_risk_etl"

_CAPAS = {
    "cliente": {"cli", "run_etl", "adls"},
    "reglas": {"etl", "reference_impl"},
    "consultas": {"excel_export", "pivot_table"},
}

# Módulos de almacenamiento/salida que las reglas no deben conocer.
_SALIDA = {"excel_export", "pivot_table"}
# Librerías de almacenamiento/salida que cliente y reglas no deben importar.
_LIBS_SALIDA = {"openpyxl", "sqlite3"}


def _modulos_importados(path: Path) -> set[str]:
    """Nombres de módulo que un archivo importa (resueltos a su raíz).

    ``from paquete.modulo import ...`` se resuelve a ``modulo`` y
    ``from openpyxl.x import ...`` a ``openpyxl``: lo que importa es
    qué parte del proyecto (o qué librería) se toca, no el submódulo.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nombres = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                partes = alias.name.split(".")
                nombres.add(partes[1] if partes[0] == "spark_credit_risk_etl" and len(partes) > 1 else partes[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                partes = node.module.split(".")
                nombres.add(partes[1] if partes[0] == "spark_credit_risk_etl" and len(partes) > 1 else partes[0])
            else:
                for alias in node.names:
                    nombres.add(alias.name.split(".")[0])
    return nombres


def _capa_de(stem: str) -> str:
    for capa, modulos in _CAPAS.items():
        if stem in modulos:
            return capa
    raise AssertionError(f"módulo sin capa asignada: {stem}")


def test_todos_los_modulos_tienen_capa():
    # Cada módulo del paquete pertenece a exactamente una capa; si
    # alguien agrega un archivo y se le olvida ubicarlo, esta prueba
    # lo delata. __init__.py es la fachada pública, no una capa.
    vistos = set()
    for path in sorted(_PACKAGE.glob("*.py")):
        if path.stem == "__init__":
            continue
        vistos.add(path.stem)
        _capa_de(path.stem)
    assert vistos == set().union(*_CAPAS.values()) - {"run_etl"}
    assert (_PROJECT / "run_etl.py").exists()


def test_capas_no_se_mezclan():
    # Cliente no toca consultas ni librerías de salida; reglas no
    # conocen la salida; consultas no llaman al cliente.
    archivos = {p.stem: p for p in _PACKAGE.glob("*.py")}
    archivos["run_etl"] = _PROJECT / "run_etl.py"
    for stem, path in sorted(archivos.items()):
        if stem == "__init__":
            continue
        capa = _capa_de(stem)
        importados = _modulos_importados(path)
        if capa == "cliente":
            assert not (importados & _CAPAS["consultas"]), (
                f"{stem} (cliente) importa de consultas: {importados & _CAPAS['consultas']}"
            )
            assert not (importados & _LIBS_SALIDA), (
                f"{stem} (cliente) importa librerías de salida: {importados & _LIBS_SALIDA}"
            )
        elif capa == "reglas":
            assert not (importados & _SALIDA), (
                f"{stem} (reglas) importa módulos de salida: {importados & _SALIDA}"
            )
            assert not (importados & _CAPAS["cliente"]), (
                f"{stem} (reglas) importa del cliente: {importados & _CAPAS['cliente']}"
            )
            assert not (importados & _LIBS_SALIDA), (
                f"{stem} (reglas) importa librerías de salida: {importados & _LIBS_SALIDA}"
            )
        elif capa == "consultas":
            assert not (importados & _CAPAS["cliente"]), (
                f"{stem} (consultas) importa del cliente: {importados & _CAPAS['cliente']}"
            )


def test_reglas_no_dependen_del_formato_de_salida(tmp_path):
    # La prueba fuerte: con openpyxl y sqlite3 bloqueados, las reglas
    # (el gemelo en pandas, que es el mismo cálculo) siguen dando las
    # cifras conocidas. Si las reglas dependieran de Excel, esto se cae.
    codigo = textwrap.dedent(
        """
        import importlib.abc
        import sys
        from pathlib import Path
        sys.path.insert(0, r"%s")

        class _Bloqueo(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.split(".")[0] in {"openpyxl", "sqlite3"}:
                    raise ImportError("bloqueado en esta prueba: " + fullname)
                return None

        sys.meta_path.insert(0, _Bloqueo())
        from spark_credit_risk_etl import reference_impl
        df = reference_impl.load_portfolio(r"%s")
        assert reference_impl.validate(df) == []
        df = reference_impl.add_expected_loss(df)
        resumen = reference_impl.portfolio_summary(df)
        assert resumen["numero_creditos"] == 24
        assert resumen["ead_total"] == 17_955_000
        assert resumen["perdida_esperada_total"] == 358_595
        assert round(resumen["pd_ponderada"], 4) == 0.0430
        assert round(resumen["morosidad_pct"], 2) == 31.30
        print("reglas OK sin openpyxl ni sqlite3")
        """
        % (str(_PROJECT), str(DATA))
    )
    resultado = subprocess.run(
        [sys.executable, "-c", codigo],
        capture_output=True,
        text=True,
        cwd=str(_PROJECT),
    )
    assert resultado.returncode == 0, resultado.stderr
    assert "reglas OK sin openpyxl ni sqlite3" in resultado.stdout
