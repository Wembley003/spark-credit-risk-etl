"""Pruebas del espejo ADLS: el ETL sube sus CSV al lago sin cambiar nada más.

Todo corre contra un falso en memoria: no hay cuenta de Azure, no hay
red. El falso tiene la misma forma que el cliente real de
``azure-storage-blob`` (``get_blob_client`` + ``upload_blob``), así
que lo que se prueba aquí es el espejo, no el SDK.
"""

from pathlib import Path

import pytest

from spark_credit_risk_etl import adls
from spark_credit_risk_etl.cli import main


class _FakeBlobClient:
    def __init__(self, store, container, blob):
        self._store = store
        self.container = container
        self.blob = blob

    def upload_blob(self, data, overwrite=False):
        if isinstance(data, (bytes, bytearray)):
            contenido = bytes(data)
        else:
            contenido = data.read()
        self._store[(self.container, self.blob)] = contenido


class _FakeServiceClient:
    """Un BlobServiceClient de mentira: guarda blobs en un dict."""

    def __init__(self):
        self.store = {}

    def get_blob_client(self, container, blob):
        return _FakeBlobClient(self.store, container, blob)


def _arbol_spark(tmp_path):
    """Árbol de salidas como lo escribe Spark: directorios con part-*.csv."""
    out = tmp_path / "outputs"
    datasets = {
        "bronze": ["cartera_cruda"],
        "silver": ["cartera_limpia"],
        "gold": ["resumen_cartera", "resumen_segmento", "resumen_estatus",
                 "resumen_buckets_pd"],
    }
    for zone, nombres in datasets.items():
        for nombre in nombres:
            d = out / zone / "csv" / nombre
            d.mkdir(parents=True)
            (d / "part-00000-abc-c000.csv").write_text("a,b\n1,2\n", encoding="utf-8")
            (d / "_SUCCESS").write_text("", encoding="utf-8")
            (d / ".part-00000-abc-c000.csv.crc").write_text("crc", encoding="utf-8")
    return out


def _arbol_reference(tmp_path):
    """Árbol de salidas como lo escribe --reference: archivos directos."""
    out = tmp_path / "outputs"
    datasets = {
        "bronze": ["cartera_cruda"],
        "silver": ["cartera_limpia"],
        "gold": ["resumen_cartera", "resumen_segmento", "resumen_estatus",
                 "resumen_buckets_pd"],
    }
    for zone, nombres in datasets.items():
        for nombre in nombres:
            d = out / zone / "csv"
            d.mkdir(parents=True, exist_ok=True)
            (d / f"{nombre}.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    return out


ESPERADOS = [
    "bronze/cartera_cruda.csv",
    "silver/cartera_limpia.csv",
    "gold/resumen_buckets_pd.csv",
    "gold/resumen_cartera.csv",
    "gold/resumen_estatus.csv",
    "gold/resumen_segmento.csv",
]


def test_collect_csv_blobs_formato_spark(tmp_path):
    out = _arbol_spark(tmp_path)
    pares = adls.collect_csv_blobs(out)
    assert [blob for _, blob in pares] == ESPERADOS
    # Los _SUCCESS y los .crc no se suben.
    assert all("SUCCESS" not in blob and "crc" not in blob for _, blob in pares)


def test_collect_csv_blobs_formato_reference(tmp_path):
    out = _arbol_reference(tmp_path)
    pares = adls.collect_csv_blobs(out)
    assert [blob for _, blob in pares] == ESPERADOS


def test_collect_csv_blobs_sin_csv(tmp_path):
    assert adls.collect_csv_blobs(tmp_path / "vacio") == []


def test_upload_outputs_sube_los_csv_al_falso(tmp_path):
    out = _arbol_spark(tmp_path)
    falso = _FakeServiceClient()
    uploader = adls.LakeUploader(falso, container="credit-risk")
    subidos = uploader.upload_outputs(out)
    assert subidos == ESPERADOS
    assert set(falso.store) == {("credit-risk", b) for b in ESPERADOS}
    assert falso.store[("credit-risk", "gold/resumen_cartera.csv")] == b"a,b\n1,2\n"


def test_upload_outputs_sin_csv_no_sube_nada(tmp_path):
    falso = _FakeServiceClient()
    uploader = adls.LakeUploader(falso)
    assert uploader.upload_outputs(tmp_path / "vacio") == []
    assert falso.store == {}


def test_build_service_client_sin_configuracion_falla():
    with pytest.raises(adls.LakeError, match="configuración"):
        adls.build_service_client()


def test_cli_reference_con_upload_adls_usa_el_falso(tmp_path, monkeypatch):
    # El CLI corre el ETL de referencia y luego sube con el falso.
    falso = _FakeServiceClient()
    monkeypatch.setattr(
        "spark_credit_risk_etl.adls.build_service_client",
        lambda **kwargs: falso,
    )
    data = Path(__file__).resolve().parent.parent / "data" / "portfolio_sample.csv"
    out = tmp_path / "outputs"
    codigo = main([
        "--input", str(data),
        "--output-dir", str(out),
        "--reference",
        "--upload-adls",
    ])
    assert codigo == 0
    assert set(falso.store) == {("credit-risk", b) for b in ESPERADOS}
    # Y las salidas locales siguen ahí: el espejo no las toca.
    assert (out / "gold" / "csv" / "resumen_cartera.csv").is_file()


def test_cli_upload_adls_sin_configuracion_es_1_y_deja_las_salidas(tmp_path, monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)
    data = Path(__file__).resolve().parent.parent / "data" / "portfolio_sample.csv"
    out = tmp_path / "outputs"
    codigo = main([
        "--input", str(data),
        "--output-dir", str(out),
        "--reference",
        "--upload-adls",
    ])
    assert codigo == 1
    # El ETL local sí terminó: las salidas existen aunque el espejo fallara.
    assert (out / "gold" / "csv" / "resumen_cartera.csv").is_file()
