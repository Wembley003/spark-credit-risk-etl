"""Espejo opcional de las salidas CSV en Azure Data Lake Storage Gen2.

Por qué existe este módulo: el ETL de Spark escribe los CSV de cada
zona en disco (``outputs/bronze/csv/...``) y este módulo los sube,
tal cual, al lago de Azure, con los mismos nombres de blob que usa
el proyecto hermano ``azure-data-lake-credit-risk`` (``bronze/``,
``silver/``, ``gold/``). Es un espejo, no una segunda implementación:
las reglas no cambian, Spark sigue siendo el protagonista y el
lago local sigue siendo el comportamiento por defecto. La subida
solo ocurre cuando el usuario la pide con ``--upload-adls``.

Dos formatos de salida, un solo espejo: el ETL de Spark escribe cada
dataset como un directorio con un ``part-*.csv`` dentro (Spark
siempre escribe así), mientras que el modo ``--reference`` (pandas)
escribe un único archivo ``<dataset>.csv``. El espejo acepta los
dos: el blob se llama siempre ``<zona>/<dataset>.csv``.

El SDK de Azure se importa tarde (dentro de ``build_service_client``),
igual que en el proyecto hermano: sin el flag, sin ``azure`` instalado
y sin credenciales, el ETL corre exactamente como siempre. Y las
pruebas usan un cliente falso con la misma forma que el real, así que
la subida se prueba sin red ni cuenta.
"""

from __future__ import annotations

from pathlib import Path

__all__ = ["LakeUploader", "build_service_client", "collect_csv_blobs"]

ZONES = ("bronze", "silver", "gold")
DEFAULT_CONTAINER = "credit-risk"


class LakeError(RuntimeError):
    """Algo falló hablando con el lago (configuración o red)."""


def collect_csv_blobs(output_dir):
    """Empareja cada CSV de zona con el nombre de blob que le toca.

    Devuelve una lista de pares ``(ruta_local, nombre_blob)`` en
    orden de zona (Bronce, Plata, Oro) y, dentro de cada zona, por
    nombre de dataset. Acepta los dos formatos de salida:

    - ``<zona>/csv/<dataset>.csv`` (modo --reference, un archivo).
    - ``<zona>/csv/<dataset>/part-*.csv`` (Spark, un directorio
      con un único archivo de parte; se ignoran ``_SUCCESS`` y
      los ``.crc``).
    """
    output_dir = Path(output_dir)
    pares = []
    for zone in ZONES:
        csv_dir = output_dir / zone / "csv"
        if not csv_dir.is_dir():
            continue
        for entry in sorted(csv_dir.iterdir()):
            if entry.is_file() and entry.suffix == ".csv":
                pares.append((entry, f"{zone}/{entry.name}"))
            elif entry.is_dir():
                partes = sorted(p for p in entry.glob("part-*.csv") if p.is_file())
                if len(partes) == 1:
                    pares.append((partes[0], f"{zone}/{entry.name}.csv"))
                elif len(partes) > 1:
                    # Sin coalesce(1) habría varias partes: se suben
                    # tal cual bajo el prefijo del dataset.
                    for parte in partes:
                        pares.append((parte, f"{zone}/{entry.name}/{parte.name}"))
    return pares


class LakeUploader:
    """Sube los CSV de zona a un contenedor de ADLS Gen2.

    Recibe un cliente de servicio ya construido (el de
    ``azure-storage-blob`` o un falso en pruebas) y un contenedor.
    No sabe nada de Spark ni de las reglas: solo copia archivos
    locales a blobs, uno por uno, sin sobreescribir nada más.
    """

    def __init__(self, service_client, container=DEFAULT_CONTAINER):
        self._service_client = service_client
        self._container = container

    @property
    def container(self):
        return self._container

    def upload_outputs(self, output_dir):
        """Sube todos los CSV de zona; devuelve los nombres de blob."""
        subidos = []
        for ruta, nombre_blob in collect_csv_blobs(output_dir):
            blob_client = self._service_client.get_blob_client(
                container=self._container, blob=nombre_blob
            )
            with open(ruta, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)
            subidos.append(nombre_blob)
        return subidos


def build_service_client(account=None, connection_string=None, key=None):
    """Construye el cliente de Azure desde la configuración dada.

    Se prefiere la cadena de conexión; si no hay, cuenta + clave.
    El SDK se importa aquí y no antes: quien no usa ADLS no tiene
    que tener ``azure-storage-blob`` instalado.
    """
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:  # pragma: no cover - depende del entorno
        raise LakeError(
            "azure-storage-blob no está instalado; instala requirements.txt "
            "(pip install azure-storage-blob>=12.0)"
        ) from exc
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)
    if account and key:
        url = f"https://{account}.blob.core.windows.net"
        return BlobServiceClient(account_url=url, credential=key)
    raise LakeError(
        "falta configuración de ADLS: pasa AZURE_STORAGE_CONNECTION_STRING "
        "o AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY"
    )
