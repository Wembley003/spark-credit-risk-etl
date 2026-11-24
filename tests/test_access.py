"""Pruebas de la exportación a Access.

Dos niveles. El primero no necesita nada fuera de Python: el vuelco
a CSV + esquema produce las 4 tablas con sus filas y sus columnas,
y las cifras coinciden con las del ETL. El segundo construye el
.accdb de verdad con Jackcess y lo verifica con mdbtools; si en la
máquina no están Java, curl o mdbtools (o no se pueden descargar los
jars), esa prueba se salta con el motivo, porque no es un fallo del
código sino del entorno.
"""

import csv
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from access.dump_from_outputs import build_tables

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ACCESS_DIR = PROJECT_ROOT / "access"


def _read_schema_json(out_dir):
    return json.loads((out_dir / "schema.json").read_text(encoding="utf-8"))


def test_dump_tablas_y_filas(tmp_path):
    from access.dump_access import dump_tables

    summary = dump_tables(build_tables(), tmp_path)
    assert [t["name"] for t in summary["tables"]] == [
        "cartera", "por_segmento", "por_estatus", "buckets_pd",
    ]
    rows = {t["name"]: t["rows"] for t in summary["tables"]}
    assert rows == {
        "cartera": 24, "por_segmento": 4, "por_estatus": 2, "buckets_pd": 4,
    }

    # El esquema plano lista las 4 tablas y sus columnas.
    schema_txt = (tmp_path / "schema.txt").read_text(encoding="utf-8")
    assert schema_txt.count("TABLE|") == 4
    assert "COL|id_credito|TEXT|" in schema_txt
    assert "COL|ead|DOUBLE|" in schema_txt
    assert "COL|numero_creditos|LONG|" in schema_txt

    # Las cifras de la cartera coinciden con las del ETL.
    with open(tmp_path / "csv" / "cartera.csv", newline="",
              encoding="utf-8") as fh:
        cartera = list(csv.DictReader(fh))
    assert len(cartera) == 24
    assert sum(float(r["ead"]) for r in cartera) == pytest.approx(17_955_000.0)
    assert sum(float(r["perdida_esperada"]) for r in cartera) == pytest.approx(
        358_595.0
    )
    assert {r["pd_bucket"] for r in cartera} == {"<2%", "2-5%", "5-10%", ">=10%"}

    # Los resúmenes suman al total y cubren toda la cartera.
    with open(tmp_path / "csv" / "por_segmento.csv", newline="",
              encoding="utf-8") as fh:
        segmentos = list(csv.DictReader(fh))
    assert sum(float(r["ead_total"]) for r in segmentos) == pytest.approx(
        17_955_000.0
    )
    assert sum(int(r["numero_creditos"]) for r in segmentos) == 24
    with open(tmp_path / "csv" / "por_estatus.csv", newline="",
              encoding="utf-8") as fh:
        estatus = list(csv.DictReader(fh))
    assert sum(float(r["ead_total"]) for r in estatus) == pytest.approx(
        17_955_000.0
    )


def test_dump_real_en_access(tmp_path):
    """El vuelco real que deja el script, con sus 4 CSV y su esquema."""
    from access.dump_access import dump_tables

    dump_tables(build_tables(), ACCESS_DIR)
    for name in ("cartera", "por_segmento", "por_estatus", "buckets_pd"):
        assert (ACCESS_DIR / "csv" / f"{name}.csv").is_file()
    assert (ACCESS_DIR / "schema.txt").is_file()
    assert (ACCESS_DIR / "schema.json").is_file()
    summary = _read_schema_json(ACCESS_DIR)
    assert [t["name"] for t in summary["tables"]] == [
        "cartera", "por_segmento", "por_estatus", "buckets_pd",
    ]


def _toolchain_missing():
    """Qué le falta al entorno para construir el .accdb (o None)."""
    for tool in ("java", "javac", "curl", "mdb-tables", "mdb-export"):
        if shutil.which(tool) is None:
            return f"falta {tool} en esta máquina"
    # Los jars tienen que poder descargarse de Maven Central.
    probe = subprocess.run(
        ["curl", "-sI", "--max-time", "15",
         "https://repo1.maven.org/maven2/com/healthmarketscience/"
         "jackcess/jackcess/4.0.5/jackcess-4.0.5.jar"],
        capture_output=True, text=True,
    )
    if probe.returncode != 0 or "200" not in probe.stdout:
        return "no se puede descargar Jackcess de Maven Central"
    return None


def test_build_accdb_en_tmp(tmp_path):
    missing = _toolchain_missing()
    if missing:
        pytest.skip(missing)

    # Una copia limpia de access/ en tmp: el build no toca el original.
    # dump_from_outputs.py necesita el paquete y los datos al lado.
    work = tmp_path / "access"
    shutil.copytree(
        ACCESS_DIR, work,
        ignore=shutil.ignore_patterns("lib", "build", "*.accdb", "__pycache__"),
    )
    shutil.copytree(
        PROJECT_ROOT / "spark_credit_risk_etl",
        tmp_path / "spark_credit_risk_etl",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copytree(PROJECT_ROOT / "data", tmp_path / "data")
    accdb = work / "prueba.accdb"
    result = subprocess.run(
        [str(work / "build_access.sh"), "prueba.accdb"],
        cwd=work, capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert accdb.is_file()

    # mdbtools ve las 4 tablas y los conteos coinciden con los CSV.
    tables = subprocess.run(
        ["mdb-tables", "-1", str(accdb)],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    assert sorted(tables) == [
        "buckets_pd", "cartera", "por_estatus", "por_segmento",
    ]
    for table, expected in (
        ("cartera", 24), ("por_segmento", 4),
        ("por_estatus", 2), ("buckets_pd", 4),
    ):
        out = subprocess.run(
            ["mdb-export", str(accdb), table],
            capture_output=True, text=True, check=True,
        ).stdout
        assert len(out.strip().splitlines()) - 1 == expected

    # Y los valores: la suma de EAD de la tabla cartera es la del ETL.
    out = subprocess.run(
        ["mdb-export", str(accdb), "cartera"],
        capture_output=True, text=True, check=True,
    ).stdout
    lines = out.strip().splitlines()
    ead_idx = lines[0].split(",").index("ead")
    el_idx = lines[0].split(",").index("perdida_esperada")
    ead_total = sum(float(l.split(",")[ead_idx]) for l in lines[1:])
    el_total = sum(float(l.split(",")[el_idx]) for l in lines[1:])
    assert ead_total == pytest.approx(17_955_000.0)
    assert el_total == pytest.approx(358_595.0)
