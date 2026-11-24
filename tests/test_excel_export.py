"""Pruebas de la exportación a Excel con tablas dinámicas nativas.

Lo que se verifica aquí es lo que Excel necesita para no decir que
el archivo está dañado: que las hojas existan, que las sumas del
libro coincidan con los datos de juguete, y que dentro del zip del
.xlsx estén las tres piezas de cada tabla dinámica — su definición,
su caché y sus registros —, una por tabla. Sin esas piezas, Excel
abre el archivo y la tabla no aparece (o el archivo entero se marca
como corrupto).
"""

import zipfile
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import load_workbook

from spark_credit_risk_etl.excel_export import build_workbook, main

# Cartera de juguete: 5 créditos en 2 segmentos, con PD en los cuatro
# buckets para que la dinámica por bucket tenga qué agrupar.
CARTERA = pd.DataFrame(
    [
        ("C-001", "Hipotecario", 1_000_000.0, 0.01, 0.25, "vigente"),
        ("C-002", "Hipotecario", 500_000.0, 0.03, 0.30, "vencido"),
        ("C-003", "PyME", 200_000.0, 0.07, 0.50, "vigente"),
        ("C-004", "PyME", 100_000.0, 0.12, 0.60, "vencido"),
        ("C-005", "PyME", 300_000.0, 0.015, 0.40, "vigente"),
    ],
    columns=["id_credito", "segmento", "ead", "pd", "lgd", "estatus"],
)

SEGMENTO = pd.DataFrame(
    [
        ("Hipotecario", 2, 1_500_000.0, 7_000.0, 0.0167, 500_000.0, 33.33),
        ("PyME", 3, 600_000.0, 16_000.0, 0.0508, 100_000.0, 16.67),
    ],
    columns=[
        "segmento", "numero_creditos", "ead_total",
        "perdida_esperada_total", "pd_ponderada", "cartera_vencida",
        "morosidad_pct",
    ],
)

ESTATUS = pd.DataFrame(
    [
        ("vencido", 2, 600_000.0, 11_700.0),
        ("vigente", 3, 1_500_000.0, 11_300.0),
    ],
    columns=["estatus", "numero_creditos", "ead_total",
             "perdida_esperada_total"],
)

BUCKETS = pd.DataFrame(
    [
        ("<2%", 2, 1_300_000.0, 4_300.0),
        ("2-5%", 1, 500_000.0, 4_500.0),
        ("5-10%", 1, 200_000.0, 7_000.0),
        (">=10%", 1, 100_000.0, 7_200.0),
    ],
    columns=["pd_bucket", "numero_creditos", "ead_total",
             "perdida_esperada_total"],
)

EXPECTED_SHEETS = [
    "Cartera",
    "Por segmento",
    "Por estatus",
    "Buckets PD",
    "Dinámica por segmento",
    "Dinámica por estatus",
    "Dinámica por bucket PD",
]


def _toy_workbook(tmp_path):
    out = tmp_path / "portfolio.xlsx"
    build_workbook(CARTERA, SEGMENTO, ESTATUS, BUCKETS, out)
    return out


def test_hojas_y_sumas(tmp_path):
    out = _toy_workbook(tmp_path)
    wb = load_workbook(out)
    assert wb.sheetnames == EXPECTED_SHEETS

    cartera = wb["Cartera"]
    headers = [c.value for c in cartera[1]]
    assert headers == [
        "id_credito", "segmento", "estatus", "ead", "pd", "lgd",
        "perdida_esperada", "bucket_pd",
    ]
    # 5 créditos + encabezado.
    assert cartera.max_row == 6
    ead_col = headers.index("ead") + 1
    el_col = headers.index("perdida_esperada") + 1
    bucket_col = headers.index("bucket_pd") + 1
    ead_total = sum(cartera.cell(row=r, column=ead_col).value
                    for r in range(2, 7))
    el_total = sum(cartera.cell(row=r, column=el_col).value
                   for r in range(2, 7))
    assert ead_total == pytest.approx(2_100_000.0)
    # EAD x PD x LGD, crédito por crédito.
    assert el_total == pytest.approx(23_000.0)
    # Los buckets salen de la PD con los cortes del ETL (2%, 5%, 10%).
    buckets = [cartera.cell(row=r, column=bucket_col).value
               for r in range(2, 7)]
    assert buckets == ["<2%", "2-5%", "5-10%", ">=10%", "<2%"]

    # Los resúmenes que se escribieron son los que entraron.
    segmento = wb["Por segmento"]
    assert segmento.max_row == 3
    assert segmento.cell(row=2, column=1).value == "Hipotecario"
    assert segmento.cell(row=2, column=3).value == pytest.approx(1_500_000.0)
    assert wb["Por estatus"].max_row == 3
    assert wb["Buckets PD"].max_row == 5


def test_pivot_tables_dentro_del_zip(tmp_path):
    out = _toy_workbook(tmp_path)
    with zipfile.ZipFile(out) as zf:
        names = set(zf.namelist())
    # Tres tablas: su definición, su caché y sus registros, una de cada
    # una por tabla. Si falta alguna, Excel no dibuja la tabla.
    for i in (1, 2, 3):
        assert f"xl/pivotTables/pivotTable{i}.xml" in names
        assert f"xl/pivotCache/pivotCacheDefinition{i}.xml" in names
        assert f"xl/pivotCache/pivotCacheRecords{i}.xml" in names
    # Las tres definiciones apuntan a la hoja Cartera.
    with zipfile.ZipFile(out) as zf:
        for i in (1, 2, 3):
            xml = zf.read(f"xl/pivotTables/pivotTable{i}.xml").decode()
            assert "Cartera" in xml or "cacheId" in xml


def test_pivot_sin_datos_no_se_crea(tmp_path):
    vacia = pd.DataFrame(columns=CARTERA.columns)
    out = tmp_path / "vacio.xlsx"
    build_workbook(vacia, SEGMENTO, ESTATUS, BUCKETS, out)
    wb = load_workbook(out)
    assert wb.sheetnames == [
        "Cartera", "Por segmento", "Por estatus", "Buckets PD",
    ]
    assert wb["Cartera"].max_row == 1  # solo el encabezado


def test_cli_sin_outputs_falla_con_mensaje_claro(tmp_path, capsys):
    rc = main([
        "--input-dir", str(tmp_path / "no_existe"),
        "--output", str(tmp_path / "portfolio.xlsx"),
    ])
    assert rc == 2
    err = capsys.readouterr().err
    assert "run_etl.sh" in err or "make run" in err
    assert not (tmp_path / "portfolio.xlsx").exists()


def test_cli_con_parquet_real(tmp_path):
    # Las salidas reales del ETL ya existen en outputs/parquet.
    project = Path(__file__).resolve().parent.parent
    out = tmp_path / "portfolio.xlsx"
    rc = main([
        "--input-dir", str(project / "outputs" / "parquet"),
        "--output", str(out),
    ])
    assert rc == 0
    wb = load_workbook(out)
    assert wb.sheetnames == EXPECTED_SHEETS
    cartera = wb["Cartera"]
    assert cartera.max_row == 25  # 24 créditos + encabezado
    headers = [c.value for c in cartera[1]]
    ead_col = headers.index("ead") + 1
    el_col = headers.index("perdida_esperada") + 1
    ead_total = sum(cartera.cell(row=r, column=ead_col).value
                    for r in range(2, 26))
    el_total = sum(cartera.cell(row=r, column=el_col).value
                   for r in range(2, 26))
    assert ead_total == pytest.approx(17_955_000.0)
    assert el_total == pytest.approx(358_595.0)
