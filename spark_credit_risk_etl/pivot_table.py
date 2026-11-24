"""Tablas dinámicas NATIVAS de Excel con openpyxl.

El problema: openpyxl sabe escribir hojas, pero una tabla dinámica
no es una hoja más: es un objeto con su propia definición XML, su
caché de datos y sus registros, todo relacionado dentro del zip. Si
falta una pieza, Excel abre el archivo y dice que está dañado.

Lo que hace este módulo es armar las tres piezas juntas a partir de
una hoja que ya tiene los datos (encabezados en la fila 1):

1. El caché: describe cada columna (qué valores distintos tiene) y
   guarda una copia de cada fila, para que Excel pueda mostrar la
   tabla sin tener que recalcular nada al abrir.
2. La definición: dice qué campo va en filas, cuál en columnas y
   qué se suma o se cuenta.
3. La hoja de la tabla: donde Excel dibuja el resultado.

Los valores se agrupan como los agrupa uno al mirar los datos: los
textos por su valor, los números por su valor, y los huecos se marcan
como faltantes para no inventar un cero donde no hay dato.

Uso típico (la hoja "Datos" ya está llena)::

    from pivot_table import add_pivot_table
    add_pivot_table(
        wb,
        source_sheet=wb["Datos"],
        pivot_sheet_title="Dinámica por segmento",
        pivot_name="PivotSegmento",
        row_fields=["Segmento"],
        data_fields=[("EAD", "Suma de EAD", "sum"),
                     ("EL", "Suma de pérdida esperada", "sum")],
    )
"""

from __future__ import annotations

from openpyxl.pivot.cache import (
    CacheDefinition,
    CacheField,
    CacheSource,
    SharedItems,
    WorksheetSource,
)
from openpyxl.pivot.fields import Boolean, Index, Missing, Number, Text
from openpyxl.pivot.record import Record, RecordList
from openpyxl.pivot.table import (
    DataField,
    FieldItem,
    Location,
    PivotField,
    RowColField,
    TableDefinition,
)
from openpyxl.utils import get_column_letter

__all__ = ["add_pivot_table"]

_MISSING = object()  # marca interna: hueco en los datos


def _column_kind(values):
    """Qué tipo de dato manda en una columna (ignorando huecos)."""
    kinds = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            kinds.add("bool")
        elif isinstance(value, (int, float)):
            kinds.add("number")
        elif isinstance(value, str):
            kinds.add("text")
        else:
            raise TypeError(
                "la tabla dinámica no acepta valores de tipo "
                f"{type(value).__name__}: pásalos como texto o número"
            )
    if len(kinds) > 1:
        raise TypeError(
            "una columna mezcla tipos distintos "
            f"({', '.join(sorted(kinds))}); normalízala antes"
        )
    return kinds.pop() if kinds else "text"


def _shared_items(distinct, kind):
    """Los valores distintos de una columna, como los quiere Excel."""
    items = []
    for value in distinct:
        if value is _MISSING:
            items.append(Missing())
        elif kind == "number":
            items.append(Number(v=float(value)))
        elif kind == "bool":
            items.append(Boolean(v=bool(value)))
        else:
            items.append(Text(v=str(value)))
    shared = SharedItems(
        _fields=items,
        containsString=kind == "text",
        containsNumber=kind == "number",
    )
    numbers = [v for v in distinct if isinstance(v, (int, float))]
    if numbers:
        shared.minValue = min(numbers)
        shared.maxValue = max(numbers)
    return shared


def _record_value(value, index_of):
    """Un valor de una fila, en el formato del caché de la tabla."""
    if value is None:
        return Index(v=index_of[_MISSING])
    if isinstance(value, bool):
        return Boolean(v=value)
    if isinstance(value, (int, float)):
        return Number(v=float(value))
    return Index(v=index_of[value])


def _build_cache(source_sheet, ref, cache_fields, records, record_count,
                 pivot_name):
    """El caché de una tabla: qué columnas hay y una copia de cada fila,
    para que Excel pueda dibujar la tabla sin recalcular al abrir.

    Cada tabla lleva SU PROPIO caché, aunque dos tablas beban de los
    mismos datos. ¿Por qué no compartirlo? Porque openpyxl compara los
    cachés por su contenido al escribir el libro: dos cachés idénticos
    se funden en una sola entrada de workbook.xml y la segunda tabla
    queda apuntando a un caché que no existe (lo comprobé: LibreOffice
    la tiraba al abrir). Para que no se fundan, cada caché lleva en
    ``refreshedBy`` el nombre de su tabla, que es lo único que los
    distingue; el contenido es el mismo.
    """
    cache = CacheDefinition(
        cacheSource=CacheSource(
            type="worksheet",
            worksheetSource=WorksheetSource(ref=ref, sheet=source_sheet.title),
        ),
        cacheFields=cache_fields,
        recordCount=record_count,
        refreshOnLoad=True,
        refreshedBy=pivot_name,
    )
    cache.records = RecordList(r=records)
    return cache


def add_pivot_table(wb, *, source_sheet, pivot_sheet_title, pivot_name,
                    row_fields, col_fields=(), data_fields):
    """Agrega una tabla dinámica nativa al libro y la devuelve.

    ``source_sheet`` es la hoja con los datos (encabezados en fila 1).
    ``row_fields`` y ``col_fields`` son nombres de columna; cada
    ``data_fields`` es (columna, nombre que se ve, subtotal), con
    subtotal en 'sum', 'count', 'countNums', 'average', 'max' o 'min'.
    Las tablas sobre la misma hoja de origen comparten un solo caché.
    """
    headers = [cell.value for cell in source_sheet[1]]
    if not headers or any(h is None for h in headers):
        raise ValueError("la hoja de origen necesita encabezados en la fila 1")
    rows = [list(r) for r in source_sheet.iter_rows(min_row=2, values_only=True)]
    if not rows:
        raise ValueError("la hoja de origen no tiene filas de datos")

    def col_index(name):
        if name not in headers:
            raise ValueError(f"la columna {name!r} no está en la hoja de origen")
        return headers.index(name)

    row_idx = [col_index(name) for name in row_fields]
    col_idx = [col_index(name) for name in col_fields]
    data_idx = [(col_index(name), label, subtotal)
                for name, label, subtotal in data_fields]

    # Los valores distintos de cada columna, en el orden en que aparecen:
    # el caché y los registros usan los mismos índices, así que el orden
    # solo tiene que ser el mismo en los dos lados.
    distinct = []
    for j in range(len(headers)):
        seen = []
        for row in rows:
            value = row[j]
            key = _MISSING if value is None else value
            if key not in seen:
                seen.append(key)
        distinct.append(seen)

    cache_fields = []
    for j, header in enumerate(headers):
        kind = _column_kind([r[j] for r in rows])
        cache_fields.append(CacheField(
            name=str(header),
            sharedItems=_shared_items(distinct[j], kind),
        ))

    records = []
    for row in rows:
        fields = []
        for j, value in enumerate(row):
            index_of = {v: i for i, v in enumerate(distinct[j])}
            fields.append(_record_value(value, index_of))
        records.append(Record(_fields=fields))

    n_rows = len(rows) + 1
    ref = f"A1:{get_column_letter(len(headers))}{n_rows}"
    cache = _build_cache(
        source_sheet, ref, cache_fields, records, len(rows), pivot_name
    )
    # Cada caché lleva un id distinto en el libro (0, 1, 2...): es el
    # que workbook.xml usa para registrarlo.
    cache_id = getattr(wb, "_pivot_cache_count", 0)
    wb._pivot_cache_count = cache_id + 1

    pivot_fields = []
    for j in range(len(headers)):
        items = [FieldItem(x=k) for k in range(len(distinct[j]))]
        items.append(FieldItem(t="default"))
        if j in row_idx:
            axis = "axisRow"
        elif j in col_idx:
            axis = "axisCol"
        elif any(j == d[0] for d in data_idx):
            axis = "axisValues"
        else:
            axis = None
        pivot_fields.append(PivotField(
            items=items, axis=axis, showAll=False,
            dataField=any(j == d[0] for d in data_idx),
        ))

    # El rectángulo que ocupa la tabla al dibujarla: una fila de
    # encabezado, una por cada combinación de valores de fila, y la del
    # total general; igual por columnas. Excel lo recalcula al abrir
    # (refreshOnLoad), pero LibreOffice dibuja desde aquí.
    n_row_items = 1
    for j in row_idx:
        n_row_items *= len(distinct[j])
    n_col_items = 1
    for j in col_idx:
        n_col_items *= len(distinct[j])
    out_rows = 1 + n_row_items + 1
    out_cols = 1 + n_col_items * len(data_idx) + 1
    ref = f"A1:{get_column_letter(out_cols)}{out_rows}"

    pivot = TableDefinition(
        name=pivot_name,
        cacheId=cache_id,
        dataCaption="Valores",
        location=Location(
            ref=ref, firstHeaderRow=1, firstDataRow=2, firstDataCol=1
        ),
        pivotFields=pivot_fields,
        rowFields=[RowColField(x=j) for j in row_idx],
        colFields=[RowColField(x=j) for j in col_idx],
        dataFields=[
            DataField(name=label, fld=j, subtotal=subtotal)
            for j, label, subtotal in data_idx
        ],
        rowGrandTotals=True,
        colGrandTotals=True,
    )
    pivot.cache = cache

    pivot_ws = wb.create_sheet(pivot_sheet_title)
    pivot_ws.add_pivot(pivot)
    return pivot
