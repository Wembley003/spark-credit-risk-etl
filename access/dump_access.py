"""Vuelca tablas a CSV + esquema, para construir el .accdb de Access.

Access no se puede fabricar desde Python en Linux, así que este
script prepara todo lo que el constructor de Java necesita: un CSV
por tabla (con encabezados, los nulos como campo vacío) y dos
archivos de esquema, uno legible para humanos (schema.json) y otro
plano para el programa (schema.txt). Los tipos se traducen así:

- INTEGER -> LONG (entero de 32 bits; los ids y conteos caben)
- REAL    -> DOUBLE
- BOOLEAN -> BOOLEAN (en SQLite vive como INTEGER 0/1; si la columna
  se declaró BOOLEAN se respeta, si no, queda como LONG y listo)
- resto   -> TEXT, con longitud = la cadena más larga vista (entre
  50 y 255, que es el máximo de un campo de texto corto en Access)

Dos entradas: ``dump`` lee una base SQLite; ``dump_tables`` recibe
las tablas ya armadas (listas de dicts), que es lo que usa el
proyecto Spark, que no tiene SQLite sino DataFrames.

Uso::

    python3 dump_access.py base.db access/
"""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path

__all__ = ["dump", "dump_tables"]


def _access_type(declared):
    """El tipo de SQLite traducido al tipo de Access."""
    declared = (declared or "").upper()
    if "INT" in declared:
        return "LONG"
    if "REAL" in declared or "FLOA" in declared or "DOUB" in declared:
        return "DOUBLE"
    if "BOOL" in declared:
        return "BOOLEAN"
    return "TEXT"


def _text_length(rows, idx):
    """La longitud del texto más largo de una columna (50..255)."""
    longest = max(
        (len(str(row[idx])) for row in rows if row[idx] is not None),
        default=0,
    )
    if longest > 255:
        print(
            f"aviso: hay textos de {longest} caracteres; se cortan a 255",
            file=sys.stderr,
        )
    return max(50, min(255, longest))


def dump_tables(tables, out_dir):
    """Escribe csv/<tabla>.csv, schema.txt y schema.json.

    ``tables`` es una lista de dicts con "name", "columns" (lista de
    dicts con "name" y "type", y "length" para TEXT) y "rows" (lista
    de tuplas, en el mismo orden que "columns").
    """
    out_dir = Path(out_dir)
    csv_dir = out_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    summary = {"tables": []}
    schema_lines = []
    for table in tables:
        name = table["name"]
        columns = table["columns"]
        rows = table["rows"]
        for idx, spec in enumerate(columns):
            if spec["type"] == "TEXT" and "length" not in spec:
                spec = dict(spec)
                spec["length"] = _text_length(rows, idx)
                columns[idx] = spec
        csv_path = csv_dir / f"{name}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow([c["name"] for c in columns])
            for row in rows:
                writer.writerow(
                    [
                        "" if value is None
                        else str(value)[:255]
                        if isinstance(value, str)
                        else value
                        for value in row
                    ]
                )
        schema_lines.append(f"TABLE|{name}")
        for spec in columns:
            length = spec.get("length", "")
            schema_lines.append(f"COL|{spec['name']}|{spec['type']}|{length}")
        summary["tables"].append(
            {"name": name, "rows": len(rows),
             "columns": columns, "csv": str(csv_path)}
        )
    (out_dir / "schema.txt").write_text(
        "\n".join(schema_lines) + "\n", encoding="utf-8"
    )
    (out_dir / "schema.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return summary


def dump(sqlite_path, out_dir):
    """Vuelca todas las tablas de una base SQLite. Devuelve el resumen."""
    sqlite_path = Path(sqlite_path)
    if not sqlite_path.exists():
        raise FileNotFoundError(f"no existe la base {sqlite_path}")
    tables = []
    with sqlite3.connect(sqlite_path) as conn:
        names = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
                " AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
            ).fetchall()
        ]
        for name in names:
            columns = [
                {"name": info[1], "type": _access_type(info[2])}
                for info in conn.execute(
                    f"PRAGMA table_info({name})"
                ).fetchall()
            ]
            rows = conn.execute(f"SELECT * FROM {name}").fetchall()
            tables.append({"name": name, "columns": columns, "rows": rows})
    summary = dump_tables(tables, out_dir)
    summary["database"] = str(sqlite_path)
    return summary


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2:
        print("uso: dump_access.py <base.sqlite> <dir_access>",
              file=sys.stderr)
        return 2
    summary = dump(argv[0], argv[1])
    for table in summary["tables"]:
        print(f"{table['name']}: {table['rows']} filas -> {table['csv']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
