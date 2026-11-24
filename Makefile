# Makefile — atajos para no tener que acordarse de los comandos largos.
#
#   make venv            crea el venv del proyecto e instala dependencias
#   make test            corre todas las pruebas (Spark + referencia)
#   make test-reference  corre solo las pruebas de pandas (sin Java)
#   make run             corre el ETL con Spark (vía run_etl.sh)
#   make run-reference   corre el ETL con pandas (sin Java)
#   make excel           exporta las salidas a outputs/portfolio.xlsx
#                        (Excel con tablas dinámicas nativas)
#   make access          construye access/spark-credit-risk-etl.accdb
#                        (base real de Access, con Jackcess/Java)
#   make clean           borra salidas, logs y cachés

VENV ?= .venv
PYTHON := $(VENV)/bin/python
# En esta VM /tmp es un tmpfs de 512 MB y pip desempaquetaba ahí los
# wheels grandes (PySpark pesa ~300 MB) hasta llenarlo. Por eso el
# TMPDIR apunta a un directorio del disco grande.
export TMPDIR ?= $(CURDIR)/.pip-tmp

.PHONY: help venv install test test-reference run run-reference excel access clean

help:
	@grep -E '^[a-z-]+:' $(MAKEFILE_LIST) | sed 's/:.*# / — /;s/^/  make /'

venv:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements.txt
	@echo "Venv listo en $(VENV). Actívalo con: source $(VENV)/bin/activate"

install: venv

test:
	$(PYTHON) -m pytest tests -q

test-reference:
	$(PYTHON) -m pytest tests/test_reference_impl.py -q

run:
	./run_etl.sh

run-reference:
	./run_etl.sh --reference

excel:
	$(PYTHON) -m spark_credit_risk_etl.excel_export

access:
	./access/build_access.sh

clean:
	rm -rf outputs logs spark-tmp .pip-tmp .pytest_cache
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
