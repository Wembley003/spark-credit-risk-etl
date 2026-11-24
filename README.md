# ETL de riesgo de crédito con PySpark

Un pipeline ETL chico y honesto: lee una cartera de créditos, revisa
que los datos tengan sentido, calcula las métricas de riesgo que un
analista miraría cada mañana y las deja guardadas en Parquet y en CSV.
Corre en modo local, sin clúster, y trae una implementación gemela en
pandas para que los números se puedan verificar por dos caminos
distintos.

## El problema que resuelve

Una cartera de crédito es, en el fondo, un sistema de muchas partes:
cada crédito tiene una exposición (EAD), una probabilidad de impago
(PD) y una pérdida dado el impago (LGD). Lo que importa no es ningún
crédito aislado, sino los promedios pesados del conjunto: cuánto dinero
hay expuesto en total, cuánta pérdida se espera en promedio y dónde se
concentra el riesgo. Eso es lo que calcula este ETL, todas las mañanas
si se programa, sin intervención manual.

## Datos

`data/portfolio_sample.csv` es la misma cartera de ejemplo del
proyecto `credit-risk-agent-adk` (24 créditos en 4 segmentos:
Hipotecario, PyME, Consumo y Tarjeta). No es un invento nuevo: es la
cartera con la que ya se verificó el agente, así que las cifras tienen
que coincidir con las suyas — EAD total $17,955,000 y pérdida esperada
$358,595 — y si no coinciden, algo está mal en el ETL. Esa es la
gracia de reutilizar los mismos datos: hay un número contra el que
contrastar.

| Columna     | Qué es                                                     |
|-------------|------------------------------------------------------------|
| id_credito  | identificador del crédito                                  |
| segmento    | Hipotecario, PyME, Consumo o Tarjeta                       |
| ead         | exposición en caso de impago, en pesos                     |
| pd          | probabilidad de impago (0 a 1)                             |
| lgd         | pérdida dado el impago, como fracción (0 a 1)              |
| estatus     | vigente o vencido                                          |

## Arquitectura por capas (Bronce → Plata → Oro)

El pipeline está organizado en tres capas, como un yacimiento
estratificado: cada capa es un estado de los datos y solo se llega a
la siguiente pasando por la anterior. La regla es simple — una capa
no lee nunca de una capa superior, solo de la inmediata inferior.

```
data/portfolio_sample.csv
        │  to_bronze: lectura con esquema fijo, sin tocar nada
        ▼
┌─ BRONCE ─────────────────────────────────────────────┐
│ outputs/bronze/cartera_cruda     (Parquet + CSV)     │
│ Los datos tal como llegan. Si una fila viene rota,   │
│ aquí se guarda igual: el Bronce preserva, no juzga.  │
└───────────────────────────┬────────────────────────────┘
                            │  to_silver: validación (la compuerta)
                            │  si falla, se aborta (código 3) y no se
                            │  escribe nada en Plata ni en Oro
                            ▼
┌─ PLATA ──────────────────────────────────────────────┐
│ outputs/silver/cartera_limpia    (Parquet + CSV)     │
│ Limpios y validados: sin nulos, PD/LGD en [0,1],     │
│ EAD > 0, estatus conocidos, sin duplicados. Además    │
│ enriquecidos: perdida_esperada = EAD x PD x LGD y    │
│ buckets de PD.                                        │
└───────────────────────────┬────────────────────────────┘
                            │  to_gold: agregaciones
                            ▼
┌─ ORO ────────────────────────────────────────────────┐
│ outputs/gold/resumen_cartera     (una fila: todo)    │
│ outputs/gold/resumen_segmento    (por segmento)      │
│ outputs/gold/resumen_estatus     (vigente / vencido) │
│ outputs/gold/resumen_buckets_pd  (por bucket de PD)  │
│ Listos para el negocio: es lo que leen el tablero,   │
│ el Excel (outputs/portfolio.xlsx) y Access.          │
└──────────────────────────────────────────────────────┘
```

La implementación gemela en pandas (`reference_impl.py`) refleja
las mismas tres capas — `to_bronze`, `to_silver`, `to_gold` — con
las mismas reglas y los mismos archivos de salida, así que los
números de Spark y de pandas se pueden comparar capa por capa.

## Las tres capas del proyecto

Bronce → Plata → Oro es el flujo de los datos; estas tres capas son
la arquitectura del proyecto (y son las que mandan sobre quién puede
llamar a quién):

- **Capa del cliente (presentación)** — `cli.py`, `run_etl.py`,
  `run_etl.sh`, `Makefile`: lo que el usuario corre. No toca el
  almacenamiento directo ni los formatos de salida.
- **Capa de reglas de negocio** — `etl.py`, `reference_impl.py`:
  la validación, el enriquecimiento (pérdida esperada = EAD x PD x
  LGD, buckets de PD) y los agregados Oro. No depende del formato de
  salida: las reglas corren igual sin Excel ni SQLite (hay una prueba
  que lo comprueba con esas librerías bloqueadas).
- **Capa de consultas o análisis** — las agregaciones Oro expresadas
  como consultas Spark SQL / DataFrame, más `excel_export.py`,
  `pivot_table.py` y `access/`: lo de Oro, preguntado y exportado.

Las pruebas de `tests/test_layers.py` verifican que las capas no se
mezclen (análisis estático de imports) además del contrato
Bronce → Plata → Oro.

La pérdida esperada es el valor esperado de la pérdida, en el sentido
literal de la probabilidad: si el crédito impaga (probabilidad PD), se
pierde la fracción LGD de lo expuesto (EAD). Y la PD ponderada no es el
promedio de las PD: cada crédito pesa lo que pesa su exposición, como
un centro de masa.

## Cómo correrlo

Requisitos: Python 3.10+, Java 11 o 17 (Spark lo necesita) y las
dependencias de `requirements.txt`.

```bash
make venv            # crea .venv e instala dependencias
make test            # corre todas las pruebas
./run_etl.sh         # corre el ETL con Spark (usa spark-submit si existe)
make run-reference   # lo mismo, pero con pandas (sin Java)
```

Otras formas, todas equivalentes:

```bash
python -m spark_credit_risk_etl.cli --input data/portfolio_sample.csv
spark-submit run_etl.py --input data/portfolio_sample.csv --master local[2]
python -m spark_credit_risk_etl.cli --input data/otra.csv --output-dir outputs/ene --reference
```

Códigos de salida: 0 = bien, 2 = argumentos mal, 3 = la cartera no
pasó la validación, 1 = cualquier otro error. El `run_etl.sh` guarda
un log por corrida en `logs/` y devuelve el mismo código que el ETL.

## Excel con tablas dinámicas y Access

El ETL deja Parquet y CSV, que son para máquinas y para mirar. Para
el analista que trabaja en Excel o en Access hay dos exportaciones
más, con las mismas cifras (EAD $17,955,000, pérdida esperada
$358,595):

- **Excel** (`outputs/portfolio.xlsx`, se genera con `make excel`):
  las hojas "Cartera" (los 24 créditos con su bucket de PD),
  "Por segmento", "Por estatus" y "Buckets PD", más tres tablas
  dinámicas NATIVAS sobre la cartera — por segmento, por estatus y
  por bucket de PD — que Excel y LibreOffice dibujan con los valores
  correctos sin tener que recalcular nada a mano. El código está en
  `spark_credit_risk_etl/excel_export.py` y el motor de las tablas
  en `spark_credit_risk_etl/pivot_table.py` (copia verbatim de la
  receta probada en `../_recipes/`). Si no hay salidas del ETL, el
  programa lo dice y sale con código 2: primero `make run`.
- **Access** (`access/spark-credit-risk-etl.accdb`, se genera con
  `make access` o `./access/build_access.sh`): una base REAL de
  Microsoft Access (formato ACE14, construida con Jackcess en Java)
  con las tablas `cartera` (24 filas), `por_segmento` (4),
  `por_estatus` (2) y `buckets_pd` (4). El detalle, incluido por qué
  el `.accdb` binario no puede subir por el push por lotes de GitHub
  (eso va por la web, desde el celular) y qué archivos sí van por
  texto, está en `access/README.md`.

## La implementación gemela (pandas)

` spark_credit_risk_etl/reference_impl.py` hace exactamente lo mismo
con pandas, sin JVM. Existe por dos motivos: corre en máquinas sin
Java, y — más importante — los tests comparan los números de Spark
contra los de pandas. Si dos motores distintos dan lo mismo, el
cálculo es el que es y no un accidente de una librería.

## Automatización (Linux)

En `automation/` hay dos formas de programar el ETL diario, con el
mismo comando:

- **cron** (`cron.example`): `0 6 * * 1-5 .../run_etl.sh`, de lunes a
  viernes a las 06:00. Se instala con `crontab -e`.
- **systemd timer** (`spark-credit-risk-etl.service` +
  `spark-credit-risk-etl.timer`): lo mismo, con `Persistent=true`
  (si la máquina estaba apagada a las 06:00, corre al encenderse).
  La sintaxis de ambos archivos se verificó con
  `systemd-analyze verify`.

## Una limitación honesta de esta máquina

En la VM donde se desarrolló esto, el puente Python↔JVM de PySpark
(el que usan `createDataFrame` y `toPandas`) se cae: el demonio de
Python acepta la conexión y la cierra de inmediato
(`SocketException: Connection reset / Broken pipe`). El Spark del lado
de la JVM funciona bien — lo probé con `spark.range` y con el ETL
completo — así que el pipeline está diseñado para no necesitar ese
puente: lee CSV, transforma con SQL y escribe Parquet, todo del lado
de la JVM, y los resultados vuelven a Python con `collect()`. En una
máquina normal ese puente funciona; aquí simplemente no hace falta.

## Pruebas

```bash
make test             # 34 pruebas: 9 de Spark + 6 de la referencia
                      # + 5 de la exportación a Excel + 3 de Access
                      # + 8 de Bronce/Plata/Oro + 3 de las tres capas
make test-reference   # solo pandas, sin Java ni Spark
```

Las pruebas verifican que la muestra se lea (24 créditos), que la
validación atrape PD fuera de rango e ids duplicados, que las cifras
sean exactas (EAD $17,955,000, pérdida esperada $358,595, morosidad
31.30%), que los segmentos sumen al total y que Spark y pandas den
lo mismo. También que `run_etl` aborte con datos rotos en vez de
inventar números, y que cada capa cumpla su contrato: el Bronce
preserva lo crudo sin juzgarlo, la Plata no deja pasar filas
inválidas y el Oro reproduce las cifras conocidas.

## Qué demuestra este proyecto

- **Arquitectura por capas (Bronce → Plata → Oro)**: lo crudo se
  preserva, lo limpio se valida y lo agregado está listo para el
  negocio; cada capa solo lee de la inmediata inferior.
- **PySpark (SQL/DataFrame API)**: lectura con esquema explícito,
  validación, agregaciones y escritura en Parquet/CSV.
- **SQL**: las métricas son agregaciones (sumas ponderadas, `groupBy`)
  que en un motor SQL se escribirían igual.
- **Linux**: orquestación en bash (`run_etl.sh`: argumentos, preflight,
  logging, códigos de salida), Makefile y automatización con cron y
  systemd timer.

## Limitaciones

- Es un ETL por lotes sobre un CSV chico (24 créditos de ejemplo), no
  un pipeline en streaming ni un clúster.
- No hay modelo de PD ni de LGD: las probabilidades vienen dadas en
  el archivo. Calcularlas sería otro proyecto.
- El ETL no inventa datos: si la cartera no pasa la validación,
  aborta.

## Autor

Wembley Emmanuel Ibarra Tepato — noviembre 2026.

## Licencia

MIT.
