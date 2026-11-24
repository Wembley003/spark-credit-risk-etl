# Base de Access del ETL de riesgo de crédito

El archivo `spark-credit-risk-etl.accdb` es una base de datos REAL
de Microsoft Access (formato ACE14, Access 2010 en adelante), con las
mismas cifras del ETL: 24 créditos, EAD total $17,955,000 y pérdida
esperada $358,595. Se construye en Linux con Jackcess (Java), sin
necesitar Access ni Windows.

## Qué trae

| Tabla        | Filas | Contenido                                                        |
|--------------|-------|------------------------------------------------------------------|
| cartera      | 24    | la cartera limpia, crédito por crédito, con pérdida esperada y bucket de PD |
| por_segmento | 4     | métricas por segmento (Hipotecario, PyME, Consumo, Tarjeta)      |
| por_estatus  | 2     | exposición y pérdida esperada por vigente/vencido                |
| buckets_pd   | 4     | exposición y pérdida esperada por tramo de PD                    |

## Cómo regenerarlo

```bash
./access/build_access.sh            # construye spark-credit-risk-etl.accdb
# o
make access
```

El script hace cuatro cosas: (1) vuelca la cartera a CSV con
`dump_from_outputs.py` (pandas sobre `data/portfolio_sample.csv`),
(2) descarga los jars de Jackcess a `access/lib/` si no están,
(3) compila `AccessBuilder.java` y construye el `.accdb`, y
(4) verifica con mdbtools que cada tabla tenga las filas que dice
su CSV. Necesita Java 17, curl y conexión la primera vez (para los
jars); después, todo es local.

## Cómo abrirlo en Access

Copia el `.accdb` a Windows y ábrelo con Microsoft Access: las
cuatro tablas aparecen en el panel de navegación, listas para
consultar, sin importar nada a mano. En Access se pueden armar
consultas y formularios sobre ellas como sobre cualquier tabla
nativa.

## Una nota honesta sobre GitHub

El `.accdb` es binario, y el push por lotes de GitHub que usa este
proyecto solo lleva archivos de texto: el `.accdb` NO se puede subir
por esa vía. La forma de publicarlo es subirlo por la web de GitHub
desde el celular, como archivo suelto. Lo que SÍ va por el push de
texto es el pack de importación: los CSV de `access/csv/` y los dos
archivos de esquema (`schema.txt` y `schema.json`), que describen
cada tabla, sus columnas y sus tipos. Con esos archivos, cualquiera
puede reconstruir la base en Access con el asistente de importación
de texto, o regenerar el `.accdb` corriendo `build_access.sh`.

Los directorios `access/lib/` (los jars descargados) y
`access/build/` (las clases compiladas) no van a Git: se regeneran
solos al correr el script.
