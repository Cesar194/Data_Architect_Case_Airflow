# Data_Architect_Test
Data Architect Test in GCP

# Diseño del Pipeline de Ingesta de Ventas

## 1. Resumen de la Arquitectura

El pipeline propuesto sigue un patrón de ETL orquestado, utilizando servicios gestionados de GCP para maximizar la escalabilidad, la fiabilidad y minimizar la sobrecarga operativa.

**Componentes Principales:**
* **Orquestación:** Cloud Composer (Airflow)
* **Procesamiento de Datos:** Dataflow (Apache Beam)
* **Almacenamiento de Datos Crudos:** Google Cloud Storage (GCS)
* **Almacenamiento de Datos Analíticos:** BigQuery

**Diagrama de Flujo:**

(Cloud Scheduler) -> [Cloud Composer DAG] -> (Inicia Job de Dataflow)
                                            |
                                            v
                                 [Pipeline de Dataflow]
                                /                      \
      Lee CSV desde GCS <--+                            +--> Escribe a BigQuery
(gs://retail-data-zone/...)   |                            |   (retail.analytics_sales_usd)
                              +-- Enriquece con datos --+
                                  de BigQuery
                                  (retail.ref_exchange_rates)


Arquitectura Imagen: https://drive.google.com/file/d/1A69qYxrGojpNSV4KMzIgPFjryV12RU60/view?usp=sharing

Dataflow Job Details: https://drive.google.com/file/d/1lBSa5_zM6-SantKTo42n8JBgywBKuIyI/view?usp=sharing

## 2. Justificación de Herramientas

* **Dataflow (Apache Beam):** Se eligió por ser la herramienta ETL serverless y nativa de GCP. Proporciona auto-scaling, lo que significa que GCP gestiona los recursos computacionales necesarios según la carga de trabajo. Apache Beam, con su SDK de Python, ofrece un modelo de programación unificado tanto para batch como para streaming y es ideal para transformaciones a nivel de fila y enriquecimientos complejos, como el join condicional requerido en este caso.

* **Cloud Composer (Airflow):** Es el estándar de la industria para la orquestación de flujos de trabajo. Se utiliza para programar, monitorear y gestionar el ciclo de vida del pipeline. El DAG manejará la ejecución diaria del job de Dataflow, pasando la fecha de ejecución como parámetro. Esto permite reprocesar días específicos fácilmente y manejar dependencias complejas si el pipeline crece en el futuro.

* **BigQuery:** Su arquitectura serverless y su capacidad para manejar petabytes de datos lo hacen el destino ideal. La tabla destino será **particionada por `sale_date`**, una optimización clave que reduce los costos de consulta y mejora el rendimiento al escanear solo las particiones relevantes. Adicionalmente, se agregarán **clusters** para mejorar aún más el rendimiento de los filtros y agregaciones comunes.

## 3. Lógica del Pipeline

1.  **Disparador (Trigger):** Un DAG en Cloud Composer se programa para ejecutarse diariamente (ej. a las 02:00 AM). El DAG utiliza la fecha de ejecución lógica para determinar qué archivo CSV procesar (ej. para la ejecución del 2025-05-03, procesará el archivo `gs://retail-data-zone/sales/2025-05-02.csv`).

2.  **Extracción (Extract):** El pipeline de Beam, ejecutado por Dataflow, lee el archivo CSV correspondiente del bucket de GCS.

3.  **Enriquecimiento y Transformación (Transform):**
    * **Side Input:** La tabla `retail.ref_exchange_rates` se carga desde BigQuery y se utiliza como un **Side Input** en Beam. Este patrón es altamente eficiente, ya que distribuye la tabla de referencia (que se asume es pequeña en comparación con los datos de ventas) a cada uno de los workers de Dataflow.
    * **Lógica de Join:** Para cada registro de venta leído del CSV, el pipeline itera sobre la lista de tasas de cambio (el side input) para encontrar la tasa correcta. La condición es que la `currency` del registro de venta coincida y que la `sale_date` se encuentre entre `valid_from` y `valid_to`.
    * **Cálculo:** Se calcula el nuevo campo `amount_usd` utilizando la fórmula: `quantity * unit_price * rate_to_usd`.
    * **Limpieza:** Se realiza una conversión de tipos de datos (ej. `sale_date` a formato `DATE`, `quantity` a `INTEGER`, precios a `FLOAT`).

4.  **Carga (Load):** Los registros transformados y enriquecidos se cargan en la tabla destino `retail.analytics_sales_usd` en BigQuery. El conector de BigQuery para Beam maneja la escritura de manera eficiente. El pipeline está configurado para **añadir** los nuevos datos (`WRITE_APPEND`), lo cual es apropiado para una carga diaria.

## 4. Pasos de Despliegue en GCP (para el evaluador):

1. Configurar Proyecto GCP: Abre una Cloud Shell o usa gcloud CLI. Ejecuta gcloud config set project TU_PROYECTO_ID.
2. Crear Bucket de GCS: gsutil mb -p TU_PROYECTO_ID gs://retail-data-zone/
3. Crear Dataset en BigQuery: bq mk --dataset TU_PROYECTO_ID:retail
4. Crear Tablas en BigQuery: Ejecuta el contenido de los dos archivos SQL DDL provistos en este repositorio en la consola de BigQuery UI o usando bq query < create_table.sql. Deberian quedar estructuradas de la siguiente manera `ref_exchange_rates`(https://drive.google.com/file/d/1CwqP2MD5weeI0SXXSVU4ZEXKRiNcUwdN/view?usp=sharing) y `analytics_sales_usd` (https://drive.google.com/file/d/1s61oG3a4St7gxgYEEpouIYB2z9Z2bD51/view?usp=sharing)
5. Subir Datos de Ejemplo:
  - Crea un archivo local 2025-06-23.csv con el contenido del ejemplo. https://drive.google.com/file/d/16yBes_dBazHdBrbitalY8Y29EJ71fLIL/view?usp=sharing
  - Súbelo a GCS: gsutil cp 2025-06-22.csv gs://retail-data-zone/sales/2025-06-23.csv
  
**Configurar Cloud Composer:**

1. Crea un ambiente de Cloud Composer. https://drive.google.com/file/d/1oRsCApCYsZFInkeTwG_izzrmotgeDnVt/view?usp=sharing
2. Asi debe de quedar configurado los Pypi Packages: https://drive.google.com/file/d/1EqJ4DXDLe25UWyi3k4OQGLVaS0GEDBkc/view?usp=sharing
3. Sube el script beam_sales_pipeline.py al subdirectorio dags/ del bucket de Composer. 
4. Sube el DAG sales_dag.py al directorio dags/ del bucket de Composer. https://drive.google.com/file/d/19rmiF3O0SdCEx9sGtvf3fqPSt71rHLcp/view?usp=sharing
5. Activar y Ejecutar el DAG:
  * Abre la UI de Airflow desde la página de Composer.
  * Busca el DAG daily_sales_processing, actívalo y dispáralo manualmente para una fecha específica (ej. 2025-06-22) para probarlo.
  * Resultado final es que el Job de Dataflow deberia de ejecurtase sin problema, lo que puedes corroborar viendo los jobs de Dataflow haciendo click aqui https://console.cloud.google.com/dataflow/jobs y asi deberian de verse los jobs de dataflow que corran con exito https://drive.google.com/file/d/17lSZAXI-y06PtwfX7vtCW1C8QRFUGcna/view?usp=sharing. la tabla de bigquery `analytics_sales_usd` debe de actualizarse de la siguiente manera: https://drive.google.com/file/d/1vAHGlzmsnEIVUm5lPC8uFilmcYkIf015/view?usp=sharing

**Actualizar el Paquete PyPI en Cloud Composer (Recomendado):**

Para que los archivos python funcionen sin problema, mi recomendacion es actualizar el paquete apache-airflow-providers-google a una versión más reciente. Esto dará acceso a los operadores más nuevos, correcciones de errores y mejoras de rendimiento.

Puede hacerlo desde la consola de Cloud Composer UI o usando gcloud:

* Encuentra la versión actual: Ve a tu ambiente de Composer en la consola de GCP -> Pestaña "Paquetes PyPI" y busca apache-airflow-providers-google.
* Actualiza el paquete:
 - Vía UI: Haz clic en "Editar", busca el paquete y especifica una versión más reciente (ej. ==10.9.0).
 - Vía gcloud (ejemplo):

Bash

gcloud composer environments update TU-AMBIENTE-COMPOSER \
    --location TU-REGION \
    --update-pypi-package "apache-airflow-providers-google==10.9.0"
    
## 5. Consideraciones Adicionales

* **Idempotencia:** El pipeline está diseñado para ser idempotente. Si se vuelve a ejecutar para una fecha específica, se puede configurar para que sobrescriba la partición de ese día (`WRITE_TRUNCATE` en una partición específica), garantizando la consistencia de los datos. La implementación actual utiliza `WRITE_APPEND` para simplicidad.

* **NOTA:** Tener en cuenta que el Job esta seteado en Zona Horaria UTC, si trata de correr un archivo csv de un dia en especifico tomar en cuenta que debe de ser el csv del dia correspondiente en el DAG que se esta corriendo. Asi que asegurarse de tener en el bucket 'gs://retail-data-zone/sales/' el dia correspondiente; de lo contrario dara este error: " OSError: No files found based on the file pattern gs://retail-data-zone/sales/2025-06-22.csv". Por ejemplo el archivo `2025-06-22.csv` que se encuentra en este repositorio se corrio temprano el dia 22 de junio 2025 y luego resulto con el error anterior porque ya en la zona horaria mencionada ya habia transcurrido al dia 23 de junio 2025, por lo que para solucionar el error tube que actualizar mi archivo `2025-06-22.csv` a `2025-06-23.csv` para que el pipeline funcionara sin problema. 

  
* **Manejo de Errores:** Tanto Airflow como Dataflow tienen mecanismos robustos para reintentos y alertas. Se pueden configurar alertas de Cloud Monitoring para notificar si un pipeline falla.
* **Alternativa (BigQuery SQL puro):** Una solución más simple podría usar solo Airflow y SQL de BigQuery. Esto implicaría cargar el CSV a una tabla temporal en BigQuery y luego ejecutar una consulta SQL para hacer el `JOIN` y la transformación. Si bien es menos flexible, puede ser más rentable para transformaciones que se pueden expresar completamente en SQL. La solución con Dataflow es más escalable y versátil si las reglas de negocio se vuelven más complejas en el futuro.
  
