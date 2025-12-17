# Cloud Provider Analytics — Entrega Final

**Materia:** Minería de Datos II  
**Universidad:** ISTEA
**Docente:** Diego Mosquera


## Alumno
Tomas Suarez

## Repositorio
- https://github.com/tomassuarezzz/cloud-provider-analytics

## Notebooks / Entregables
- `cloud_provider_analytics.ipynb`
- `Entrega_Final_Proyecto_Cloud_Provider_Analytics.md`
- Link video: <>


---

## 1) Resumen Ejecutivo

Resumen Ejecutivo

Este proyecto presenta el diseño e implementación de un pipeline analítico end-to-end para un proveedor de servicios cloud ficticio, desarrollado como trabajo final de la materia Minería de Datos II.

El objetivo principal es ingestar, limpiar, enriquecer y publicar datos provenientes de múltiples dominios (uso de servicios, clientes, facturación, soporte, marketing y GenAI), combinando ingesta batch y near real-time, y estructurándolos bajo un Data Lakehouse con capas Landing, Bronze, Silver y Gold.

La solución fue implementada utilizando PySpark en Google Colab, con almacenamiento intermedio en Parquet, y un diseño de marts analíticos preparados para ser servidos en AstraDB (Cassandra) siguiendo un enfoque query-first.
El pipeline permite:

Procesar eventos de uso en casi tiempo real mediante Structured Streaming.

Aplicar reglas de calidad de datos y aislar registros inválidos en una capa de quarantine.

Enriquecer los eventos con información de clientes, recursos y contexto organizacional.

Detectar anomalías de costos utilizando métodos estadísticos (percentil 99, z-score y MAD).

Construir métricas orientadas a FinOps, Soporte y GenAI, listas para ser consumidas por herramientas de visualización.

El resultado final es un sistema reproducible, modular y extensible, que demuestra el uso práctico de técnicas de ingeniería de datos y analítica vistas durante la cursada, simulando un escenario real de explotación de datos en la industria cloud.

---

## 2) Arquitectura final

### 2.1 Diagrama de alto nivel (end-to-end)

Diagrama: https://ibb.co/8DX77tc9

Flujo: Landing → Bronze → Silver → Gold → Serving (AstraDB/Cassandra)

Landing: archivos crudos (CSV/JSONL) tal como llegan desde el dataset.

Bronze: persistencia en Parquet con tipificación mínima + columnas técnicas (ingest_ts, source_file) y quarantine para registros inválidos.

Silver: datos integrados/enriquecidos (joins entre usage + customers + resources/users) + features de análisis (métricas derivadas, tokens GenAI, carbono, etc.).

Gold: marts analíticos agregados por caso de uso (FinOps, tickets, GenAI, anomalías).

Serving (AstraDB/Cassandra): tablas modeladas “query-first” para consultas rápidas por org_id y fecha.

### 2.2 Patrón elegido (Lambda / Kappa) y justificación

Se adopta un enfoque Lambda:

Batch para datasets de baja frecuencia (customers, users, resources, billing, support, marketing, nps).

Streaming (Structured Streaming) para eventos de uso (usage_events_stream), permitiendo ingestión casi en tiempo real.

Justificación técnica: el uso requiere actualización continua, mientras que maestros/billing cambian menos; Lambda simplifica combinando ambas entradas sin complejizar el diseño.

### 2.3 Decisiones clave

Formato: Parquet en Bronze/Silver/Gold (eficiencia de lectura, compresión, compatibilidad Spark).

Particionado Bronze (usage): por event_date y service para acelerar filtros por fecha/servicio.

Quarantine: separación explícita de registros inválidos (ej. event_id nulo, event_ts nulo, costo negativo, unit faltante con value_num).

Deduplicación streaming: watermark + dropDuplicates(event_id) para evitar duplicados con llegada tardía.

Gold FinOps: agregación por org_id + event_date + service con métricas (costo diario, requests, cpu_hours, storage_gb_hours, genai_tokens, carbon_kg).

AstraDB (Serving): tablas diseñadas para consultas por organización y rango temporal (claves por org_id y usage_date/event_date).

### 2.4 Componentes y dependencias

Ejecución: Google Colab + PySpark.

Storage: filesystem del repo (datalake/landing|bronze|silver|gold).

Streaming: Spark Structured Streaming (archivos JSONL como fuente).

Serving: AstraDB (Cassandra) mediante Spark Cassandra Connector (cuando se ejecuta con el jar/paquete correspondiente + secure connect bundle).

---

## 3) Datos y Supuestos

### 3.1 Datasets de landing

Se utilizan datasets sintéticos provistos por el profesor, ubicados en la carpeta cloud_provider_dataset/, que simulan la operación de un proveedor de cloud.

Eventos de uso (JSONL)

Rutas:

events_batch_1/*.jsonl

events_batch_2/*.jsonl

Streaming: landing/usage_events_stream/

Grano: 1 fila = 1 evento de uso

Representan consumo de servicios cloud (compute, storage, genai, networking, etc.).

Customers / Organizations (CSV)

Archivo: customers_orgs.csv

Información maestra de las organizaciones clientes (industria, región, lifecycle).

Users (CSV)

Archivo: users.csv

Usuarios asociados a las organizaciones (usado como dato auxiliar).

Resources (CSV)

Archivo: resources.csv

Catálogo de recursos cloud consumidos.

Billing mensual (CSV)

Archivo: billing_monthly.csv

Costos mensuales por organización y moneda.

Soporte, Marketing y NPS (CSV)

support_tickets.csv

marketing_touches.csv

nps_surveys.csv

Usados para enriquecer análisis operativos y de experiencia del cliente.

### 3.2 Supuestos y normalizaciones

Timezones

Los timestamps se interpretan en UTC y se normalizan a timestamp Spark.

Se deriva event_date como fecha de partición.

Normalización de tipos

timestamp → event_ts (timestamp)

value (string) → value_num (double)

schema_version nulo → 1

cost_usd_increment nulo → 0.0

Calidad de datos (quarantine)
Un evento se considera inválido si:

event_id es nulo

event_ts es nulo

cost_usd_increment < -0.01

value_num no es nulo y unit es nula

Los eventos inválidos se almacenan en Bronze Quarantine.

Idempotencia

event_id se usa como clave única para deduplicación en streaming.

Costos

En billing mensual:
total_cost_usd = (subtotal - credits + taxes) * exchange_rate_to_usd

En eventos de uso:
cost_usd_increment representa el costo incremental por evento.

GenAI

Se consideran eventos GenAI aquellos con genai_tokens > 0.

El costo GenAI se aproxima sumando cost_usd_increment de dichos eventos.

### 3.3 Evolución de esquema y compatibilización

Se soporta evolución de esquema mediante el campo schema_version.

Campos nuevos (genai_tokens, carbon_kg) pueden no existir en versiones anteriores.

Se utiliza coalesce para compatibilizar esquemas históricos sin romper el pipeline.

El diseño sigue un enfoque schema-on-read, manteniendo flexibilidad ante cambios futuros.

Diccionario de datos clave

| Campo              | Dataset      | Tipo      | Descripción                      | Observaciones                 |
| ------------------ | ------------ | --------- | -------------------------------- | ----------------------------- |
| event_id           | usage_events | string    | Identificador único del evento   | Usado para deduplicación      |
| event_ts           | usage_events | timestamp | Timestamp del evento             | Normalizado a UTC             |
| event_date         | usage_events | date      | Fecha derivada del evento        | Usada para partición          |
| org_id             | varios       | string    | Identificador de la organización | Clave de joins                |
| service            | usage_events | string    | Servicio cloud consumido         | compute, storage, genai, etc. |
| metric             | usage_events | string    | Métrica reportada                | requests, cpu_hours, etc.     |
| value_num          | usage_events | double    | Valor numérico de la métrica     | Derivado de `value`           |
| cost_usd_increment | usage_events | double    | Costo incremental del evento     | En USD                        |
| genai_tokens       | usage_events | long      | Tokens GenAI consumidos          | Puede ser nulo                |
| carbon_kg          | usage_events | double    | Huella de carbono estimada       | Opcional                      |
| schema_version     | usage_events | int       | Versión del esquema              | Default = 1                   |

---

## 4) Data Lake: Zonas y Particionado
4) Data Lake: Zonas y Particionado

El Data Lake se organiza siguiendo el patrón Landing → Bronze → Silver → Gold, utilizando formato Parquet y particionamiento por columnas de uso frecuente para optimizar consultas y costos.

### 4.1 Zona Bronze

Objetivo:
Conservar los datos lo más fieles posible a la fuente original, con tipificación básica y control de calidad inicial.

Características:

Tipificación explícita (strings, doubles, timestamps).

Agregado de columnas técnicas:

ingest_ts: timestamp de ingesta.

source_file: origen del dato.

Dedupe por event_id (en eventos de uso).

Separación de datos inválidos en Bronze Quarantine.

Ejemplos de datasets Bronze:

bronze/usage_events/

bronze/quarantine_usage/

bronze/customers/

bronze/users/

bronze/billing_monthly/

bronze/support_tickets/

Particionado (usage_events):

event_date

service

Ejemplo de ruta:

datalake/bronze/usage_events/
└── event_date=2025-08-01/
    └── service=compute/
        └── part-0000.snappy.parquet


### 4.2 Zona Silver

Objetivo:
Datos limpios, conformados y listos para análisis, manteniendo granularidad consistente.

Transformaciones aplicadas:

Normalización de tipos (value_num, event_ts, fechas).

Manejo de nulos y valores extremos.

Compatibilización de esquemas mediante schema_version.

Enriquecimiento con dimensiones (customers).

Derivación de métricas (costos, tokens GenAI, carbono).

Ejemplos de datasets Silver:

silver/usage_enriched

silver/billing_enriched

silver/support_tickets_enriched

silver/marketing_enriched

silver/nps_enriched

Ejemplo de ruta:

datalake/silver/usage_enriched/
├── part-00000.snappy.parquet
├── part-00001.snappy.parquet
└── _SUCCESS


### 4.3 Zona Gold

Objetivo:
Data marts optimizados para consumo analítico y reporting, agregados por dominio de negocio.

Marts implementados:

FinOps

gold/finops_by_org_day

Costos diarios por organización, servicio y métricas operativas.

GenAI

gold/genai_tokens_by_org_date

Tokens GenAI y costos asociados por organización y fecha.

Revenue

gold/revenue_by_org_month

Ingresos mensuales por organización.

Soporte

gold/tickets_by_org_date

Métricas de tickets, CSAT y SLA.

Ejemplo de ruta Gold:

datalake/gold/finops_by_org_day/
├── part-00000.snappy.parquet
└── _SUCCESS


### 4.4 Estrategia de Particionado y Formato

Formato: Parquet + Snappy (compresión).

Criterios de particionado:

event_date: reduce escaneo temporal.

service: segmenta por dominio de uso.

Beneficios:

Mejor performance en queries analíticas.

Menor costo de lectura.

Escalabilidad ante crecimiento de volumen.


### Imágenes: 
https://ibb.co/p6XhCcfk
https://ibb.co/p6XhCcfk
https://ibb.co/dJmFNS02
https://ibb.co/6RpLYbzb

---

## 5) Ingesta y Calidad de Datos
### 5.1 Ingesta Batch
La ingesta batch se realizó a partir de archivos CSV y JSON ubicados en la zona Landing.
Los datasets fueron leídos con PySpark, aplicando tipificación explícita y normalización básica, y luego persistidos en la zona Bronze en formato Parquet.

Principales características:

Lectura de datasets maestros (customers, users, billing_monthly, resources, etc.) y eventos históricos.

Conversión a Parquet para optimizar almacenamiento y consultas.

Inclusión de columnas técnicas como ingest_ts y source_file.

Eliminación de duplicados mediante claves naturales (por ejemplo org_id, billing_month).

Evidencia:

Código de lectura con spark.read.csv / json.

Escritura en Bronze con .write.mode("overwrite").parquet(...).

Conteos de filas y show() para validación.

### 5.2 Ingesta Streaming (Structured Streaming)
Para los eventos de uso (usage_events) se implementó una ingesta streaming utilizando Structured Streaming, simulando llegada continua de datos desde una carpeta landing.

Características implementadas:

Esquema explícito definido con StructType.

Parseo de timestamp (event_ts) y derivación de event_date.

Uso de watermark (withWatermark("event_ts", "2 days")) para manejo de late data.

Deduplicación por event_id mediante dropDuplicates.

Separación de eventos válidos e inválidos.

Escritura en Bronze en dos destinos:

usage_events (eventos válidos).

quarantine_usage (eventos inválidos).

Uso de checkpointing para garantizar idempotencia y tolerancia a fallos.

Checkpointing utilizado:

chk_usage_stream

chk_quarantine_stream

### 5.3 Reglas de Calidad Implementadas
Se implementaron reglas de calidad directamente en PySpark, aplicadas tanto en batch como en streaming.

Reglas principales:

event_id no nulo.

event_ts no nulo.

cost_usd_increment ≥ -0.01.

Si value_num es no nulo, unit debe estar presente.

Tipificación segura con cast + coalesce para evitar fallos.

Quarantine:

Los registros que no cumplen las reglas se etiquetan con un campo invalid_reason.

Se almacenan en una tabla Bronze separada (quarantine_usage) para análisis posterior.

Detección de anomalías:

En la capa Gold se generan métricas agregadas (FinOps y consumo).

Se preparó un mart de anomalías basado en costos diarios por organización y servicio.

La lógica permite extenderse a métodos como z-score, MAD o percentiles.

Evidencia:

Conteos de eventos válidos vs inválidos.

Ejemplos de registros en quarantine.

Resultados agregados en marts Gold (FinOps, GenAI).

---

## 6) Transformaciones (Silver)
La capa Silver consolida y estandariza la información proveniente de Bronze, aplicando normalización de tipos, enriquecimiento con dimensiones y el cálculo de métricas intermedias que luego alimentan los marts Gold.

Normalizaciones aplicadas

Se realizaron las siguientes transformaciones sobre los datasets de Bronze:

Fechas y timestamps

Conversión de strings a timestamp (event_ts, created_ts, resolved_ts).

Derivación de fechas (event_date, ticket_date, billing_month).

Tipos numéricos

Conversión segura con cast y coalesce para evitar valores nulos:

value_num

cost_usd_increment

carbon_kg

genai_tokens

nps_score, csat

Estandarización de campos

Regiones (org_region).

Servicios (service).

Métricas (metric).

Control de versión de esquema

Uso de schema_version con valor por defecto (1) para compatibilizar evolución de eventos.

Enriquecimiento (joins)

Los eventos de uso fueron enriquecidos con información organizacional proveniente del dataset de customers:

Join principal:

usage_events (válidos) ⟶ customers

Clave: org_id

Tipo: LEFT JOIN

Campos incorporados:

org_name

industry

org_region

lifecycle_stage

Este enriquecimiento permitió contextualizar el consumo técnico con información de negocio.

Features calculadas

En Silver y Gold intermedio se calcularon las siguientes métricas y features:

Costos

daily_cost_usd (suma diaria por org y servicio).

Uso de recursos

requests

cpu_hours

storage_gb_hours

GenAI

genai_tokens

Sustentabilidad

carbon_kg

Soporte

resolution_hours

sla_breached_bool

Clasificación NPS

nps_class (promoter / neutral / detractor).

Estas features se generan a partir de agregaciones, casts y expresiones condicionales (when).

Evidencias

Ejemplos de DataFrames Silver:

usage_enriched

billing_enriched

support_tickets_enriched

marketing_enriched

nps_enriched

Se verificaron:

Esquemas finales con printSchema().

Muestras de datos con show().

Persistencia correcta en formato Parquet dentro de la zona Silver.

---

## 7) Modelado Gold y Serving en **AstraDB (Cassandra)**
### 7.1 Diseño por consulta (query‑first)
7.1 Diseño por consulta (query-first)

El modelo en Cassandra se definió con enfoque query-first: primero se identificaron las consultas principales del dominio (FinOps / métricas de uso), y luego se diseñaron tablas desnormalizadas para responderlas con baja latencia (sin joins).

Caso de uso implementado y validado en Astra (FinOps):

Consultar el consumo/costo diario por organización, segmentado por servicio.

Filtrar por:

org_id (organización)

event_date (día)

service (servicio: compute, storage, analytics, database, networking, genai)

Consultas objetivo típicas:

“Dame el costo diario y requests/cpu/storage de una org en un rango de fechas”

“Dame el detalle por servicio para un día dado”

“Revisar si GenAI (tokens) tuvo consumo en un período”

Decisión de claves (partition + clustering):

Partition key: org_id
Motivo: el acceso más frecuente es por organización (dashboard / monitoreo por cliente).

Clustering: event_date, service
Motivo:

event_date permite ordenar y filtrar por rangos temporales dentro de la partición.

service permite ver el breakdown por servicio dentro de cada día.

Nota sobre tipos:

En Astra (CQL) event_date se maneja como date.

Métricas numéricas se guardan como double o bigint según corresponda.

### 7.2 Scripts CQL

A continuación se incluye el CQL para keyspace y tabla FinOps.
(Importante: en Astra Serverless/Managed el replication puede variar; en caso de conflicto, se usa el que Astra recomienda por defecto y se mantiene la tabla.)

```sql

-- KEYSPACE (ajustar replication según Astra si fuera necesario)
CREATE KEYSPACE IF NOT EXISTS cloud_provider
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- TABLA FINOPS: costos diarios por org y servicio
CREATE TABLE IF NOT EXISTS cloud_provider.finops_usage_by_org_day (
  org_id text,
  event_date date,
  service text,

  daily_cost_usd double,
  requests double,
  cpu_hours double,
  storage_gb_hours double,
  genai_tokens double,
  carbon_kg double,

  PRIMARY KEY ((org_id), event_date, service)
) WITH CLUSTERING ORDER BY (event_date ASC, service ASC);

```
Justificación del diseño:

((org_id), event_date, service) permite:

Consultar eficientemente por org.

Ordenar naturalmente por fecha.

Obtener el detalle por servicio para cada día.

### 7.3 Carga a Cassandra
Método utilizado:

La carga de datos se realizó desde los marts Gold generados en el Data Lake.

El enfoque propuesto (y estándar) para automatizar la carga es:

Spark Cassandra Connector (org.apache.spark.sql.cassandra) para escrituras batch, o

foreachBatch si se quisiera cargar incrementalmente desde streaming.

Nota sobre ejecución en Colab:

En entornos tipo Google Colab es común que el conector no esté disponible sin configurar jars/paquetes al iniciar la SparkSession.

Para la entrega, se incluyó el código de configuración y escritura, y la validación final se realizó directamente en AstraDB (Data Explorer), verificando que los registros están cargados en el keyspace/tablas.

Evidencias solicitadas (conteos / primeras filas):

Se verificó la tabla cloud_provider.finops_usage_by_org_day en Astra Data Explorer, observando registros con:

org_id

event_date

service

métricas (daily_cost_usd, requests, cpu_hours, storage_gb_hours, carbon_kg, genai_tokens)

https://ibb.co/C3ThjrFy

---

## 8) Idempotencia y Reprocesos
### 8.1 Estrategia de idempotencia

En Data Lake (Bronze/Silver/Gold):

Streaming (Bronze usage_events): idempotencia por deduplicación de eventos usando event_id como clave natural.

Se aplica withWatermark("event_ts", "2 days") + dropDuplicates(["event_id"]) para evitar duplicados y tolerar late data dentro de la ventana.

Checkpointing: se usan carpetas de checkpoint (chk_usage_stream, chk_quarantine_stream) para asegurar exactly-once semantics “prácticas” en Structured Streaming (reintentos / reinicio sin reprocesar lo ya confirmado).

Batch / capas Silver y Gold: las escrituras se hacen en general con .mode("overwrite") sobre paths de salida (Silver/Gold), lo que permite reprocesar desde cero y obtener siempre el mismo resultado para la misma entrada.

En Serving (AstraDB/Cassandra):

El modelo es query-first y se escribe por upsert (en Cassandra un INSERT con misma PK reemplaza/actualiza la fila).

La idempotencia depende de elegir correctamente la Primary Key (Partition + Clustering) para que la misma entidad/periodo escriba sobre la misma fila.

### 8.2 Demostración (re-ejecución sin duplicar)

Streaming:

Al re-ejecutar el streaming con los mismos checkpoints, Spark no vuelve a procesar archivos ya confirmados.

Además, aunque se “re-inyecten” archivos con el mismo contenido, dropDuplicates(event_id) evita duplicados a nivel evento (dentro del watermark).

Silver/Gold:

Re-ejecutar el notebook no duplica porque:

Silver/Gold se reconstruyen con mode("overwrite") (se reemplaza el dataset completo).

AstraDB:

Si se re-ejecuta la carga con la misma PK, Cassandra hace upsert (no duplica registros, los pisa).

p8_checkpoints_folders: https://ibb.co/BK4y7w0K

p8_bronze_counts_before:https://ibb.co/99P34kfH
p8_bronze_counts_after:https://ibb.co/4nJFqGCm

Los conteos se mantienen iguales al re-ejecutar con los mismos checkpoints → no hay duplicación.



### 8.3 Backfills y evolución de esquema

Backfills (histórico):

Se pueden re-cargar eventos históricos (batch) o re-procesar desde Bronze hacia Silver/Gold usando overwrite.

En streaming, para backfills grandes conviene procesarlos como batch hacia Bronze o resetear checkpoints (solo si se busca reprocesar completo).

Evolución de esquema:

En usage_events se contempla schema_version y se normalizan campos con coalesce/casts para compatibilizar:

schema_version nulo → 1

cost_usd_increment nulo → 0.0

value_num derivado desde value

Para campos nuevos (ej. genai_tokens, carbon_kg), se manejan como nullable y con defaults al agregar métricas agregadas.

p8_silver_gold_folders: https://ibb.co/tPNL4Rxs

p8_silver_gold_counts: https://ibb.co/qYv4d8YN

Silver/Gold se escriben con mode("overwrite"), por eso al reprocesar se reemplaza el dataset y no se duplican filas.

p8_astra_table_preview: https://ibb.co/JRS3M4zd

En Cassandra, los inserts sobre la misma PRIMARY KEY funcionan como upsert: si se re-carga la misma PK, se actualiza la fila en vez de duplicar.
La idempotencia depende de las claves (partition+clustering) definidas en las tablas.

---

## 9) Performance

### 9.1 Estrategias de joins y cache

Joins “dimensiones” chicas (customers/users/resources): se prioriza broadcast para evitar shuffle cuando el DataFrame de dimensiones es pequeño (lookup por org_id, resource_id, etc.).

Proyección temprana: se seleccionan solo las columnas necesarias antes del join (select), reduciendo ancho y costo de shuffle.

Cache/persist en puntos clave:

usage_enriched (Silver) se puede cachear si se usa en varios marts Gold (FinOps + GenAI).

df_customers (Bronze) se reutiliza en varios joins → cache opcional.

Particionado en escritura:

En Bronze streaming: partición por event_date y service, lo que mejora lecturas acotadas por fecha/servicio.

Dedupe + watermark: reduce eventos duplicados y trabajo innecesario en streaming (procesamiento incremental estable).

### 9.2 Métricas simples de tiempo/volumen (evidencias)

A) Tiempo de lectura y conteo (Silver / Gold) https://ibb.co/3yd6Sr6d

B) Plan de ejecución (para mostrar broadcast/shuffle) Pego la consulta y debajo el resultado.

''' from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# ejemplo típico: join de eventos con customers
bronze_customers_path = os.path.join(BRONZE_BASE, "customers")
df_customers = spark.read.parquet(bronze_customers_path).select("org_id","org_name","industry","org_region","lifecycle_stage")

df_join_test = df_silver.join(broadcast(df_customers), on="org_id", how="left")

df_join_test.explain(True) '''

Resultado:


== Parsed Logical Plan ==
'Join UsingJoin(LeftOuter, [org_id])
:- Relation [event_id#4255,event_ts#4256,event_date#4257,org_id#4258,org_name#4259,industry#4260,org_region#4261,lifecycle_stage#4262,resource_id#4263,service#4264,metric#4265,unit#4266,value#4267,value_num#4268,cost_usd_increment#4269,carbon_kg#4270,genai_tokens#4271L,schema_version#4272,source_file#4273] parquet
+- ResolvedHint (strategy=broadcast)
   +- Project [org_id#4329, org_name#4330, industry#4331, org_region#4332, lifecycle_stage#4337]
      +- Relation [org_id#4329,org_name#4330,industry#4331,org_region#4332,plan_tier#4333,is_enterprise#4334,signup_date#4335,sales_rep#4336,lifecycle_stage#4337,marketing_source#4338,nps_score#4339,ingest_ts#4340,source_file#4341] parquet

== Analyzed Logical Plan ==
org_id: string, event_id: string, event_ts: timestamp, event_date: date, org_name: string, industry: string, org_region: string, lifecycle_stage: string, resource_id: string, service: string, metric: string, unit: string, value: string, value_num: double, cost_usd_increment: double, carbon_kg: double, genai_tokens: bigint, schema_version: int, source_file: string, org_name: string, industry: string, org_region: string, lifecycle_stage: string
Project [org_id#4258, event_id#4255, event_ts#4256, event_date#4257, org_name#4259, industry#4260, org_region#4261, lifecycle_stage#4262, resource_id#4263, service#4264, metric#4265, unit#4266, value#4267, value_num#4268, cost_usd_increment#4269, carbon_kg#4270, genai_tokens#4271L, schema_version#4272, source_file#4273, org_name#4330, industry#4331, org_region#4332, lifecycle_stage#4337]
+- Join LeftOuter, (org_id#4258 = org_id#4329)
   :- Relation [event_id#4255,event_ts#4256,event_date#4257,org_id#4258,org_name#4259,industry#4260,org_region#4261,lifecycle_stage#4262,resource_id#4263,service#4264,metric#4265,unit#4266,value#4267,value_num#4268,cost_usd_increment#4269,carbon_kg#4270,genai_tokens#4271L,schema_version#4272,source_file#4273] parquet
   +- ResolvedHint (strategy=broadcast)
      +- Project [org_id#4329, org_name#4330, industry#4331, org_region#4332, lifecycle_stage#4337]
         +- Relation [org_id#4329,org_name#4330,industry#4331,org_region#4332,plan_tier#4333,is_enterprise#4334,signup_date#4335,sales_rep#4336,lifecycle_stage#4337,marketing_source#4338,nps_score#4339,ingest_ts#4340,source_file#4341] parquet

== Optimized Logical Plan ==
Project [org_id#4258, event_id#4255, event_ts#4256, event_date#4257, org_name#4259, industry#4260, org_region#4261, lifecycle_stage#4262, resource_id#4263, service#4264, metric#4265, unit#4266, value#4267, value_num#4268, cost_usd_increment#4269, carbon_kg#4270, genai_tokens#4271L, schema_version#4272, source_file#4273, org_name#4330, industry#4331, org_region#4332, lifecycle_stage#4337]
+- Join LeftOuter, (org_id#4258 = org_id#4329), rightHint=(strategy=broadcast)
   :- Relation [event_id#4255,event_ts#4256,event_date#4257,org_id#4258,org_name#4259,industry#4260,org_region#4261,lifecycle_stage#4262,resource_id#4263,service#4264,metric#4265,unit#4266,value#4267,value_num#4268,cost_usd_increment#4269,carbon_kg#4270,genai_tokens#4271L,schema_version#4272,source_file#4273] parquet
   +- Project [org_id#4329, org_name#4330, industry#4331, org_region#4332, lifecycle_stage#4337]
      +- Filter isnotnull(org_id#4329)
         +- Relation [org_id#4329,org_name#4330,industry#4331,org_region#4332,plan_tier#4333,is_enterprise#4334,signup_date#4335,sales_rep#4336,lifecycle_stage#4337,marketing_source#4338,nps_score#4339,ingest_ts#4340,source_file#4341] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [org_id#4258, event_id#4255, event_ts#4256, event_date#4257, org_name#4259, industry#4260, org_region#4261, lifecycle_stage#4262, resource_id#4263, service#4264, metric#4265, unit#4266, value#4267, value_num#4268, cost_usd_increment#4269, carbon_kg#4270, genai_tokens#4271L, schema_version#4272, source_file#4273, org_name#4330, industry#4331, org_region#4332, lifecycle_stage#4337]
   +- BroadcastHashJoin [org_id#4258], [org_id#4329], LeftOuter, BuildRight, false
      :- FileScan parquet [event_id#4255,event_ts#4256,event_date#4257,org_id#4258,org_name#4259,industry#4260,org_region#4261,lifecycle_stage#4262,resource_id#4263,service#4264,metric#4265,unit#4266,value#4267,value_num#4268,cost_usd_increment#4269,carbon_kg#4270,genai_tokens#4271L,schema_version#4272,source_file#4273] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/content/cloud-provider-analytics/datalake/silver/usage_enriched], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<event_id:string,event_ts:timestamp,event_date:date,org_id:string,org_name:string,industry:...
      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=8278]
         +- Filter isnotnull(org_id#4329)
            +- FileScan parquet [org_id#4329,org_name#4330,industry#4331,org_region#4332,lifecycle_stage#4337] Batched: true, DataFilters: [isnotnull(org_id#4329)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/content/cloud-provider-analytics/datalake/bronze/customers], PartitionFilters: [], PushedFilters: [IsNotNull(org_id)], ReadSchema: struct<org_id:string,org_name:string,industry:string,org_region:string,lifecycle_stage:string>




---

## 12) Resultados: **Consultas mínimas desde AstraDB**

### 12.1 
https://ibb.co/ymHf2nCW

### 12.2
https://ibb.co/8D7krhYR

### 12.3
Se ejecutó una consulta sobre la tabla tickets_by_org_day_severity en AstraDB para analizar la evolución diaria de tickets de alta severidad (high) y su tasa de incumplimiento de SLA para una organización específica.

https://ibb.co/JRkv725c



### 12.4 
https://ibb.co/Jw7sF8Hp

### 12.5
En el modelo Gold cargado en AstraDB (finops_usage_by_org_day) no se registran filas correspondientes a servicios GenAI.
Como evidencia, se ejecutó una consulta exploratoria sobre la tabla, observándose que:

El campo service solo contiene valores como analytics, storage, compute y database.

El campo genai_tokens se encuentra con valores NULL en todas las filas consultadas.

https://ibb.co/x858FnmJ

Dado que no existen eventos asociados a servicios GenAI en el dataset persistido en Cassandra, no es posible calcular la evolución diaria de tokens GenAI ni su costo estimado.
La consulta fue igualmente definida y ejecutada como validación del modelo y del pipeline end-to-end.

---

## 14) Video (≤ 20 min) — Guion sugerido
1. Problema y arquitectura (2–3 min).
2. Data Lake y calidad (3–4 min).
3. Demo **batch** + **streaming** (5–6 min).
4. Carga a Cassandra y consultas (5–6 min).
5. Cierre: decisiones, limitaciones y futuros (1–2 min).

---

## 15) Limitaciones y Trabajo Futuro

### Limitaciones

El entorno de ejecución en Google Colab impone restricciones de recursos (memoria y tiempo), por lo que el volumen de datos y la concurrencia del streaming fueron simulados y no representan una carga productiva real.

La ingesta en streaming se implementó con Structured Streaming sobre archivos, y no con una fuente real de eventos (Kafka / PubSub), lo que limita el análisis de escenarios de alta frecuencia.

El modelo en Cassandra fue diseñado para un conjunto acotado de consultas; consultas ad-hoc complejas requieren ALLOW FILTERING o rediseño de tablas.

La detección de anomalías utiliza métodos estadísticos simples (z-score, percentil, MAD), sin modelos avanzados de machine learning.

La integración con AstraDB se realizó a nivel de esquema y consultas; la carga automática desde Spark puede requerir ajustes adicionales de dependencias en un entorno productivo.

### Trabajo Futuro

Migrar la ingesta en streaming a una arquitectura basada en Kafka o Pub/Sub, con mayor volumen y latencia real.

Incorporar orquestación con Airflow o similar para gestionar dependencias, backfills y re-ejecuciones.

Implementar modelos predictivos para forecasting de costos, detección de anomalías basada en ML y churn prediction.

Agregar una capa de visualización (Superset, Looker, Power BI) conectada a Cassandra o a la capa Gold.

Optimizar el diseño de tablas en Cassandra para nuevos casos de uso y mayor escalabilidad.

Automatizar controles de calidad con frameworks como Great Expectations.

---



