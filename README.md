Cloud Provider Analytics — Data Lakehouse

Proyecto Final – Minería de Datos II
Autora: Agustina Nogueira
Tecnologías: PySpark, Structured Streaming, Delta-like Lakehouse, AstraDB (Cassandra)

Este proyecto implementa un pipeline end-to-end para un proveedor de servicios cloud ficticio.
El objetivo es construir un Data Lakehouse con las capas Landing → Bronze → Silver → Gold, incorporando ingesta batch + streaming, calidad de datos, curación, enriquecimiento y publicación final para consumo analítico.


🚀 Objetivos del proyecto

✔ Implementar un pipeline completo usando PySpark.

✔ Ingerir y procesar datos en JSONL y CSV.

✔ Diseñar un Data Lake con capas Bronze/Silver/Gold.

✔ Detectar anomalías de costos mediante Z-score, MAD y percentil 99.

✔ Construir marts analíticos orientados a FinOps.

✔ Publicar los resultados en una base NoSQL (AstraDB / Cassandra).

✔ Simular una ingesta continua con Structured Streaming.

📂 Estructura del repositorio


## 📂 Estructura del repositorio

```text
cloud-provider-analytics/
│
├── notebooks/
│   └── cloud_provider_pipeline.ipynb        # Pipeline ETL completo
│
├── scripts/
│   └── cassandra_schema.cql                # Tablas para AstraDB / Cassandra
│
├── cloud_provider_dataset/                 # Datos fuente (CSV, JSONL)
│
├── datalake/
│   ├── landing/                            # Llegada de archivos crudos
│   ├── bronze/                             # Normalización y calidad
│   ├── silver/                             # Datos limpios y enriquecidos
│   └── gold/                               # Marts analíticos
│
└── README.md
```




🛠 Tecnologías utilizadas

- Python 3.10 / PySpark 3.5+
- Structured Streaming
- Google Colab
- Cassandra / AstraDB
- Parquet
- Lakehouse pattern basado en Parquet

  🧱 Arquitectura del pipeline


  <img width="1900" height="1272" alt="image" src="https://github.com/user-attachments/assets/d7c525d7-bc6d-4fb2-81af-839e2b292dc2" />


1️⃣ Landing Layer

Se ingestan archivos crudos desde el repositorio:

- events_batch_1/*.jsonl
- events_batch_2/*.jsonl
- CSV maestros: customers, users, resources, billing, NPS, tickets, marketing.

También se configura un directorio dedicado para ingesta continua: datalake/landing/usage_events_stream/

2️⃣ Bronze Layer

- Normalización de tipos
- Agregado de columnas técnicas (ingest_ts, source_file)
- Validaciones de calidad
- Quarantine para eventos de uso con schema inválido
- Structured Streaming → Bronze para simular tráfico real
- Particionamiento por event_date
  

Tablas Bronze principales:

| Tabla                                              | Descripción                             |
| -------------------------------------------------- | --------------------------------------- |
| `usage_events`                                     | Eventos de uso válidos (stream + batch) |
| `quarantine_usage`                                 | Eventos rechazados con `invalid_reason` |
| `customers`, `users`, `resources`, `tickets`, etc. | Maestros normalizados                   |


3️⃣ Silver Layer

Se generan datasets limpios y enriquecidos:

- usage_enriched = eventos de uso + datos del cliente
- Derivados: resolución de tickets, totales mensuales, features temporales
- Conversión de unidades y costos

También se construye:
org_daily_usage_by_service dataset agregado derivado de usage_enriched

Con agregaciones por:
- Cliente
- Fecha
- Servicio

Incluye métricas:
- daily_cost_usd
- daily_genai_tokens
- daily_carbon_kg

Y features para anomalías:
- z_score_cost
- mad_cost, mad_score_cost
- p99_cost
- Flags: is_zscore_anomaly, is_mad_anomaly, is_p99_anomaly, is_any_anomaly

  4️⃣ Gold Layer

Se crean marts orientados al análisis:

⭐ finops_by_org_day
Incluye:
- costo diario por servicio
- uso de recursos
- almacenamiento
- compute
- tokens generados
- emisiones estimadas

⭐ cost_anomaly_mart
Contiene:
- costo diario por servicio
- features estadísticos
- banderas de anomalía
- metadata del cliente (industria, región, lifecycle)

5️⃣ Serving Layer (AstraDB / Cassandra)

Se dejan preparadas las celdas para:
- Crear tablas Cassandra
- Escribir tablas GOLD en AstraDB
- Validar lectura

Nota:
En Colab, el conector spark-cassandra-connector no está disponible por defecto.
El código es correcto, pero requiere ejecutarse en un entorno con el JAR configurado (cluster local o Databricks).

📊 Resultados principales

- Pipeline completo de ingesta → calidad → enriquecimiento → análisis.
- Identificación automática de anomalías de costos por cliente y servicio.
- Mart Gold orientado a FinOps, útil para dashboards y monitoreo de costos.
- Preparación para publicar en Cassandra como base transaccional rápida.

▶️ Cómo ejecutar este proyecto

1) Abrir notebooks/cloud_provider_pipeline.ipynb en Google Colab.
2) Ejecutar las celdas en orden desde arriba hacia abajo.
3) La sección de Serving requiere un entorno con el conector de Cassandra.

 📌 Licencia

Uso académico – Minería de Datos II.  

