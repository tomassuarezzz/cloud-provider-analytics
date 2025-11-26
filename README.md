# Cloud Provider Analytics — Data Lakehouse

Proyecto de **Minería de Datos II**  
Autor: **Tomás Suárez**  

Objetivo: demostrar un flujo end-to-end mínimo:

Landing → Bronze → Silver → Gold → Serving (Cassandra / AstraDB)

usando **PySpark (Colab) + Structured Streaming**.

---

## 1. Estructura del repositorio

La estructura objetivo del repositorio es:


cloud-provider-analytics/
│
├── notebooks/
│   └── cloud_provider_pipeline.ipynb    # Notebook principal con PySpark
│
├── scripts/
│   └── cql_tables.cql                  # Script CQL para Cassandra / AstraDB
│
├── datalake/
│   ├── landing/                        # Datos crudos (CSV / JSONL)
│   ├── bronze/                         # Tablas normalizadas mínimas
│   ├── silver/                         # Datos limpios / enriquecidos
│   └── gold/                           # Marts analíticos (FinOps / NPS, etc.)
│
└── README.md
