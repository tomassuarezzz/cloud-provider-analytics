-- ============================================
--  Cloud Provider Analytics - Cassandra Schema
--  Autor: Tomás Suárez
-- ============================================

-- Crear keyspace (o usá el que te dé Astra)
CREATE KEYSPACE IF NOT EXISTS cloud_provider
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE cloud_provider;

-- ============================================
--  TABLA 1: Mart FinOps por organización + día
--  Grano: (org_id, service, event_date)
-- ============================================

CREATE TABLE IF NOT EXISTS finops_usage_by_org_day (
  org_id text,
  service text,
  event_date date,
  daily_cost_usd double,
  requests double,
  cpu_hours double,
  storage_gb_hours double,
  genai_tokens double,
  carbon_kg double,
  PRIMARY KEY ((org_id, service), event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

-- ============================================
--  TABLA 2 (opcional): NPS por organización
-- ============================================

CREATE TABLE IF NOT EXISTS nps_by_org (
  org_id text PRIMARY KEY,
  last_survey_date date,
  last_nps_score int,
  nps_bucket text
);

-- ============================================
-- Ejemplos de consultas 
-- ============================================

-- 1. Últimos días de FinOps para una organización
SELECT *
FROM cloud_provider.finops_usage_by_org_day
WHERE org_id = 'org_1swjckjl'
  AND service = 'compute'
LIMIT 30;

-- 2. Organizaciones con NPS bajo (detractores)
SELECT *
FROM cloud_provider.nps_by_org
WHERE nps_bucket = 'detractor'
LIMIT 50;
