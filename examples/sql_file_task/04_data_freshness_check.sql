-- Data Freshness Check
-- Creates an audit table and checks data freshness

-- Create or replace audit table with current timestamp
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.data_freshness_audit') AS
SELECT 
  'sample_table' as table_name,
  CURRENT_TIMESTAMP() as last_update_time;

-- Check freshness of the audit table
WITH latest_update AS (
  SELECT 
    table_name,
    MAX(last_update_time) as last_update_time,
    CURRENT_TIMESTAMP() as check_time,
    TIMESTAMPDIFF(HOUR, MAX(last_update_time), CURRENT_TIMESTAMP()) as hours_since_update
  FROM IDENTIFIER(:catalog || '.' || :schema || '.data_freshness_audit')
  GROUP BY table_name
)
SELECT 
  table_name,
  last_update_time,
  check_time,
  hours_since_update,
  CASE 
    WHEN hours_since_update > CAST(:max_hours AS INT) THEN 'STALE'
    WHEN hours_since_update IS NULL THEN 'NO_DATA'
    ELSE 'FRESH'
  END as freshness_status
FROM latest_update
