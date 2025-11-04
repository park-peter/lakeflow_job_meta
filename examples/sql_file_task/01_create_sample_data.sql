-- Create Sample Data Table
-- Creates a sample table with test data for use in other SQL tasks

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.sample_sales_data') AS
SELECT 
  'ORD001' as order_id,
  'CUST001' as customer_id,
  'PROD001' as product_id,
  CURRENT_DATE() as order_date,
  10 as quantity,
  29.99 as unit_price,
  299.90 as revenue,
  'completed' as status,
  CURRENT_TIMESTAMP() as processed_at
UNION ALL
SELECT 
  'ORD002' as order_id,
  'CUST002' as customer_id,
  'PROD002' as product_id,
  CURRENT_DATE() as order_date,
  5 as quantity,
  49.99 as unit_price,
  249.95 as revenue,
  'completed' as status,
  CURRENT_TIMESTAMP() as processed_at
UNION ALL
SELECT 
  'ORD003' as order_id,
  'CUST001' as customer_id,
  'PROD003' as product_id,
  CURRENT_DATE() as order_date,
  2 as quantity,
  99.99 as unit_price,
  199.98 as revenue,
  'pending' as status,
  CURRENT_TIMESTAMP() as processed_at

