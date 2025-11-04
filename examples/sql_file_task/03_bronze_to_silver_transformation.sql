-- Sample Data Transformation
-- Transforms and cleanses sales data into a silver layer table

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.silver_sales') AS
  SELECT 
  order_id,
    customer_id,
  product_id,
  CAST(order_date AS DATE) as order_date,
  quantity,
  unit_price,
  revenue,
  UPPER(status) as status,
  processed_at,
    CURRENT_TIMESTAMP() as updated_at
FROM IDENTIFIER(:catalog || '.' || :schema || '.sample_sales_data')
WHERE order_id IS NOT NULL
    AND customer_id IS NOT NULL
  AND revenue > 0
