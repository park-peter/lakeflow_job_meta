-- Incremental Load Example
-- Demonstrates MERGE pattern for incremental data loading

-- Create source table with sample data if it doesn't exist
CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.sample_sales_data') AS
SELECT 
  'ORD001' as order_id,
  'CUST001' as customer_id,
  'PROD001' as product_id,
  CURRENT_DATE() as order_date,
  10 as quantity,
  29.99 as unit_price,
  299.90 as revenue,
  'completed' as status,
  CURRENT_TIMESTAMP() as processed_at;

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.silver_sales') (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  order_date DATE,
  quantity INT,
  unit_price DOUBLE,
  revenue DOUBLE,
  status STRING,
  processed_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- Perform incremental load using MERGE
MERGE INTO IDENTIFIER(:catalog || '.' || :schema || '.silver_sales') AS target
USING (
  SELECT 
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    revenue,
    status,
    processed_at,
    CURRENT_TIMESTAMP() as updated_at
  FROM IDENTIFIER(:catalog || '.' || :schema || '.sample_sales_data')
  WHERE processed_at >= COALESCE(
    (SELECT MAX(updated_at) FROM IDENTIFIER(:catalog || '.' || :schema || '.silver_sales')),
    TIMESTAMP('1970-01-01 00:00:00')  -- Fallback if no existing data
  )
) AS source
ON target.order_id = source.order_id
WHEN MATCHED AND target.updated_at < source.updated_at THEN
  UPDATE SET
    customer_id = source.customer_id,
    product_id = source.product_id,
    order_date = source.order_date,
    quantity = source.quantity,
    unit_price = source.unit_price,
    revenue = source.revenue,
    status = source.status,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, product_id, order_date, quantity, unit_price, 
          revenue, status, processed_at, updated_at)
  VALUES (source.order_id, source.customer_id, source.product_id, source.order_date,
          source.quantity, source.unit_price, source.revenue, source.status,
          source.processed_at, source.updated_at)
