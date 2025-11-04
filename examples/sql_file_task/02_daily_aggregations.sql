-- Daily Sales Aggregations
-- Aggregates daily sales data and creates a summary table

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.daily_sales_summary') AS
SELECT 
  order_date,
  COUNT(*) as total_orders,
  SUM(revenue) as total_revenue,
  SUM(quantity) as total_quantity,
  COUNT(DISTINCT customer_id) as unique_customers,
  AVG(revenue) as avg_order_value,
  COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
  CURRENT_TIMESTAMP() as processed_at
FROM IDENTIFIER(:catalog || '.' || :schema || '.sample_sales_data')
WHERE order_date = CURRENT_DATE()
GROUP BY order_date
