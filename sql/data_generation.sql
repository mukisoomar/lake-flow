-- ========================================
-- DATABRICKS LAKE FLOW - SAMPLE DATA GENERATION
-- ========================================
-- This file contains scripts to generate 100+ sample records
-- to test the CTAS statements in scripts.txt

-- ========================================
-- 1. RAW ORDERS TABLE (100 RECORDS)
-- ========================================
CREATE OR REPLACE TABLE raw_orders AS
SELECT 
  CAST(row_number() OVER (ORDER BY rand()) AS INT) as order_id,
  CAST(ceil(rand() * 50) AS INT) as customer_id,
  element_at(array('North', 'South', 'East', 'West', 'Central'), cast(ceil(rand() * 5) as int)) as region,
  element_at(array('Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty'), cast(ceil(rand() * 6) as int)) as product_category,
  date_add('2024-01-01', cast(rand() * 300 as int)) as order_date,
  round(rand() * 1000 + 10, 2) as order_amount,
  current_timestamp() as created_at
FROM range(100);

-- ========================================
-- 2. CUSTOMERS TABLE (50 UNIQUE CUSTOMERS)
-- ========================================
CREATE OR REPLACE TABLE customers AS
SELECT 
  customer_id,
  CONCAT('Customer_', customer_id) as first_name,
  CONCAT('LastName_', customer_id) as last_name,
  CONCAT('customer', customer_id, '@email.com') as email,
  CONCAT('+1-555-', lpad(cast(customer_id as string), 4, '0')) as phone,
  date_sub(current_date(), cast(rand() * 365 * 30 + 6570 as int)) as birth_date, -- Ages 18-48
  date_sub(current_date(), cast(rand() * 365 * 5 + 365 as int)) as registration_date, -- 1-6 years ago
  round(rand() * 50000 + 500, 2) as total_spent,
  cast(ceil(rand() * 50) as int) as total_orders,
  element_at(array('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'), cast(ceil(rand() * 5) as int)) as address_city,
  element_at(array('NY', 'CA', 'IL', 'TX', 'AZ'), cast(ceil(rand() * 5) as int)) as address_state,
  'USA' as address_country,
  round(rand() * 180 - 90, 6) as latitude,
  round(rand() * 360 - 180, 6) as longitude,
  to_json(struct(
    element_at(array('online', 'referral', 'social', 'direct'), cast(ceil(rand() * 4) as int)) as acquisition_channel,
    struct(
      element_at(array('email', 'sms', 'phone'), cast(ceil(rand() * 3) as int)) as communication
    ) as preferences
  )) as customer_metadata,
  to_json(struct(
    element_at(array('email', 'sms', 'phone'), cast(ceil(rand() * 3) as int)) as communication_preference,
    element_at(array('weekly', 'monthly', 'quarterly'), cast(ceil(rand() * 3) as int)) as newsletter_frequency
  )) as preferences,
  rand() > 0.2 as email_opt_in,
  rand() > 0.7 as sms_opt_in,
  rand() > 0.4 as marketing_opt_in
FROM (
  SELECT DISTINCT customer_id
  FROM (SELECT CAST(ceil(rand() * 50) AS INT) as customer_id FROM range(100))
);

-- ========================================
-- 3. ENHANCED ORDERS TABLE (WITH JOINS)
-- ========================================
CREATE OR REPLACE TABLE orders AS
SELECT 
  o.order_id,
  o.customer_id,
  CAST(ceil(rand() * 200) AS INT) as product_id,
  o.region,
  o.product_category,
  o.order_date,
  o.order_amount,
  c.customer_metadata
FROM raw_orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- ========================================
-- 4. PRODUCT INVENTORY (FOR STREAMING)
-- ========================================
CREATE OR REPLACE TABLE product_inventory_raw AS
SELECT 
  CAST(row_number() OVER (ORDER BY rand()) AS INT) as product_id,
  CAST(ceil(rand() * 10) AS INT) as warehouse_id,
  CAST(ceil(rand() * 1000) AS INT) as available_quantity,
  CAST(ceil(rand() * 100) AS INT) as reserved_quantity,
  CAST(ceil(rand() * 1000) + ceil(rand() * 100) AS INT) as total_quantity,
  timestamp_add(current_timestamp(), cast((rand() - 0.5) * 86400 as int)) as last_updated
FROM range(100);

-- ========================================
-- 5. EVENTS TABLE (FOR ANALYTICS)
-- ========================================
CREATE OR REPLACE TABLE events AS
SELECT 
  CONCAT('event_', row_number() OVER (ORDER BY rand())) as event_id,
  element_at(array('page_view', 'click', 'purchase', 'signup', 'logout'), cast(ceil(rand() * 5) as int)) as event_type,
  element_at(array('web', 'mobile_app', 'api', 'batch'), cast(ceil(rand() * 4) as int)) as source_system,
  CAST(ceil(rand() * 100) AS INT) as user_id,
  CONCAT('session_', ceil(rand() * 50)) as session_id,
  element_at(array('desktop', 'mobile', 'tablet'), cast(ceil(rand() * 3) as int)) as device_type,
  timestamp_sub(current_timestamp(), cast(rand() * 604800 as int)) as event_timestamp, -- Last 7 days
  CAST(rand() * 5000 + 100 AS INT) as duration_ms,
  CASE WHEN rand() > 0.9 THEN CONCAT('ERROR_', ceil(rand() * 5)) ELSE NULL END as error_code,
  rand() > 0.1 as success_flag
FROM range(100);

-- ========================================
-- 6. CUSTOMER RAW DATA (FOR ADVANCED PROFILES)
-- ========================================
CREATE OR REPLACE TABLE customer_raw_data AS
SELECT 
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  birth_date,
  registration_date,
  total_spent,
  total_orders,
  address_city,
  address_state,
  address_country,
  latitude,
  longitude,
  preferences,
  email_opt_in,
  sms_opt_in,
  marketing_opt_in,
  DATEDIFF(CURRENT_DATE(), registration_date) / 365.25 as years_active
FROM customers;

-- ========================================
-- 7. TRANSACTION DATA (FOR BRONZE/SILVER/GOLD)
-- ========================================

-- Create sample transactions view (simulating JSON ingestion)
CREATE OR REPLACE VIEW transactions_json_view AS
SELECT 
  CONCAT('txn_', lpad(cast(row_number() OVER (ORDER BY rand()) as string), 6, '0')) as transaction_id,
  CAST(ceil(rand() * 50) AS INT) as customer_id,
  CAST(ceil(rand() * 100) AS INT) as merchant_id,
  CAST(round(rand() * 2000 + 1, 2) AS STRING) as amount, -- String to simulate raw JSON
  element_at(array('USD', 'EUR', 'GBP', 'CAD'), cast(ceil(rand() * 4) as int)) as currency_code,
  timestamp_sub(current_timestamp(), cast(rand() * 2592000 as int)) as transaction_timestamp, -- Last 30 days
  round(rand() * 180 - 90, 6) as latitude,
  round(rand() * 360 - 180, 6) as longitude,
  '/mnt/raw/transactions/batch_001.json' as source_file
FROM range(100);

-- Create table to simulate read_files() function
CREATE OR REPLACE TABLE mock_transaction_files AS
SELECT 
  transaction_id,
  customer_id,
  merchant_id,
  amount,
  currency_code,
  transaction_timestamp,
  latitude,
  longitude
FROM transactions_json_view;

-- ========================================
-- 8. ADDITIONAL SAMPLE DATA TABLES
-- ========================================

-- Products catalog
CREATE OR REPLACE TABLE products AS
SELECT 
  CAST(row_number() OVER (ORDER BY rand()) AS INT) as product_id,
  CONCAT('Product_', row_number() OVER (ORDER BY rand())) as product_name,
  element_at(array('Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty'), cast(ceil(rand() * 6) as int)) as category,
  element_at(array('Brand_A', 'Brand_B', 'Brand_C', 'Brand_D'), cast(ceil(rand() * 4) as int)) as brand,
  round(rand() * 500 + 10, 2) as unit_price,
  cast(ceil(rand() * 1000) as int) as stock_quantity,
  rand() > 0.8 as is_active,
  current_timestamp() as created_at
FROM range(200);

-- Merchants table
CREATE OR REPLACE TABLE merchants AS
SELECT 
  CAST(row_number() OVER (ORDER BY rand()) AS INT) as merchant_id,
  CONCAT('Merchant_', row_number() OVER (ORDER BY rand())) as merchant_name,
  element_at(array('Retail', 'Food', 'Services', 'Online', 'Gas Station'), cast(ceil(rand() * 5) as int)) as merchant_category,
  element_at(array('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'), cast(ceil(rand() * 5) as int)) as city,
  element_at(array('NY', 'CA', 'IL', 'TX', 'AZ'), cast(ceil(rand() * 5) as int)) as state,
  round(rand() * 180 - 90, 6) as latitude,
  round(rand() * 360 - 180, 6) as longitude,
  date_sub(current_date(), cast(rand() * 365 * 10 as int)) as registration_date,
  rand() > 0.95 as is_active
FROM range(100);

-- User sessions (for web analytics)
CREATE OR REPLACE TABLE user_sessions AS
SELECT 
  CONCAT('session_', row_number() OVER (ORDER BY rand())) as session_id,
  CAST(ceil(rand() * 100) AS INT) as user_id,
  timestamp_sub(current_timestamp(), cast(rand() * 2592000 as int)) as session_start, -- Last 30 days
  timestamp_add(timestamp_sub(current_timestamp(), cast(rand() * 2592000 as int)), cast(rand() * 3600 as int)) as session_end,
  element_at(array('desktop', 'mobile', 'tablet'), cast(ceil(rand() * 3) as int)) as device_type,
  element_at(array('Chrome', 'Safari', 'Firefox', 'Edge'), cast(ceil(rand() * 4) as int)) as browser,
  element_at(array('Windows', 'macOS', 'iOS', 'Android', 'Linux'), cast(ceil(rand() * 5) as int)) as operating_system,
  element_at(array('organic', 'paid', 'direct', 'social', 'referral'), cast(ceil(rand() * 5) as int)) as traffic_source,
  cast(ceil(rand() * 20) as int) as page_views,
  round(rand() * 1800 + 30, 0) as session_duration_seconds
FROM range(200);

-- ========================================
-- DATA VALIDATION QUERIES
-- ========================================

-- Verify record counts
SELECT 'Data Generation Summary:' as info;

SELECT 
  'raw_orders' as table_name, 
  COUNT(*) as record_count,
  MIN(order_date) as min_date,
  MAX(order_date) as max_date
FROM raw_orders
UNION ALL
SELECT 
  'customers', 
  COUNT(*), 
  MIN(registration_date),
  MAX(registration_date)
FROM customers
UNION ALL
SELECT 
  'orders', 
  COUNT(*), 
  MIN(order_date),
  MAX(order_date)
FROM orders
UNION ALL
SELECT 
  'events', 
  COUNT(*), 
  MIN(event_timestamp),
  MAX(event_timestamp)
FROM events
UNION ALL
SELECT 
  'product_inventory_raw', 
  COUNT(*), 
  MIN(last_updated),
  MAX(last_updated)
FROM product_inventory_raw
UNION ALL
SELECT 
  'mock_transaction_files', 
  COUNT(*), 
  MIN(transaction_timestamp),
  MAX(transaction_timestamp)
FROM mock_transaction_files
UNION ALL
SELECT 
  'products', 
  COUNT(*), 
  MIN(created_at),
  MAX(created_at)
FROM products
UNION ALL
SELECT 
  'merchants', 
  COUNT(*), 
  MIN(registration_date),
  MAX(registration_date)
FROM merchants
UNION ALL
SELECT 
  'user_sessions', 
  COUNT(*), 
  MIN(session_start),
  MAX(session_start)
FROM user_sessions;

-- Sample data preview
SELECT 'Raw Orders Sample:' as info;
SELECT * FROM raw_orders LIMIT 3;

SELECT 'Customers Sample:' as info;
SELECT customer_id, first_name, last_name, email, total_spent, address_city FROM customers LIMIT 3;

SELECT 'Events Sample:' as info;
SELECT event_id, event_type, source_system, user_id, device_type, event_timestamp FROM events LIMIT 3;

SELECT 'Transactions Sample:' as info;
SELECT transaction_id, customer_id, merchant_id, amount, currency_code, transaction_timestamp FROM mock_transaction_files LIMIT 3;

-- Data quality checks
SELECT 'Data Quality Checks:' as info;

SELECT 
  'Customers with valid emails' as check_name,
  COUNT(*) as count
FROM customers 
WHERE email LIKE '%@%'
UNION ALL
SELECT 
  'Orders with positive amounts',
  COUNT(*)
FROM raw_orders 
WHERE order_amount > 0
UNION ALL
SELECT 
  'Events with valid timestamps',
  COUNT(*)
FROM events 
WHERE event_timestamp IS NOT NULL
UNION ALL
SELECT 
  'Transactions with valid amounts',
  COUNT(*)
FROM mock_transaction_files 
WHERE CAST(amount AS DECIMAL(10,2)) > 0;

-- Distribution analysis
SELECT 'Distribution Analysis:' as info;

SELECT 
  'Orders by Region' as analysis,
  region,
  COUNT(*) as count,
  SUM(order_amount) as total_revenue
FROM raw_orders 
GROUP BY region 
ORDER BY total_revenue DESC;

SELECT 
  'Customers by Segment (based on spending)' as analysis,
  CASE 
    WHEN total_spent > 30000 THEN 'High Value'
    WHEN total_spent > 15000 THEN 'Medium Value'
    ELSE 'Standard'
  END as segment,
  COUNT(*) as customer_count,
  AVG(total_spent) as avg_spending
FROM customers 
GROUP BY 
  CASE 
    WHEN total_spent > 30000 THEN 'High Value'
    WHEN total_spent > 15000 THEN 'Medium Value'
    ELSE 'Standard'
  END;

-- ========================================
-- USAGE INSTRUCTIONS
-- ========================================

/*
EXECUTION ORDER:
1. Run this entire file to create all sample data tables
2. Verify data creation with the validation queries at the end
3. Then run the CTAS examples in scripts.txt

TABLES CREATED:
- raw_orders (100 records)
- customers (50 unique customers)  
- orders (enhanced with joins)
- product_inventory_raw (100 records)
- events (100 records)
- customer_raw_data (derived from customers)
- mock_transaction_files (100 transactions)
- products (200 products)
- merchants (100 merchants)
- user_sessions (200 sessions)

These tables provide comprehensive sample data to test all the
CTAS patterns and Lake Flow capabilities in scripts.txt
*/
