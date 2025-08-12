-- ================================================================================
-- DATABRICKS LAKE FLOW - CREATE TABLE AS SELECT (CTAS) EXAMPLES
-- ================================================================================
-- 
-- PURPOSE:
-- This file demonstrates comprehensive CTAS patterns for Databricks Lake Flow,
-- showcasing various capabilities from basic aggregations to advanced analytics.
-- Lake Flow enables declarative data pipeline development with automatic 
-- dependency management and error handling.
--
-- PREREQUISITES:
-- 1. Run data_generation.sql first to create sample data tables
-- 2. Ensure you have a Databricks workspace with Delta Lake enabled
-- 3. Have appropriate permissions to create tables and databases
--
-- LAKE FLOW CONCEPTS DEMONSTRATED:
-- - Delta Lake table creation with CTAS
-- - Auto-optimization and performance tuning
-- - Partitioning strategies for large datasets
-- - Streaming table processing
-- - Multi-hop architecture (Bronze/Silver/Gold)
-- - Advanced analytics with window functions
-- - Data quality and validation patterns
-- - Change data capture and time travel
-- ================================================================================

-- ========================================
-- 1. BASIC CTAS WITH DELTA TABLE
-- ========================================
-- 
-- CONCEPT: Basic aggregation table using CTAS
-- This creates a Delta table with daily sales summaries by region and category.
-- 
-- KEY FEATURES:
-- - CREATE OR REPLACE: Overwrites existing table structure if it exists
-- - USING DELTA: Specifies Delta Lake format (ACID transactions, time travel, etc.)
-- - LOCATION: External location for data storage (optional, can use managed tables)
-- - TBLPROPERTIES: Delta-specific optimization settings
-- - Basic aggregations: COUNT, SUM, AVG
-- 
-- PERFORMANCE OPTIMIZATIONS:
-- - autoOptimize.optimizeWrite: Automatically optimizes file sizes during writes
-- - autoOptimize.autoCompact: Automatically compacts small files
-- 
-- USE CASE: Daily reporting dashboards, sales analytics
CREATE OR REPLACE TABLE sales_summary
USING DELTA                                    -- Delta Lake format for ACID transactions
LOCATION '/mnt/lake/sales_summary'             -- External storage location (optional)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true', -- Auto-optimize file sizes during writes
  'delta.autoOptimize.autoCompact' = 'true'    -- Auto-compact small files periodically
)
AS
SELECT 
  DATE(order_date) as order_date,              -- Convert timestamp to date for daily grouping
  region,                                       -- Geographic region for analysis
  product_category,                            -- Product category for segmentation
  COUNT(*) as order_count,                     -- Total number of orders per day/region/category
  SUM(order_amount) as total_revenue,          -- Sum of all order amounts
  AVG(order_amount) as avg_order_value,        -- Average order value for trend analysis
  CURRENT_TIMESTAMP() as created_at            -- Audit trail: when this summary was created
FROM raw_orders                                -- Source table with raw order data
WHERE order_date >= '2024-01-01'              -- Filter for recent data only
GROUP BY DATE(order_date), region, product_category; -- Group by dimensions for aggregation

-- ========================================
-- 2. PARTITIONED TABLE WITH ADVANCED FEATURES
-- ========================================
-- 
-- CONCEPT: Advanced customer analytics with partitioning and window functions
-- This creates a partitioned Delta table with complex customer analytics,
-- demonstrating advanced SQL patterns and Delta Lake features.
-- 
-- KEY FEATURES:
-- - PARTITIONED BY: Physical partitioning for query performance
-- - Window functions: FIRST_VALUE, LAST_VALUE for customer journey analysis
-- - JSON processing: Extract data from JSON columns
-- - Customer segmentation: Business logic using CASE statements
-- - Advanced TBLPROPERTIES: File retention and log management
-- 
-- PARTITIONING STRATEGY:
-- - Partitioned by (year, month) for time-based queries
-- - Enables efficient pruning for date range queries
-- - Balances partition size with query performance
-- 
-- WINDOW FUNCTIONS EXPLAINED:
-- - FIRST_VALUE: Gets the first order date for each customer
-- - LAST_VALUE: Gets the most recent order date for each customer
-- - ROWS BETWEEN UNBOUNDED PRECEDING AND FOLLOWING: Full window for LAST_VALUE
-- 
-- JSON PROCESSING:
-- - GET_JSON_OBJECT: Extracts specific fields from JSON metadata
-- - Useful for semi-structured data analysis
-- 
-- USE CASE: Customer lifetime value analysis, segmentation, retention analysis
CREATE OR REPLACE TABLE customer_analytics
USING DELTA                                    -- Delta Lake format
LOCATION '/mnt/lake/customer_analytics'        -- External storage location
PARTITIONED BY (year, month)                   -- Physical partitioning for performance
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true', -- Optimize writes automatically
  'delta.autoOptimize.autoCompact' = 'true',   -- Compact small files automatically
  'delta.deletedFileRetentionDuration' = 'interval 30 days',  -- Keep deleted files for 30 days (time travel)
  'delta.logRetentionDuration' = 'interval 90 days'           -- Keep transaction logs for 90 days
)
AS
SELECT 
  customer_id,                                 -- Primary customer identifier
  YEAR(order_date) as year,                   -- Partition column: year for time-based partitioning
  MONTH(order_date) as month,                 -- Partition column: month for finer granularity
  COUNT(DISTINCT order_id) as total_orders,   -- Count unique orders per customer per month
  SUM(order_amount) as lifetime_value,        -- Monthly customer lifetime value
  
  -- WINDOW FUNCTIONS: Customer journey analysis
  -- Get the very first order date for this customer (across all time)
  FIRST_VALUE(order_date) OVER (
    PARTITION BY customer_id                   -- Separate window for each customer
    ORDER BY order_date                        -- Sort by order date ascending
  ) as first_order_date,
  
  -- Get the most recent order date for this customer (across all time)
  LAST_VALUE(order_date) OVER (
    PARTITION BY customer_id                   -- Separate window for each customer
    ORDER BY order_date                        -- Sort by order date ascending
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  -- Full window scope
  ) as last_order_date,
  
  -- Calculate days since customer's last order (recency analysis)
  DATEDIFF(CURRENT_DATE(), MAX(order_date)) as days_since_last_order,
  
  -- CUSTOMER SEGMENTATION: Business rules using CASE statement
  CASE 
    WHEN SUM(order_amount) > 10000 THEN 'VIP'       -- High-value customers
    WHEN SUM(order_amount) > 5000 THEN 'Premium'    -- Medium-high value customers
    WHEN SUM(order_amount) > 1000 THEN 'Standard'   -- Regular customers
    ELSE 'Basic'                                     -- Low-value or new customers
  END as customer_segment,
  
  -- JSON PROCESSING: Extract structured data from JSON columns
  GET_JSON_OBJECT(customer_metadata, '$.acquisition_channel') as acquisition_channel,
  GET_JSON_OBJECT(customer_metadata, '$.preferences.communication') as communication_preference
  
FROM orders o                                  -- Enhanced orders table (includes customer metadata)
JOIN customers c ON o.customer_id = c.customer_id  -- Join with customer master data
WHERE order_date >= '2023-01-01'              -- Filter for recent data (last ~2 years)
GROUP BY 
  customer_id,                                 -- Group by customer
  YEAR(order_date),                           -- Group by year (partition column)
  MONTH(order_date),                          -- Group by month (partition column)
  customer_metadata;                          -- Include metadata in grouping for JSON extraction

-- ========================================
-- 3. STREAMING TABLE WITH CHANGE DATA CAPTURE
-- ========================================
-- 
-- CONCEPT: Real-time data processing with streaming tables
-- This demonstrates streaming table creation for real-time inventory processing
-- with change data capture (CDC) capabilities.
-- 
-- KEY FEATURES:
-- - STREAMING TABLE: Processes data incrementally as it arrives
-- - STREAM(): Function to read streaming data from source table
-- - Change Data Feed: Tracks all changes (inserts, updates, deletes)
-- - Surrogate key generation: SHA2 hash for unique record identification
-- - Real-time processing: Continuous data pipeline execution
-- 
-- STREAMING TABLE BENEFITS:
-- - Automatic incremental processing
-- - Exactly-once semantics
-- - Automatic recovery and restart
-- - Built-in checkpointing
-- 
-- CHANGE DATA CAPTURE:
-- - enableChangeDataFeed: Tracks all table changes
-- - Enables downstream systems to process only changed data
-- - Supports real-time analytics and data synchronization
-- 
-- SURROGATE KEY:
-- - SHA2 hash ensures unique identification
-- - Combines multiple columns for uniqueness
-- - Useful for deduplication and tracking
-- 
-- USE CASE: Real-time inventory management, supply chain optimization
CREATE OR REPLACE STREAMING TABLE product_inventory_stream
USING DELTA                                    -- Delta Lake format for streaming
LOCATION '/mnt/lake/product_inventory_stream'  -- External storage location
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'        -- Enable CDC to track all changes
)
AS
SELECT 
  product_id,                                  -- Product identifier
  warehouse_id,                               -- Warehouse location identifier
  available_quantity,                         -- Current available inventory
  reserved_quantity,                          -- Inventory reserved for orders
  total_quantity,                             -- Total inventory (available + reserved)
  last_updated,                               -- Timestamp of last inventory update
  
  -- SURROGATE KEY GENERATION
  -- Creates a unique hash-based identifier for each record
  SHA2(CONCAT(product_id, warehouse_id, last_updated), 256) as record_key,
  
  -- CHANGE TRACKING METADATA
  'INSERT' as change_type,                    -- Track the type of change operation
  CURRENT_TIMESTAMP() as processed_at         -- When this record was processed
  
FROM STREAM(product_inventory_raw)            -- STREAM() enables incremental processing
WHERE last_updated IS NOT NULL;              -- Data quality filter

-- ========================================
-- 4. MATERIALIZED VIEW WITH COMPLEX AGGREGATIONS
-- ========================================
-- 
-- CONCEPT: Pre-computed analytics with advanced statistical functions
-- This creates a materialized view for real-time event analytics with
-- complex aggregations, statistical measures, and array processing.
-- 
-- KEY FEATURES:
-- - MATERIALIZED VIEW: Pre-computed results for fast query performance
-- - Statistical functions: PERCENTILE_APPROX, STDDEV, VARIANCE
-- - Approximate aggregations: APPROX_COUNT_DISTINCT for large cardinality
-- - Array aggregation: COLLECT_LIST for grouping values
-- - Conditional aggregations: Complex CASE statements in aggregations
-- - Time-based windowing: DATE_TRUNC for hourly grouping
-- 
-- STATISTICAL FUNCTIONS EXPLAINED:
-- - PERCENTILE_APPROX: Calculates approximate percentiles (faster than exact)
-- - STDDEV: Standard deviation for variability analysis
-- - VARIANCE: Statistical variance for distribution analysis
-- - APPROX_COUNT_DISTINCT: Approximate distinct count (HyperLogLog algorithm)
-- 
-- ARRAY AGGREGATION:
-- - COLLECT_LIST: Aggregates values into an array
-- - DISTINCT: Removes duplicates within the array
-- - Useful for grouping categorical data
-- 
-- TIME WINDOWING:
-- - DATE_TRUNC('HOUR'): Truncates timestamp to hour boundary
-- - Enables hourly aggregation and trend analysis
-- 
-- USE CASE: Real-time operational dashboards, performance monitoring, SLA tracking
CREATE OR REPLACE MATERIALIZED VIEW hourly_metrics
USING DELTA                                    -- Delta Lake format for materialized view
LOCATION '/mnt/lake/hourly_metrics'           -- External storage location
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true' -- Optimize write performance
)
AS
SELECT 
  -- TIME WINDOWING: Group events into hourly buckets
  DATE_TRUNC('HOUR', event_timestamp) as hour,  -- Truncate to hour boundary (e.g., 2024-01-01 14:00:00)
  event_type,                                    -- Type of event (page_view, click, purchase, etc.)
  source_system,                                 -- Origin system (web, mobile_app, api, batch)
  
  -- BASIC AGGREGATIONS
  COUNT(*) as event_count,                       -- Total events in this hour
  COUNT(DISTINCT user_id) as unique_users,       -- Distinct active users
  
  -- APPROXIMATE AGGREGATIONS (for large cardinality)
  APPROX_COUNT_DISTINCT(session_id) as approx_unique_sessions,  -- Approximate distinct sessions (faster)
  
  -- STATISTICAL MEASURES: Performance percentiles
  PERCENTILE_APPROX(duration_ms, 0.5) as median_duration,      -- 50th percentile (median response time)
  PERCENTILE_APPROX(duration_ms, 0.95) as p95_duration,        -- 95th percentile (SLA monitoring)
  
  -- ARRAY AGGREGATION: Collect all device types seen this hour
  COLLECT_LIST(DISTINCT device_type) as device_types,          -- Array of unique device types
  
  -- ADVANCED STATISTICAL FUNCTIONS
  STDDEV(duration_ms) as duration_stddev,                      -- Standard deviation of response times
  VARIANCE(duration_ms) as duration_variance,                  -- Variance of response times
  
  -- CONDITIONAL AGGREGATIONS: Error and success tracking
  SUM(CASE WHEN error_code IS NOT NULL THEN 1 ELSE 0 END) as error_count,     -- Count of errors
  SUM(CASE WHEN success_flag = true THEN 1 ELSE 0 END) as success_count       -- Count of successful events
  
FROM events                                    -- Source events table
WHERE event_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS  -- Process last 7 days only
GROUP BY 
  DATE_TRUNC('HOUR', event_timestamp),         -- Group by hour
  event_type,                                  -- Group by event type
  source_system;                               -- Group by source system

-- ========================================
-- 5. LIQUID CLUSTERING (DATABRICKS RUNTIME 13.3+)
-- ========================================
-- 
-- CONCEPT: Advanced data clustering for optimal query performance
-- This demonstrates Liquid Clustering, a modern alternative to traditional
-- partitioning and Z-ordering that automatically maintains data layout.
-- 
-- KEY FEATURES:
-- - CLUSTER BY: Defines clustering columns for data organization
-- - Automatic maintenance: No manual OPTIMIZE commands needed
-- - Dynamic clustering: Adapts to query patterns over time
-- - Better than Z-ordering: More efficient for evolving datasets
-- - tuneFileSizesForRewrites: Optimizes file sizes during clustering
-- 
-- LIQUID CLUSTERING BENEFITS:
-- - Self-optimizing: Automatically reorganizes data based on usage patterns
-- - No manual maintenance: Eliminates need for scheduled OPTIMIZE jobs
-- - Handles data skew: Adapts to changing data distributions
-- - Multi-dimensional: Can cluster on multiple columns efficiently
-- 
-- CLUSTERING STRATEGY:
-- - Primary cluster: region (high cardinality, frequently filtered)
-- - Secondary cluster: product_category (medium cardinality, often in WHERE clauses)
-- - Tertiary cluster: order_date (time-based queries)
-- 
-- WINDOW FUNCTIONS FOR ANALYTICS:
-- - ROW_NUMBER(): Sequence orders within customer
-- - LAG(): Previous order amount for comparison
-- - SUM() with ROWS: Running total calculation
-- 
-- USE CASE: Large-scale analytical queries, historical trend analysis
CREATE OR REPLACE TABLE sales_clustered
USING DELTA                                    -- Delta Lake format
LOCATION '/mnt/lake/sales_clustered'          -- External storage location
CLUSTER BY (region, product_category, order_date)  -- Liquid clustering specification
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true', -- Auto-optimize writes
  'delta.tuneFileSizesForRewrites' = 'true'    -- Optimize file sizes during clustering operations
)
AS
SELECT 
  order_id,                                    -- Primary order identifier
  customer_id,                                 -- Customer identifier
  product_id,                                  -- Product identifier
  region,                                      -- Clustering column: geographic region
  product_category,                            -- Clustering column: product category
  order_date,                                  -- Clustering column: order date
  order_amount,                                -- Order monetary value
  
  -- ADVANCED WINDOW FUNCTIONS: Customer analytics
  -- Sequential numbering of orders per customer
  ROW_NUMBER() OVER (
    PARTITION BY customer_id                   -- Separate sequence for each customer
    ORDER BY order_date                        -- Order by date (chronological)
  ) as order_sequence,
  
  -- Previous order amount for comparison and trend analysis
  LAG(order_amount) OVER (
    PARTITION BY customer_id                   -- Previous order for same customer
    ORDER BY order_date                        -- Ordered by date
  ) as previous_order_amount,
  
  -- Running total of customer spending (cumulative sum)
  SUM(order_amount) OVER (
    PARTITION BY customer_id                   -- Sum for each customer
    ORDER BY order_date                        -- Chronological order
    ROWS UNBOUNDED PRECEDING                   -- Sum from first order to current
  ) as running_total,
  
  -- DATE/TIME FUNCTIONS: Additional temporal dimensions
  DAYOFWEEK(order_date) as day_of_week,        -- Day of week (1=Sunday, 7=Saturday)
  QUARTER(order_date) as quarter,              -- Quarter of year (1-4)
  DATE_ADD(order_date, 30) as expected_next_order  -- Predicted next order date (+30 days)
  
FROM orders                                    -- Source orders table
WHERE order_date >= '2024-01-01';             -- Filter for recent data

-- ========================================
-- 6. MULTI-HOP ARCHITECTURE EXAMPLE
-- ========================================
-- 
-- CONCEPT: Medallion Architecture (Bronze → Silver → Gold)
-- This demonstrates the industry-standard multi-hop data architecture
-- for progressive data refinement and quality improvement.
-- 
-- ARCHITECTURE LAYERS:
-- 1. BRONZE: Raw data ingestion with minimal processing
-- 2. SILVER: Cleaned, validated, and enriched data
-- 3. GOLD: Business-ready aggregated data for analytics
-- 
-- BRONZE LAYER CHARACTERISTICS:
-- - Ingests raw data "as-is" from source systems
-- - Adds metadata for lineage and auditing
-- - Preserves original data for reprocessing
-- - Minimal transformations and validations
-- 
-- SILVER LAYER CHARACTERISTICS:
-- - Data quality and cleansing operations
-- - Schema enforcement and standardization
-- - Data enrichment and feature engineering
-- - Business rule application
-- 
-- GOLD LAYER CHARACTERISTICS:
-- - Aggregated data for business consumption
-- - Optimized for analytical query performance
-- - Business metrics and KPI calculations
-- - Ready for reporting and dashboards
-- 
-- BENEFITS:
-- - Clear separation of concerns
-- - Incremental data quality improvement
-- - Flexibility for different use cases
-- - Audit trail and data lineage
-- - Reprocessing capabilities

-- BRONZE LAYER (Raw Data Ingestion)
-- PURPOSE: Ingest raw data with minimal processing, preserve original format
CREATE OR REPLACE TABLE bronze_transactions
USING DELTA                                    -- Delta Lake format
LOCATION '/mnt/lake/bronze/transactions'       -- Bronze layer storage location
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'  -- Optimize for write performance
)
AS
SELECT 
  *,                                          -- All original columns from source
  
  -- METADATA COLUMNS: Add audit and lineage information
  current_timestamp() as ingestion_time,      -- When data was ingested
  '/mnt/raw/transactions/batch_001.json' as source_file,  -- Source file reference
  
  -- DATA LINEAGE: Generate hash for change detection and deduplication
  SHA2(CONCAT_WS('|', transaction_id, customer_id, merchant_id, amount, currency_code), 256) as record_hash
  
FROM mock_transaction_files;                  -- Source table (simulates file ingestion)

-- SILVER LAYER (Cleaned and Enriched Data)
-- PURPOSE: Clean, validate, and enrich data for reliable consumption
CREATE OR REPLACE TABLE silver_transactions
USING DELTA                                    -- Delta Lake format
LOCATION '/mnt/lake/silver/transactions'       -- Silver layer storage location
PARTITIONED BY (transaction_date)             -- Partition by date for performance
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true', -- Optimize writes
  'delta.autoOptimize.autoCompact' = 'true'    -- Compact small files automatically
)
AS
SELECT 
  transaction_id,                              -- Primary transaction identifier
  customer_id,                                 -- Customer reference
  merchant_id,                                 -- Merchant reference
  
  -- DATA QUALITY AND CLEANSING
  -- Handle null/empty amounts, convert string to decimal
  COALESCE(NULLIF(TRIM(amount), ''), '0')::DECIMAL(10,2) as amount,
  
  -- Standardize currency codes, default to USD
  COALESCE(currency_code, 'USD') as currency_code,
  
  -- DATE PARSING AND STANDARDIZATION
  TO_DATE(transaction_timestamp) as transaction_date,    -- Extract date for partitioning
  transaction_timestamp,                                 -- Keep original timestamp
  
  -- DATA ENRICHMENT: Business logic and categorization
  CASE 
    WHEN amount < 10 THEN 'Micro'             -- Micro transactions
    WHEN amount < 100 THEN 'Small'            -- Small transactions
    WHEN amount < 1000 THEN 'Medium'          -- Medium transactions
    ELSE 'Large'                              -- Large transactions
  END as transaction_size,
  
  -- GEOSPATIAL PROCESSING: Create spatial point from coordinates
  ST_POINT(longitude, latitude) as geo_point,
  
  -- DATA VALIDATION FLAGS: Mark records for quality assessment
  CASE 
    WHEN amount <= 0 THEN false              -- Invalid: non-positive amount
    WHEN customer_id IS NULL THEN false      -- Invalid: missing customer
    WHEN merchant_id IS NULL THEN false      -- Invalid: missing merchant
    ELSE true                                 -- Valid transaction
  END as is_valid_transaction
  
FROM bronze_transactions                      -- Source from bronze layer
WHERE ingestion_time >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY;  -- Process recent data only

-- GOLD LAYER (Business Metrics and Analytics)
-- PURPOSE: Aggregated, business-ready data for reporting and analytics
CREATE OR REPLACE TABLE gold_daily_merchant_summary
USING DELTA                                    -- Delta Lake format
LOCATION '/mnt/lake/gold/daily_merchant_summary'  -- Gold layer storage location
PARTITIONED BY (summary_date)                 -- Partition by summary date
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true' -- Optimize for analytical queries
)
AS
SELECT 
  merchant_id,                                 -- Merchant identifier
  transaction_date as summary_date,            -- Date of summary (partition column)
  
  -- CORE TRANSACTION METRICS
  COUNT(*) as total_transactions,              -- Total transaction count
  COUNT(DISTINCT customer_id) as unique_customers,  -- Unique customer count
  SUM(amount) as total_revenue,                -- Total revenue generated
  AVG(amount) as avg_transaction_amount,       -- Average transaction size
  
  -- STATISTICAL MEASURES: Distribution analysis
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount,  -- Median transaction amount
  STDDEV(amount) as amount_stddev,             -- Standard deviation of amounts
  
  -- CATEGORICAL BREAKDOWNS: Transaction size distribution
  SUM(CASE WHEN transaction_size = 'Micro' THEN 1 ELSE 0 END) as micro_transactions,
  SUM(CASE WHEN transaction_size = 'Small' THEN 1 ELSE 0 END) as small_transactions,
  SUM(CASE WHEN transaction_size = 'Medium' THEN 1 ELSE 0 END) as medium_transactions,
  SUM(CASE WHEN transaction_size = 'Large' THEN 1 ELSE 0 END) as large_transactions,
  
  -- DATA QUALITY METRICS: Track data quality at merchant level
  SUM(CASE WHEN is_valid_transaction THEN 1 ELSE 0 END) as valid_transactions,
  COUNT(*) - SUM(CASE WHEN is_valid_transaction THEN 1 ELSE 0 END) as invalid_transactions
  
FROM silver_transactions                       -- Source from silver layer
WHERE transaction_date >= CURRENT_DATE() - INTERVAL 30 DAYS    -- Last 30 days
  AND is_valid_transaction = true              -- Only valid transactions
GROUP BY merchant_id, transaction_date;       -- Aggregate by merchant and date

-- ========================================
-- 7. ADVANCED FEATURES SHOWCASE
-- ========================================
-- 
-- CONCEPT: Advanced Delta Lake features for enterprise data management
-- This demonstrates cutting-edge features like generated columns, column mapping,
-- deletion vectors, and complex data transformations.
-- 
-- KEY FEATURES:
-- - Generated columns: Automatically computed columns
-- - Column mapping: Schema evolution support
-- - Deletion vectors: Efficient row-level deletes
-- - Complex transformations: Advanced business logic
-- - Audit columns: Track data lifecycle
-- 
-- GENERATED COLUMNS:
-- - Automatically computed based on other columns
-- - Updated automatically when source columns change
-- - Useful for derived attributes and standardization
-- 
-- COLUMN MAPPING:
-- - Enables schema evolution (rename columns, change order)
-- - Supports column-level changes without rewriting data
-- - Essential for evolving data schemas
-- 
-- DELETION VECTORS:
-- - Efficient row-level deletes without file rewrites
-- - Faster DELETE and UPDATE operations
-- - Reduced storage costs for frequently updated tables
-- 
-- USE CASE: Master data management, customer 360, data warehousing

-- ADVANCED CUSTOMER PROFILE TABLE
-- Demonstrates enterprise-grade features for master data management
CREATE OR REPLACE TABLE advanced_customer_profile
USING DELTA                                    -- Delta Lake format
LOCATION '/mnt/lake/advanced_customer_profile' -- External storage location
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',         -- Enable column mapping for schema evolution
  'delta.enableDeletionVectors' = 'true'       -- Enable efficient row-level deletes
)
AS
SELECT 
  customer_id,                                 -- Primary customer identifier
  first_name,                                  -- Customer first name
  last_name,                                   -- Customer last name
  
  -- GENERATED COLUMN: Automatically computed full name
  CONCAT(first_name, ' ', last_name) as full_name,
  
  email,                                       -- Email address
  phone,                                       -- Phone number
  birth_date,                                  -- Date of birth
  
  -- CALCULATED FIELDS: Age computation
  DATEDIFF(CURRENT_DATE(), birth_date) / 365.25 as age_years,
  
  -- COMPLEX BUSINESS LOGIC: Customer tier calculation
  CASE 
    WHEN total_spent > 50000 AND years_active > 5 THEN 'Platinum'  -- High value, long tenure
    WHEN total_spent > 25000 AND years_active > 3 THEN 'Gold'      -- Medium-high value, good tenure
    WHEN total_spent > 10000 AND years_active > 1 THEN 'Silver'    -- Medium value, some tenure
    ELSE 'Bronze'                                                   -- New or low-value customers
  END as customer_tier,
  
  registration_date,                           -- When customer registered
  
  -- TENURE CALCULATION: Years since registration
  DATEDIFF(CURRENT_DATE(), registration_date) / 365.25 as years_active,
  
  total_spent,                                 -- Lifetime spending
  total_orders,                                -- Total number of orders
  
  -- GEOGRAPHIC DATA: Customer location information
  address_city,                                -- City
  address_state,                               -- State/Province
  address_country,                             -- Country
  latitude,                                    -- Geographic latitude
  longitude,                                   -- Geographic longitude
  
  -- PREFERENCES: JSON data for complex attributes
  preferences,                                 -- Customer preferences (JSON)
  
  -- COMMUNICATION FLAGS: Opt-in preferences
  email_opt_in,                               -- Email marketing consent
  sms_opt_in,                                 -- SMS marketing consent
  marketing_opt_in,                           -- General marketing consent
  
  -- AUDIT COLUMN: Track when profile was last updated
  CURRENT_TIMESTAMP() as profile_updated_at
  
FROM customer_raw_data                        -- Source customer data
WHERE customer_id IS NOT NULL;               -- Ensure valid customer ID

-- ========================================
-- LAKE FLOW SPECIFIC FEATURES
-- ========================================
-- 
-- CONCEPT: Lake Flow advanced capabilities for data quality and management
-- These features enable enterprise-grade data governance, quality assurance,
-- and operational capabilities.
-- 
-- FEATURES DEMONSTRATED:
-- 1. Expectations (Data Quality Constraints)
-- 2. Change Data Feed (CDC tracking)
-- 3. Time Travel (Historical queries)
-- 4. Vacuum and Optimize Operations
-- 
-- DATA QUALITY EXPECTATIONS:
-- - Constraints ensure data quality at write time
-- - Prevent invalid data from entering tables
-- - Enable data contracts between systems
-- 
-- CHANGE DATA FEED:
-- - Tracks all changes (inserts, updates, deletes)
-- - Enables real-time downstream processing
-- - Supports incremental data processing
-- 
-- TIME TRAVEL:
-- - Query historical versions of data
-- - Recover from accidental changes
-- - Audit data changes over time
-- 
-- VACUUM AND OPTIMIZE:
-- - VACUUM: Clean up old file versions
-- - OPTIMIZE: Improve query performance
-- - Z-ORDER: Co-locate related data

-- 1. EXPECTATIONS (Data Quality Constraints)
-- Add data quality constraints after table creation to ensure data integrity
-- These constraints validate data at write time and prevent invalid records

-- EXAMPLE: Add constraints to silver_transactions table
-- ALTER TABLE silver_transactions 
-- SET TBLPROPERTIES (
--   'delta.constraints.valid_amount' = 'amount > 0',                    -- Ensure positive amounts
--   'delta.constraints.valid_currency' = 'currency_code IN (''USD'', ''EUR'', ''GBP'')',  -- Valid currencies
--   'delta.constraints.future_date_check' = 'transaction_date <= current_date()'  -- No future dates
-- );

-- 2. CHANGE DATA FEED (CDC) - Track all table changes
-- Enable change data capture on existing tables for downstream processing
-- This tracks INSERT, UPDATE, DELETE operations with before/after values

-- EXAMPLE: Enable CDC on customer analytics table
-- ALTER TABLE customer_analytics 
-- SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- 3. TIME TRAVEL QUERIES - Query historical versions of data
-- Delta Lake automatically maintains version history for time travel queries
-- This enables data recovery, auditing, and historical analysis

-- EXAMPLES: Query specific points in time
-- SELECT * FROM sales_summary TIMESTAMP AS OF '2024-08-01T00:00:00.000Z';  -- Query by timestamp
-- SELECT * FROM sales_summary VERSION AS OF 42;                           -- Query by version number

-- ADDITIONAL TIME TRAVEL EXAMPLES:
-- SELECT * FROM sales_summary@v25;                                        -- Alternative syntax
-- DESCRIBE HISTORY sales_summary;                                         -- View version history
-- RESTORE TABLE sales_summary TO VERSION AS OF 25;                        -- Restore to previous version

-- 4. VACUUM AND OPTIMIZE OPERATIONS - Maintenance commands
-- These operations maintain table performance and manage storage costs

-- VACUUM: Remove old file versions (frees up storage)
-- VACUUM sales_summary RETAIN 168 HOURS;                                  -- Keep 7 days of history

-- OPTIMIZE: Compact small files and improve query performance
-- OPTIMIZE sales_summary ZORDER BY (region, product_category);            -- Z-order by common filters

-- ADDITIONAL OPTIMIZE EXAMPLES:
-- OPTIMIZE sales_summary;                                                  -- Basic compaction
-- ANALYZE TABLE sales_summary COMPUTE STATISTICS;                         -- Update table statistics

-- ========================================
-- PERFORMANCE OPTIMIZATION REFERENCE GUIDE
-- ========================================
-- 
-- COMPREHENSIVE GUIDE: Best practices for optimizing Delta Lake tables
-- This section provides detailed guidance on performance optimization
-- strategies for different use cases and data patterns.
-- 
-- OPTIMIZATION STRATEGIES:
-- 1. Partitioning Strategy
-- 2. Liquid Clustering
-- 3. Auto Optimize Features
-- 4. Data Skipping Techniques
-- 5. Bloom Filters
-- 6. Column Statistics
-- 7. File Management
-- 
-- PERFORMANCE CONSIDERATIONS:
-- - Query patterns and access patterns
-- - Data volume and growth rate
-- - Update frequency and patterns
-- - Storage costs vs. query performance
-- - Maintenance overhead

/*
PERFORMANCE OPTIMIZATION DETAILED GUIDE
=======================================

1. PARTITIONING STRATEGY:
   CONCEPT: Physical data organization for query performance
   - Partition by frequently filtered columns (date, region, country, etc.)
   - Avoid over-partitioning (aim for 1GB+ partitions)
   - Use PARTITIONED BY clause in CTAS statements
   
   EXAMPLES:
   - Time-based: PARTITIONED BY (year, month) for time-series data
   - Geographic: PARTITIONED BY (country, region) for location-based queries
   - Category: PARTITIONED BY (product_category) for catalog data
   
   BEST PRACTICES:
   - Monitor partition sizes with DESCRIBE DETAIL table_name
   - Consider partition evolution as data grows
   - Balance between too few (large files) and too many (metadata overhead)

2. LIQUID CLUSTERING:
   CONCEPT: Next-generation data organization (Databricks Runtime 13.3+)
   - Use CLUSTER BY for columns frequently used in WHERE, JOIN, GROUP BY
   - Better than Z-ordering for many use cases
   - Automatically maintains clustering over time
   
   ADVANTAGES:
   - Self-optimizing: Adapts to query patterns
   - No manual maintenance required
   - Handles evolving data distributions
   - Multi-dimensional clustering support
   
   EXAMPLE: CLUSTER BY (customer_id, order_date, region)

3. AUTO OPTIMIZE FEATURES:
   CONCEPT: Automatic performance optimization during writes
   - Enable optimizeWrite for better file sizes during writes
   - Enable autoCompact for automatic compaction of small files
   - Set appropriate file size targets (128MB - 1GB per file)
   
   CONFIGURATION:
   - 'delta.autoOptimize.optimizeWrite' = 'true'
   - 'delta.autoOptimize.autoCompact' = 'true'
   - 'delta.tuneFileSizesForRewrites' = 'true'

4. DATA SKIPPING OPTIMIZATION:
   CONCEPT: Automatic query optimization using file statistics
   - Use min/max statistics automatically collected by Delta
   - Structure predicates to leverage data skipping
   - Monitor data skipping effectiveness with query plans
   
   TECHNIQUES:
   - Order data by commonly filtered columns
   - Use clustered tables for better statistics
   - Avoid functions in WHERE clauses that prevent skipping
   
   MONITORING: Use EXPLAIN to see "Data skipping" in query plans

5. BLOOM FILTERS:
   CONCEPT: Probabilistic data structure for fast lookups
   - Use for high-cardinality columns frequently used in point lookups
   - Excellent for JOIN optimizations and EXISTS queries
   - Small storage overhead with significant query speedup
   
   EXAMPLE: 
   ALTER TABLE sales_summary 
   SET TBLPROPERTIES ('delta.bloomFilter.customer_id' = 'true');
   
   USE CASES:
   - Customer ID lookups
   - Product SKU searches
   - Email address matching

6. COLUMN STATISTICS:
   CONCEPT: Detailed statistics for query optimization
   - Helps query optimizer choose better execution plans
   - Essential for complex joins and aggregations
   - Update regularly for best performance
   
   COMMANDS:
   - ANALYZE TABLE sales_summary COMPUTE STATISTICS FOR ALL COLUMNS;
   - ANALYZE TABLE sales_summary COMPUTE STATISTICS FOR COLUMNS (customer_id, region);
   
   BENEFITS:
   - Better join strategies
   - Improved predicate pushdown
   - More accurate cost estimates

7. FILE MANAGEMENT:
   CONCEPT: Optimal file organization for performance and cost
   - Set optimal file sizes (128MB - 1GB per file)
   - Use OPTIMIZE regularly for read-heavy workloads
   - Consider VACUUM retention period based on time travel needs
   
   OPERATIONS:
   - OPTIMIZE table_name: Compact small files
   - OPTIMIZE table_name ZORDER BY (col1, col2): Co-locate related data
   - VACUUM table_name RETAIN 168 HOURS: Clean old versions
   
   SCHEDULING:
   - Daily OPTIMIZE for frequently updated tables
   - Weekly VACUUM for storage management
   - Monitor file sizes with DESCRIBE DETAIL

QUERY OPTIMIZATION PATTERNS:
============================

A. PREDICATE PUSHDOWN:
   - Put most selective filters first
   - Use partition columns in WHERE clauses
   - Avoid functions on filtered columns

B. JOIN OPTIMIZATION:
   - Use broadcast joins for small dimension tables
   - Consider bucketing for large-to-large joins
   - Enable auto join optimization

C. AGGREGATION PATTERNS:
   - Pre-aggregate in materialized views
   - Use approximate functions for large datasets
   - Consider incremental aggregation patterns

D. WINDOW FUNCTION OPTIMIZATION:
   - Minimize window frame size
   - Use appropriate partitioning
   - Consider pre-sorted data

MONITORING AND TROUBLESHOOTING:
==============================

1. Query Performance:
   - Use EXPLAIN to analyze query plans
   - Monitor data skipping effectiveness
   - Check for full table scans

2. Storage Optimization:
   - Monitor file sizes and counts
   - Track storage costs over time
   - Analyze partition distribution

3. Maintenance Operations:
   - Schedule regular OPTIMIZE operations
   - Monitor VACUUM effectiveness
   - Track table statistics freshness

EXAMPLE MONITORING QUERIES:
==========================

-- Check table file statistics
DESCRIBE DETAIL sales_summary;

-- View table history
DESCRIBE HISTORY sales_summary;

-- Check partition information
SHOW PARTITIONS sales_summary;

-- Analyze query performance
EXPLAIN EXTENDED SELECT * FROM sales_summary WHERE region = 'North';
*/

-- ========================================
-- EXECUTION ORDER AND TESTING GUIDE
-- ========================================
-- 
-- COMPREHENSIVE TESTING WORKFLOW
-- This section provides step-by-step instructions for executing and testing
-- all CTAS examples with proper validation and verification steps.
-- 
-- PREREQUISITES CHECKLIST:
-- ☐ Databricks workspace access with Delta Lake enabled
-- ☐ Appropriate permissions (CREATE TABLE, database access)
-- ☐ Sample data generated (run data_generation.sql first)
-- ☐ Target database created (e.g., lake_flow_demo)
-- 
-- EXECUTION WORKFLOW:
-- 1. Data Generation Phase
-- 2. CTAS Execution Phase  
-- 3. Validation Phase
-- 4. Feature Testing Phase
-- 5. Performance Testing Phase

/*
PHASE 1: DATA GENERATION
========================

STEP 1.1: Run data generation
Execute data_generation.sql file completely to create all sample tables:
- This creates 100+ records across 9 tables
- Includes: raw_orders, customers, orders, product_inventory_raw, events, etc.
- Contains built-in validation queries to verify data creation

STEP 1.2: Verify data generation
Run these queries to confirm data is ready:
*/

-- Verify all source tables exist and have data
SELECT 'Source Data Verification:' as info;
SELECT 
  'raw_orders' as table_name, 
  COUNT(*) as record_count,
  MIN(order_date) as earliest_date,
  MAX(order_date) as latest_date
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
FROM events;

/*
PHASE 2: CTAS EXECUTION
=======================

STEP 2.1: Execute CTAS examples in dependency order
Run each CTAS statement in the correct order to respect dependencies:

EXECUTION ORDER:
1. sales_summary (depends on: raw_orders)
2. customer_analytics (depends on: orders, customers) 
3. product_inventory_stream (depends on: product_inventory_raw)
4. hourly_metrics (depends on: events)
5. sales_clustered (depends on: orders)
6. bronze_transactions (depends on: mock_transaction_files)
7. silver_transactions (depends on: bronze_transactions)
8. gold_daily_merchant_summary (depends on: silver_transactions)
9. advanced_customer_profile (depends on: customer_raw_data)

STEP 2.2: Monitor execution
Check for successful completion of each table creation.
*/

-- Check CTAS execution results
SELECT 'CTAS Results Verification:' as info;
SELECT 
  'sales_summary' as table_name, 
  COUNT(*) as record_count,
  COUNT(DISTINCT region) as unique_regions,
  SUM(total_revenue) as total_revenue
FROM sales_summary
UNION ALL
SELECT 
  'customer_analytics', 
  COUNT(*), 
  COUNT(DISTINCT customer_segment),
  SUM(lifetime_value)
FROM customer_analytics
UNION ALL
SELECT 
  'hourly_metrics', 
  COUNT(*), 
  COUNT(DISTINCT event_type),
  SUM(event_count)
FROM hourly_metrics;

/*
PHASE 3: VALIDATION PHASE
=========================

STEP 3.1: Data quality validation
Verify data integrity and business logic correctness:
*/

-- Data Quality Checks
SELECT 'Data Quality Validation:' as info;

-- Check for NULL values in key columns
SELECT 
  'NULL Check - sales_summary' as test_name,
  COUNT(*) as total_records,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_dates,
  SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as null_revenue
FROM sales_summary
UNION ALL
-- Check customer segmentation logic
SELECT 
  'Segmentation Logic - customer_analytics',
  COUNT(*),
  SUM(CASE WHEN customer_segment NOT IN ('VIP', 'Premium', 'Standard', 'Basic') THEN 1 ELSE 0 END),
  0
FROM customer_analytics;

-- Business Logic Validation
SELECT 'Business Logic Validation:' as info;

-- Verify aggregation accuracy (spot check)
SELECT 
  'Aggregation Accuracy Test' as test_name,
  region,
  product_category,
  -- Compare aggregated values with source
  summary.total_revenue as summary_revenue,
  source.calculated_revenue as source_revenue,
  ABS(summary.total_revenue - source.calculated_revenue) as difference
FROM sales_summary summary
JOIN (
  SELECT 
    region, 
    product_category, 
    SUM(order_amount) as calculated_revenue
  FROM raw_orders 
  WHERE order_date >= '2024-01-01'
  GROUP BY region, product_category
) source 
ON summary.region = source.region 
AND summary.product_category = source.product_category
WHERE ABS(summary.total_revenue - source.calculated_revenue) > 0.01  -- Allow for rounding
LIMIT 5;

/*
PHASE 4: FEATURE TESTING
========================

STEP 4.1: Test Lake Flow specific features
*/

-- Time Travel Testing
SELECT 'Time Travel Feature Test:' as info;
-- Query current version
SELECT COUNT(*) as current_count FROM sales_summary VERSION AS OF 0;

-- Change Data Feed Testing (after enabling CDC)
-- ALTER TABLE customer_analytics SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Performance Feature Testing
-- Test data skipping with EXPLAIN
-- EXPLAIN SELECT * FROM sales_summary WHERE region = 'North' AND order_date = '2024-02-15';

/*
STEP 4.2: Test optimization features
*/

-- File Statistics Check
SELECT 'File Statistics Check:' as info;
DESCRIBE DETAIL sales_summary;

-- Table Properties Verification
SELECT 'Table Properties Check:' as info;
SHOW TBLPROPERTIES sales_summary;

/*
PHASE 5: PERFORMANCE TESTING
============================

STEP 5.1: Query performance tests
*/

-- Performance Test Queries
SELECT 'Performance Test Results:' as info;

-- Test 1: Regional analysis (should leverage partitioning/clustering)
SELECT 
  'Regional Revenue Analysis' as test_name,
  region, 
  SUM(total_revenue) as total_revenue,
  COUNT(DISTINCT product_category) as categories,
  AVG(avg_order_value) as avg_order_value
FROM sales_summary 
GROUP BY region 
ORDER BY total_revenue DESC;

-- Test 2: Customer segmentation analysis (should use efficient aggregations)
SELECT 
  'Customer Segmentation Analysis' as test_name,
  customer_segment, 
  COUNT(*) as customer_count, 
  AVG(lifetime_value) as avg_lifetime_value,
  SUM(total_orders) as total_orders
FROM customer_analytics 
GROUP BY customer_segment 
ORDER BY avg_lifetime_value DESC;

-- Test 3: Time-series analysis (should benefit from time-based partitioning)
SELECT 
  'Time Series Analysis' as test_name,
  year,
  month,
  SUM(lifetime_value) as monthly_revenue,
  COUNT(DISTINCT customer_id) as active_customers
FROM customer_analytics 
GROUP BY year, month 
ORDER BY year, month;

/*
STEP 5.2: Advanced analytics validation
*/

-- Advanced Analytics Validation
SELECT 'Advanced Analytics Validation:' as info;

-- Window function results validation
SELECT 
  customer_id,
  order_sequence,
  order_amount,
  previous_order_amount,
  running_total,
  -- Validate running total calculation
  SUM(order_amount) OVER (
    PARTITION BY customer_id 
    ORDER BY order_date 
    ROWS UNBOUNDED PRECEDING
  ) as manual_running_total
FROM sales_clustered 
WHERE customer_id IN (1, 2, 3)  -- Test sample customers
ORDER BY customer_id, order_sequence;

/*
TROUBLESHOOTING GUIDE
====================

Common Issues and Solutions:

1. TABLE NOT FOUND ERROR:
   - Ensure data_generation.sql was run successfully
   - Check database context: USE database_name;
   - Verify table names match exactly

2. PERMISSION DENIED:
   - Check CREATE TABLE permissions
   - Verify access to specified LOCATION paths
   - Confirm database write permissions

3. DATA TYPE ERRORS:
   - Review column data types in source tables
   - Check for NULL handling in CASE statements
   - Verify date/timestamp format compatibility

4. PERFORMANCE ISSUES:
   - Run ANALYZE TABLE statements for statistics
   - Check partition pruning with EXPLAIN
   - Monitor file sizes with DESCRIBE DETAIL

5. MEMORY ERRORS:
   - Reduce data window for testing
   - Increase cluster resources
   - Consider incremental processing

VALIDATION CHECKLIST:
====================

☐ All source tables contain expected record counts
☐ All CTAS tables created successfully
☐ Data quality checks pass
☐ Business logic validation passes
☐ Time travel queries work
☐ Performance tests complete within acceptable time
☐ File statistics show optimal file sizes
☐ No unexpected NULL values or data anomalies

SUCCESS CRITERIA:
=================

✅ 9 tables created from CTAS statements
✅ All tables contain expected data volumes
✅ Aggregations match source data calculations
✅ Advanced features (time travel, CDC) functional
✅ Query performance meets expectations
✅ Data quality constraints enforced
✅ Storage optimization features active

NEXT STEPS AFTER VALIDATION:
============================

1. Implement production data pipelines using these patterns
2. Set up monitoring and alerting for data quality
3. Schedule maintenance operations (OPTIMIZE, VACUUM)
4. Create dashboards and reports using gold layer tables
5. Implement incremental processing patterns
6. Set up data governance and access controls
*/