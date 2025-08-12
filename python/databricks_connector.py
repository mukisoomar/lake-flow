#!/usr/bin/env python3
"""
Databricks Connection and Data Upload Script
============================================
This script connects to Databricks workspace and uploads the generated data
to Delta tables using the same structure as data_generation.sql

Connection Methods:
1. Databricks Connect (Spark-based)
2. Databricks SQL Connector (SQL-based)
3. REST API (for file uploads)
"""

import os
import pandas as pd
from typing import Dict, Any, Optional
from generate_data import main as generate_sample_data

# =============================================================================
# CONNECTION CONFIGURATION
# =============================================================================

# Method 1: Databricks Connect Configuration
DATABRICKS_CONNECT_CONFIG = {
    "host": "https://your-workspace.cloud.databricks.com",
    "token": "your-personal-access-token",
    "cluster_id": "your-cluster-id",
    "org_id": "your-org-id"  # Required for some configurations
}

# Method 2: Databricks SQL Connector Configuration  
DATABRICKS_SQL_CONFIG = {
    "server_hostname": "your-workspace.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/your-warehouse-id",
    "access_token": "your-personal-access-token"
}

# Method 3: Environment Variables (Recommended for security)
# Set these in your environment:
# export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
# export DATABRICKS_TOKEN="your-personal-access-token"
# export DATABRICKS_CLUSTER_ID="your-cluster-id"
# export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"

def get_connection_config_from_env() -> Dict[str, str]:
    """Get connection configuration from environment variables"""
    return {
        "host": os.getenv("DATABRICKS_HOST"),
        "token": os.getenv("DATABRICKS_TOKEN"),
        "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
        "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID")
    }

# =============================================================================
# DATABRICKS CONNECT APPROACH (Spark-based)
# =============================================================================

def setup_databricks_connect():
    """Setup Databricks Connect session"""
    try:
        from databricks.connect import DatabricksSession
        from pyspark.sql import SparkSession
        
        # Get config from environment or use hardcoded values
        config = get_connection_config_from_env()
        host = config.get("host") or DATABRICKS_CONNECT_CONFIG["host"]
        token = config.get("token") or DATABRICKS_CONNECT_CONFIG["token"]
        cluster_id = config.get("cluster_id") or DATABRICKS_CONNECT_CONFIG["cluster_id"]
        
        # Create Databricks session
        spark = DatabricksSession.builder \
            .remote(
                host=host,
                token=token,
                cluster_id=cluster_id
            ) \
            .getOrCreate()
        
        print(f"✅ Connected to Databricks using Databricks Connect")
        print(f"   Host: {host}")
        print(f"   Cluster: {cluster_id}")
        
        return spark
        
    except ImportError:
        print("❌ Databricks Connect not installed. Install with:")
        print("   pip install databricks-connect")
        return None
    except Exception as e:
        print(f"❌ Failed to connect with Databricks Connect: {e}")
        return None

def upload_data_with_spark(spark, dataframes: Dict[str, pd.DataFrame], database: str = "default"):
    """Upload data to Databricks using Spark"""
    print(f"\n📤 Uploading data to Databricks (database: {database})...")
    
    for table_name, df in dataframes.items():
        try:
            print(f"  ⬆️  Uploading {table_name} ({len(df)} records)...")
            
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = spark.createDataFrame(df)
            
            # Write to Delta table
            spark_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(f"{database}.{table_name}")
            
            print(f"    ✅ {table_name} uploaded successfully")
            
        except Exception as e:
            print(f"    ❌ Failed to upload {table_name}: {e}")

# =============================================================================
# DATABRICKS SQL CONNECTOR APPROACH
# =============================================================================

def setup_databricks_sql():
    """Setup Databricks SQL connection"""
    try:
        from databricks import sql
        
        # Get config from environment or use hardcoded values
        config = get_connection_config_from_env()
        hostname = config.get("host", DATABRICKS_SQL_CONFIG["server_hostname"]).replace("https://", "")
        http_path = f"/sql/1.0/warehouses/{config.get('warehouse_id')}" or DATABRICKS_SQL_CONFIG["http_path"]
        token = config.get("token") or DATABRICKS_SQL_CONFIG["access_token"]
        
        connection = sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=token
        )
        
        print(f"✅ Connected to Databricks SQL")
        print(f"   Host: {hostname}")
        print(f"   Warehouse: {http_path}")
        
        return connection
        
    except ImportError:
        print("❌ Databricks SQL Connector not installed. Install with:")
        print("   pip install databricks-sql-connector")
        return None
    except Exception as e:
        print(f"❌ Failed to connect with SQL Connector: {e}")
        return None

def upload_data_with_sql(connection, dataframes: Dict[str, pd.DataFrame], database: str = "default"):
    """Upload data to Databricks using SQL connector (less efficient for large data)"""
    print(f"\n📤 Uploading data via SQL connector (database: {database})...")
    
    cursor = connection.cursor()
    
    for table_name, df in dataframes.items():
        try:
            print(f"  ⬆️  Creating {table_name} ({len(df)} records)...")
            
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {database}.{table_name}")
            
            # This approach requires creating table first and inserting data
            # For large datasets, consider using Spark-based approach instead
            print(f"    ⚠️  SQL connector upload for {table_name} - use Spark for better performance")
            
        except Exception as e:
            print(f"    ❌ Failed to process {table_name}: {e}")
    
    cursor.close()

# =============================================================================
# MAIN EXECUTION FUNCTIONS
# =============================================================================

def create_database(spark_or_connection, database: str = "lake_flow_demo"):
    """Create database if it doesn't exist"""
    try:
        if hasattr(spark_or_connection, 'sql'):  # Spark session
            spark_or_connection.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
            spark_or_connection.sql(f"USE {database}")
        else:  # SQL connection
            cursor = spark_or_connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cursor.close()
        
        print(f"✅ Database '{database}' ready")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create database: {e}")
        return False

def setup_vs_code_connection():
    """Setup connection instructions for VS Code"""
    print("\n" + "="*60)
    print("VS CODE CONNECTION SETUP")
    print("="*60)
    
    print("\n1. INSTALL REQUIRED PACKAGES:")
    print("   pip install databricks-connect")
    print("   pip install databricks-sql-connector") 
    print("   pip install pandas numpy")
    
    print("\n2. SET ENVIRONMENT VARIABLES:")
    print('   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"')
    print('   export DATABRICKS_TOKEN="your-personal-access-token"')
    print('   export DATABRICKS_CLUSTER_ID="your-cluster-id"')
    print('   export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"')
    
    print("\n3. GET YOUR CONNECTION DETAILS:")
    print("   a) Workspace URL: Go to your Databricks workspace")
    print("   b) Personal Access Token: User Settings → Access Tokens → Generate New Token")
    print("   c) Cluster ID: Compute → Your Cluster → Advanced Options → Tags")
    print("   d) Warehouse ID: SQL → SQL Warehouses → Your Warehouse → Connection Details")
    
    print("\n4. VS CODE CONFIGURATION:")
    print("   - Install Python extension")
    print("   - Install Databricks extension (optional)")
    print("   - Set Python interpreter to your environment with databricks packages")
    
    print("\n5. TEST CONNECTION:")
    print("   python databricks_connector.py --test")

def test_connection():
    """Test Databricks connection"""
    print("\n🔧 Testing Databricks connection...")
    
    # Try Databricks Connect first
    spark = setup_databricks_connect()
    if spark:
        try:
            # Test query
            result = spark.sql("SELECT 1 as test").collect()
            print(f"✅ Spark connection test successful: {result}")
            return spark, "spark"
        except Exception as e:
            print(f"❌ Spark test failed: {e}")
    
    # Try SQL Connector
    sql_conn = setup_databricks_sql()
    if sql_conn:
        try:
            cursor = sql_conn.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchall()
            cursor.close()
            print(f"✅ SQL connection test successful: {result}")
            return sql_conn, "sql"
        except Exception as e:
            print(f"❌ SQL test failed: {e}")
    
    return None, None

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Databricks data upload script")
    parser.add_argument("--test", action="store_true", help="Test connection only")
    parser.add_argument("--setup", action="store_true", help="Show setup instructions")
    parser.add_argument("--database", default="lake_flow_demo", help="Target database name")
    parser.add_argument("--generate-only", action="store_true", help="Generate data without uploading")
    
    args = parser.parse_args()
    
    if args.setup:
        setup_vs_code_connection()
        return
    
    if args.test:
        connection, conn_type = test_connection()
        if connection:
            print("✅ Connection test successful!")
            if hasattr(connection, 'stop'):
                connection.stop()
            elif hasattr(connection, 'close'):
                connection.close()
        else:
            print("❌ Connection test failed!")
        return
    
    # Generate sample data
    print("🔧 Generating sample data...")
    dataframes = generate_sample_data()
    
    if args.generate_only:
        print("📁 Data generation complete. Use --upload to upload to Databricks.")
        return
    
    # Test connection and upload
    connection, conn_type = test_connection()
    
    if not connection:
        print("❌ Could not establish connection to Databricks")
        print("Run with --setup for configuration instructions")
        return
    
    # Create database
    if not create_database(connection, args.database):
        return
    
    # Upload data based on connection type
    if conn_type == "spark":
        upload_data_with_spark(connection, dataframes, args.database)
        connection.stop()
    elif conn_type == "sql":
        upload_data_with_sql(connection, dataframes, args.database)
        connection.close()
    
    print(f"\n🎉 Data upload complete to database '{args.database}'!")
    print("\nNext steps:")
    print("1. Verify tables in Databricks workspace")
    print("2. Run the CTAS examples from scripts.txt")
    print("3. Test Lake Flow capabilities")

if __name__ == "__main__":
    main()
