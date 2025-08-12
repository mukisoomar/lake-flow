#!/usr/bin/env python3
"""
Simple Databricks Connection Example
===================================
Minimal example showing how to connect to Databricks from VS Code
"""

# Method 1: Using databricks-sql-connector (Recommended for SQL operations)
def connect_with_sql_connector():
    """Simple SQL connector example"""
    from databricks import sql
    import os
    
    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        access_token=os.getenv("DATABRICKS_TOKEN")
    )
    
    cursor = connection.cursor()
    cursor.execute("SELECT 'Hello from Databricks!' as message")
    result = cursor.fetchall()
    print("SQL Connector Result:", result)
    
    cursor.close()
    connection.close()

# Method 2: Using databricks-connect (For Spark operations)
def connect_with_databricks_connect():
    """Simple Databricks Connect example"""
    from databricks.connect import DatabricksSession
    import os
    
    spark = DatabricksSession.builder \
        .remote(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN"),
            cluster_id=os.getenv("DATABRICKS_CLUSTER_ID")
        ) \
        .getOrCreate()
    
    df = spark.sql("SELECT 'Hello from Spark!' as message")
    df.show()
    
    spark.stop()

# Method 3: Environment setup
def setup_environment():
    """Setup environment variables from .env file"""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Loaded environment variables from .env file")
    except ImportError:
        print("⚠️  python-dotenv not installed. Set environment variables manually:")
        print("   export DATABRICKS_HOST='your-workspace.cloud.databricks.com'")
        print("   export DATABRICKS_TOKEN='your-token'")
        print("   export DATABRICKS_CLUSTER_ID='your-cluster-id'")
        print("   export DATABRICKS_WAREHOUSE_ID='your-warehouse-id'")

if __name__ == "__main__":
    setup_environment()
    
    print("Testing Databricks connections...")
    
    try:
        print("\n1. Testing SQL Connector...")
        connect_with_sql_connector()
    except Exception as e:
        print(f"SQL Connector failed: {e}")
    
    try:
        print("\n2. Testing Databricks Connect...")
        connect_with_databricks_connect()
    except Exception as e:
        print(f"Databricks Connect failed: {e}")
