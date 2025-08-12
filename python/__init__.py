# Databricks Lake Flow Python Package
"""
Python utilities for Databricks Lake Flow CTAS examples.

This package contains:
- databricks_connector.py: Connection utilities for Databricks
- generate_data.py: Python data generation equivalent to SQL scripts
- setup.py: Environment setup and configuration
- simple_connection_test.py: Basic connection testing
"""

__version__ = "1.0.0"
__author__ = "Databricks Lake Flow Team"

# Import main functions for easy access
try:
    from .databricks_connector import DatabricksConnector, upload_data_to_databricks
    from .generate_data import generate_all_data, save_data_to_csv
    from .simple_connection_test import test_sql_connection, test_connect_connection
except ImportError:
    # Handle case where dependencies might not be installed
    pass
