# Databricks Lake Flow - CTAS Examples & Data Generation

This repository contains examples of **CREATE TABLE AS SELECT (CTAS)** statements for **Databricks Lake Flow**, along with Python scripts to generate sample data and connect to Databricks remotely from VS Code.

## Key Features

- **Organized Structure**: Separate directories for SQL, Python, and documentation
- **Comprehensive CTAS Examples**: 7 different Lake Flow patterns and capabilities
- **Python Integration**: Generate data and connect to Databricks from VS Code
- **Ready-to-Use**: Complete setup with environment templates and configurations
- **Extensive Documentation**: Detailed explanations and best practices
- **Testing Framework**: Connection tests and validation scripts

## Quick Reference - New Structure

| **What you want to do** | **Command** |
|--------------------------|-------------|
| **Setup environment** | `python python/setup.py` |
| **Test connection** | `python python/simple_connection_test.py` |
| **Generate data** | `python python/generate_data.py` |
| **Upload to Databricks** | `python python/databricks_connector.py` |
| **Run SQL examples** | Open `sql/scripts.sql` in Databricks |
| **Interactive testing** | Upload `notebooks/databricks_lake_flow_ctas_examples.ipynb` to Databricks |
| **Read documentation** | Check `docs/README-overview.md` |
| **Validate project** | `python validate_structure.py` |

## Repository Structure

```
lake-flow/
├── .env                          # Environment variables (your actual credentials)
├── .env.template                 # Environment template for setup
├── .gitignore                    # Git ignore rules
├── .vscode/                      # VS Code configuration
│   ├── settings.json            # Python interpreter and editor settings
│   └── launch.json              # Debug configurations
├── README.md                     # Main project documentation (this file)
├── requirements.txt              # Python dependencies
├── docs/                         # Documentation
│   └── README-overview.md       # Detailed project overview and setup guide
├── notebooks/                    # Jupyter Notebooks
│   └── databricks_lake_flow_ctas_examples.ipynb # Interactive CTAS examples
├── python/                       # Python Utilities & Scripts
│   ├── __init__.py              # Python package initialization
│   ├── databricks_connector.py  # Main connection and upload script
│   ├── generate_data.py         # Python data generation (equivalent to SQL)
│   ├── setup.py                 # Environment setup and configuration script
│   ├── simple_connection_test.py # Simple connection test examples
│   └── venv/                    # Virtual environment (isolated packages)
├── sql/                          # SQL Scripts & CTAS Examples
│   ├── scripts.sql              # Main CTAS examples and Lake Flow patterns
│   └── data_generation.sql      # SQL scripts to generate sample data
├── scripts.txt                   # Original scripts reference
├── installed_packages.txt        # Package installation log
└── validate_structure.py         # Project validation utility
```

### Clean & Organized Structure Benefits

- **Easy Navigation**: Find SQL and Python files in dedicated directories
- **Modular Design**: Import Python utilities as a package (`from python.generate_data import ...`)
- **Centralized Documentation**: All documentation files in the `docs/` folder
- **Security**: Clear separation of configuration templates and actual credentials
- **Scalability**: Easy to add new SQL scripts, Python modules, or documentation
- **No Duplicates**: Single source of truth for each file - no duplicate files in different locations
- **Proper Isolation**: Virtual environment located in `python/venv/` for complete package isolation
- **VS Code Ready**: Configured for optimal development experience with correct interpreter paths

## Quick Start

### 1. Setup Environment

```bash
# Clone or download this repository
cd lake-flow

# Validate the project structure (optional)
python3 validate_structure.py

# Run setup script
python python/setup.py

# Activate virtual environment
source python/venv/bin/activate  # On Windows: python\venv\Scripts\activate

# Install dependencies (done automatically by setup script)
# pip install -r requirements.txt

# Validate again with dependencies installed
python validate_structure.py
```

### 2. Configure Databricks Connection

```bash
# Copy environment template
cp .env.template .env

# Edit .env with your Databricks credentials
# Get these from your Databricks workspace (see setup guide below)
```

Example `.env` file:
```env
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef...
DATABRICKS_CLUSTER_ID=0123-456789-abc123
DATABRICKS_WAREHOUSE_ID=1234567890abcdef
DATABASE_NAME=lake_flow_demo
```

### 3. Test Connection

```bash
# Test your Databricks connection
python python/databricks_connector.py --test

# Or use the simple test
python python/simple_connection_test.py
```

### 4. Generate and Upload Data

```bash
# Generate sample data and upload to Databricks
python python/databricks_connector.py

# Or generate locally only (CSV files)
python python/generate_data.py
```

### 5. Run CTAS Examples

Open `sql/scripts.sql` in Databricks and run the CTAS examples to see Lake Flow capabilities in action.

### 6. Interactive Testing with Notebooks

For a hands-on interactive experience:

1. **Upload the notebook**: Go to your Databricks workspace and upload `notebooks/databricks_lake_flow_ctas_examples.ipynb`
2. **Attach to cluster**: Connect the notebook to a Databricks cluster (Runtime 13.3+ recommended)
3. **Run cells sequentially**: The notebook includes:
   - Complete setup with sample data generation
   - All 7 CTAS examples with detailed explanations
   - Interactive queries you can modify and test
   - Progressive learning from basic to advanced concepts

The notebook is self-contained and includes all necessary sample data, so you can immediately start testing Lake Flow capabilities.

## Generated Sample Data

The scripts generate realistic sample data across multiple tables:

| Table | Records | Description |
|-------|---------|-------------|
| `raw_orders` | 100 | Order transactions with regions, categories, amounts |
| `customers` | 50 | Customer profiles with demographics and preferences |
| `orders` | 100 | Enhanced orders with customer metadata |
| `product_inventory_raw` | 100 | Inventory data for streaming examples |
| `events` | 100 | User events for analytics (clicks, purchases, etc.) |
| `mock_transaction_files` | 100 | Transaction data for bronze/silver/gold pipeline |
| `products` | 200 | Product catalog with categories and pricing |
| `merchants` | 100 | Merchant information with locations |
| `user_sessions` | 200 | Web session data for analytics |

## CTAS Examples Included

### 1. Basic CTAS with Delta Table
- Auto-optimization settings
- Basic aggregations
- Delta Lake properties

### 2. Partitioned Tables with Advanced Features
- Partitioning strategies
- Window functions
- JSON data extraction
- Customer segmentation

### 3. Streaming Tables with Change Data Capture
- Real-time data processing
- Change data feed
- Surrogate key generation

### 4. Materialized Views with Complex Aggregations
- Statistical functions (percentiles, stddev)
- Array aggregations
- Conditional aggregations

### 5. Liquid Clustering (Databricks Runtime 13.3+)
- Advanced clustering
- Performance optimization
- Analytics window functions

### 6. Multi-Hop Architecture (Bronze/Silver/Gold)
- Data quality and cleansing
- Geospatial processing
- Data validation patterns

### 7. Advanced Features
- Generated columns
- Column mapping
- Deletion vectors
- Time travel capabilities

## Connection Methods

This repository supports multiple ways to connect to Databricks:

### Method 1: Databricks Connect (Recommended for Spark workloads)
```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder \
    .remote(
        host="your-workspace.cloud.databricks.com",
        token="your-token",
        cluster_id="your-cluster-id"
    ) \
    .getOrCreate()
```

### Method 2: Databricks SQL Connector (Recommended for SQL workloads)
```python
from databricks import sql

connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/your-warehouse-id",
    access_token="your-token"
)
```

## Getting Databricks Credentials

### 1. Workspace URL
- Go to your Databricks workspace
- Copy URL (e.g., `https://your-workspace.cloud.databricks.com`)
- Remove `https://` for the `.env` file

### 2. Personal Access Token
- In Databricks: **User Settings** → **Access Tokens**
- Click **Generate New Token**
- Copy the token (save it - you won't see it again!)

### 3. Cluster ID (for Databricks Connect)
- Go to **Compute** → Select your cluster
- In the URL or cluster details, find the cluster ID
- Format: `0123-456789-abc123`

### 4. SQL Warehouse ID (for SQL Connector)
- Go to **SQL** → **SQL Warehouses**
- Select your warehouse → **Connection Details**
- Extract warehouse ID from the HTTP path
- Format: `1234567890abcdef`

## VS Code Setup

### Extensions (Recommended)
- **Python** - Microsoft
- **Databricks** - Databricks (optional)
- **SQL Tools** - mtxr (optional)

### Debug Configuration
The repository includes VS Code launch configurations for:
- Running data generation
- Testing Databricks connection
- Uploading data to Databricks

Press **F5** and select the appropriate configuration.

## Usage Examples

### Generate Data Locally
```bash
# Generate all sample data as pandas DataFrames
python -c "from python.generate_data import main; data = main()"

# Save to CSV files for inspection
python python/generate_data.py
# Follow prompts to save CSV files
```

### Upload Specific Database
```bash
# Upload to custom database
python python/databricks_connector.py --database my_custom_db

# Generate without uploading
python python/databricks_connector.py --generate-only
```

### Test Individual Components
```bash
# Test just the connection
python python/databricks_connector.py --test

# Show setup instructions
python python/databricks_connector.py --setup
```

## Lake Flow Features Demonstrated

- **Auto Optimize**: Automatic file optimization and compaction
- **Liquid Clustering**: Advanced data clustering for performance
- **Change Data Feed**: Track changes over time
- **Time Travel**: Query historical versions of data
- **Data Quality**: Constraints and expectations
- **Streaming**: Real-time data processing
- **Multi-hop Architecture**: Bronze → Silver → Gold patterns
- **Geospatial**: Location data processing
- **JSON Processing**: Extract and transform JSON data

## Performance Optimization Tips

1. **Partitioning**: Use date/region columns for frequently filtered data
2. **Liquid Clustering**: Better than Z-ordering for evolving data
3. **Auto Optimize**: Enable for automatic performance tuning
4. **Bloom Filters**: For high-cardinality point lookups
5. **File Sizes**: Target 128MB - 1GB per file
6. **Statistics**: Run `ANALYZE TABLE` for better query planning

## Troubleshooting

### Connection Issues
```bash
# Check environment variables
python -c "import os; print(os.getenv('DATABRICKS_HOST'))"

# Test basic connectivity
curl -H "Authorization: Bearer $DATABRICKS_TOKEN" \
     https://$DATABRICKS_HOST/api/2.0/clusters/list
```

### Package Installation Issues
```bash
# Update pip first
pip install --upgrade pip

# Install with verbose output
pip install -v databricks-connect

# Use conda if pip fails
conda install -c conda-forge pandas numpy
```

### Import Errors
- Ensure virtual environment is activated
- Check Python version (3.8+ required)
- Verify package installation: `pip list | grep databricks`

## Additional Resources

- **[Detailed Documentation](docs/README-overview.md)** - Overview of all features
- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Connect Guide](https://docs.databricks.com/dev-tools/databricks-connect.html)
- [Lake Flow Best Practices](https://docs.databricks.com/lakehouse/lake-flow.html)

## Using as a Python Package

With the organized structure, you can now import utilities as a proper Python package:

```python
# Import specific functions
from python.databricks_connector import DatabricksConnector, upload_data_to_databricks
from python.generate_data import generate_all_data, save_data_to_csv
from python.simple_connection_test import test_sql_connection, test_connect_connection

# Use the connector
connector = DatabricksConnector()
connector.test_connection()

# Generate and upload data
data = generate_all_data()
upload_data_to_databricks(data)
```

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve these examples.

## License

This project is provided as-is for educational and demonstration purposes.
