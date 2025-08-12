#!/usr/bin/env python3
"""
Databricks Lake Flow - Setup and Configuration Script
====================================================
This script helps set up the environment and provides connection examples.
"""

import os
import sys
import subprocess
from pathlib import Path

def install_requirements():
    """Install required Python packages"""
    print("📦 Installing required packages...")
    
    # Get the project root directory (parent of the python directory)
    project_root = Path(__file__).parent.parent
    requirements_file = project_root / "requirements.txt"
    
    if not requirements_file.exists():
        print(f"❌ requirements.txt not found at {requirements_file}")
        return False
    
    # Get the virtual environment's Python executable (in python directory)
    venv_path = Path(__file__).parent / "venv"
    if os.name == 'nt':  # Windows
        venv_python = venv_path / "Scripts" / "python.exe"
        venv_pip = venv_path / "Scripts" / "pip.exe"
    else:  # Unix/Linux/macOS
        venv_python = venv_path / "bin" / "python"
        venv_pip = venv_path / "bin" / "pip"
    
    # Check if virtual environment exists
    if not venv_python.exists():
        print(f"❌ Virtual environment not found at {venv_path}")
        print("   Please run the virtual environment setup first.")
        return False
    
    try:
        # Use the virtual environment's pip to install packages
        subprocess.check_call([
            str(venv_python), "-m", "pip", "install", "-r", str(requirements_file)
        ])
        print("✅ Packages installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install packages: {e}")
        return False

def create_env_file():
    """Create .env file template"""
    env_content = """# Databricks Connection Configuration
# ===================================
# Copy this file to .env and fill in your actual values

# Your Databricks workspace URL (without https://)
DATABRICKS_HOST=your-workspace.cloud.databricks.com

# Personal Access Token (generate from User Settings → Access Tokens)
DATABRICKS_TOKEN=your-personal-access-token

# Cluster ID for Databricks Connect (get from Compute → Your Cluster)
DATABRICKS_CLUSTER_ID=your-cluster-id

# SQL Warehouse ID for SQL Connector (get from SQL → SQL Warehouses)
DATABRICKS_WAREHOUSE_ID=your-warehouse-id

# Optional: Organization ID (sometimes required)
DATABRICKS_ORG_ID=your-org-id

# Database name to use for this demo
DATABASE_NAME=lake_flow_demo
"""
    
    # Get the project root directory (parent of the python directory)
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env.template"
    
    # Check if .env.template already exists
    if env_file.exists():
        print(f"✅ .env.template already exists at {env_file}")
        return True
    
    try:
        env_file.write_text(env_content)
        print(f"✅ Created {env_file} - copy to .env and update with your values")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env.template: {e}")
        return False

def create_vs_code_config():
    """Create VS Code configuration"""
    # Get the project root directory (parent of the python directory)
    project_root = Path(__file__).parent.parent
    vscode_dir = project_root / ".vscode"
    vscode_dir.mkdir(exist_ok=True)
    
    # Settings for Python
    settings = {
        "python.defaultInterpreterPath": "./python/venv/bin/python",
        "python.terminal.activateEnvironment": True,
        "files.associations": {
            "*.sql": "sql",
            "*.py": "python"
        },
        "python.linting.enabled": True,
        "python.linting.pylintEnabled": True,
        "python.formatting.provider": "black"
    }
    
    import json
    (vscode_dir / "settings.json").write_text(json.dumps(settings, indent=2))
    
    # Launch configuration for debugging
    launch_config = {
        "version": "0.2.0",
        "configurations": [
            {
                "name": "Python: Generate Data",
                "type": "python",
                "request": "launch",
                "program": "${workspaceFolder}/python/generate_data.py",
                "console": "integratedTerminal",
                "justMyCode": True
            },
            {
                "name": "Python: Test Databricks Connection",
                "type": "python",
                "request": "launch",
                "program": "${workspaceFolder}/python/databricks_connector.py",
                "args": ["--test"],
                "console": "integratedTerminal",
                "justMyCode": True
            },
            {
                "name": "Python: Upload to Databricks",
                "type": "python",
                "request": "launch",
                "program": "${workspaceFolder}/python/databricks_connector.py",
                "args": ["--database", "lake_flow_demo"],
                "console": "integratedTerminal",
                "justMyCode": True
            }
        ]
    }
    
    (vscode_dir / "launch.json").write_text(json.dumps(launch_config, indent=2))
    
    print("✅ Created VS Code configuration files")
    return True

def setup_virtual_environment():
    """Setup Python virtual environment"""
    print("🐍 Setting up Python virtual environment...")
    
    # Create virtual environment in the python directory
    python_dir = Path(__file__).parent
    venv_path = python_dir / "venv"
    
    try:
        # Create virtual environment in python directory
        subprocess.check_call([sys.executable, "-m", "venv", str(venv_path)])
        
        # Get the activation script path
        if os.name == 'nt':  # Windows
            activate_script = "python\\venv\\Scripts\\activate"
            pip_path = "python\\venv\\Scripts\\pip"
        else:  # Unix/Linux/macOS
            activate_script = "python/venv/bin/activate"
            pip_path = "python/venv/bin/pip"
        
        print(f"✅ Virtual environment created at {venv_path}!")
        print(f"   Activate with: source {activate_script}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to create virtual environment: {e}")
        return False

def show_databricks_setup_guide():
    """Show detailed setup guide"""
    guide = """
🔧 DATABRICKS SETUP GUIDE
=========================

1. CREATE DATABRICKS WORKSPACE
   - Go to https://databricks.com/
   - Sign up for free community edition or use your organization's workspace
   - Note your workspace URL (e.g., https://your-workspace.cloud.databricks.com)

2. GENERATE PERSONAL ACCESS TOKEN
   - In Databricks workspace: User Settings → Access Tokens
   - Click "Generate New Token"
   - Copy the token (you won't see it again!)
   - Paste in .env file as DATABRICKS_TOKEN

3. GET CLUSTER ID (for Databricks Connect)
   - Go to Compute → Create or select existing cluster
   - Click on cluster name → Configuration → Advanced Options → Tags
   - Look for "cluster-id" or copy from URL
   - Paste in .env file as DATABRICKS_CLUSTER_ID

4. GET SQL WAREHOUSE ID (for SQL Connector)
   - Go to SQL → SQL Warehouses
   - Create or select existing warehouse
   - Click on warehouse name → Connection Details
   - Copy "HTTP Path" - extract warehouse ID from the path
   - Paste in .env file as DATABRICKS_WAREHOUSE_ID

5. INSTALL DATABRICKS CLI (Optional)
   pip install databricks-cli
   databricks configure --token

6. TEST YOUR CONNECTION
   python python/databricks_connector.py --test

🎯 QUICK START COMMANDS
======================
1. python python/setup.py              # Run this setup
2. source python/venv/bin/activate     # Activate virtual environment  
3. pip install -r requirements.txt     # Install packages (done automatically)
4. cp .env.template .env               # Copy environment template
5. # Edit .env with your credentials
6. python python/databricks_connector.py --test  # Test connection
7. python python/databricks_connector.py      # Generate and upload data
8. # Run CTAS examples from sql/scripts.sql in Databricks
"""
    
    print(guide)

def main():
    """Main setup function"""
    print("🚀 Databricks Lake Flow Setup")
    print("=" * 40)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required")
        return
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    
    # Setup steps
    steps = [
        ("Creating virtual environment", setup_virtual_environment),
        ("Creating environment template", create_env_file),
        ("Creating VS Code configuration", create_vs_code_config),
        ("Installing requirements", install_requirements),
    ]
    
    for step_name, step_func in steps:
        print(f"\n📋 {step_name}...")
        if not step_func():
            print(f"❌ Failed: {step_name}")
            return
    
    print("\n🎉 Setup complete!")
    print("\nNext steps:")
    print("1. Copy .env.template to .env")
    print("2. Fill in your Databricks credentials in .env")
    print("3. Activate virtual environment: source python/venv/bin/activate")
    print("4. Test connection: python python/databricks_connector.py --test")
    print("5. Generate and upload data: python python/databricks_connector.py")
    
    show_guide = input("\nShow detailed Databricks setup guide? (y/n): ").lower().strip() == 'y'
    if show_guide:
        show_databricks_setup_guide()

if __name__ == "__main__":
    main()
