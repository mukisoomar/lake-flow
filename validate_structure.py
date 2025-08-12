#!/usr/bin/env python3
"""
Validation script to test the new directory structure.
Run this to verify everything is working correctly after reorganization.
"""

import sys
import os
from pathlib import Path

def validate_structure():
    """Validate that all expected files are in the correct locations."""
    
    print("🔍 Validating Databricks Lake Flow Project Structure...")
    print("=" * 60)
    
    # Define expected structure
    expected_files = {
        "Root Files": [
            "README.md",
            "requirements.txt", 
            ".env.template",
            ".gitignore"
        ],
        "SQL Directory": [
            "sql/scripts.sql",
            "sql/data_generation.sql"
        ],
        "Python Directory": [
            "python/__init__.py",
            "python/databricks_connector.py",
            "python/generate_data.py", 
            "python/setup.py",
            "python/simple_connection_test.py"
        ],
        "Documentation Directory": [
            "docs/README-overview.md"
        ]
    }
    
    project_root = Path(__file__).parent
    all_good = True
    
    for category, files in expected_files.items():
        print(f"\n📁 {category}:")
        for file_path in files:
            full_path = project_root / file_path
            if full_path.exists():
                print(f"  ✅ {file_path}")
            else:
                print(f"  ❌ {file_path} - MISSING")
                all_good = False
    
    print("\n" + "=" * 60)
    
    if all_good:
        print("🎉 SUCCESS: All files are in the correct locations!")
        print("\n🚀 Next steps:")
        print("  1. Run: python python/setup.py")
        print("  2. Copy .env.template to .env and configure credentials")
        print("  3. Test connection: python python/simple_connection_test.py")
        print("  4. Generate data: python python/generate_data.py")
        print("  5. Upload to Databricks: python python/databricks_connector.py")
        return True
    else:
        print("❌ VALIDATION FAILED: Some files are missing!")
        print("Please check the file structure and ensure all files were moved correctly.")
        return False

def test_python_imports():
    """Test that Python imports work with the new structure."""
    
    print("\n🐍 Testing Python Package Imports...")
    print("=" * 60)
    
    try:
        # Test if we can import from the python package
        sys.path.insert(0, str(Path(__file__).parent))
        
        # Test basic imports (without actually executing functions that need credentials)
        print("  📦 Testing python package import...")
        import python
        print("  ✅ python package imported successfully")
        
        print("  🔧 Testing utility imports...")
        from python.generate_data import generate_all_data
        print("  ✅ generate_data.generate_all_data imported")
        
        from python.databricks_connector import DatabricksConnector
        print("  ✅ databricks_connector.DatabricksConnector imported")
        
        print("\n🎉 All imports successful!")
        return True
        
    except ImportError as e:
        print(f"  ❌ Import failed: {e}")
        return False
    except Exception as e:
        print(f"  ⚠️  Unexpected error: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Databricks Lake Flow - Structure Validation")
    print("This script validates the  project structure.\n")
    
    # Run validations
    structure_ok = validate_structure()
    imports_ok = test_python_imports()
    
    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY:")
    print(f"  File Structure: {'✅ PASS' if structure_ok else '❌ FAIL'}")
    print(f"  Python Imports: {'✅ PASS' if imports_ok else '❌ FAIL'}")
    
    if structure_ok and imports_ok:
        print("\n🎯 PROJECT READY TO USE!")
        exit_code = 0
    else:
        print("\n🚨 PLEASE FIX ISSUES BEFORE PROCEEDING")
        exit_code = 1
    
    sys.exit(exit_code)
