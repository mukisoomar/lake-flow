#!/usr/bin/env python3
"""
Databricks Lake Flow - Sample Data Generation (Python)
=====================================================
This Python script generates the same sample data as data_generation.sql
but using Python/PySpark for remote execution from VS Code.

Requirements:
- databricks-connect or databricks-sql-connector
- pandas, numpy (for data generation)
- spark session (if using databricks-connect)
"""

import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json

# Configuration
SAMPLE_SIZE = 100
CUSTOMER_COUNT = 50
PRODUCT_COUNT = 200
MERCHANT_COUNT = 100
SESSION_COUNT = 200

def generate_random_choice(choices: List[str]) -> str:
    """Generate random choice from list"""
    return random.choice(choices)

def generate_random_date(start_date: datetime, days_range: int) -> datetime:
    """Generate random date within range"""
    return start_date + timedelta(days=random.randint(0, days_range))

def generate_raw_orders(size: int = SAMPLE_SIZE) -> pd.DataFrame:
    """Generate raw orders table"""
    print(f"Generating {size} raw orders...")
    
    regions = ['North', 'South', 'East', 'West', 'Central']
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']
    
    data = []
    for i in range(1, size + 1):
        order_date = generate_random_date(datetime(2024, 1, 1), 300)
        data.append({
            'order_id': i,
            'customer_id': random.randint(1, CUSTOMER_COUNT),
            'region': generate_random_choice(regions),
            'product_category': generate_random_choice(categories),
            'order_date': order_date,
            'order_amount': round(random.uniform(10, 1010), 2),
            'created_at': datetime.now()
        })
    
    return pd.DataFrame(data)

def generate_customers(size: int = CUSTOMER_COUNT) -> pd.DataFrame:
    """Generate customers table"""
    print(f"Generating {size} customers...")
    
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ']
    acquisition_channels = ['online', 'referral', 'social', 'direct']
    communication_prefs = ['email', 'sms', 'phone']
    newsletter_freq = ['weekly', 'monthly', 'quarterly']
    
    data = []
    for i in range(1, size + 1):
        birth_date = datetime.now() - timedelta(days=random.randint(6570, 17520))  # 18-48 years
        reg_date = datetime.now() - timedelta(days=random.randint(365, 2190))  # 1-6 years ago
        
        # Generate JSON metadata
        customer_metadata = {
            "acquisition_channel": generate_random_choice(acquisition_channels),
            "preferences": {
                "communication": generate_random_choice(communication_prefs)
            }
        }
        
        preferences = {
            "communication_preference": generate_random_choice(communication_prefs),
            "newsletter_frequency": generate_random_choice(newsletter_freq)
        }
        
        data.append({
            'customer_id': i,
            'first_name': f'Customer_{i}',
            'last_name': f'LastName_{i}',
            'email': f'customer{i}@email.com',
            'phone': f'+1-555-{i:04d}',
            'birth_date': birth_date,
            'registration_date': reg_date,
            'total_spent': round(random.uniform(500, 50500), 2),
            'total_orders': random.randint(1, 50),
            'address_city': generate_random_choice(cities),
            'address_state': generate_random_choice(states),
            'address_country': 'USA',
            'latitude': round(random.uniform(-90, 90), 6),
            'longitude': round(random.uniform(-180, 180), 6),
            'customer_metadata': json.dumps(customer_metadata),
            'preferences': json.dumps(preferences),
            'email_opt_in': random.random() > 0.2,
            'sms_opt_in': random.random() > 0.7,
            'marketing_opt_in': random.random() > 0.4
        })
    
    return pd.DataFrame(data)

def generate_orders(raw_orders_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Generate enhanced orders table with joins"""
    print("Generating enhanced orders with customer metadata...")
    
    # Merge raw orders with customers
    orders = raw_orders_df.merge(
        customers_df[['customer_id', 'customer_metadata']], 
        on='customer_id', 
        how='inner'
    )
    
    # Add product_id
    orders['product_id'] = [random.randint(1, PRODUCT_COUNT) for _ in range(len(orders))]
    
    return orders

def generate_product_inventory(size: int = SAMPLE_SIZE) -> pd.DataFrame:
    """Generate product inventory for streaming"""
    print(f"Generating {size} product inventory records...")
    
    data = []
    for i in range(1, size + 1):
        available_qty = random.randint(0, 1000)
        reserved_qty = random.randint(0, 100)
        
        data.append({
            'product_id': i,
            'warehouse_id': random.randint(1, 10),
            'available_quantity': available_qty,
            'reserved_quantity': reserved_qty,
            'total_quantity': available_qty + reserved_qty,
            'last_updated': datetime.now() + timedelta(seconds=random.randint(-86400, 86400))
        })
    
    return pd.DataFrame(data)

def generate_events(size: int = SAMPLE_SIZE) -> pd.DataFrame:
    """Generate events table for analytics"""
    print(f"Generating {size} events...")
    
    event_types = ['page_view', 'click', 'purchase', 'signup', 'logout']
    source_systems = ['web', 'mobile_app', 'api', 'batch']
    device_types = ['desktop', 'mobile', 'tablet']
    
    data = []
    for i in range(1, size + 1):
        event_timestamp = datetime.now() - timedelta(seconds=random.randint(0, 604800))  # Last 7 days
        
        data.append({
            'event_id': f'event_{i}',
            'event_type': generate_random_choice(event_types),
            'source_system': generate_random_choice(source_systems),
            'user_id': random.randint(1, 100),
            'session_id': f'session_{random.randint(1, 50)}',
            'device_type': generate_random_choice(device_types),
            'event_timestamp': event_timestamp,
            'duration_ms': random.randint(100, 5100),
            'error_code': f'ERROR_{random.randint(1, 5)}' if random.random() > 0.9 else None,
            'success_flag': random.random() > 0.1
        })
    
    return pd.DataFrame(data)

def generate_transactions(size: int = SAMPLE_SIZE) -> pd.DataFrame:
    """Generate transaction data for bronze/silver/gold pattern"""
    print(f"Generating {size} transactions...")
    
    currencies = ['USD', 'EUR', 'GBP', 'CAD']
    
    data = []
    for i in range(1, size + 1):
        transaction_timestamp = datetime.now() - timedelta(seconds=random.randint(0, 2592000))  # Last 30 days
        
        data.append({
            'transaction_id': f'txn_{i:06d}',
            'customer_id': random.randint(1, CUSTOMER_COUNT),
            'merchant_id': random.randint(1, MERCHANT_COUNT),
            'amount': str(round(random.uniform(1, 2001), 2)),  # String to simulate raw JSON
            'currency_code': generate_random_choice(currencies),
            'transaction_timestamp': transaction_timestamp,
            'latitude': round(random.uniform(-90, 90), 6),
            'longitude': round(random.uniform(-180, 180), 6)
        })
    
    return pd.DataFrame(data)

def generate_products(size: int = PRODUCT_COUNT) -> pd.DataFrame:
    """Generate products catalog"""
    print(f"Generating {size} products...")
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']
    brands = ['Brand_A', 'Brand_B', 'Brand_C', 'Brand_D']
    
    data = []
    for i in range(1, size + 1):
        data.append({
            'product_id': i,
            'product_name': f'Product_{i}',
            'category': generate_random_choice(categories),
            'brand': generate_random_choice(brands),
            'unit_price': round(random.uniform(10, 510), 2),
            'stock_quantity': random.randint(0, 1000),
            'is_active': random.random() > 0.8,
            'created_at': datetime.now()
        })
    
    return pd.DataFrame(data)

def generate_merchants(size: int = MERCHANT_COUNT) -> pd.DataFrame:
    """Generate merchants table"""
    print(f"Generating {size} merchants...")
    
    categories = ['Retail', 'Food', 'Services', 'Online', 'Gas Station']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ']
    
    data = []
    for i in range(1, size + 1):
        reg_date = datetime.now() - timedelta(days=random.randint(0, 3650))  # Last 10 years
        
        data.append({
            'merchant_id': i,
            'merchant_name': f'Merchant_{i}',
            'merchant_category': generate_random_choice(categories),
            'city': generate_random_choice(cities),
            'state': generate_random_choice(states),
            'latitude': round(random.uniform(-90, 90), 6),
            'longitude': round(random.uniform(-180, 180), 6),
            'registration_date': reg_date,
            'is_active': random.random() > 0.95
        })
    
    return pd.DataFrame(data)

def generate_user_sessions(size: int = SESSION_COUNT) -> pd.DataFrame:
    """Generate user sessions for web analytics"""
    print(f"Generating {size} user sessions...")
    
    device_types = ['desktop', 'mobile', 'tablet']
    browsers = ['Chrome', 'Safari', 'Firefox', 'Edge']
    operating_systems = ['Windows', 'macOS', 'iOS', 'Android', 'Linux']
    traffic_sources = ['organic', 'paid', 'direct', 'social', 'referral']
    
    data = []
    for i in range(1, size + 1):
        session_start = datetime.now() - timedelta(seconds=random.randint(0, 2592000))  # Last 30 days
        session_duration = random.randint(30, 1830)
        session_end = session_start + timedelta(seconds=session_duration)
        
        data.append({
            'session_id': f'session_{i}',
            'user_id': random.randint(1, 100),
            'session_start': session_start,
            'session_end': session_end,
            'device_type': generate_random_choice(device_types),
            'browser': generate_random_choice(browsers),
            'operating_system': generate_random_choice(operating_systems),
            'traffic_source': generate_random_choice(traffic_sources),
            'page_views': random.randint(1, 20),
            'session_duration_seconds': session_duration
        })
    
    return pd.DataFrame(data)

def validate_data(dataframes: Dict[str, pd.DataFrame]) -> None:
    """Validate generated data"""
    print("\n" + "="*50)
    print("DATA VALIDATION SUMMARY")
    print("="*50)
    
    for name, df in dataframes.items():
        print(f"{name}: {len(df)} records")
        
        # Check for nulls in key columns
        if name == 'customers':
            null_emails = df['email'].isnull().sum()
            print(f"  - Customers with valid emails: {len(df) - null_emails}")
        
        if name == 'raw_orders':
            positive_amounts = (df['order_amount'] > 0).sum()
            print(f"  - Orders with positive amounts: {positive_amounts}")
    
    print("\nSample data preview:")
    print(f"Raw Orders: {len(dataframes['raw_orders'])} records")
    print(dataframes['raw_orders'].head(2))
    
    print(f"\nCustomers: {len(dataframes['customers'])} records")
    print(dataframes['customers'][['customer_id', 'first_name', 'email', 'total_spent']].head(2))

def main():
    """Main function to generate all sample data"""
    print("Starting data generation...")
    random.seed(42)  # For reproducible results
    np.random.seed(42)
    
    # Generate all tables
    dataframes = {}
    
    # Core tables
    dataframes['raw_orders'] = generate_raw_orders()
    dataframes['customers'] = generate_customers()
    dataframes['orders'] = generate_orders(dataframes['raw_orders'], dataframes['customers'])
    dataframes['product_inventory_raw'] = generate_product_inventory()
    dataframes['events'] = generate_events()
    dataframes['mock_transaction_files'] = generate_transactions()
    
    # Additional tables
    dataframes['products'] = generate_products()
    dataframes['merchants'] = generate_merchants()
    dataframes['user_sessions'] = generate_user_sessions()
    
    # Customer raw data (derived from customers)
    dataframes['customer_raw_data'] = dataframes['customers'].copy()
    dataframes['customer_raw_data']['years_active'] = (
        (datetime.now() - dataframes['customer_raw_data']['registration_date']).dt.days / 365.25
    )
    
    # Validate generated data
    validate_data(dataframes)
    
    print(f"\n✅ Successfully generated {sum(len(df) for df in dataframes.values())} total records across {len(dataframes)} tables")
    
    return dataframes

if __name__ == "__main__":
    # Generate data
    data = main()
    
    # Optionally save to CSV files for inspection
    save_to_csv = input("\nSave data to CSV files for inspection? (y/n): ").lower().strip() == 'y'
    if save_to_csv:
        print("Saving to CSV files...")
        for name, df in data.items():
            filename = f"{name}.csv"
            df.to_csv(filename, index=False)
            print(f"  - Saved {filename}")
        print("CSV files saved!")
