import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create a SQLite database
conn = sqlite3.connect('sample_sales.db')

# Create sample sales data
np.random.seed(42)
start_date = datetime(2023, 1, 1)

# Create dates
dates = [start_date + timedelta(days=i) for i in range(365)]

# Create sample data
data = {
    'OrderID': range(1000, 1000+len(dates)),
    'OrderDate': dates,
    'CustomerID': np.random.randint(100, 500, size=len(dates)),
    'Revenue': np.random.normal(1000, 250, size=len(dates)),
    'Cost': np.random.normal(600, 150, size=len(dates)),
    'ProductCategory': np.random.choice(['Electronics', 'Clothing', 'Food', 'Home Goods'], size=len(dates)),
    'Region': np.random.choice(['North', 'South', 'East', 'West'], size=len(dates))
}

# Create DataFrame
df = pd.DataFrame(data)

# Save to SQLite
df.to_sql('SalesData', conn, if_exists='replace', index=False)

# Create a customer table
customers = pd.DataFrame({
    'CustomerID': range(100, 500),
    'CustomerName': ['Customer_' + str(i) for i in range(100, 500)],
    'JoinDate': [start_date - timedelta(days=np.random.randint(1, 1000)) for _ in range(400)],
    'LastPurchaseDate': [start_date + timedelta(days=np.random.randint(0, 365)) for _ in range(400)],
    'PurchaseCount': np.random.randint(1, 50, size=400),
    'TotalSpend': np.random.normal(5000, 2000, size=400)
})

customers.to_sql('Customers', conn, if_exists='replace', index=False)

print("Sample database created successfully with tables: SalesData and Customers")
conn.close()
