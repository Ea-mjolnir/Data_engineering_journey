#!/usr/bin/env python3
"""
Generate sample data for Delta Lake exercises
"""

import random
import pandas as pd
from datetime import datetime, timedelta
import os

# Configuration
DATA_DIR = "data/bronze"
os.makedirs(DATA_DIR, exist_ok=True)

def generate_customers(n=10000):
    """Generate customer data"""
    first_names = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer',
                   'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
                  'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
              'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    
    customers = []
    for i in range(1, n+1):
        customers.append({
            'customer_id': i,
            'first_name': random.choice(first_names),
            'last_name': random.choice(last_names),
            'email': f'customer{i}@example.com',
            'city': random.choice(cities),
            'state': random.choice(['NY', 'CA', 'TX', 'IL', 'FL', 'PA']),
            'signup_date': (datetime.now() - timedelta(days=random.randint(0, 730))).date(),
            'is_active': random.choice([True, True, True, False])
        })
    
    df = pd.DataFrame(customers)
    df.to_csv(f"{DATA_DIR}/customers.csv", index=False)
    print(f"✅ Generated {n} customers")

def generate_products(n=5000):
    """Generate product data"""
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
    products = []
    
    for i in range(1, n+1):
        category = random.choice(categories)
        products.append({
            'product_id': i,
            'product_name': f'{category} Product {i}',
            'category': category,
            'price': round(random.uniform(10, 1000), 2),
            'cost': round(random.uniform(5, 500), 2),
            'supplier': f'Supplier {random.randint(1, 50)}'
        })
    
    df = pd.DataFrame(products)
    df.to_csv(f"{DATA_DIR}/products.csv", index=False)
    print(f"✅ Generated {n} products")

def generate_orders(n=100000):
    """Generate order data"""
    orders = []
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(1, n+1):
        customer_id = random.randint(1, 10000)
        order_date = start_date + timedelta(days=random.randint(0, 365))
        total = round(random.uniform(20, 500), 2)
        
        orders.append({
            'order_id': i,
            'customer_id': customer_id,
            'order_date': order_date.date(),
            'total_amount': total,
            'status': random.choice(['Completed', 'Pending', 'Shipped']),
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Debit Card'])
        })
    
    df = pd.DataFrame(orders)
    df.to_csv(f"{DATA_DIR}/orders.csv", index=False)
    print(f"✅ Generated {n} orders")

def generate_order_items(n=300000):
    """Generate order line items"""
    items = []
    
    for i in range(1, n+1):
        order_id = random.randint(1, 100000)
        product_id = random.randint(1, 500)
        quantity = random.randint(1, 5)
        price = round(random.uniform(10, 200), 2)
        
        items.append({
            'item_id': i,
            'order_id': order_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': price,
            'line_total': round(quantity * price, 2)
        })
    
    df = pd.DataFrame(items)
    df.to_csv(f"{DATA_DIR}/order_items.csv", index=False)
    print(f"✅ Generated {n} order items")

if __name__ == "__main__":
    print("🚀 Generating sample data...")
    generate_customers(10000)
    generate_products(5000)
    generate_orders(100000)
    generate_order_items(300000)
    print("\n✅ All data generated in data/bronze/")
