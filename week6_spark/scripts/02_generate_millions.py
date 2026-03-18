#!/usr/bin/env python3
"""
Week 6: Generate MILLIONS of rows for Spark learning
This script creates 5-10 million records with CONTINUOUS IDs (no duplicates across runs)
Each run appends new data with unique IDs
"""

import csv
import random
import os
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import json

# ============================================================================
# Configuration - Using absolute paths
# ============================================================================
BASE_DIR = "/home/odinsbeard/Data_engineering_Journey/week6_spark"
DATA_DIR = os.path.join(BASE_DIR, "data", "input")
ID_TRACKER_FILE = os.path.join(BASE_DIR, "data", "id_tracker.json")
os.makedirs(DATA_DIR, exist_ok=True)

# ============================================================================
# ID Tracking System (prevents Week 4 duplicate issues!)
# ============================================================================
def get_next_ids():
    """Get the next available IDs from tracker file"""
    default_ids = {
        'sale_id': 1,
        'user_id': 1,
        'product_id': 1,
        'batch_number': 1
    }
    
    if os.path.exists(ID_TRACKER_FILE):
        with open(ID_TRACKER_FILE, 'r') as f:
            return json.load(f)
    
    return default_ids

def save_next_ids(ids):
    """Save the next available IDs for next run"""
    with open(ID_TRACKER_FILE, 'w') as f:
        json.dump(ids, f, indent=2)

# ============================================================================
# Sample Data Lists (for realistic random generation)
# ============================================================================
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Donald", "Sandra", "Mark", "Ashley",
    "Paul", "Kimberly", "Steven", "Emily", "Andrew", "Donna", "Kenneth", "Michelle"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores"
]

PRODUCT_CATEGORIES = {
    "Electronics": [
        "Smartphone", "Laptop", "Tablet", "Headphones", "Smart Watch", "Camera",
        "Speaker", "Monitor", "Keyboard", "Mouse", "External Drive", "Router"
    ],
    "Clothing": [
        "T-Shirt", "Jeans", "Dress", "Jacket", "Sweater", "Skirt", "Shorts",
        "Socks", "Hat", "Scarf", "Gloves", "Belt"
    ],
    "Home": [
        "Sofa", "Table", "Chair", "Lamp", "Rug", "Curtains", "Pillow",
        "Blanket", "Towel", "Dishes", "Pots", "Tools"
    ],
    "Sports": [
        "Basketball", "Football", "Tennis Racket", "Yoga Mat", "Dumbbells",
        "Bicycle", "Treadmill", "Jump Rope", "Water Bottle", "Gym Bag"
    ],
    "Books": [
        "Fiction", "Non-Fiction", "Textbook", "Cookbook", "Biography",
        "Science", "History", "Poetry", "Comics", "Magazine"
    ]
}

COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia", "Brazil"]
CITIES = {
    "USA": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "UK": ["London", "Manchester", "Birmingham", "Liverpool", "Edinburgh"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Brazil": ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"]
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]

# ============================================================================
# Data Generators
# ============================================================================

def generate_users(num_users, start_id):
    """Generate user records with continuous IDs"""
    print(f"  Generating {num_users:,} users...")
    users = []
    
    for i in range(num_users):
        user_id = start_id + i
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES[country])
        signup_date = datetime.now() - timedelta(days=random.randint(0, 1095))  # Up to 3 years ago
        
        users.append({
            'user_id': user_id,
            'first_name': random.choice(FIRST_NAMES),
            'last_name': random.choice(LAST_NAMES),
            'email': f"user{user_id}@example.com",
            'age': random.randint(18, 80),
            'country': country,
            'city': city,
            'signup_date': signup_date.strftime("%Y-%m-%d"),
            'is_active': random.choice([True, True, True, False])  # 75% active
        })
        
        if (i + 1) % 100000 == 0:
            print(f"    Progress: {i+1:,}/{num_users:,} users")
    
    return users

def generate_products(num_products, start_id):
    """Generate product records with continuous IDs"""
    print(f"  Generating {num_products:,} products...")
    products = []
    
    all_products = []
    for category, items in PRODUCT_CATEGORIES.items():
        for item in items:
            all_products.append((category, item))
    
    for i in range(num_products):
        product_id = start_id + i
        category, product_type = random.choice(all_products)
        
        # Realistic pricing by category
        if category == "Electronics":
            price = round(random.uniform(50, 2000), 2)
        elif category == "Clothing":
            price = round(random.uniform(15, 300), 2)
        elif category == "Home":
            price = round(random.uniform(20, 800), 2)
        elif category == "Sports":
            price = round(random.uniform(10, 500), 2)
        else:  # Books
            price = round(random.uniform(8, 150), 2)
        
        products.append({
            'product_id': product_id,
            'product_name': f"{product_type} {random.choice(['Pro', 'Lite', 'Plus', 'Max', 'Basic'])}",
            'category': category,
            'subcategory': product_type,
            'price': price,
            'cost': round(price * random.uniform(0.4, 0.7), 2),
            'brand': f"Brand_{random.choice(['A','B','C','D','E','F','G'])}",
            'supplier': f"Supplier_{random.randint(1, 50)}",
            'is_active': random.choice([True, True, True, True, False])  # 80% active
        })
    
    return products

def generate_sales(num_sales, start_id, max_user_id, max_product_id):
    """Generate sales records with continuous IDs"""
    print(f"  Generating {num_sales:,} sales...")
    sales = []
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_range = (end_date - start_date).days
    
    for i in range(num_sales):
        sale_id = start_id + i
        
        # Random date within range
        random_days = random.randint(0, date_range)
        sale_date = start_date + timedelta(days=random_days)
        
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        price = round(random.uniform(10, 500), 2)
        discount = random.choices([0, 0, 0, 5, 10, 15, 20], weights=[60, 0, 0, 15, 10, 10, 5])[0]
        
        sales.append({
            'sale_id': sale_id,
            'user_id': random.randint(1, max_user_id),
            'product_id': random.randint(1, max_product_id),
            'quantity': quantity,
            'price': price,
            'discount_percent': discount,
            'final_amount': round(quantity * price * (1 - discount/100), 2),
            'sale_date': sale_date.strftime("%Y-%m-%d"),
            'payment_method': random.choice(PAYMENT_METHODS),
            'country': random.choice(COUNTRIES)
        })
        
        if (i + 1) % 500000 == 0:
            print(f"    Progress: {i+1:,}/{num_sales:,} sales")
    
    return sales

def save_to_csv(data, filename, fieldnames):
    """Save data to CSV file (creates new file each run)"""
    filepath = os.path.join(DATA_DIR, filename)
    
    # Create timestamped filename to avoid overwriting
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base, ext = os.path.splitext(filename)
    timestamped_file = f"{base}_{timestamp}{ext}"
    filepath = os.path.join(DATA_DIR, timestamped_file)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    # Create symlink to latest file for easy access
    latest_link = os.path.join(DATA_DIR, f"latest_{filename}")
    if os.path.exists(latest_link):
        os.remove(latest_link)
    os.symlink(timestamped_file, latest_link)
    
    return filepath, len(data)

def main():
    """Main function to generate millions of rows"""
    parser = argparse.ArgumentParser(description='Generate millions of test records')
    parser.add_argument('--users', type=int, default=1_000_000, help='Number of users to generate')
    parser.add_argument('--products', type=int, default=100_000, help='Number of products to generate')
    parser.add_argument('--sales', type=int, default=5_000_000, help='Number of sales to generate')
    parser.add_argument('--force', action='store_true', help='Force generation even if files exist')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 WEEK 6: GENERATING MILLIONS OF ROWS FOR SPARK")
    print("=" * 70)
    print(f"📁 Output directory: {DATA_DIR}")
    print()
    
    # Get next available IDs
    next_ids = get_next_ids()
    print(f"📊 Starting IDs from previous run:")
    print(f"   • Last sale_id: {next_ids['sale_id'] - 1:,}")
    print(f"   • Last user_id: {next_ids['user_id'] - 1:,}")
    print(f"   • Last product_id: {next_ids['product_id'] - 1:,}")
    print(f"   • Batch number: {next_ids['batch_number']}")
    print()
    
    print(f"🎯 Generating this batch:")
    print(f"   • {args.users:,} new users")
    print(f"   • {args.products:,} new products")
    print(f"   • {args.sales:,} new sales")
    print()
    
    start_time = datetime.now()
    
    try:
        # Generate users
        print("📊 STEP 1: Generating users...")
        users = generate_users(args.users, next_ids['user_id'])
        user_file, user_count = save_to_csv(users, 'users.csv', 
            ['user_id', 'first_name', 'last_name', 'email', 'age', 'country', 'city', 'signup_date', 'is_active'])
        print(f"  ✅ Saved: {user_file}")
        
        # Generate products
        print("\n📦 STEP 2: Generating products...")
        products = generate_products(args.products, next_ids['product_id'])
        product_file, product_count = save_to_csv(products, 'products.csv',
            ['product_id', 'product_name', 'category', 'subcategory', 'price', 'cost', 'brand', 'supplier', 'is_active'])
        print(f"  ✅ Saved: {product_file}")
        
        # Calculate max IDs for sales references
        max_user_id = next_ids['user_id'] + args.users - 1
        max_product_id = next_ids['product_id'] + args.products - 1
        
        # Generate sales
        print("\n💰 STEP 3: Generating sales (this will take a few minutes)...")
        sales = generate_sales(args.sales, next_ids['sale_id'], max_user_id, max_product_id)
        sale_file, sale_count = save_to_csv(sales, 'sales.csv',
            ['sale_id', 'user_id', 'product_id', 'quantity', 'price', 'discount_percent', 'final_amount', 'sale_date', 'payment_method', 'country'])
        print(f"  ✅ Saved: {sale_file}")
        
        # Update ID tracker for next run
        next_ids['user_id'] += args.users
        next_ids['product_id'] += args.products
        next_ids['sale_id'] += args.sales
        next_ids['batch_number'] += 1
        save_next_ids(next_ids)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print("✅ GENERATION COMPLETE!")
        print("=" * 70)
        print(f"⏱️  Time taken: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print()
        print("📊 FILES CREATED:")
        print(f"   • Users:    {user_count:>12,} rows → {os.path.basename(user_file)}")
        print(f"   • Products: {product_count:>12,} rows → {os.path.basename(product_file)}")
        print(f"   • Sales:    {sale_count:>12,} rows → {os.path.basename(sale_file)}")
        print(f"   • TOTAL:    {user_count + product_count + sale_count:>12,} rows")
        print()
        print("📈 NEXT RUN WILL START FROM:")
        print(f"   • user_id:    {next_ids['user_id']:,}")
        print(f"   • product_id: {next_ids['product_id']:,}")
        print(f"   • sale_id:    {next_ids['sale_id']:,}")
        print(f"   • batch:      {next_ids['batch_number']}")
        print()
        print("🔍 LATEST FILES (symlinks for easy access):")
        print(f"   • {DATA_DIR}/latest_users.csv → {os.path.basename(user_file)}")
        print(f"   • {DATA_DIR}/latest_products.csv → {os.path.basename(product_file)}")
        print(f"   • {DATA_DIR}/latest_sales.csv → {os.path.basename(sale_file)}")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Generation interrupted by user")
        print("ID tracker NOT updated - you can resume later")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
