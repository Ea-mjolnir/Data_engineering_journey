#!/bin/bash

################################################################################
# Self-Contained Data Generator with BATCHED OUTPUT
# Each run creates a NEW timestamped folder with its own CSV files
# Real-world scenario: separate files for each data load batch
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🏭 Self-Contained Data Generator with BATCHED OUTPUT${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMP_DIR="$PROJECT_ROOT/temp_python_env"

# Create a NEW timestamped folder for this batch
BATCH_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BATCH_DIR="$PROJECT_ROOT/data/batch_$BATCH_TIMESTAMP"
mkdir -p "$BATCH_DIR"

echo -e "${CYAN}📂 Project root: ${PROJECT_ROOT}${NC}"
echo -e "${CYAN}📂 Temp directory: ${TEMP_DIR}${NC}"
echo -e "${CYAN}📂 This batch directory: ${BATCH_DIR}${NC}"

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up temporary environment...${NC}"
    
    # Deactivate virtual environment if active
    if [ -n "${VIRTUAL_ENV:-}" ]; then
        echo "   Deactivating virtual environment..."
        deactivate 2>/dev/null || true
    fi
    
    # Remove temp directory
    if [ -d "$TEMP_DIR" ]; then
        echo "   Removing temporary directory: $TEMP_DIR"
        rm -rf "$TEMP_DIR"
    fi
    
    echo -e "${GREEN}✅ Cleanup complete!${NC}"
}

# Set trap to ensure cleanup happens even on error or interrupt
trap cleanup EXIT INT TERM

# Check if Python3 is installed
echo -e "\n${YELLOW}🔍 Checking Python installation...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${YELLOW}⚠️  Python3 not found. Installing Python3...${NC}"
    
    # Update package list
    sudo apt update
    
    # Install Python3 and pip
    sudo apt install -y python3 python3-pip python3-venv
    
    # Verify installation
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version)
        echo -e "${GREEN}✅ ${PYTHON_VERSION} installed successfully!${NC}"
    else
        echo -e "${RED}❌ Failed to install Python3${NC}"
        exit 1
    fi
else
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✅ ${PYTHON_VERSION} found${NC}"
fi

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo -e "${YELLOW}⚠️  pip3 not found. Installing pip3...${NC}"
    sudo apt install -y python3-pip
fi

# Create and setup virtual environment
echo -e "\n${YELLOW}🔧 Creating isolated Python environment...${NC}"

# Remove existing temp directory if it exists
if [ -d "$TEMP_DIR" ]; then
    rm -rf "$TEMP_DIR"
fi

# Create virtual environment
python3 -m venv "$TEMP_DIR/venv"
echo -e "${GREEN}✅ Virtual environment created${NC}"

# Activate virtual environment
source "$TEMP_DIR/venv/bin/activate"
echo -e "${GREEN}✅ Virtual environment activated${NC}"

# Upgrade pip in virtual environment
echo -e "\n${YELLOW}📦 Upgrading pip...${NC}"
pip install --upgrade pip

echo -e "${GREEN}✅ Python environment ready${NC}"

# Track global ID progression across batches
GLOBAL_ID_FILE="$PROJECT_ROOT/data/.global_ids.txt"

# Initialize or read global IDs
if [ ! -f "$GLOBAL_ID_FILE" ]; then
    echo "customer:0" > "$GLOBAL_ID_FILE"
    echo "product:0" >> "$GLOBAL_ID_FILE"
    echo "order:0" >> "$GLOBAL_ID_FILE"
    echo "payment:0" >> "$GLOBAL_ID_FILE"
    echo "store:50" >> "$GLOBAL_ID_FILE"  # Stores are fixed at 50
fi

# Read current global IDs
declare -A GLOBAL_IDS
while IFS=: read -r key value; do
    GLOBAL_IDS[$key]=$value
done < "$GLOBAL_ID_FILE"

echo -e "\n${YELLOW}📊 Global ID state before this batch:${NC}"
echo "   Customers: ${GLOBAL_IDS[customer]}"
echo "   Products: ${GLOBAL_IDS[product]}"
echo "   Orders: ${GLOBAL_IDS[order]}"
echo "   Payments: ${GLOBAL_IDS[payment]}"
echo "   Stores: ${GLOBAL_IDS[store]}"

# Create the Python generator script
echo -e "\n${YELLOW}📝 Creating Python data generator script for BATCHED output...${NC}"

cat > "$TEMP_DIR/generate_data.py" << EOF
#!/usr/bin/env python3
"""
Generate LARGE sample e-commerce data for warehouse loading
FEATURE: Each run creates files in a new timestamped folder
Global IDs tracked across all batches
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
BATCH_DIR = Path("$BATCH_DIR")

# Global ID state from bash
GLOBAL_IDS = {
    'customer': ${GLOBAL_IDS[customer]},
    'product': ${GLOBAL_IDS[product]},
    'order': ${GLOBAL_IDS[order]},
    'payment': ${GLOBAL_IDS[payment]},
    'store': ${GLOBAL_IDS[store]}
}

print(f"📊 Starting IDs for this batch:")
print(f"   Customers: {GLOBAL_IDS['customer'] + 1}")
print(f"   Products: {GLOBAL_IDS['product'] + 1}")
print(f"   Orders: {GLOBAL_IDS['order'] + 1}")

# ============================================================================
# SAMPLE DATA LISTS
# ============================================================================
CITIES = [
    ('New York', 'NY'), ('Los Angeles', 'CA'), ('Chicago', 'IL'),
    ('Houston', 'TX'), ('Phoenix', 'AZ'), ('Philadelphia', 'PA'),
    ('San Antonio', 'TX'), ('San Diego', 'CA'), ('Dallas', 'TX'),
    ('San Jose', 'CA'), ('Austin', 'TX'), ('Jacksonville', 'FL'),
]

CATEGORIES = {
    'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories', 'Cameras', 'Audio'],
    'Clothing': ['Mens', 'Womens', 'Kids', 'Footwear', 'Accessories'],
    'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Garden', 'Tools'],
    'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Equipment', 'Apparel'],
    'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children', 'Digital']
}

FIRST_NAMES = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 
               'Lisa', 'William', 'Jennifer', 'James', 'Mary', 'Christopher']

LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 
              'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez']

PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Bank Transfer']
ORDER_STATUSES = ['Completed', 'Completed', 'Completed', 'Pending', 'Processing', 'Shipped', 'Cancelled', 'Returned']

def write_csv(filename, data, fieldnames):
    """Write data to CSV file in the batch directory"""
    filepath = BATCH_DIR / filename
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"  ✅ Created {filepath.name} ({len(data):,} records)")
        return True
    except Exception as e:
        print(f"  ❌ Error writing {filename}: {e}")
        return False

def generate_stores(num_stores=50):
    """Generate store records"""
    stores = []
    start_id = GLOBAL_IDS['store'] + 1
    
    for i in range(start_id, start_id + num_stores):
        stores.append({
            'store_id': i,
            'store_name': f"Store {i}",
            'store_type': random.choice(['Flagship', 'Mall', 'Outlet']),
            'address': f"{random.randint(100, 9999)} Main St",
            'city': random.choice(CITIES)[0],
            'state': random.choice(['NY', 'CA', 'TX', 'IL', 'FL']),
            'country': 'USA',
            'postal_code': f"{random.randint(10000, 99999)}",
            'latitude': round(40.0 + random.uniform(-5, 5), 6),
            'longitude': round(-75.0 + random.uniform(-5, 5), 6),
            'region': random.choice(['North', 'South', 'East', 'West']),
            'district': f"District {random.randint(1,10)}",
            'square_footage': random.choice([1500, 2500, 5000, 10000]),
            'opening_date': (datetime.now().date() - timedelta(days=random.randint(0, 1825))).isoformat(),
            'manager_name': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            'phone': f"555-{random.randint(100,999)}-{random.randint(1000,9999)}",
            'is_active': random.choice([True, True, True, False])
        })
    
    fieldnames = ['store_id', 'store_name', 'store_type', 'address', 'city', 'state', 
                  'country', 'postal_code', 'latitude', 'longitude', 'region', 
                  'district', 'square_footage', 'opening_date', 'manager_name', 
                  'phone', 'is_active']
    
    success = write_csv('stores.csv', stores, fieldnames)
    if success:
        GLOBAL_IDS['store'] = start_id + num_stores - 1
    return success

def generate_customers(num_customers=10000):
    """Generate customer records"""
    customers = []
    start_id = GLOBAL_IDS['customer'] + 1
    start_date = datetime.now().date() - timedelta(days=730)
    
    for i in range(start_id, start_id + num_customers):
        if (i - start_id) % 2500 == 0:
            print(f"    ... generated {i - start_id:,} customers")
            
        city, state = random.choice(CITIES)
        reg_date = start_date + timedelta(days=random.randint(0, 730))
        
        customers.append({
            'customer_id': i,
            'first_name': random.choice(FIRST_NAMES),
            'last_name': random.choice(LAST_NAMES),
            'email': f'customer{i}@example.com',
            'phone': f'555-{random.randint(1000,9999)}-{random.randint(10,99)}',
            'address_line1': f'{random.randint(100,9999)} Main St',
            'address_line2': '',
            'city': city,
            'state': state,
            'country': 'USA',
            'postal_code': f'{random.randint(10000,99999)}',
            'registration_date': reg_date.isoformat(),
            'customer_segment': random.choice(['Regular', 'Gold', 'Platinum', 'New']),
            'is_active': random.choice([True, True, True, False])
        })
    
    fieldnames = ['customer_id', 'first_name', 'last_name', 'email', 'phone', 
                  'address_line1', 'address_line2', 'city', 'state', 'country', 
                  'postal_code', 'registration_date', 'customer_segment', 'is_active']
    
    success = write_csv('customers.csv', customers, fieldnames)
    if success:
        GLOBAL_IDS['customer'] = start_id + num_customers - 1
    return success

def generate_products(num_products=5000):
    """Generate product records"""
    products = []
    start_id = GLOBAL_IDS['product'] + 1
    
    for i in range(start_id, start_id + num_products):
        if (i - start_id) % 1000 == 0:
            print(f"    ... generated {i - start_id:,} products")
            
        main_category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[main_category])
        unit_price = round(random.uniform(20, 2000), 2)
        
        products.append({
            'product_id': i,
            'product_name': f'{main_category} {subcategory} Model {random.randint(100,999)}',
            'product_description': f'High quality {main_category.lower()} product',
            'sku': f'SKU-{main_category[:3].upper()}-{i:05d}',
            'barcode': f'BAR{random.randint(10000000,99999999)}',
            'category': main_category,
            'subcategory': subcategory,
            'brand': f'Brand {random.choice(["A","B","C","D","E","F","G"])}',
            'unit_price': unit_price,
            'unit_cost': round(unit_price * random.uniform(0.4, 0.7), 2),
            'msrp': round(unit_price * random.uniform(1.1, 1.3), 2),
            'supplier_name': f'Supplier {random.randint(1, 50)}',
            'color': random.choice(['Red', 'Blue', 'Black', 'White', 'Silver', 'Gold']),
            'size': random.choice(['S', 'M', 'L', 'XL', 'One Size']),
            'weight_kg': round(random.uniform(0.1, 10.0), 2),
            'is_active': random.choice([True, True, True, True, False])
        })
    
    fieldnames = ['product_id', 'product_name', 'product_description', 'sku', 'barcode',
                  'category', 'subcategory', 'brand', 'unit_price', 'unit_cost', 'msrp',
                  'supplier_name', 'color', 'size', 'weight_kg', 'is_active']
    
    success = write_csv('products.csv', products, fieldnames)
    if success:
        GLOBAL_IDS['product'] = start_id + num_products - 1
    return success

def generate_orders(customers, num_orders=35000):
    """Generate order records"""
    orders = []
    start_id = GLOBAL_IDS['order'] + 1
    
    for order_id in range(start_id, start_id + num_orders):
        if (order_id - start_id) % 5000 == 0:
            print(f"    ... generated {order_id - start_id:,} orders")
            
        customer = random.choice(customers)
        days_ago = random.randint(0, 540)
        order_date = datetime.now().date() - timedelta(days=days_ago)
        
        subtotal = round(random.uniform(50, 500), 2)
        
        orders.append({
            'order_id': order_id,
            'invoice_number': f'INV-{order_id:08d}',
            'order_date': order_date.isoformat(),
            'customer_id': customer['customer_id'],
            'store_id': random.randint(1, 50),
            'subtotal': subtotal,
            'tax_amount': round(subtotal * 0.08, 2),
            'shipping_amount': round(random.uniform(5, 25), 2),
            'total_amount': round(subtotal * 1.08 + random.uniform(5, 25), 2),
            'order_status': random.choice(ORDER_STATUSES),
            'payment_method': random.choice(PAYMENT_METHODS),
            'shipping_method': random.choice(['Standard', 'Express', 'Next Day']),
            'created_at': f"{order_date} {random.randint(8,20):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            'updated_at': f"{order_date} {random.randint(8,20):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        })
    
    fieldnames = ['order_id', 'invoice_number', 'order_date', 'customer_id', 'store_id',
                  'subtotal', 'tax_amount', 'shipping_amount', 'total_amount',
                  'order_status', 'payment_method', 'shipping_method', 'created_at', 'updated_at']
    
    success = write_csv('orders.csv', orders, fieldnames)
    if success:
        GLOBAL_IDS['order'] = start_id + num_orders - 1
    return success

def generate_order_items(orders, products):
    """Generate order line items"""
    order_items = []
    
    for order in orders:
        num_items = random.choices([1,2,3,4,5], weights=[30,30,20,15,5])[0]
        
        for line_num in range(1, num_items + 1):
            product = random.choice(products)
            quantity = random.choices([1,2,3,4,5], weights=[50,25,15,7,3])[0]
            
            order_items.append({
                'order_id': order['order_id'],
                'line_number': line_num,
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': product['unit_price'],
                'discount_percent': random.choice([0, 0, 0, 5, 10, 15]),
                'discount_amount': 0,
                'tax_percent': 8,
                'tax_amount': 0,
                'line_total': quantity * product['unit_price']
            })
    
    fieldnames = ['order_id', 'line_number', 'product_id', 'quantity', 'unit_price',
                  'discount_percent', 'discount_amount', 'tax_percent', 'tax_amount', 'line_total']
    
    return write_csv('order_items.csv', order_items, fieldnames)

def generate_payments(orders, num_payments=None):
    """Generate payment records"""
    payments = []
    start_id = GLOBAL_IDS['payment'] + 1
    payment_id = start_id
    
    for order in orders:
        if random.random() < 0.95:
            payments.append({
                'payment_id': payment_id,
                'order_id': order['order_id'],
                'payment_date': order['order_date'],
                'payment_method': order['payment_method'],
                'payment_amount': order['total_amount'],
                'transaction_id': f'TXN{random.randint(100000,999999)}',
                'payment_status': random.choice(['Completed', 'Completed', 'Completed', 'Pending', 'Failed'])
            })
            payment_id += 1
    
    fieldnames = ['payment_id', 'order_id', 'payment_date', 'payment_method',
                  'payment_amount', 'transaction_id', 'payment_status']
    
    success = write_csv('payments.csv', payments, fieldnames)
    if success:
        GLOBAL_IDS['payment'] = payment_id - 1
    return success

def main():
    """Generate all sample data in new batch folder"""
    print("=" * 70)
    print(f"🏭 Generating BATCH: {BATCH_DIR.name}")
    print("=" * 70)
    
    start_time = datetime.now()
    
    try:
        # Generate stores
        stores_file = generate_stores(50)
        if not stores_file:
            raise Exception("Failed to generate stores")
        
        # Generate customers
        customers_file = generate_customers(10000)
        if not customers_file:
            raise Exception("Failed to generate customers")
        
        # Generate products
        products_file = generate_products(5000)
        if not products_file:
            raise Exception("Failed to generate products")
        
        # Read customers back for orders
        customers = []
        with open(BATCH_DIR / 'customers.csv', 'r') as f:
            reader = csv.DictReader(f)
            customers = list(reader)
            for c in customers:
                c['customer_id'] = int(c['customer_id'])
        
        # Generate orders
        orders_file = generate_orders(customers, 35000)
        if not orders_file:
            raise Exception("Failed to generate orders")
        
        # Read products back for order items
        products = []
        with open(BATCH_DIR / 'products.csv', 'r') as f:
            reader = csv.DictReader(f)
            products = list(reader)
            for p in products:
                p['product_id'] = int(p['product_id'])
                p['unit_price'] = float(p['unit_price'])
        
        # Read orders back
        orders = []
        with open(BATCH_DIR / 'orders.csv', 'r') as f:
            reader = csv.DictReader(f)
            orders = list(reader)
            for o in orders:
                o['order_id'] = int(o['order_id'])
        
        # Generate order items
        order_items_file = generate_order_items(orders, products)
        if not order_items_file:
            raise Exception("Failed to generate order items")
        
        # Generate payments
        payments_file = generate_payments(orders)
        if not payments_file:
            raise Exception("Failed to generate payments")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print()
        print("=" * 70)
        print("✅ BATCH GENERATION COMPLETE!")
        print("=" * 70)
        print(f"📍 Batch folder: {BATCH_DIR}")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print()
        print("📊 ID RANGES IN THIS BATCH:")
        print(f"   • Stores      : {GLOBAL_IDS['store']-49} - {GLOBAL_IDS['store']}")
        print(f"   • Customers   : {GLOBAL_IDS['customer']-9999} - {GLOBAL_IDS['customer']}")
        print(f"   • Products    : {GLOBAL_IDS['product']-4999} - {GLOBAL_IDS['product']}")
        print(f"   • Orders      : {GLOBAL_IDS['order']-34999} - {GLOBAL_IDS['order']}")
        print("=" * 70)
        
        # Return the new global IDs
        return GLOBAL_IDS
        
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    new_ids = main()
    
    # Print global IDs for bash to capture
    print("\n📊 NEW GLOBAL IDS:")
    for key, value in new_ids.items():
        print(f"{key}:{value}")
EOF

# Run the Python script and capture the new global IDs
echo -e "\n${YELLOW}⚙️  Running data generator for new batch...${NC}"
echo -e "${BLUE}--------------------------------------------------${NC}"

cd "$PROJECT_ROOT"
python "$TEMP_DIR/generate_data.py" | tee "$BATCH_DIR/generation.log"
PYTHON_EXIT_CODE=$?

# Extract new global IDs from Python output
if [ $PYTHON_EXIT_CODE -eq 0 ]; then
    # Update global ID file
    grep -E "^(customer|product|order|payment|store):" "$BATCH_DIR/generation.log" | while IFS=: read -r key value; do
        if [ -n "$key" ] && [ -n "$value" ]; then
            sed -i "s/^$key:.*/$key:$value/" "$GLOBAL_ID_FILE"
        fi
    done
    
    echo -e "${BLUE}--------------------------------------------------${NC}"
    echo -e "${GREEN}✅ Batch generation completed successfully!${NC}"
    
    # Show batch info
    echo -e "\n${YELLOW}📁 Batch created:${NC}"
    echo -e "   ${CYAN}${BATCH_DIR}${NC}"
    echo -e "\n${YELLOW}📊 Global IDs updated:${NC}"
    cat "$GLOBAL_ID_FILE"
    
else
    echo -e "${RED}❌ Failed to generate batch (exit code: $PYTHON_EXIT_CODE)${NC}"
    exit 1
fi

# Cleanup
echo -e "\n${YELLOW}🧹 Cleaning up temporary environment...${NC}"
# Cleanup happens automatically via trap

echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Batch generation complete!${NC}"
echo -e "${YELLOW}📁 New batch folder: ${CYAN}${BATCH_DIR}${NC}"
echo -e "${YELLOW}📋 Next batch will continue with higher IDs${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
