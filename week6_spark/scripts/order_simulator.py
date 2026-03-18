#!/usr/bin/env python3
import csv
import random
import time
import os
import sys
from datetime import datetime

STREAMING_DIR = r"/home/odinsbeard/Data_engineering_Journey/week6_spark/data/streaming"

# Create a flag file to know when to stop
stop_flag = os.path.join(STREAMING_DIR, "SIMULATOR_RUNNING")

# Create stop flag
with open(stop_flag, 'w') as f:
    f.write("running")

customers = list(range(1, 101))
products = list(range(1, 51))

batch_num = 1
try:
    while os.path.exists(stop_flag):
        try:
            filename = os.path.join(STREAMING_DIR, "input", f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["order_id", "customer_id", "product_id", "quantity", "unit_price", "timestamp"])

                for _ in range(random.randint(3, 7)):
                    order_id = batch_num * 100 + random.randint(1, 100)
                    timestamp = datetime.now().isoformat()
                    unit_price = random.uniform(10, 200)

                    writer.writerow([
                        order_id,
                        random.choice(customers),
                        random.choice(products),
                        random.randint(1, 5),
                        round(unit_price, 2),
                        timestamp
                    ])

            print(f"📁 Created batch {batch_num}: {os.path.basename(filename)}")
            batch_num += 1
            time.sleep(10)

        except Exception as e:
            print(f"⚠️ Simulator warning: {e}")
            time.sleep(5)
finally:
    # Clean up stop flag
    if os.path.exists(stop_flag):
        os.remove(stop_flag)
    print("✅ Simulator stopped cleanly")
