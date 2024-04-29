from confluent_kafka import Producer
import json
import random
import time

# Kafka broker configuration
bootstrap_servers = 'localhost:19092'
topic_name = 'sales_data'

# Create Kafka producer
producer = Producer({'bootstrap.servers': bootstrap_servers})

# Function to generate random sales data
def generate_sales_data(store_id, product_id):
    sale_amount = round(random.uniform(10, 1000), 2)  # Random sale amount between 10 and 1000
    timestamp = int(time.time())  # Current timestamp in seconds
    return {
        'store_id': store_id,
        'product_id': product_id,
        'sale_amount': sale_amount,
        'timestamp': timestamp
    }

# Function to send sales data messages to Kafka
def send_sales_data(store_id, product_id):
    sales_data = generate_sales_data(store_id, product_id)
    message_key = f"{store_id}-{product_id}"  # Using a composite key for message routing
    message_value = json.dumps(sales_data)
    producer.produce(topic=topic_name, key=message_key, value=message_value)
    print(f"Sent message: {message_value}")
    producer.flush()  # Ensure all messages are sent

# Simulate sales data for multiple stores and products

while True:
    # Simulate sales for store IDs 1 to 5 and product IDs 1 to 3
    for store_id in range(1, 6):
        for product_id in range(1, 4):
            send_sales_data(store_id, product_id)
            time.sleep(1)  # Sleep for 1 second between messages
