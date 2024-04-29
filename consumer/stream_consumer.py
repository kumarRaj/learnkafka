from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import pytz
import json

# Kafka broker configuration
bootstrap_servers = 'localhost:19092'
input_topic = 'sales_data'
output_topic = 'sales_aggregate'

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'sales_aggregate_group',
    'auto.offset.reset': 'earliest'
}

# Create Kafka consumer
consumer = Consumer(consumer_config)
consumer.subscribe([input_topic])

# Create Kafka producer
producer = Producer({'bootstrap.servers': bootstrap_servers})

# Function to calculate total sales amount and average sales per hour
def process_sales_data(data):
    sales_sum = 0
    sales_count = 0
    for sale_data in data:
        sales_sum += sale_data['sale_amount']
        sales_count += 1
    if sales_count > 0:
        average_sales = sales_sum / sales_count
    else:
        average_sales = 0
    return sales_sum, average_sales

# Function to send aggregated sales data to Kafka
def send_aggregate_data(store_id, product_id, total_sales, average_sales):
    aggregate_data = {
        'store_id': store_id,
        'product_id': product_id,
        'total_sales': total_sales,
        'average_sales_per_hour': average_sales
    }
    message_key = f"{store_id}-{product_id}"
    message_value = json.dumps(aggregate_data)
    producer.produce(topic=output_topic, key=message_key, value=message_value)
    print(f"Sent aggregated data: {message_value}")
    producer.flush()

# Main processing loop
while True:
    message = consumer.poll(timeout=1.0)  # Poll for new messages
    if message is None:
        continue
    if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
            # End of partition
            continue
        else:
            print(f"Error: {message.error()}")
            break
    message_value = message.value().decode('utf-8')
    try:
        sale_data = json.loads(message_value)
        store_id = sale_data['store_id']
        product_id = sale_data['product_id']
        sale_amount = sale_data['sale_amount']
        timestamp = sale_data['timestamp']
        # Convert timestamp to datetime object
        sale_time = datetime.fromtimestamp(timestamp, tz=pytz.utc)
        current_time = datetime.now(tz=pytz.utc)
        # Calculate hours difference
        hours_diff = (current_time - sale_time).total_seconds() / 3600.0
        if hours_diff <= 1:  # Process data within the last hour
            send_aggregate_data(store_id, product_id, sale_amount, sale_amount)
    except Exception as e:
        print(f"Error processing message: {e}")

# Close Kafka consumer and producer
consumer.close()
producer.close()
