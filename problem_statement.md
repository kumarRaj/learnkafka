"Problem Statement: Real-Time Data Processing with Kafka
You are tasked with building a real-time data processing pipeline using Kafka for a retail analytics platform. The system needs to ingest sales data from multiple stores, process it in real-time to calculate sales trends, and store aggregated results in a database for reporting purposes.

Requirements:

- Implement a Kafka topic named sales_data with multiple partitions to handle data from different stores concurrently.

- Create a Kafka producer application that simulates sales data from different stores. Each message should include store ID, product ID, sale amount, and timestamp.

- Develop a Kafka Streams application that consumes data from the sales_data topic, aggregates sales data by store and product, and calculates total sales amount and average sales per hour for each store and product.

- ~~Use Avro or JSON Schema for message serialization and ensure schema compatibility in Kafka.~~

Configure replication for fault tolerance and set appropriate retention policies for the sales_data topic.

Implement consumer applications to subscribe to the aggregated data topics and store the results in a database (e.g., MySQL, PostgreSQL).
Implement error handling, logging, and monitoring for the Kafka cluster, producers, consumers, and Kafka Streams application.
"