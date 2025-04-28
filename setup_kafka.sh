#!/bin/bash

# Start Kafka using Docker Compose
echo "Starting Kafka and Zookeeper..."
docker-compose up -d

# Wait for Kafka to start
echo "Waiting for Kafka to start (30 seconds)..."
sleep 30

# Create Kafka topic
echo "Creating Kafka topic 'market-events'..."
docker exec kafka kafka-topics --create --topic market-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Check topic was created
echo "Listing Kafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo "Kafka setup complete!"