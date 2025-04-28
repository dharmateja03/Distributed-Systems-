#!/bin/bash

# Check if required Python package is installed
if ! pip list | grep -q kafka-python; then
    echo "Installing Kafka Python client..."
    pip install kafka-python
fi

# Run the Kafka producer
echo "Starting Kafka producer..."
python kafka_producer.py --bootstrap-servers localhost:9092 --topic market-events --events 0 --interval 0.1