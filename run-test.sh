#!/bin/bash

# Run Script for Apache Spark Stream Processing Enhancement

# Check if PySpark is installed
if ! pip list | grep -q pyspark; then
    echo "PySpark not found. Installing PySpark..."
    pip install pyspark matplotlib pandas numpy
fi

# Create necessary directories
mkdir -p data/market_events
mkdir -p results
mkdir -p checkpoint

# Run the test implementation
echo "Running PySpark implementation..."
python pyspark_implementation.py

echo "Test complete. Check the results directory for performance comparison plots."`