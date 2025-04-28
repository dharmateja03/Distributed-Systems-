#!/bin/bash

# Run Script for Apache Spark Stream Processing Enhancement

# Check command line arguments
MODE=${1:-local}
DURATION=${2:-120}

# Check if PySpark is installed
if ! pip list | grep -q pyspark; then
    echo "PySpark not found. Installing PySpark..."
    pip install pyspark matplotlib pandas numpy
fi

# Create necessary directories
mkdir -p data/market_events
mkdir -p results
mkdir -p checkpoint
mkdir -p metrics

# Run the production implementation
echo "Running production implementation in $MODE mode for $DURATION seconds..."
python production_pipeline.py $MODE $DURATION

echo "Processing complete. Check the results and metrics directories for outputs."