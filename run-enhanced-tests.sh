#!/bin/bash

# Run enhanced implementation tests
echo "Running enhanced implementation tests..."

# Check if PySpark is installed
if ! pip list | grep -q pyspark; then
    echo "PySpark not found. Installing PySpark..."
    pip install pyspark matplotlib pandas numpy
fi

# Run the test implementation
python test_enhanced_implementation.py

echo "Tests completed."