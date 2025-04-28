#!/bin/bash

# Run Script for Dashboard

# Check if required Python packages are installed
# if ! pip list | grep -q flask || ! pip list | grep -q pandas || ! pip list | grep -q plotly; then
#     echo "Installing required packages..."
#     pip install -r dashboard_requirements.txt
# fi

# Run the dashboard
echo "Starting dashboard on http://localhost:5006"
python dashboard.py