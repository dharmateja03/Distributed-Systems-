#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Visualization Dashboard for Apache Spark Stream Processing Enhancement
This module provides a Flask-based web dashboard for visualizing metrics
"""

from flask import Flask, render_template, jsonify
import pandas as pd
import json
import os
import glob
import datetime
import plotly
import plotly.graph_objs as go
import plotly.express as px
import threading
import time

app = Flask(__name__)

# Configuration
METRICS_DIR = "metrics"
REFRESH_INTERVAL = 5  # seconds

# Global metrics cache
metrics_cache = {
    "latency": None,
    "throughput": None,
    "memory": None,
    "watermark_delays": None,
    "last_update": None
}

def load_latest_metrics():
    """Load the latest metrics files from the metrics directory"""
    global metrics_cache
    
    # Find the latest files for each metric type
    latest_files = {}
    for metric_type in ["latency", "throughput", "memory", "watermark_delays"]:
        files = glob.glob(f"{METRICS_DIR}/{metric_type}_*.csv")
        if files:
            latest_file = max(files, key=os.path.getctime)
            latest_files[metric_type] = latest_file
    
    # Load data from latest files
    updated = False
    for metric_type, filepath in latest_files.items():
        # Check if file modified time is newer than last update
        mod_time = os.path.getmtime(filepath)
        if (metrics_cache["last_update"] is None or 
            mod_time > metrics_cache["last_update"]):
            try:
                df = pd.read_csv(filepath)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                metrics_cache[metric_type] = df
                updated = True
            except Exception as e:
                print(f"Error loading {metric_type} metrics: {e}")
    
    if updated:
        metrics_cache["last_update"] = time.time()
        print(f"Metrics updated at {datetime.datetime.now().isoformat()}")

def refresh_metrics_periodically():
    """Background thread to refresh metrics periodically"""
    while True:
        try:
            load_latest_metrics()
        except Exception as e:
            print(f"Error refreshing metrics: {e}")
        time.sleep(REFRESH_INTERVAL)

# Routes
@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/metrics')
def get_metrics_summary():
    """API endpoint for current metrics summary"""
    if metrics_cache["last_update"] is None:
        load_latest_metrics()
    
    # Create summary of metrics
    summary = {
        "last_update": metrics_cache["last_update"]
    }
    
    # Add window types
    window_types = set()
    for metric_type in ["latency", "throughput", "memory"]:
        if metrics_cache[metric_type] is not None:
            window_types.update(metrics_cache[metric_type]["window_type"].unique())
    
    summary["window_types"] = list(window_types)
    
    # Add latest values for each metric and window type
    summary["latest"] = {}
    for window_type in window_types:
        summary["latest"][window_type] = {}
        for metric_type in ["latency", "throughput", "memory"]:
            if metrics_cache[metric_type] is not None:
                df = metrics_cache[metric_type]
                latest = df[df["window_type"] == window_type].iloc[-1] if not df[df["window_type"] == window_type].empty else None
                if latest is not None:
                    summary["latest"][window_type][metric_type] = float(latest["value"])
    
    return jsonify(summary)

@app.route('/api/charts/latency')
def get_latency_chart():
    """Generate latency chart"""
    if metrics_cache["latency"] is None:
        load_latest_metrics()
    
    if metrics_cache["latency"] is None:
        return jsonify({})
    
    # Create plotly figure
    df = metrics_cache["latency"]
    fig = px.line(df, x="timestamp", y="value", color="window_type",
                 labels={"value": "Latency (seconds)", "timestamp": "Time"},
                 title="Processing Latency by Window Type")
    
    fig.update_layout(template="plotly_dark")
    
    # Convert to JSON
    chart_json = json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder))
    return jsonify(chart_json)

@app.route('/api/charts/throughput')
def get_throughput_chart():
    """Generate throughput chart"""
    if metrics_cache["throughput"] is None:
        load_latest_metrics()
    
    if metrics_cache["throughput"] is None:
        return jsonify({})
    
    # Create plotly figure
    df = metrics_cache["throughput"]
    fig = px.line(df, x="timestamp", y="value", color="window_type",
                 labels={"value": "Throughput (events/second)", "timestamp": "Time"},
                 title="Processing Throughput by Window Type")
    
    fig.update_layout(template="plotly_dark")
    
    # Convert to JSON
    chart_json = json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder))
    return jsonify(chart_json)

@app.route('/api/charts/memory')
def get_memory_chart():
    """Generate memory usage chart"""
    if metrics_cache["memory"] is None:
        load_latest_metrics()
    
    if metrics_cache["memory"] is None:
        return jsonify({})
    
    # Create plotly figure
    df = metrics_cache["memory"]
    fig = px.line(df, x="timestamp", y="value", color="window_type",
                 labels={"value": "Memory Usage (MB)", "timestamp": "Time"},
                 title="Memory Usage by Window Type")
    
    fig.update_layout(template="plotly_dark")
    
    # Convert to JSON
    chart_json = json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder))
    return jsonify(chart_json)

@app.route('/api/charts/comparison')
def get_comparison_chart():
    """Generate window strategy comparison chart"""
    if metrics_cache["latency"] is None or metrics_cache["throughput"] is None:
        load_latest_metrics()
    
    if metrics_cache["latency"] is None or metrics_cache["throughput"] is None:
        return jsonify({})
    
    # Calculate averages for each window type
    latency_df = metrics_cache["latency"]
    throughput_df = metrics_cache["throughput"]
    
    window_types = latency_df["window_type"].unique()
    avg_latency = []
    avg_throughput = []
    
    for window_type in window_types:
        avg_lat = latency_df[latency_df["window_type"] == window_type]["value"].mean()
        avg_thr = throughput_df[throughput_df["window_type"] == window_type]["value"].mean()
        avg_latency.append(avg_lat)
        avg_throughput.append(avg_thr)
    
    # Create plotly figure
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=window_types,
        y=avg_latency,
        name="Avg Latency (s)",
        marker_color="indianred"
    ))
    
    fig.add_trace(go.Bar(
        x=window_types,
        y=avg_throughput,
        name="Avg Throughput (events/s)",
        marker_color="lightsalmon",
        yaxis="y2"
    ))
    
    fig.update_layout(
        title="Window Strategy Performance Comparison",
        xaxis=dict(title="Window Type"),
        yaxis=dict(
            title="Avg Latency (seconds)",
            side="left"
        ),
        yaxis2=dict(
            title="Avg Throughput (events/second)",
            side="right",
            overlaying="y",
            rangemode="tozero"
        ),
        barmode="group",
        template="plotly_dark"
    )
    
    # Convert to JSON
    chart_json = json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder))
    return jsonify(chart_json)

# Start background refresh thread
refresh_thread = threading.Thread(target=refresh_metrics_periodically, daemon=True)

if __name__ == '__main__':
    # Load initial metrics
    load_latest_metrics()
    
    # Start background refresh thread
    refresh_thread.start()
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5006)