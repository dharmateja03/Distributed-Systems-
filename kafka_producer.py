#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Producer for Market Events
Generates simulated financial market data and publishes to Kafka
"""

import json
import time
import random
import datetime
from kafka import KafkaProducer
import argparse

class MarketDataProducer:
    """Produces market data events to Kafka"""
    
    def __init__(self, bootstrap_servers, topic_name="market-events"):
        """Initialize the producer with Kafka connection settings"""
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "FB", "NFLX"]
        self.trade_types = ["BUY", "SELL"]
        self.user_ids = [f"user_{i}" for i in range(1, 101)]
        
        # Create Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print(f"Connected to Kafka at {bootstrap_servers}")
        print(f"Producing to topic: {topic_name}")
    
    def generate_event(self, add_delay=False):
        """Generate a random market event"""
        # Base timestamp is current time
        event_time = datetime.datetime.now()
        
        # Add random delay for some events (simulate late data)
        if add_delay and random.random() < 0.1:  # 10% chance of late event
            event_time = event_time - datetime.timedelta(minutes=random.randint(1, 5))
        
        # Create event data
        event = {
            "event_time": event_time.isoformat(),
            "symbol": random.choice(self.symbols),
            "price": round(random.uniform(100.0, 1000.0), 2),
            "volume": random.randint(10, 1000),
            "user_id": random.choice(self.user_ids),
            "trade_type": random.choice(self.trade_types)
        }
        
        return event
    
    def produce_events(self, num_events=1000, interval=0.1, infinite=False, add_delays=True):
        """Produce a stream of events to Kafka"""
        count = 0
        
        try:
            while infinite or count < num_events:
                # Generate and send event
                event = self.generate_event(add_delays)
                self.producer.send(self.topic_name, event)
                
                # Log progress
                count += 1
                if count % 100 == 0:
                    print(f"Produced {count} events")
                
                # Wait a bit between events
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print("Producer stopped by user")
        finally:
            # Ensure all messages are sent
            self.producer.flush()
            print(f"Total events produced: {count}")
    
    def close(self):
        """Close the producer connection"""
        self.producer.close()
        print("Producer closed")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Kafka Market Data Producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="market-events", help="Kafka topic to produce to")
    parser.add_argument("--events", type=int, default=1000, help="Number of events to produce (0 for infinite)")
    parser.add_argument("--interval", type=float, default=0.1, help="Interval between events in seconds")
    
    args = parser.parse_args()
    
    # Create and run producer
    producer = MarketDataProducer(args.bootstrap_servers, args.topic)
    
    try:
        producer.produce_events(
            num_events=args.events,
            interval=args.interval,
            infinite=args.events <= 0,
            add_delays=True
        )
    finally:
        producer.close()

if __name__ == "__main__":
    main()