"""
Data Generator for E-commerce Events
=====================================
This script generates fake e-commerce user events and saves them
as CSV files for Spark Structured Streaming to process.

Features:
- Configurable event rate (events per second)
- Multiple event types: view, add_to_cart, purchase
- Realistic product data using Faker library
- Logging for monitoring
- Graceful shutdown handling

Usage:
    python data_generator.py --events-per-second 10 --duration 60
    python data_generator.py --events-per-second 5  # Run indefinitely

Author: E-commerce Pipeline Project
"""

import os
import csv
import uuid
import time
import random
import signal
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional

# Try to import Faker, provide fallback if not available
try:
    from faker import Faker
    fake = Faker()
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False
    print("Warning: Faker not installed. Using basic random data generation.")

# ============================================================
# Configuration
# ============================================================

# Event types with their relative probabilities
EVENT_TYPES = {
    'view': 0.6,        # 60% of events are views
    'add_to_cart': 0.3, # 30% are add to cart
    'purchase': 0.1     # 10% are purchases
}

# Product categories and sample products
CATEGORIES = {
    'Electronics': [
        ('Wireless Headphones', 79.99, 149.99),
        ('Smartphone Case', 15.99, 45.99),
        ('USB-C Cable', 9.99, 24.99),
        ('Portable Charger', 29.99, 79.99),
        ('Bluetooth Speaker', 39.99, 199.99),
        ('Laptop Stand', 25.99, 89.99),
        ('Webcam HD', 49.99, 129.99),
        ('Wireless Mouse', 19.99, 79.99),
    ],
    'Clothing': [
        ('Cotton T-Shirt', 14.99, 34.99),
        ('Denim Jeans', 39.99, 89.99),
        ('Running Shoes', 59.99, 149.99),
        ('Winter Jacket', 79.99, 199.99),
        ('Baseball Cap', 12.99, 29.99),
        ('Wool Sweater', 45.99, 99.99),
    ],
    'Home & Garden': [
        ('LED Desk Lamp', 24.99, 59.99),
        ('Plant Pot Set', 19.99, 49.99),
        ('Throw Blanket', 29.99, 79.99),
        ('Wall Clock', 15.99, 45.99),
        ('Coffee Maker', 49.99, 149.99),
    ],
    'Books': [
        ('Programming Python', 29.99, 49.99),
        ('Data Science Handbook', 34.99, 54.99),
        ('Mystery Novel', 9.99, 19.99),
        ('Self-Help Guide', 14.99, 24.99),
        ('Cooking Recipe Book', 19.99, 39.99),
    ],
    'Sports': [
        ('Yoga Mat', 19.99, 49.99),
        ('Dumbbell Set', 39.99, 129.99),
        ('Basketball', 19.99, 39.99),
        ('Tennis Racket', 49.99, 149.99),
        ('Fitness Tracker', 49.99, 199.99),
    ]
}

# ============================================================
# Setup Logging
# ============================================================

def setup_logging() -> logging.Logger:
    """Configure logging for the data generator."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('DataGenerator')

logger = setup_logging()

# ============================================================
# Global flag for graceful shutdown
# ============================================================

shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info("Shutdown signal received. Finishing current batch...")
    shutdown_requested = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================
# Event Generation Functions
# ============================================================

def generate_user_id() -> str:
    """Generate a realistic user ID."""
    if FAKER_AVAILABLE:
        return f"user_{fake.uuid4()[:8]}"
    return f"user_{uuid.uuid4().hex[:8]}"

def generate_product_id() -> str:
    """Generate a unique product ID."""
    return f"prod_{uuid.uuid4().hex[:8]}"

def select_event_type() -> str:
    """Select an event type based on configured probabilities."""
    rand = random.random()
    cumulative = 0
    for event_type, probability in EVENT_TYPES.items():
        cumulative += probability
        if rand <= cumulative:
            return event_type
    return 'view'  # Default fallback

def select_product() -> tuple:
    """Select a random product from the catalog."""
    category = random.choice(list(CATEGORIES.keys()))
    product_name, min_price, max_price = random.choice(CATEGORIES[category])
    price = round(random.uniform(min_price, max_price), 2)
    return category, product_name, price

def generate_event() -> Dict:
    """
    Generate a single e-commerce event.
    
    Returns:
        Dict with event data including:
        - event_id: Unique identifier for the event
        - user_id: User who performed the action
        - product_id: Product involved
        - product_name: Name of the product
        - category: Product category
        - price: Product price
        - event_type: Type of action (view, add_to_cart, purchase)
        - timestamp: When the event occurred
    """
    category, product_name, price = select_product()
    
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': generate_user_id(),
        'product_id': generate_product_id(),
        'product_name': product_name,
        'category': category,
        'price': price,
        'event_type': select_event_type(),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def generate_batch(batch_size: int) -> List[Dict]:
    """
    Generate a batch of events.
    
    Args:
        batch_size: Number of events to generate
        
    Returns:
        List of event dictionaries
    """
    return [generate_event() for _ in range(batch_size)]

# ============================================================
# File Writing Functions
# ============================================================

def get_output_directory() -> str:
    """Get the output directory path, creating it if needed."""
    # Determine base path (works in Docker and local)
    if os.path.exists('/app/data/raw'):
        output_dir = '/app/data/raw'
    else:
        # Local development - relative to script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, '..', 'data', 'raw')
        output_dir = os.path.abspath(output_dir)
    
    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def write_events_to_csv(events: List[Dict], output_dir: str) -> str:
    """
    Write a batch of events to a CSV file.
    
    Args:
        events: List of event dictionaries
        output_dir: Directory to write the file to
        
    Returns:
        Path to the created file
    """
    # Generate unique filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    filename = f"events_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Define CSV columns (order matters)
    fieldnames = [
        'event_id', 'user_id', 'product_id', 'product_name',
        'category', 'price', 'event_type', 'timestamp'
    ]
    
    # Write events to CSV
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)
    
    return filepath

# ============================================================
# Rate Limiting
# ============================================================

class RateLimiter:
    """
    Simple rate limiter to control event generation rate.
    
    This ensures we generate exactly the specified number of
    events per second, regardless of processing time.
    """
    
    def __init__(self, events_per_second: int):
        """
        Initialize the rate limiter.
        
        Args:
            events_per_second: Target rate of event generation
        """
        self.events_per_second = events_per_second
        self.interval = 1.0 / events_per_second if events_per_second > 0 else 1.0
        self.last_event_time = time.time()
        self.events_this_second = 0
        self.second_start = time.time()
    
    def wait(self):
        """Wait if necessary to maintain the target rate."""
        current_time = time.time()
        
        # Reset counter every second
        if current_time - self.second_start >= 1.0:
            self.events_this_second = 0
            self.second_start = current_time
        
        # If we've hit the rate limit, wait until next second
        if self.events_this_second >= self.events_per_second:
            sleep_time = 1.0 - (current_time - self.second_start)
            if sleep_time > 0:
                time.sleep(sleep_time)
            self.events_this_second = 0
            self.second_start = time.time()
        
        self.events_this_second += 1

# ============================================================
# Main Generator Loop
# ============================================================

def run_generator(
    events_per_second: int = 10,
    batch_size: int = 10,
    duration: Optional[int] = None,
    output_dir: Optional[str] = None
):
    """
    Main function to run the event generator.
    
    Args:
        events_per_second: Number of events to generate per second
        batch_size: Number of events per CSV file
        duration: How long to run in seconds (None for indefinite)
        output_dir: Override output directory
    """
    global shutdown_requested
    
    # Setup
    if output_dir is None:
        output_dir = get_output_directory()
    
    rate_limiter = RateLimiter(events_per_second)
    start_time = time.time()
    total_events = 0
    total_files = 0
    
    logger.info("=" * 60)
    logger.info("E-commerce Event Generator Started")
    logger.info("=" * 60)
    logger.info(f"Events per second: {events_per_second}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Duration: {'Indefinite' if duration is None else f'{duration} seconds'}")
    logger.info("=" * 60)
    logger.info("Press Ctrl+C to stop gracefully")
    logger.info("")
    
    try:
        while not shutdown_requested:
            # Check duration limit
            if duration is not None:
                elapsed = time.time() - start_time
                if elapsed >= duration:
                    logger.info(f"Duration limit ({duration}s) reached.")
                    break
            
            # Generate batch of events
            batch_start = time.time()
            events = []
            
            for _ in range(batch_size):
                if shutdown_requested:
                    break
                rate_limiter.wait()
                events.append(generate_event())
            
            # Write to file
            if events:
                filepath = write_events_to_csv(events, output_dir)
                total_events += len(events)
                total_files += 1
                
                # Log progress every file
                elapsed = time.time() - start_time
                rate = total_events / elapsed if elapsed > 0 else 0
                logger.info(
                    f"File #{total_files}: {os.path.basename(filepath)} | "
                    f"Events: {len(events)} | Total: {total_events} | "
                    f"Rate: {rate:.1f} evt/s"
                )
    
    except Exception as e:
        logger.error(f"Error during generation: {e}")
        raise
    
    finally:
        # Print summary
        elapsed = time.time() - start_time
        logger.info("")
        logger.info("=" * 60)
        logger.info("Generator Stopped - Summary")
        logger.info("=" * 60)
        logger.info(f"Total events generated: {total_events}")
        logger.info(f"Total files created: {total_files}")
        logger.info(f"Elapsed time: {elapsed:.1f} seconds")
        logger.info(f"Average rate: {total_events / elapsed:.1f} events/second" if elapsed > 0 else "N/A")
        logger.info("=" * 60)

# ============================================================
# Command Line Interface
# ============================================================

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate fake e-commerce events for Spark Streaming',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate 10 events/second indefinitely
    python data_generator.py --events-per-second 10
    
    # Generate 5 events/second for 60 seconds
    python data_generator.py --events-per-second 5 --duration 60
    
    # Generate with custom batch size
    python data_generator.py --events-per-second 20 --batch-size 20
        """
    )
    
    parser.add_argument(
        '--events-per-second', '-e',
        type=int,
        default=10,
        help='Number of events to generate per second (default: 10)'
    )
    
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=10,
        help='Number of events per CSV file (default: 10)'
    )
    
    parser.add_argument(
        '--duration', '-d',
        type=int,
        default=None,
        help='Duration in seconds (default: run indefinitely)'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default=None,
        help='Output directory for CSV files (default: data/raw)'
    )
    
    return parser.parse_args()

# ============================================================
# Entry Point
# ============================================================

if __name__ == '__main__':
    args = parse_arguments()
    
    run_generator(
        events_per_second=args.events_per_second,
        batch_size=args.batch_size,
        duration=args.duration,
        output_dir=args.output_dir
    )
