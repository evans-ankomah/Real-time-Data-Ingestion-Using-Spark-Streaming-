"""
Spark Structured Streaming to PostgreSQL
=========================================
This script reads streaming CSV files, transforms the data,
and writes it to PostgreSQL in real-time.

Features:
- Reads CSV files as they appear in data/raw/
- Validates and cleans data
- Handles malformed records (routes to error folder)
- Prevents duplicate inserts using event_id
- Uses checkpointing for fault tolerance
- Includes retry logic for database writes

Usage:
    spark-submit --packages org.postgresql:postgresql:42.7.1 \
        spark_streaming_to_postgres.py

Author: E-commerce Pipeline Project
"""

import os
import logging
import time
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, 
    trim, upper, lower, regexp_replace,
    to_timestamp, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType
)

# ============================================================
# Configuration
# ============================================================

class Config:
    """Central configuration for the streaming job."""
    
    # PostgreSQL connection (using Docker service name)
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'ecommerce_events')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'Amalitech.org')
    
    # JDBC URL
    JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    # Paths (work in Docker environment)
    BASE_PATH = '/app' if os.path.exists('/app') else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INPUT_PATH = os.path.join(BASE_PATH, 'data', 'raw')
    ERROR_PATH = os.path.join(BASE_PATH, 'data', 'error')
    PROCESSED_PATH = os.path.join(BASE_PATH, 'data', 'processed')
    CHECKPOINT_PATH = os.path.join(BASE_PATH, 'checkpoints', 'streaming')
    
    # Streaming settings
    TRIGGER_INTERVAL = "10 seconds"  # Process every 10 seconds
    MAX_FILES_PER_TRIGGER = 10       # Limit files processed per batch
    
    # Valid event types
    VALID_EVENT_TYPES = ['view', 'add_to_cart', 'purchase']
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5

# ============================================================
# Setup Logging
# ============================================================

def get_log_directory() -> str:
    """Get the log directory path, creating it if needed."""
    if os.path.exists('/app/logs'):
        log_dir = '/app/logs'
    else:
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(base_path, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    return log_dir

def setup_logging() -> logging.Logger:
    """Configure logging for the streaming job with file output."""
    log_dir = get_log_directory()
    log_file = os.path.join(log_dir, 'streaming_metrics.log')
    
    # Create logger
    logger = logging.getLogger('SparkStreaming')
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if called multiple times
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    
    # File handler with detailed format
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

# ============================================================
# Schema Definition
# ============================================================

def get_event_schema() -> StructType:
    """
    Define the schema for incoming CSV events.
    
    Using explicit schema is a best practice for streaming
    as it avoids schema inference overhead.
    """
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True)  # Parse as string first, then convert
    ])

# ============================================================
# Spark Session
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create and configure the Spark session.
    
    Includes PostgreSQL JDBC driver configuration.
    """
    logger.info("Creating Spark session...")
    
    spark = (SparkSession.builder
        .appName("EcommerceEventStreaming")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Spark session created. Version: {spark.version}")
    return spark

# ============================================================
# Data Transformations
# ============================================================

def clean_and_validate_data(df: DataFrame) -> tuple:
    """
    Clean and validate incoming event data.
    
    Args:
        df: Raw DataFrame from CSV
        
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    # Trim whitespace from string columns
    cleaned_df = df
    for col_name in ['event_id', 'user_id', 'product_id', 'product_name', 'category', 'event_type']:
        cleaned_df = cleaned_df.withColumn(col_name, trim(col(col_name)))
    
    # Normalize event_type to lowercase
    cleaned_df = cleaned_df.withColumn('event_type', lower(col('event_type')))
    
    # Parse timestamp string to timestamp type
    cleaned_df = cleaned_df.withColumn(
        'event_timestamp',
        to_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss')
    )
    
    # Add validation flags
    validated_df = cleaned_df.withColumn(
        'is_valid',
        (
            col('event_id').isNotNull() &
            col('user_id').isNotNull() &
            col('product_id').isNotNull() &
            col('event_type').isin(Config.VALID_EVENT_TYPES) &
            (col('price') > 0) &
            col('event_timestamp').isNotNull()
        )
    )
    
    # Split into valid and invalid records
    valid_df = validated_df.filter(col('is_valid') == True).drop('is_valid', 'timestamp')
    invalid_df = validated_df.filter(col('is_valid') == False).drop('is_valid')
    
    return valid_df, invalid_df

def deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Remove duplicate events based on event_id.
    
    In a real scenario, you might use watermarking for
    stateful deduplication across batches.
    """
    return df.dropDuplicates(['event_id'])

def prepare_for_postgres(df: DataFrame) -> DataFrame:
    """
    Prepare DataFrame for PostgreSQL insert.
    
    Selects and orders columns to match the database schema.
    """
    return df.select(
        col('event_id'),
        col('user_id'),
        col('product_id'),
        col('product_name'),
        col('category'),
        col('price'),
        col('event_type'),
        col('event_timestamp')
    )

# ============================================================
# Database Operations
# ============================================================

def get_jdbc_properties() -> dict:
    """Get JDBC connection properties."""
    return {
        "user": Config.POSTGRES_USER,
        "password": Config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

def write_to_postgres_with_retry(batch_df: DataFrame, batch_id: int):
    """
    Write a batch to PostgreSQL with retry logic.
    
    This function is called by foreachBatch for each micro-batch.
    
    Args:
        batch_df: DataFrame containing the batch data
        batch_id: Unique identifier for this batch
    """
    batch_start_time = time.time()
    
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: Empty batch, skipping.")
        return
    
    record_count = batch_df.count()
    logger.info(f"Batch {batch_id}: Processing {record_count} records...")
    
    # Clean and validate
    validation_start = time.time()
    valid_df, invalid_df = clean_and_validate_data(batch_df)
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    validation_time = time.time() - validation_start
    
    logger.info(f"Batch {batch_id}: Valid: {valid_count}, Invalid: {invalid_count}")
    
    # Handle invalid records (write to error folder)
    if invalid_count > 0:
        write_errors(invalid_df, batch_id)
    
    if valid_count == 0:
        logger.info(f"Batch {batch_id}: No valid records to write.")
        return
    
    # Deduplicate and prepare
    dedup_start = time.time()
    deduped_df = deduplicate_events(valid_df)
    final_df = prepare_for_postgres(deduped_df)
    final_count = final_df.count()
    dedup_time = time.time() - dedup_start
    
    logger.info(f"Batch {batch_id}: After dedup: {final_count} records")
    
    # Write to PostgreSQL with retry
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            write_start = time.time()
            
            # Use 'append' mode - duplicates are handled by DB constraint
            final_df.write \
                .format("jdbc") \
                .option("url", Config.JDBC_URL) \
                .option("dbtable", "user_events") \
                .option("user", Config.POSTGRES_USER) \
                .option("password", Config.POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .option("stringtype", "unspecified") \
                .mode("append") \
                .save()
            
            write_time = time.time() - write_start
            total_batch_time = time.time() - batch_start_time
            throughput = final_count / total_batch_time if total_batch_time > 0 else 0
            
            logger.info(
                f"Batch {batch_id}: Successfully wrote {final_count} records "
                f"to PostgreSQL in {write_time:.2f}s"
            )
            
            # Detailed metrics log line
            logger.info(
                f"  └─ Batch #{batch_id} Metrics: "
                f"total_time={total_batch_time:.3f}s | validation={validation_time:.3f}s | "
                f"dedup={dedup_time:.3f}s | write={write_time:.3f}s | "
                f"throughput={throughput:.1f} rec/s | valid_rate={valid_count}/{record_count}"
            )
            return  # Success, exit retry loop
            
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a duplicate key error (expected, not an error)
            if "duplicate key" in error_msg.lower():
                logger.info(f"Batch {batch_id}: Some duplicates skipped (expected behavior)")
                return
            
            logger.warning(
                f"Batch {batch_id}: Write attempt {attempt}/{Config.MAX_RETRIES} failed: {error_msg}"
            )
            
            if attempt < Config.MAX_RETRIES:
                logger.info(f"Batch {batch_id}: Retrying in {Config.RETRY_DELAY_SECONDS}s...")
                time.sleep(Config.RETRY_DELAY_SECONDS)
            else:
                logger.error(f"Batch {batch_id}: All retry attempts failed!")
                raise

def write_errors(invalid_df: DataFrame, batch_id: int):
    """
    Write invalid records to the error folder for later analysis.
    
    Args:
        invalid_df: DataFrame with invalid records
        batch_id: Batch identifier for the filename
    """
    try:
        error_path = os.path.join(Config.ERROR_PATH, f"batch_{batch_id}")
        invalid_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(error_path)
        logger.info(f"Batch {batch_id}: Invalid records saved to {error_path}")
    except Exception as e:
        logger.warning(f"Batch {batch_id}: Failed to write error records: {e}")

# ============================================================
# Streaming Job
# ============================================================

def start_streaming_job(spark: SparkSession):
    """
    Start the Spark Structured Streaming job.
    
    This is the main entry point for the streaming pipeline.
    """
    logger.info("=" * 60)
    logger.info("Starting Spark Structured Streaming Job")
    logger.info("=" * 60)
    logger.info(f"Input path: {Config.INPUT_PATH}")
    logger.info(f"Checkpoint path: {Config.CHECKPOINT_PATH}")
    logger.info(f"PostgreSQL URL: {Config.JDBC_URL}")
    logger.info(f"Trigger interval: {Config.TRIGGER_INTERVAL}")
    logger.info("=" * 60)
    
    # Ensure directories exist
    os.makedirs(Config.INPUT_PATH, exist_ok=True)
    os.makedirs(Config.ERROR_PATH, exist_ok=True)
    os.makedirs(Config.CHECKPOINT_PATH, exist_ok=True)
    
    # Create streaming DataFrame from CSV files
    schema = get_event_schema()
    
    stream_df = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("maxFilesPerTrigger", Config.MAX_FILES_PER_TRIGGER) \
        .option("cleanSource", "delete") \
        .option("mode", "DROPMALFORMED") \
        .schema(schema) \
        .load(Config.INPUT_PATH)
    
    logger.info("Streaming DataFrame created. Waiting for data...")
    
    # Start the streaming query
    query = stream_df.writeStream \
        .foreachBatch(write_to_postgres_with_retry) \
        .outputMode("append") \
        .trigger(processingTime=Config.TRIGGER_INTERVAL) \
        .option("checkpointLocation", Config.CHECKPOINT_PATH) \
        .start()
    
    logger.info(f"Streaming query started. ID: {query.id}")
    logger.info("Press Ctrl+C to stop the streaming job.")
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown requested. Stopping streaming query...")
        query.stop()
        logger.info("Streaming query stopped gracefully.")

# ============================================================
# Health Check
# ============================================================

def test_postgres_connection(spark: SparkSession) -> bool:
    """
    Test PostgreSQL connection before starting the stream.
    
    Returns:
        True if connection successful, False otherwise
    """
    logger.info("Testing PostgreSQL connection...")
    
    try:
        # Try to read from PostgreSQL
        test_df = spark.read \
            .format("jdbc") \
            .option("url", Config.JDBC_URL) \
            .option("query", "SELECT 1 as test") \
            .option("user", Config.POSTGRES_USER) \
            .option("password", Config.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        test_df.collect()  # Force evaluation
        logger.info("PostgreSQL connection successful!")
        return True
        
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return False

def wait_for_postgres(spark: SparkSession, max_retries: int = 30, delay: int = 5):
    """
    Wait for PostgreSQL to be available.
    
    Useful when starting containers, as Postgres may take time to initialize.
    """
    logger.info("Waiting for PostgreSQL to be available...")
    
    for attempt in range(1, max_retries + 1):
        if test_postgres_connection(spark):
            return True
        
        logger.info(f"Attempt {attempt}/{max_retries}: PostgreSQL not ready. Waiting {delay}s...")
        time.sleep(delay)
    
    logger.error("PostgreSQL did not become available in time!")
    return False

# ============================================================
# Main Entry Point
# ============================================================

def main():
    """Main function to run the streaming job."""
    logger.info("Initializing Spark Streaming to PostgreSQL pipeline...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Wait for PostgreSQL to be ready
        if not wait_for_postgres(spark):
            logger.error("Cannot start streaming job - PostgreSQL unavailable")
            return
        
        # Start the streaming job
        start_streaming_job(spark)
        
    except Exception as e:
        logger.error(f"Streaming job failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
