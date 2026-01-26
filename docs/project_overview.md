# Project Overview: Real-Time E-commerce Event Pipeline

## Introduction

This project implements a **real-time data ingestion pipeline** that simulates an e-commerce platform tracking user activity. The pipeline generates fake user events, streams them using Apache Spark Structured Streaming, and stores the processed data in a PostgreSQL database.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Docker Environment                               │
│                                                                          │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐ │
│  │                  │     │                  │     │                  │ │
│  │  Data Generator  │────▶│  data/raw/       │────▶│  Spark Streaming │ │
│  │  (Python)        │     │  (CSV files)     │     │  (PySpark)       │ │
│  │                  │     │                  │     │                  │ │
│  └──────────────────┘     └──────────────────┘     └────────┬─────────┘ │
│                                                              │           │
│                           ┌──────────────────┐               │           │
│                           │  checkpoints/    │◀──────────────┤           │
│                           │  (Recovery)      │               │           │
│                           └──────────────────┘               │           │
│                                                              │           │
│        ┌──────────────────┐                    ┌─────────────▼────────┐ │
│        │  data/error/     │◀───────────────────│                      │ │
│        │  (Bad records)   │    Invalid data    │    PostgreSQL        │ │
│        └──────────────────┘                    │    (user_events)     │ │
│                                                │                      │ │
│                                                └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Data Generator (`src/data_generator.py`)

**Purpose**: Simulates e-commerce user activity by generating fake events.

**Features**:
- Configurable event rate (default: 10 events/second)
- Three event types: `view` (60%), `add_to_cart` (30%), `purchase` (10%)
- Realistic product data across 5 categories
- Rate limiting to control throughput
- Graceful shutdown handling

**Output**: CSV files in `data/raw/` with fields:
- `event_id`, `user_id`, `product_id`, `product_name`
- `category`, `price`, `event_type`, `timestamp`

---

### 2. Spark Streaming Job (`src/spark_streaming_to_postgres.py`)

**Purpose**: Reads CSV files in real-time, transforms data, and writes to PostgreSQL.

**Features**:
- File-based streaming source (monitors `data/raw/`)
- Data validation and cleaning
- Duplicate handling via `event_id`
- Error records routed to `data/error/`
- Checkpointing for fault tolerance
- Retry logic for database writes

**Processing Steps**:
1. Read new CSV files (every 10 seconds)
2. Validate schema and data quality
3. Remove duplicates
4. Write to PostgreSQL
5. Update checkpoint

---

### 3. PostgreSQL Database

**Purpose**: Persistent storage for processed events.

**Table**: `user_events`
- Unique constraint on `event_id` prevents duplicates
- Indexes on `user_id`, `event_type`, `timestamp` for query performance

---

## Data Flow

1. **Generation**: Data generator creates CSV files with fake events
2. **Ingestion**: Spark detects new files in `data/raw/`
3. **Processing**: Data is validated, cleaned, and deduplicated
4. **Storage**: Valid records inserted into PostgreSQL
5. **Recovery**: Checkpoints enable restart without data loss

---

## Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Container Runtime | Docker | 29.x |
| Stream Processing | Apache Spark | 3.5 |
| Database | PostgreSQL | 15 |
| Data Generation | Python + Faker | 3.x |

---

## Key Design Decisions

1. **File-based Streaming**: Using CSV files as the streaming source simplifies testing and visualization of data flow.

2. **Docker-based Architecture**: Ensures consistent environment across all machines without manual Spark/Postgres installation.

3. **foreachBatch Processing**: Enables custom logic per micro-batch, including retry handling and error routing.

4. **Checkpoint-based Recovery**: Spark maintains state to resume from the last processed file after restart.

---

## Performance Characteristics

- **Latency**: ~10-15 seconds (configurable trigger interval)
- **Throughput**: Scales with Spark worker resources
- **Recovery Time**: Near-instant restart from checkpoint
