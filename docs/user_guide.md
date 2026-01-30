# User Guide: Running the E-commerce Event Pipeline

This guide provides step-by-step instructions to run the real-time data ingestion pipeline.

---

## Prerequisites

Before starting, ensure you have:

- ✅ **Docker Desktop** installed (version 20.x or higher)
- ✅ **4GB+ RAM** available for containers
- ✅ **Ports available**: 5432, 4040, 8080

**Verify installation:**
```powershell
docker --version
docker compose version
```

---

## Quick Start (5 Minutes)

### Step 1: Navigate to Project Directory

```powershell
cd "c:\Users\EvansAnkomah\Downloads\py\Real-Time Data Ingestion Using Spark Streaming"
```

### Step 2: Build and Start Containers

```powershell
docker compose up -d --build
```

This starts:
- PostgreSQL database
- Spark container (all-in-one: master + worker)

**Wait 30-60 seconds** for all services to initialize.

### Step 3: Verify Containers Are Running

```powershell
docker compose ps
```

Expected output shows all containers as "Up":
```
NAME                 STATUS              PORTS
ecommerce_postgres   Up (healthy)        0.0.0.0:5432->5432/tcp
spark_app            Up                  0.0.0.0:4040->4040/tcp, 0.0.0.0:8080->8080/tcp
```

### Step 4: Start the Spark Streaming Job

```powershell
docker compose exec spark spark-submit /app/src/spark_streaming_to_postgres.py
```

You'll see:
```
Spark Structured Streaming Job Started
Waiting for PostgreSQL...
PostgreSQL connection successful!
Streaming query started. Waiting for data...
```

### Step 5: Generate Events (New Terminal)

Open a **new terminal** and start the data generator:

```powershell
cd "c:\Users\EvansAnkomah\Downloads\py\Real-Time Data Ingestion Using Spark Streaming"
docker compose exec spark python3 /app/src/data_generator.py --events-per-second 10
```

You'll see events being generated with detailed metrics:
```
E-commerce Event Generator Started
File #1: events_20260130_160000_123456.csv | Events: 10 | Total: 10 | Rate: 10.1 evt/s
  └─ Batch #1 Metrics: batch_time=0.991s | types=[view:6, add_to_cart:3, purchase:1] | avg_price=$54.99
```

### Step 6: Verify Data in PostgreSQL

Open a **third terminal**:

```powershell
docker compose exec postgres psql -U postgres -d ecommerce_events -c "SELECT COUNT(*) FROM user_events;"
```

You should see the count increasing as events are processed!

---

## Detailed Usage

### Changing Event Rate

Generate more events per second:
```powershell
docker compose exec spark python3 /app/src/data_generator.py --events-per-second 50
```

Generate for a specific duration (e.g., 60 seconds):
```powershell
docker compose exec spark python3 /app/src/data_generator.py --events-per-second 10 --duration 60
```

### Monitoring Spark UI

Open your browser and navigate to:
- **Spark Master UI**: http://localhost:8080
- **Streaming Job UI**: http://localhost:4040 (while job is running)

### Viewing Performance Logs

Check the `logs/` directory for detailed batch-by-batch metrics:

```powershell
# Generator metrics
Get-Content logs\generator_metrics.log -Tail 20

# Streaming metrics
Get-Content logs\streaming_metrics.log -Tail 20
```

Sample log output:
```
2026-01-30 07:56:55 | INFO | SparkStreaming | Batch 48: Successfully wrote 100 records to PostgreSQL in 0.72s
2026-01-30 07:56:55 | INFO | SparkStreaming |   └─ Batch #48 Metrics: total_time=2.835s | validation=1.148s | dedup=0.541s | write=0.720s | throughput=35.3 rec/s
```

### Querying PostgreSQL

Connect to PostgreSQL:
```powershell
docker compose exec postgres psql -U postgres -d ecommerce_events
```

Sample queries:
```sql
-- Total events
SELECT COUNT(*) FROM user_events;

-- Events by type
SELECT event_type, COUNT(*) 
FROM user_events 
GROUP BY event_type;

-- Recent events
SELECT * FROM user_events 
ORDER BY event_timestamp DESC 
LIMIT 10;

-- Events per minute
SELECT 
    DATE_TRUNC('minute', event_timestamp) as minute,
    COUNT(*) as events
FROM user_events
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;

-- Using pre-built reporting views 
SELECT * FROM v_event_summary;           -- Aggregated stats by type
SELECT * FROM v_hourly_events;           -- Time-series trends
SELECT * FROM v_purchase_funnel;         -- User journey analysis
SELECT * FROM v_top_products LIMIT 10;   -- Top selling products
SELECT * FROM v_recent_activity;         -- Latest 100 events
```

Exit psql: `\q`

---

## Connecting with pgAdmin

You can also use pgAdmin or any PostgreSQL client:

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `ecommerce_events` |
| Username | `postgres` |
| Password | `Amalitech.org` |

---

## Stopping the Pipeline

### Stop Event Generator
Press `Ctrl+C` in the generator terminal.

### Stop Spark Streaming
Press `Ctrl+C` in the streaming terminal.

### Stop All Containers
```powershell
docker compose down
```

### Stop and Remove Data (Fresh Start)
```powershell
docker compose down -v
Remove-Item -Recurse -Force data\raw\*, checkpoints\*, logs\*
```

---

## Restarting After a Crash

The pipeline is designed to recover from crashes:

1. **Checkpoints**: Spark remembers which files were processed
2. **No duplicates**: Database constraint prevents duplicate `event_id`

Simply restart the streaming job:
```powershell
docker compose exec spark spark-submit /app/src/spark_streaming_to_postgres.py
```

---

## Troubleshooting

### Container Won't Start
```powershell
docker compose logs postgres
docker compose logs spark
```

### Port Already in Use
Stop conflicting service or change ports in `docker-compose.yml`.

### Database Connection Refused
Wait longer for PostgreSQL to initialize, or check:
```powershell
docker compose exec postgres pg_isready -U postgres
```

### No Data Appearing in Database
1. Check streaming job is running
2. Check CSV files exist in `data/raw/`
3. Check `data/error/` for invalid records
4. Check `logs/streaming_metrics.log` for errors

---

## Common Commands Reference

| Action | Command |
|--------|---------|
| Start all | `docker compose up -d --build` |
| Stop all | `docker compose down` |
| View logs | `docker compose logs -f` |
| Check status | `docker compose ps` |
| Enter Postgres | `docker compose exec postgres psql -U postgres -d ecommerce_events` |
| Enter Spark container | `docker compose exec spark bash` |
| Run streaming job | `docker compose exec spark spark-submit /app/src/spark_streaming_to_postgres.py` |
| Generate events | `docker compose exec spark python3 /app/src/data_generator.py --events-per-second 10` |
| View generator logs | `Get-Content logs\generator_metrics.log -Tail 20` |
| View streaming logs | `Get-Content logs\streaming_metrics.log -Tail 20` |
