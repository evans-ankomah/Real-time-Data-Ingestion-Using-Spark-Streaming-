# Performance Metrics Report

This document records the performance characteristics of the real-time data ingestion pipeline.

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Test Date | 2026-01-30 |
| Duration | 60 seconds |
| Event Rate | 10 events/sec |


---

## 1. Throughput Metrics

### Generator Throughput

| Metric | Value |
|--------|-------|
| Target Rate | 10 events/sec |
| Actual Rate | 10.2 events/sec |
| Total Events Generated | 620 |
| Files Created | 62 |

### Spark Processing Throughput

| Metric | Value |
|--------|-------|
| Events Processed | 620 |
| Processing Time | 60 seconds |
| Effective Rate | 10.3 events/sec |
| Batches Processed | 6 |

**Query to measure**:
```sql
SELECT 
    COUNT(*) as total_events,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event,
    EXTRACT(EPOCH FROM (MAX(event_timestamp) - MIN(event_timestamp))) as duration_seconds
FROM user_events;
```

---

## 2. Latency Metrics

### End-to-End Latency

| Metric | Value |
|--------|-------|
| Average Latency | 8.62 seconds |
| Max Latency | 25.35 seconds |
| Min Latency | 2.06 seconds |

**Measurement Method**:
Latency = Time record appears in DB - Event timestamp


**Query to measure**:
```sql
SELECT 
    AVG(EXTRACT(EPOCH FROM (created_at - event_timestamp))) as avg_latency,
    MAX(EXTRACT(EPOCH FROM (created_at - event_timestamp))) as max_latency,
    MIN(EXTRACT(EPOCH FROM (created_at - event_timestamp))) as min_latency
FROM user_events
WHERE created_at IS NOT NULL;
```

### Batch Processing Time

| Batch | Records | Processing Time |
|-------|---------|-----------------|
| 1 | ~100 | 2.1 sec |
| 2 | ~100 | 1.8 sec |
| 3 | ~100 | 1.9 sec |

---

## 3. Resource Usage

### Container Memory Usage

```powershell
docker stats --no-stream
```

| Container | Memory Usage | Memory Limit |
|-----------|--------------|--------------|
| spark_app | 804 MB | 2048 MB |
| ecommerce_postgres | 44 MB | 512 MB |

### CPU Usage

| Container | CPU % |
|-----------|-------|
| spark_app | 0.55% |
| ecommerce_postgres | 0.00% |

---

## 4. Load Testing Results

### Test 1: Low Load (10 events/sec)

| Metric | Value |
|--------|-------|
| Duration | 60 sec |
| Events Generated | 620 |
| Events Processed | 620 |
| Success Rate | 100% |
| Avg Latency | 8.62 sec |

### Test 2: Medium Load (50 events/sec)

| Metric | Value |
|--------|-------|
| Duration | 60 sec |
| Events Generated | ~3000 |
| Events Processed | (test pending) |
| Success Rate | (test pending) |
| Avg Latency | (test pending) |

### Test 3: High Load (100 events/sec)

| Metric | Value |
|--------|-------|
| Duration | 60 sec |
| Events Generated | ~6000 |
| Events Processed | (test pending) |
| Success Rate | (test pending) |
| Avg Latency | (test pending) |

---

## 5. Recovery Performance

### Restart Time

| Scenario | Time to Resume |
|----------|----------------|
| Graceful stop + restart | ~5 sec |
| Kill + restart | ~10 sec |
| Container crash + restart | ~15 sec |

### Data Integrity After Restart

| Metric | Before | After |
|--------|--------|-------|
| Total Records | 4111 | 4111 |
| Duplicates | 0 | 0 |
| Lost Records | N/A | 0 |

---

## 6. Query Performance

### Sample Queries

| Query | Execution Time |
|-------|----------------|
| SELECT COUNT(*) | 0.45 ms |
| GROUP BY event_type | 1.87 ms |
| ORDER BY timestamp LIMIT 100 | ~2 ms |
| Date range filter | ~3 ms |

---

## 7. Observations & Recommendations

### Bottlenecks Identified

1. Max latency of 25s occurs when events arrive just after a batch completes (10s trigger interval)
2. Single Spark worker limits parallel processing capacity

### Recommendations for Improvement

1. Reduce trigger interval to 5 seconds for lower latency (trade-off: more frequent DB writes)
2. Add more Spark workers for higher throughput scenarios
3. Consider partitioning the PostgreSQL table by date for better query performance at scale

---

## Summary

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Throughput | 10+ evt/s | 10.2 evt/s | ✅ |
| Latency | < 30s | 8.62s avg | ✅ |
| Success Rate | 99%+ | 100% | ✅ |
| Recovery | < 1 min | ~15 sec | ✅ |

**Overall Assessment**: Pipeline performs well under low load (10 evt/s) with 100% success rate and sub-30s latency. Resource usage is efficient. Ready for production deployment with recommended optimizations for higher load scenarios.
