# Performance Metrics Report

This document records the performance characteristics of the real-time data ingestion pipeline.

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Test Date | _______________ |
| Duration | _______________ |
| Event Rate | _____ events/sec |
| Spark Workers | 1 |
| Worker Memory | 2 GB |
| Worker Cores | 2 |
| Trigger Interval | 10 seconds |

---

## 1. Throughput Metrics

### Generator Throughput

| Metric | Value |
|--------|-------|
| Target Rate | _____ events/sec |
| Actual Rate | _____ events/sec |
| Total Events Generated | _____ |
| Files Created | _____ |

### Spark Processing Throughput

| Metric | Value |
|--------|-------|
| Events Processed | _____ |
| Processing Time | _____ seconds |
| Effective Rate | _____ events/sec |
| Batches Processed | _____ |

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
| Average Latency | _____ seconds |
| Max Latency | _____ seconds |
| Min Latency | _____ seconds |

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
| 1 | _____ | _____ sec |
| 2 | _____ | _____ sec |
| 3 | _____ | _____ sec |

---

## 3. Resource Usage

### Container Memory Usage

```powershell
docker stats --no-stream
```

| Container | Memory Usage | Memory Limit |
|-----------|--------------|--------------|
| spark_master | _____ MB | _____ MB |
| spark_worker | _____ MB | _____ MB |
| spark_submit | _____ MB | _____ MB |
| postgres | _____ MB | _____ MB |

### CPU Usage

| Container | CPU % |
|-----------|-------|
| spark_master | _____ % |
| spark_worker | _____ % |
| spark_submit | _____ % |
| postgres | _____ % |

---

## 4. Load Testing Results

### Test 1: Low Load (10 events/sec)

| Metric | Value |
|--------|-------|
| Duration | 60 sec |
| Events Generated | ~600 |
| Events Processed | _____ |
| Success Rate | _____ % |
| Avg Latency | _____ sec |

### Test 2: Medium Load (50 events/sec)

| Metric | Value |
|--------|-------|
| Duration | 60 sec |
| Events Generated | ~3000 |
| Events Processed | _____ |
| Success Rate | _____ % |
| Avg Latency | _____ sec |

### Test 3: High Load (100 events/sec)

| Metric | Value |
|--------|-------|
| Duration | 60 sec |
| Events Generated | ~6000 |
| Events Processed | _____ |
| Success Rate | _____ % |
| Avg Latency | _____ sec |

---

## 5. Recovery Performance

### Restart Time

| Scenario | Time to Resume |
|----------|----------------|
| Graceful stop + restart | _____ sec |
| Kill + restart | _____ sec |
| Container crash + restart | _____ sec |

### Data Integrity After Restart

| Metric | Before | After |
|--------|--------|-------|
| Total Records | _____ | _____ |
| Duplicates | 0 | _____ |
| Lost Records | N/A | _____ |

---

## 6. Query Performance

### Sample Queries

| Query | Execution Time |
|-------|----------------|
| SELECT COUNT(*) | _____ ms |
| GROUP BY event_type | _____ ms |
| ORDER BY timestamp LIMIT 100 | _____ ms |
| Date range filter | _____ ms |

---

## 7. Observations & Recommendations

### Bottlenecks Identified

1. _________________________________
2. _________________________________

### Recommendations for Improvement

1. _________________________________
2. _________________________________

---

## Summary

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Throughput | 10+ evt/s | _____ | ☐ |
| Latency | < 30s | _____ | ☐ |
| Success Rate | 99%+ | _____ | ☐ |
| Recovery | < 1 min | _____ | ☐ |

**Overall Assessment**: ____________________
