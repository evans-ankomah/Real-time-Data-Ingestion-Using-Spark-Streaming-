# Test Cases: E-commerce Event Pipeline

This document outlines test cases to verify the pipeline works correctly.

---

## Test Environment Setup

Before running tests, ensure:
1. All containers are running: `docker-compose ps`
2. PostgreSQL is healthy
3. No old data: `docker-compose down -v` for fresh start

---

## Test Case 1: CSV File Generation

**Objective**: Verify data generator creates valid CSV files.

**Steps**:
1. Start the generator for 10 seconds:
   ```powershell
   docker-compose exec spark-submit python /app/src/data_generator.py --events-per-second 5 --duration 10
   ```
2. List generated files:
   ```powershell
   Get-ChildItem data\raw\
   ```

**Expected Result**:
- [ ] 5+ CSV files created
- [ ] Files have ~5-10 events each
- [ ] Proper CSV format with headers

**Actual Result**: _________________

**Status** : ☐ Pass ☐ Fail

---

## Test Case 2: Streaming Job Starts Successfully

**Objective**: Verify Spark streaming job connects and starts.

**Steps**:
1. Start the streaming job:
   ```powershell
   docker-compose exec spark-submit spark-submit --packages org.postgresql:postgresql:42.7.1 /app/src/spark_streaming_to_postgres.py
   ```

**Expected Result**:
- [ ] "PostgreSQL connection successful!" message
- [ ] "Streaming query started" message
- [ ] No error messages

**Actual Result**: _________________

**Status**: ☐ Pass ☐ Fail

---

## Test Case 3: End-to-End Data Flow

**Objective**: Verify data flows from generator → Spark → PostgreSQL.

**Steps**:
1. Start streaming job (Terminal 1)
2. Start generator for 30 seconds (Terminal 2):
   ```powershell
   docker-compose exec spark-submit python /app/src/data_generator.py --events-per-second 10 --duration 30
   ```
3. Wait 30 seconds
4. Query PostgreSQL:
   ```powershell
   docker-compose exec postgres psql -U postgres -d ecommerce_events -c "SELECT COUNT(*) FROM user_events;"
   ```

**Expected Result**:
- [ ] ~300 events generated
- [ ] ~300 records in database
- [ ] Count increases over time

**Actual Result**: _________________

**Status**: ☐ Pass ☐ Fail

---

## Test Case 4: Data Validation

**Objective**: Verify data in PostgreSQL matches expected schema.

**Steps**:
1. After running Test Case 3, query random records:
   ```sql
   SELECT * FROM user_events ORDER BY RANDOM() LIMIT 5;
   ```

**Expected Result**:
- [ ] `event_id` is UUID format
- [ ] `event_type` is one of: view, add_to_cart, purchase
- [ ] `price` is positive decimal
- [ ] `event_timestamp` is valid timestamp

**Actual Result**: _________________

**Status**: ☐ Pass ☐ Fail

---

## Test Case 5: Duplicate Prevention

**Objective**: Verify duplicate events are not inserted.

**Steps**:
1. Get current count:
   ```sql
   SELECT COUNT(*) FROM user_events;
   ```
2. Stop streaming job (Ctrl+C)
3. Move processed files back to raw:
   ```powershell
   Copy-Item data\processed\* data\raw\ -Recurse
   ```
4. Restart streaming job
5. Wait 30 seconds
6. Get new count

**Expected Result**:
- [ ] Count stays the same (duplicates rejected)
- [ ] No errors in streaming job

**Actual Result**: _________________

**Status**: ☐ Pass ☐ Fail

---

## Test Case 6: Fault Tolerance (Restart Recovery)

**Objective**: Verify pipeline recovers from restart without data loss.

**Steps**:
1. Start streaming job
2. Generate 100 events over 10 seconds
3. Note the count in PostgreSQL
4. Kill streaming job (Ctrl+C)
5. Generate 100 more events
6. Restart streaming job
7. Wait 30 seconds
8. Check final count

**Expected Result**:
- [ ] Final count ≈ 200
- [ ] No data loss after restart
- [ ] Checkpoints ensure continuity

**Actual Result**: _________________

**Status**: ☐ Pass ☐ Fail

---

## Test Case 7: Malformed Data Handling

**Objective**: Verify invalid records are routed to error folder.

**Steps**:
1. Create a malformed CSV in `data/raw/`:
   ```powershell
   echo "event_id,user_id,product_id,product_name,category,price,event_type,timestamp" > data\raw\malformed.csv
   echo "bad_id,user1,prod1,Test,Cat,invalid_price,view,2026-01-15 10:00:00" >> data\raw\malformed.csv
   ```
2. Wait for streaming job to process
3. Check error folder:
   ```powershell
   Get-ChildItem data\error\
   ```

**Expected Result**:
- [ ] Malformed record in `data/error/`
- [ ] Streaming job continues without crash

**Actual Result**: _________________

**Status**: ☐ Pass ☐ Fail

---

## Test Case 8: Performance Under Load

**Objective**: Verify pipeline handles high throughput.

**Steps**:
1. Start streaming job
2. Generate high-volume events:
   ```powershell
   docker-compose exec spark-submit python /app/src/data_generator.py --events-per-second 100 --duration 60
   ```
3. Monitor Spark UI at http://localhost:4040
4. Query final count

**Expected Result**:
- [ ] ~6000 events generated
- [ ] ~6000 records in database
- [ ] Processing lag < 30 seconds
- [ ] No memory errors

**Actual Result**: _________________

**Status**: ☐ Pass ☐ Fail

---

## Test Summary

| Test Case | Description | Status |
|-----------|-------------|--------|
| TC-1 | CSV File Generation | ☐ |
| TC-2 | Streaming Job Starts | ☐ |
| TC-3 | End-to-End Data Flow | ☐ |
| TC-4 | Data Validation | ☐ |
| TC-5 | Duplicate Prevention | ☐ |
| TC-6 | Fault Tolerance | ☐ |
| TC-7 | Malformed Data Handling | ☐ |
| TC-8 | Performance Under Load | ☐ |

**Overall Status**: ____/8 Tests Passed
