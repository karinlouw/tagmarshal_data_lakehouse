-- Migration: Create ingestion_log table for tracking data pipeline state
-- This table prevents duplicate ingestions and helps detect missing data

CREATE TABLE IF NOT EXISTS ingestion_log (
    id SERIAL PRIMARY KEY,
    
    -- File identification
    filename VARCHAR(255) NOT NULL,
    course_id VARCHAR(100) NOT NULL,
    ingest_date DATE NOT NULL,
    
    -- Pipeline layer (bronze, silver, gold)
    layer VARCHAR(20) NOT NULL,
    
    -- Processing metadata
    rows_processed INTEGER,
    file_size_bytes BIGINT,
    file_hash VARCHAR(64),  -- MD5 hash for change detection
    
    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, running, success, failed, skipped
    
    -- Storage location
    s3_path VARCHAR(500),
    
    -- Timing
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    duration_seconds NUMERIC(10,2),
    
    -- Error handling
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Airflow metadata
    dag_run_id VARCHAR(255),
    task_id VARCHAR(100),
    
    -- Prevent duplicate ingestions for same file+date+layer
    UNIQUE(filename, ingest_date, layer)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_ingestion_log_course_date 
    ON ingestion_log(course_id, ingest_date);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_status 
    ON ingestion_log(status);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_layer_date 
    ON ingestion_log(layer, ingest_date);

-- View for today's ingestion summary
CREATE OR REPLACE VIEW ingestion_summary AS
SELECT 
    course_id,
    layer,
    status,
    rows_processed,
    duration_seconds,
    completed_at
FROM ingestion_log
WHERE ingest_date = CURRENT_DATE
ORDER BY completed_at DESC;

-- View for missing ingestions (courses not processed today)
CREATE OR REPLACE VIEW missing_ingestions AS
SELECT DISTINCT 
    l.course_id,
    l.layer,
    MAX(l.ingest_date) as last_ingested
FROM ingestion_log l
WHERE l.status = 'success'
  AND l.ingest_date < CURRENT_DATE
  AND NOT EXISTS (
      SELECT 1 FROM ingestion_log l2 
      WHERE l2.course_id = l.course_id 
        AND l2.layer = l.layer
        AND l2.ingest_date = CURRENT_DATE
        AND l2.status = 'success'
  )
GROUP BY l.course_id, l.layer;

COMMENT ON TABLE ingestion_log IS 'Tracks all data ingestion attempts for idempotency and observability';

