-- ============================================================
-- Support Logs
-- ============================================================
CREATE TABLE public.support_logs (
    timestamp       TIMESTAMP,
    log_level       VARCHAR(20),
    component       VARCHAR(100),
    ticket_id       VARCHAR(50),
    session_id      VARCHAR(50),
    ip              VARCHAR(45),
    response_time   BIGINT,
    cpu             DOUBLE PRECISION,
    event_type      VARCHAR(50),
    error           BOOLEAN,
    user_agent      VARCHAR(300),
    message         VARCHAR(1000),
    debug           VARCHAR(1000)
);


-- Load processed Parquet data from Amazon S3
COPY public.support_logs
FROM 's3://***/support-logs/processed/'
IAM_ROLE '***'
FORMAT AS PARQUET
REGION 'us-east-1';


-- Test the loaded data
SELECT COUNT(*) FROM public.support_logs;
SELECT * FROM public.support_logs;

-- **********************************************************************

-- ============================================================
-- Support Tickets
-- ============================================================
CREATE TABLE public.support_tickets (
    ticket_id       VARCHAR(50),
    created_at      TIMESTAMP,
    resolved_at     TIMESTAMP,
    agent           VARCHAR(100),
    priority        VARCHAR(20),
    num_interactions BIGINT,
    issue_category  VARCHAR(100),
    channel         VARCHAR(50),
    status          VARCHAR(20)
);


-- Load processed Parquet data from Amazon S3
COPY public.support_tickets
FROM 's3://***/support-tickets/processed/'
IAM_ROLE '***'
FORMAT AS PARQUET
REGION 'us-east-1';


-- Test the loaded data
SELECT COUNT(*) FROM public.support_tickets;
SELECT * FROM public.support_tickets;
