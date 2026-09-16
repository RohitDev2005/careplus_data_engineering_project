--Create the database:
create database careplus_db;


-- SQL Queries:
-- Q1: What is the ticket load by channel (Email, Chat, etc.)?
-- 	Insight: Understand user preference and optimize staffing.
SELECT channel, COUNT(*) AS ticket_count
FROM support_tickets_processed
GROUP BY channel
ORDER BY ticket_count DESC;


-- Q2: Total Tickets By Status
SELECT
  COUNT(*) AS total_tickets,
  SUM(CASE WHEN status = 'Resolved'  THEN 1 ELSE 0 END) AS resolved_tickets,
  SUM(CASE WHEN status = 'Open'      THEN 1 ELSE 0 END) AS open_tickets,
  SUM(CASE WHEN status = 'Escalated' THEN 1 ELSE 0 END) AS escalated_tickets
FROM support_tickets_processed DESC;


-- Q3: Query to find the total number of events per user agent
SELECT user_agent, COUNT(*) AS event_count
FROM support_logs_processed
GROUP BY user_agent
ORDER BY event_count DESC;


-- Q4: Ticket trend by day
SELECT
  DATE(created_at) AS day,
  COUNT(*) AS tickets_created
FROM support_tickets_processed
GROUP BY DATE(created_at)
ORDER BY day;


-- Q5: Query to find the number of events with debug level logs
SELECT COUNT(*) AS debug_event_count
FROM support_logs_processed
WHERE log_level = 'DEBUG';


-- Q6: Query to find the average CPU usage per user agent
SELECT user_agent, AVG(cpu) AS avg_cpu_usage
FROM support_logs_processed
GROUP BY user_agent
ORDER BY avg_cpu_usage DESC;
