CREATE OR REPLACE VIEW dw.v_ticket_volume_per_day AS
SELECT
    DATE(created_time) AS ticket_date,
    COUNT(ticket_id) AS total_ticket
FROM dw.fact_ticket
GROUP BY DATE(created_time)
ORDER BY ticket_date;

CREATE OR REPLACE VIEW dw.v_mttr AS
SELECT
    DATE(created_time) AS ticket_date,
    AVG(resolution_time_min) AS avg_resolution_minutes
FROM dw.fact_ticket
WHERE resolution_time_min IS NOT NULL
GROUP BY DATE(created_time)
ORDER BY ticket_date;

CREATE OR REPLACE VIEW dw.v_sla_breach_rate AS
SELECT
    DATE(created_time) AS ticket_date,
    ROUND(100.0 * SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) / COUNT(*), 2) AS breach_rate_pct
FROM dw.fact_ticket
GROUP BY DATE(created_time)
ORDER BY ticket_date;