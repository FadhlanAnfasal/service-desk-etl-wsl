CREATE INDEX IF NOT EXISTS ix_fact_ticket_user ON dw.fact_ticket(user_id);
CREATE INDEX IF NOT EXISTS ix_fact_ticket_category ON dw.fact_ticket(category_id);
CREATE INDEX IF NOT EXISTS ix_fact_ticket_sla ON dw.fact_ticket(sla_id);
CREATE INDEX IF NOT EXISTS ix_fact_ticket_created_time ON dw.fact_ticket(created_time);
