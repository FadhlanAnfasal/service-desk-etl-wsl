CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.fact_reviews (
  review_id TEXT PRIMARY KEY,
  username TEXT,
  rating INT,
  review_text TEXT,
  date TIMESTAMPTZ,
  sentiment TEXT,
  load_dts TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.etl_run_log (
  run_id SERIAL PRIMARY KEY,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  rows_loaded INT,
  dq_issues TEXT,
  status TEXT,
  load_dts TIMESTAMPTZ DEFAULT now()
);


CREATE TABLE IF NOT EXISTS dw.dim_sentiment (
    sentiment_id SERIAL PRIMARY KEY,
    sentiment_name TEXT UNIQUE,     
    sentiment_label TEXT,           
    sentiment_color TEXT,           
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_rating (
    rating_id SERIAL PRIMARY KEY,
    rating_value INT UNIQUE,        
    rating_label TEXT,              
    satisfaction_level TEXT,        
    created_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE IF EXISTS dw.fact_reviews
ADD COLUMN IF NOT EXISTS sentiment_id INT REFERENCES dw.dim_sentiment(sentiment_id),
ADD COLUMN IF NOT EXISTS rating_id INT REFERENCES dw.dim_rating(rating_id);