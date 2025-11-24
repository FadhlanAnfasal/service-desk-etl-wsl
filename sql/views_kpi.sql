-- 1️⃣ Jumlah Review per Hari
CREATE OR REPLACE VIEW dw.vw_review_volume_per_day AS
SELECT
    DATE(date) AS review_date,
    COUNT(*) AS total_reviews
FROM dw.fact_reviews
GROUP BY DATE(date)
ORDER BY review_date;


-- 2️⃣ Rata-rata Rating per Hari
CREATE OR REPLACE VIEW dw.vw_avg_rating_per_day AS
SELECT
    DATE(date) AS review_date,
    ROUND(AVG(rating)::numeric, 2) AS avg_rating
FROM dw.fact_reviews
GROUP BY DATE(date)
ORDER BY review_date;


-- 3️⃣ Distribusi Sentiment
CREATE OR REPLACE VIEW dw.vw_sentiment_distribution AS
SELECT
    ds.sentiment_label,
    ds.sentiment_color,
    COUNT(fr.sentiment_id) AS total_reviews,
    ROUND(
        (COUNT(fr.sentiment_id)::numeric / (SELECT COUNT(*) FROM dw.fact_reviews)) * 100,
        2
    ) AS percentage
FROM dw.fact_reviews fr
LEFT JOIN dw.dim_sentiment ds ON fr.sentiment_id = ds.sentiment_id
GROUP BY ds.sentiment_label, ds.sentiment_color
ORDER BY total_reviews DESC;


-- 4️⃣ Distribusi Rating
CREATE OR REPLACE VIEW dw.vw_rating_distribution AS
SELECT
    dr.rating_label,
    dr.satisfaction_level,
    COUNT(fr.rating_id) AS total_reviews,
    ROUND(
        (COUNT(fr.rating_id)::numeric / (SELECT COUNT(*) FROM dw.fact_reviews)) * 100,
        2
    ) AS percentage
FROM dw.fact_reviews fr
LEFT JOIN dw.dim_rating dr ON fr.rating_id = dr.rating_id
GROUP BY dr.rating_label, dr.satisfaction_level
ORDER BY dr.rating_value DESC;
