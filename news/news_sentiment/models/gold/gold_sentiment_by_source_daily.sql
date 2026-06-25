SELECT 
    "source",
    DATE("timestamp") AS article_date,
    AVG("sentiment_score") AS avg_sentiment_score,
    COUNT(*) AS total_articles
FROM {{ ref('gold_articles_scored') }}
GROUP BY "source", DATE("timestamp")
ORDER BY article_date DESC, "source"