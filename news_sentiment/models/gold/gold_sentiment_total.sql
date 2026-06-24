SELECT
    "source",
    AVG("sentiment_score") AS avg_sentiment_score, 
    COUNT(*) AS total_articles
FROM {{ ref('gold_articles_scored') }}
GROUP BY "source"
