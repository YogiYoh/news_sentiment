SELECT
    s."link",
    s."headline",
    s."body",
    s."source",
    s."timestamp",
    sc."sentiment_score",
    sc."scored_at"
FROM {{ ref('silver_articles') }} s
JOIN {{source('SOURCE', 'ARTICLE_SENTIMENT')}} sc ON s."link" = sc."link"
