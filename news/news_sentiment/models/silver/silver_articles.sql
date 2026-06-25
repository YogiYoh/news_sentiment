WITH silver_articles AS(
    SELECT * FROM {{ ref('bronze_BBC')}}

    UNION ALL 

    SELECT * FROM {{ ref('bronze_CNN')}}

    UNION ALL 

    SELECT * FROM {{ ref('bronze_FOX')}}

    UNION ALL 

    SELECT * FROM {{ ref('bronze_GUARD')}}

    UNION ALL 

    SELECT * FROM {{ ref('bronze_NYT')}}

    UNION ALL 

    SELECT * FROM {{ ref('bronze_NPR')}}
),

deduped_articles AS (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY "link", "source" ORDER BY "timestamp" DESC) AS row_num
    FROM silver_articles
)

SELECT * EXCLUDE (row_num) FROM deduped_articles
WHERE row_num = 1

