{{
    config(
        materialized='incremental',
        unique_key='"link"'
    )
}}


SELECT * FROM {{source('SOURCE', 'raw.foxnews_articles')}}

{% if is_incremental() %}
    WHERE "timestamp" > (SELECT MAX("timestamp") FROM {{this}})
{% endif %}