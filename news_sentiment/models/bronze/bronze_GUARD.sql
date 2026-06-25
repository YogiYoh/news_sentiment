{{
    config(
        materialized='incremental',
        unique_key='"link"'
    )
}}


SELECT * FROM {{source('SOURCE', 'raw.guardian_articles')}}

{% if is_incremental() %}
    WHERE "timestamp" > (SELECT MAX("timestamp") FROM {{this}})
{% endif %}

QUALIFY ROW_NUMBER() OVER (PARTITION BY "link" ORDER BY "timestamp" DESC) = 1