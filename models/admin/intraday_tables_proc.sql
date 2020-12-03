{{ config(
    materialized='ephemeral'
)}}

SELECT * FROM 
`{{ target.project }}.{{ target.schema }}.__TABLES__`
WHERE table_id like 'events_intraday_%'