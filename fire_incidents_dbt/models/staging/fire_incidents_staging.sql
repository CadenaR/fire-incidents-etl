SELECT
    *
FROM {{ source('public', 'fire_incidents_staging') }}