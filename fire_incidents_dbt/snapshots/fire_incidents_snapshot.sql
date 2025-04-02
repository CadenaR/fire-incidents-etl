{% snapshot fire_incidents_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='check',
    check_cols=['incident_number', 'incident_date', 'data_as_of']
) }}

SELECT *
FROM {{ ref('fire_incidents_silver') }}

{% endsnapshot %}