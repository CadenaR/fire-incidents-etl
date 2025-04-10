WITH latest_rows AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY data_as_of DESC) AS row_num
    FROM {{ ref('fire_incidents_staging') }}
)
SELECT
    CAST(incident_number AS TEXT) AS incident_number,
    CAST(exposure_number AS INTEGER) AS exposure_number,
    CAST(id AS TEXT) AS id,
    CAST(address AS TEXT) AS address,
    CAST(incident_date AS TIMESTAMP) AS incident_date,
    CAST(call_number AS TEXT) AS call_number,
    CAST(alarm_dttm AS TIMESTAMP) AS alarm_dttm,
    CAST(arrival_dttm AS TIMESTAMP) AS arrival_dttm,
    CAST(close_dttm AS TIMESTAMP) AS close_dttm,
    CAST(city AS TEXT) AS city,
    CAST(zipcode AS TEXT) AS zipcode,
    CAST(battalion AS TEXT) AS battalion,
    CAST(station_area AS TEXT) AS station_area,
    CAST(box AS TEXT) AS box,
    CAST(suppression_units AS INTEGER) AS suppression_units,
    CAST(suppression_personnel AS INTEGER) AS suppression_personnel,
    CAST(ems_units AS INTEGER) AS ems_units,
    CAST(ems_personnel AS INTEGER) AS ems_personnel,
    CAST(other_units AS INTEGER) AS other_units,
    CAST(other_personnel AS INTEGER) AS other_personnel,
    CAST(first_unit_on_scene AS TEXT) AS first_unit_on_scene,
    CAST(estimated_property_loss AS INTEGER) AS estimated_property_loss,
    CAST(estimated_contents_loss AS INTEGER) AS estimated_contents_loss,
    CAST(fire_fatalities AS INTEGER) AS fire_fatalities,
    CAST(fire_injuries AS INTEGER) AS fire_injuries,
    CAST(civilian_fatalities AS INTEGER) AS civilian_fatalities,
    CAST(civilian_injuries AS INTEGER) AS civilian_injuries,
    CAST(number_of_alarms AS INTEGER) AS number_of_alarms,
    CAST(primary_situation AS TEXT) AS primary_situation,
    CAST(mutual_aid AS TEXT) AS mutual_aid,
    CAST(action_taken_primary AS TEXT) AS action_taken_primary,
    CAST(action_taken_secondary AS TEXT) AS action_taken_secondary,
    CAST(action_taken_other AS TEXT) AS action_taken_other,
    CAST(detector_alerted_occupants AS TEXT) AS detector_alerted_occupants,
    CAST(property_use AS TEXT) AS property_use,
    CAST(area_of_fire_origin AS TEXT) AS area_of_fire_origin,
    CAST(ignition_cause AS TEXT) AS ignition_cause,
    CAST(ignition_factor_primary AS TEXT) AS ignition_factor_primary,
    CAST(ignition_factor_secondary AS TEXT) AS ignition_factor_secondary,
    CAST(heat_source AS TEXT) AS heat_source,
    CAST(item_first_ignited AS TEXT) AS item_first_ignited,
    CAST(human_factors_associated_with_ignition AS TEXT) AS human_factors_associated_with_ignition,
    CAST(structure_type AS TEXT) AS structure_type,
    CAST(structure_status AS TEXT) AS structure_status,
    CAST(floor_of_fire_origin AS INTEGER) AS floor_of_fire_origin,
    CAST(fire_spread AS TEXT) AS fire_spread,
    CAST(no_flame_spread AS TEXT) AS no_flame_spread,
    CAST(number_of_floors_with_minimum_damage AS INTEGER) AS number_of_floors_with_minimum_damage,
    CAST(number_of_floors_with_significant_damage AS INTEGER) AS number_of_floors_with_significant_damage,
    CAST(number_of_floors_with_heavy_damage AS INTEGER) AS number_of_floors_with_heavy_damage,
    CAST(number_of_floors_with_extreme_damage AS INTEGER) AS number_of_floors_with_extreme_damage,
    CAST(detectors_present AS TEXT) AS detectors_present,
    CAST(detector_type AS TEXT) AS detector_type,
    CAST(detector_operation AS TEXT) AS detector_operation,
    CAST(detector_effectiveness AS TEXT) AS detector_effectiveness,
    CAST(detector_failure_reason AS TEXT) AS detector_failure_reason,
    CAST(automatic_extinguishing_system_present AS TEXT) AS automatic_extinguishing_system_present,
    CAST(automatic_extinguishing_sytem_type AS TEXT) AS automatic_extinguishing_sytem_type,
    CAST(automatic_extinguishing_sytem_perfomance AS TEXT) AS automatic_extinguishing_sytem_perfomance,
    CAST(automatic_extinguishing_sytem_failure_reason AS TEXT) AS automatic_extinguishing_sytem_failure_reason,
    CAST(number_of_sprinkler_heads_operating AS INTEGER) AS number_of_sprinkler_heads_operating,
    CAST(supervisor_district AS TEXT) AS supervisor_district,
    CAST(neighborhood_district AS TEXT) AS neighborhood_district,
    CAST(latitude AS DOUBLE PRECISION) AS latitude,
    CAST(longitude AS DOUBLE PRECISION) AS longitude,
    CAST(data_as_of AS TIMESTAMP) AS data_as_of,
    CAST(data_loaded_at AS DATE) AS data_loaded_at
FROM latest_rows
WHERE row_num = 1