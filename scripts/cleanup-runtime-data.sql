-- Remove dashboard/demo/runtime data while preserving zone and station metadata.
-- This is safe to rerun before a real-data backfill.

BEGIN;

TRUNCATE TABLE
    alerts,
    risk_scores_current,
    risk_scores_history,
    sensors,
    audit_log
RESTART IDENTITY CASCADE;

DELETE FROM satellite_data
WHERE source IN ('nasa_gpm', 'sentinel1', 'sentinel2')
   OR source LIKE '%simulated%'
   OR metadata->>'source' LIKE '%simulated%';
DELETE FROM dhm_station_readings;
DELETE FROM landslide_catalog;
DELETE FROM bipad_incidents;
DELETE FROM bipad_alerts;

DELETE FROM osm_roads;
DELETE FROM osm_settlements;

COMMIT;
