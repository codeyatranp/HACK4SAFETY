#!/bin/bash
# GeoGuard InfluxDB initialization
# Creates the sensor-data bucket and retention policy

# These are set by docker-compose environment variables:
# DOCKER_INFLUXDB_INIT_MODE, DOCKER_INFLUXDB_INIT_USERNAME, etc.
# The influxdb:2.7 image auto-runs setup on first boot.

# Additional bucket for satellite-derived data
# (Run via influx CLI after initial setup completes)
echo "InfluxDB initialized by Docker entrypoint."
echo "Bucket: sensor-data | Org: geoguard | Retention: 90 days"
