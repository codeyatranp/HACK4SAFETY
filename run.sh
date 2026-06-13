#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

case "${1:-up}" in
  up)
    echo "Starting GeoGuard..."
    docker compose up -d
    echo ""
    echo "Dashboard API:  http://localhost:8000"
    echo "Dashboard App:  http://localhost:3002"
    echo "PostgreSQL:     localhost:5432"
    echo "InfluxDB:       http://localhost:8086"
    ;;
  down)
    docker compose down
    echo "Stopped."
    ;;
  logs)
    docker compose logs -f "${@:2}"
    ;;
  rebuild)
    docker compose up -d --build
    echo "Rebuilt and started."
    ;;
  *)
    echo "Usage: $0 {up|down|logs|rebuild}"
    exit 1
    ;;
esac
