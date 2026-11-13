#!/usr/bin/env bash
# start_postgres.sh - Start PostgreSQL 16 in Docker for the project (Section 2.2).
#
# Creates a container named cs236-postgres the first time, and just starts it after
# that. Data is kept in the Docker volume cs236-pgdata, so it survives restarts.
# Settings can be overridden with environment variables. They must match the ones
# the Python scripts use (db_common.py):
#   PG_USER=cs236  PG_PASSWORD=cs236  PG_DB=cs236  PG_PORT=5432
#
# Usage: ./start_postgres.sh
# Stop:  docker stop cs236-postgres
# Reset: docker rm -f cs236-postgres && docker volume rm cs236-pgdata
set -euo pipefail

NAME=cs236-postgres
PG_USER="${PG_USER:-cs236}"
PG_PASSWORD="${PG_PASSWORD:-cs236}"
PG_DB="${PG_DB:-cs236}"
PG_PORT="${PG_PORT:-5432}"

if docker container inspect "$NAME" >/dev/null 2>&1; then
  docker start "$NAME" >/dev/null
  echo "started existing container $NAME"
else
  docker run -d \
    --name "$NAME" \
    -e POSTGRES_USER="$PG_USER" \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -e POSTGRES_DB="$PG_DB" \
    -p "$PG_PORT":5432 \
    -v cs236-pgdata:/var/lib/postgresql/data \
    postgres:16 >/dev/null
  echo "created container $NAME (postgres:16) on port $PG_PORT"
fi

# Wait until the server accepts connections.
for _ in $(seq 1 30); do
  if docker exec "$NAME" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then
    echo "PostgreSQL is ready: postgresql://$PG_USER@localhost:$PG_PORT/$PG_DB"
    exit 0
  fi
  sleep 1
done
echo "PostgreSQL did not become ready in 30s; check: docker logs $NAME" >&2
exit 1
