#!/bin/sh
set -eu

: "${DURO_POSTGRES_PASSWORD:?set DURO_POSTGRES_PASSWORD}"
: "${DURO_ADMIN_DSN:?set DURO_ADMIN_DSN}"

command -v docker >/dev/null 2>&1 || { printf '%s\n' 'quickstart: docker is required' >&2; exit 1; }
command -v duro >/dev/null 2>&1 || { printf '%s\n' 'quickstart: duro is required (build/install it first)' >&2; exit 1; }

docker compose up --detach --wait postgres18
duro init --postgres "$DURO_ADMIN_DSN"
