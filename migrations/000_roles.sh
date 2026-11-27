#!/bin/sh
set -eu

ADMIN_PW="${LEDGER_ADMIN_PASSWORD:-ledger_admin_dev_pw}"
APP_PW="${LEDGER_APP_PASSWORD:-ledger_app_dev_pw}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v admin_pw="$ADMIN_PW" \
  -v app_pw="$APP_PW" <<'EOSQL'
SELECT CASE
  WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ledger_admin')
  THEN format('ALTER ROLE ledger_admin WITH LOGIN PASSWORD %L', :'admin_pw')
  ELSE format('CREATE ROLE ledger_admin LOGIN PASSWORD %L', :'admin_pw')
END \gexec

SELECT CASE
  WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ledger_app')
  THEN format('ALTER ROLE ledger_app WITH LOGIN PASSWORD %L', :'app_pw')
  ELSE format('CREATE ROLE ledger_app LOGIN PASSWORD %L', :'app_pw')
END \gexec
EOSQL
