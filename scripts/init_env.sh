#!/usr/bin/env bash
# Create .env from .env.example and fill in freshly generated secrets.
# Never overwrites an existing .env. Secrets are not printed (safe for screen recordings).
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  echo ".env already exists, leaving it unchanged."
  exit 0
fi
command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }

umask 077
fernet_key=$(openssl rand -base64 32 | tr '+/' '-_')
sed \
  -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$(id -u)|" \
  -e "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=$(openssl rand -hex 12)|" \
  -e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=${fernet_key}|" \
  -e "s|^AIRFLOW_API_SECRET_KEY=.*|AIRFLOW_API_SECRET_KEY=$(openssl rand -hex 32)|" \
  -e "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$(openssl rand -hex 32)|" \
  -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$(openssl rand -hex 16)|" \
  .env.example > .env
echo "Created .env (readable only by you). The Airflow password is AIRFLOW_ADMIN_PASSWORD in .env."

key_file=$(grep '^GCP_KEY_FILE=' .env | cut -d= -f2-)
if [[ ! -f "$key_file" ]]; then
  echo "WARNING: service-account key not found at $key_file. Copy it there or edit GCP_KEY_FILE in .env."
elif [[ "$(stat -c %a "$key_file")" != "600" ]]; then
  echo "WARNING: $key_file should be private: chmod 600 $key_file"
fi
