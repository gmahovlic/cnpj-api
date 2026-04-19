#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Pulling latest changes..."
git pull

echo "==> Installing dependencies..."
source .venv/bin/activate
pip install -r requirements.txt

echo "==> Restarting service..."
systemctl restart cnpj-api

echo "==> Service status:"
systemctl status cnpj-api --no-pager
