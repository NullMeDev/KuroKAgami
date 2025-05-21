#!/usr/bin/env bash
set -euxo pipefail

cd "$(dirname "$0")"
git pull origin main
go build -o kurokagami ./cmd/kurokagami
systemctl restart kurokagami.service