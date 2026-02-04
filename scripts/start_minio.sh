#!/bin/bash
# Copyright 2026 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# start_minio.sh - Start MinIO for local S3 testing
#
# This script installs MinIO from source and starts it in the background
# with HTTPS enabled for testing Multigres S3 backup functionality.
#
# Usage:
#   ./scripts/start_minio.sh
#
# Environment:
#   - MinIO endpoint: https://localhost:9000
#   - Console: https://localhost:9001
#   - Root user: minioadmin
#   - Root password: minioadmin
#   - Data directory: /tmp/minio
#
# To use with tests:
#   export MULTIGRES_MINIO_ENDPOINT="https://localhost:9000"
#   export AWS_ACCESS_KEY_ID="minioadmin"
#   export AWS_SECRET_ACCESS_KEY="minioadmin"
#   go test -v ./go/tools/s3/...

set -euo pipefail

echo "Setting up MinIO for Multigres S3 testing"

# Configuration
MINIO_DATA_DIR="/tmp/minio"
MINIO_CERTS_DIR="/tmp/minio-certs"
MINIO_PORT="9000"
MINIO_CONSOLE_PORT="9001"

# Clean up existing data and certs
echo "Cleaning up existing MinIO data and certificates..."
rm -rf "${MINIO_DATA_DIR}"
rm -rf "${MINIO_CERTS_DIR}"

# Create directories
mkdir -p "${MINIO_DATA_DIR}"
mkdir -p "${MINIO_CERTS_DIR}"

# Generate self-signed certificate for HTTPS
echo "Generating self-signed TLS certificate..."
openssl req -x509 -newkey rsa:4096 -keyout "${MINIO_CERTS_DIR}/private.key" \
  -out "${MINIO_CERTS_DIR}/public.crt" -days 365 -nodes \
  -subj "/C=US/ST=State/L=City/O=Multigres/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

echo "✓ TLS certificates generated"

# Install MinIO binary
echo "Installing MinIO from source..."
# Determine project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Install to project bin directory
GOBIN="${PROJECT_ROOT}/bin" go install github.com/minio/minio@9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a

# Verify installation
if [ ! -f "${PROJECT_ROOT}/bin/minio" ]; then
  echo "Error: MinIO installation failed"
  exit 1
fi

echo "✓ MinIO installed to ${PROJECT_ROOT}/bin/minio"

# Set MinIO credentials
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin"

# Start MinIO server with HTTPS
echo "Starting MinIO server..."
echo "  Data directory: ${MINIO_DATA_DIR}"
echo "  HTTPS endpoint: https://localhost:${MINIO_PORT}"
echo "  Console: https://localhost:${MINIO_CONSOLE_PORT}"
echo "  Root user: ${MINIO_ROOT_USER} (password: *** - see script)"
echo ""

# Run MinIO with certificates
MINIO_OPTS=(--address ":${MINIO_PORT}" --console-address ":${MINIO_CONSOLE_PORT}")

# Verify certificates were generated
if [[ ! -f "${MINIO_CERTS_DIR}/private.key" ]] || [[ ! -f "${MINIO_CERTS_DIR}/public.crt" ]]; then
  echo "Error: TLS certificates not found in ${MINIO_CERTS_DIR}"
  exit 1
fi

# Copy certs to MinIO's expected location
mkdir -p "${HOME}/.minio/certs"
cp "${MINIO_CERTS_DIR}/private.key" "${HOME}/.minio/certs/private.key"
cp "${MINIO_CERTS_DIR}/public.crt" "${HOME}/.minio/certs/public.crt"

# Function to create test bucket
create_test_bucket() {
  # Wait for MinIO to be ready with healthcheck retry loop
  echo "Waiting for MinIO to start..."
  for i in {1..30}; do
    if curl -k -sf https://localhost:${MINIO_PORT}/minio/health/live >/dev/null 2>&1; then
      echo "✓ MinIO is ready"
      break
    fi
    if [ "$i" -eq 30 ]; then
      echo "Warning: MinIO health check timeout, bucket creation skipped"
      return
    fi
    sleep 1
  done

  # Install mc (MinIO client) if not present
  if [ ! -f "${PROJECT_ROOT}/bin/mc" ]; then
    echo "Installing MinIO client (mc)..."
    # Pin to specific commit SHA for supply chain security
    if ! GOBIN="${PROJECT_ROOT}/bin" go install github.com/minio/mc@ee72571936f15b0e65dc8b4a231a4dd445e5ccb6; then
      echo "Warning: Failed to install mc, bucket creation skipped"
      return
    fi
  fi

  # Configure mc
  echo "Configuring mc client..."
  if ! "${PROJECT_ROOT}/bin/mc" alias set local https://localhost:${MINIO_PORT} "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --insecure; then
    echo "Warning: Failed to configure mc, bucket creation skipped"
    return
  fi

  # Create test bucket
  echo "Creating test bucket 'multigres'..."
  if ! "${PROJECT_ROOT}/bin/mc" mb local/multigres --insecure 2>/dev/null; then
    # Check if the error is because bucket already exists
    if "${PROJECT_ROOT}/bin/mc" stat local/multigres --insecure >/dev/null 2>&1; then
      echo "✓ Test bucket 'multigres' already exists"
      # Clean bucket contents to ensure fresh state for tests
      echo "Cleaning bucket contents..."
      if "${PROJECT_ROOT}/bin/mc" rm --recursive --force local/multigres --insecure >/dev/null 2>&1; then
        echo "✓ Test bucket 'multigres' cleaned"
      else
        echo "Warning: Failed to clean bucket contents"
      fi
    else
      echo "Warning: Failed to create bucket 'multigres', you may need to create it manually"
      return
    fi
  else
    echo "✓ Test bucket 'multigres' created"
  fi
}

# Start MinIO server in background
echo "Starting MinIO server in background..."
"${PROJECT_ROOT}/bin/minio" server "${MINIO_OPTS[@]}" "${MINIO_DATA_DIR}" >/tmp/minio.log 2>&1 &
MINIO_PID=$!
echo "MinIO started with PID ${MINIO_PID}"

# Wait for MinIO to be ready
echo "Waiting for MinIO to start..."
for i in {1..30}; do
  if curl -k -sf https://localhost:${MINIO_PORT}/minio/health/live >/dev/null 2>&1; then
    echo "✓ MinIO is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "Error: MinIO failed to start"
    cat /tmp/minio.log
    exit 1
  fi
  sleep 1
done

# Create test bucket (now that MinIO is running)
create_test_bucket

echo ""
echo "MinIO is running in background (PID: ${MINIO_PID})"
echo "Logs: /tmp/minio.log"
echo ""
echo "To use in tests, export these environment variables:"
echo "  export MULTIGRES_MINIO_ENDPOINT=https://localhost:${MINIO_PORT}"
echo "  export AWS_ACCESS_KEY_ID=minioadmin"
echo "  export AWS_SECRET_ACCESS_KEY=minioadmin"
echo ""
echo "To stop MinIO: kill ${MINIO_PID}"
echo "To view logs: tail -f /tmp/minio.log"
