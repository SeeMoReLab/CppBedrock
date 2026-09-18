#!/usr/bin/env bash
# Generates the RSA key pairs a CppBedrock committee needs:
#   server_<id>_private.pem / server_<id>_public.pem for replica ids 0..N-1
#   client_private.pem / client_public.pem for the benchmark client
# Usage: gen_server_keys.sh <num_replicas> [out_dir]
set -euo pipefail

NUM_SERVERS="${1:?usage: $0 <num_replicas> [out_dir]}"
OUT_DIR="${2:-.}"
mkdir -p "${OUT_DIR}"

for ((i = 0; i < NUM_SERVERS; i++)); do
    openssl genpkey -algorithm RSA -out "${OUT_DIR}/server_${i}_private.pem" -pkeyopt rsa_keygen_bits:2048 2>/dev/null
    openssl rsa -pubout -in "${OUT_DIR}/server_${i}_private.pem" -out "${OUT_DIR}/server_${i}_public.pem" 2>/dev/null
done
openssl genpkey -algorithm RSA -out "${OUT_DIR}/client_private.pem" -pkeyopt rsa_keygen_bits:2048 2>/dev/null
openssl rsa -pubout -in "${OUT_DIR}/client_private.pem" -out "${OUT_DIR}/client_public.pem" 2>/dev/null

echo "Generated keys for ${NUM_SERVERS} replicas (ids 0..$((NUM_SERVERS - 1))) and the client in ${OUT_DIR}"
