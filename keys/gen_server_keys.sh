#!/bin/bash

NUM_SERVERS=${1:-100}  # Default to 100 servers if not specified

for i in $(seq 1 $NUM_SERVERS); do
    echo "Generating keys for server $i..."
    openssl genpkey -algorithm RSA -out server_${i}_private.pem -pkeyopt rsa_keygen_bits:2048
    openssl rsa -pubout -in server_${i}_private.pem -out server_${i}_public.pem
done

echo "Done. Keys generated for $NUM_SERVERS servers."
