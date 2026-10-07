#!/bin/sh
# Generates a development CA and one cluster certificate shared by every
# component. Components verify each other against the name "conductor"
# (CONDUCTOR_TLS_SERVER_NAME), so the certificate works for any worker IP.
#
# Usage: scripts/gen-dev-certs.sh [output-dir]   (default: ./certs)
set -eu
export MSYS_NO_PATHCONV=1 # stop Git Bash on Windows from mangling -subj

dir=${1:-certs}
mkdir -p "$dir"
cd "$dir"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 365 \
  -subj "/CN=conductor-dev-ca" -keyout ca.key -out ca.crt 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=conductor" -keyout cluster.key -out cluster.csr 2>/dev/null
printf 'subjectAltName=DNS:conductor\nextendedKeyUsage=serverAuth,clientAuth\n' > ext.cnf
openssl x509 -req -in cluster.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 \
  -extfile ext.cnf -out cluster.crt 2>/dev/null
rm -f cluster.csr ext.cnf ca.srl
# Containers run as a different user than the one creating these files.
# Fine for development certificates only.
chmod 644 cluster.key

echo "Wrote $(pwd)/{ca.crt,ca.key,cluster.crt,cluster.key}"
