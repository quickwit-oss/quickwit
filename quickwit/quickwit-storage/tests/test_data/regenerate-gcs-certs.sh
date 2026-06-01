#!/usr/bin/env bash

# Regenerate the DER-encoded test CA, server certificate, and server private
# key embedded in `src/opendal_storage/google_cloud_storage.rs`.

set -euo pipefail

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

cat > "$tmp_dir/ca.conf" <<'EOF'
[req]
distinguished_name=dn
[dn]
[v3_ca]
basicConstraints=critical,CA:true
keyUsage=critical,keyCertSign,cRLSign
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always
EOF

openssl req -x509 -newkey ec \
  -pkeyopt ec_paramgen_curve:prime256v1 \
  -nodes -subj "/CN=Quickwit Test CA" \
  -set_serial 0x1001 \
  -not_before 20200101000000Z \
  -not_after 30200101000000Z \
  -extensions v3_ca -config "$tmp_dir/ca.conf" \
  -keyout "$tmp_dir/ca.key" -out "$tmp_dir/ca.crt" 2>/dev/null

cat > "$tmp_dir/server.conf" <<'EOF'
[req]
distinguished_name=dn
[dn]
[v3_server]
basicConstraints=critical,CA:false
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
EOF

openssl req -x509 -newkey ec \
  -pkeyopt ec_paramgen_curve:prime256v1 \
  -nodes -subj "/CN=localhost" \
  -CA "$tmp_dir/ca.crt" -CAkey "$tmp_dir/ca.key" \
  -set_serial 0x1002 \
  -not_before 20200101000000Z \
  -not_after 30200101000000Z \
  -extensions v3_server -config "$tmp_dir/server.conf" \
  -keyout "$tmp_dir/server.key" -out "$tmp_dir/server.crt" 2>/dev/null

printf "CA_DER="
openssl x509 -in "$tmp_dir/ca.crt" -outform der | base64 | tr -d "\n"
printf "\nSERVER_DER="
openssl x509 -in "$tmp_dir/server.crt" -outform der | base64 | tr -d "\n"
printf "\nSERVER_KEY_DER="
openssl ec -in "$tmp_dir/server.key" -outform der 2>/dev/null | base64 | tr -d "\n"
printf "\n"
