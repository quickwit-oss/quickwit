#!/usr/bin/env bash

# Regenerate the DER-encoded test CA, server certificate, and server private
# key embedded in `src/opendal_storage/google_cloud_storage.rs`.

set -euo pipefail

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

cert_not_before=20200101000000Z
cert_not_after=30200101000000Z

mkdir "$tmp_dir/certs"
: > "$tmp_dir/index.txt"
export OPENSSL_TEST_DIR="$tmp_dir"

run_quietly() {
  local stderr_file="$tmp_dir/openssl-stderr.log"
  if ! "$@" >/dev/null 2>"$stderr_file"; then
    cat "$stderr_file" >&2
    return 1
  fi
}

cat > "$tmp_dir/openssl.conf" <<'EOF'
[ca]
default_ca=test_ca

[test_ca]
dir=$ENV::OPENSSL_TEST_DIR
database=$dir/index.txt
new_certs_dir=$dir/certs
serial=$dir/serial
default_md=sha256
policy=test_policy

[test_policy]
commonName=supplied

[req]
distinguished_name=dn

[dn]

[v3_ca]
basicConstraints=critical,CA:true
keyUsage=critical,keyCertSign,cRLSign
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always

[v3_server]
basicConstraints=critical,CA:false
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
EOF

openssl ecparam -name prime256v1 -genkey -noout -out "$tmp_dir/ca.key"
openssl req -new -key "$tmp_dir/ca.key" -subj "/CN=Quickwit Test CA" \
  -out "$tmp_dir/ca.csr" -config "$tmp_dir/openssl.conf"

printf "1001\n" > "$tmp_dir/serial"
run_quietly openssl ca -batch -selfsign -config "$tmp_dir/openssl.conf" \
  -in "$tmp_dir/ca.csr" -keyfile "$tmp_dir/ca.key" \
  -out "$tmp_dir/ca.crt" \
  -extensions v3_ca \
  -startdate "$cert_not_before" -enddate "$cert_not_after" \
  -notext

openssl ecparam -name prime256v1 -genkey -noout -out "$tmp_dir/server.key"
openssl req -new -key "$tmp_dir/server.key" -subj "/CN=localhost" \
  -out "$tmp_dir/server.csr" -config "$tmp_dir/openssl.conf"

printf "1002\n" > "$tmp_dir/serial"
run_quietly openssl ca -batch -config "$tmp_dir/openssl.conf" \
  -cert "$tmp_dir/ca.crt" -keyfile "$tmp_dir/ca.key" \
  -in "$tmp_dir/server.csr" -out "$tmp_dir/server.crt" \
  -extensions v3_server \
  -startdate "$cert_not_before" -enddate "$cert_not_after" \
  -notext

printf "CA_DER="
openssl x509 -in "$tmp_dir/ca.crt" -outform der | base64 | tr -d "\n"
printf "\nSERVER_DER="
openssl x509 -in "$tmp_dir/server.crt" -outform der | base64 | tr -d "\n"
printf "\nSERVER_KEY_DER="
openssl ec -in "$tmp_dir/server.key" -outform der 2>/dev/null | base64 | tr -d "\n"
printf "\n"
