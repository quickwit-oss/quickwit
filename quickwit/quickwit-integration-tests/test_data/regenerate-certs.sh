#!/usr/bin/env bash

# this script regenerate cryptographic material used in tests. These are valid for 10y, but better
# keep how to regenerate them than get stuck with failing tests eventually

rm ca.{crt,key,srl} server.{csr,key,v3.ext,crt}
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3653 -out ca.crt -subj '/CN=qw test CA'
openssl req -new -nodes -out server.csr -newkey rsa:4096 -keyout server.key -subj '/CN=qw test certificate'

cat > server.v3.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
DNS.1 = quickwit.local
IP.1 = 127.0.0.1
EOF

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3653 -sha256 -extfile server.v3.ext
