#!/usr/bin/env bash

set -o nounset -o pipefail -o errexit -o errtrace

export KEYOP_MESSENGER_TMPDIR="/tmp/keyop-messenger-example"
rm -rf $KEYOP_MESSENGER_TMPDIR
rm -f ca.key ca.crt

echo "Generating CA keys"
keyop-messenger keygen ca

for host in host1 host2; do
  mkdir -p $KEYOP_MESSENGER_TMPDIR/$host/cert

  echo "Generating $host keys"
  keyop-messenger keygen instance \
    --ca ca.crt --ca-key ca.key \
    --name localhost \
    --out-cert $KEYOP_MESSENGER_TMPDIR/$host/cert/$host.crt \
    --out-key  $KEYOP_MESSENGER_TMPDIR/$host/cert/$host.key

  # copy CA cert to messenger directories for verification
  # the key is only needed to sign certs, it's not needed for verification
  cp ca.crt $KEYOP_MESSENGER_TMPDIR/$host/cert/
done

# remove CA keys since they have been copied to messanger directories
rm ca.crt ca.key

echo "Building example"
go run ./...


