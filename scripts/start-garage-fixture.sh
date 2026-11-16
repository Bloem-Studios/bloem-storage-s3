#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
image=docker.io/dxflrs/garage@sha256:dac0c92add4f1a0b41035e94b41036a270ffbe88a37c7ac9c3f19e6dc5bdccf2
name=bloem-storage-s3-garage-20261006
if docker inspect "$name" >/dev/null 2>&1; then
 echo "Fixture exists; inspect its task label before using it" >&2
 exit 1
fi
umask 077
mkdir -p .fixture
chmod 700 .fixture
cat > .fixture/garage.toml <<'CONFIG'
metadata_dir = "/tmp/meta"
data_dir = "/tmp/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "127.0.0.1:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "0000000000000000000000000000000000000000000000000000000000000000"
[s3_api]
s3_region = "garage"
api_bind_addr = "0.0.0.0:3900"
CONFIG
chmod 600 .fixture/garage.toml
docker run -d --name "$name" --label bloem.task=storage-s3-20261006  --memory=512m --cpus=2 -p 127.0.0.1:19341:3900  -v "$PWD/.fixture/garage.toml:/etc/garage.toml:ro"  -e GARAGE_DEFAULT_ACCESS_KEY=GK22222222222222222222222222222222  -e GARAGE_DEFAULT_SECRET_KEY=2222222222222222222222222222222222222222222222222222222222222222  -e GARAGE_DEFAULT_BUCKET=fixture-books  "$image" /garage server --single-node --default-bucket
ready=0
for attempt in $(seq 1 20); do
 if docker exec "$name" /garage status >/dev/null 2>&1; then ready=1; break; fi
 sleep 1
done
[ "$ready" = 1 ]
# Synthetic fixture reader: read/list only; never customer credentials.
docker exec "$name" /garage key import --yes -n fixture-reader  GK11111111111111111111111111111111  1111111111111111111111111111111111111111111111111111111111111111 >/dev/null
docker exec "$name" /garage bucket allow --read fixture-books --key fixture-reader
