#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# All credentials and media in this fixture are synthetic.
image=docker.io/chrislusf/seaweedfs@sha256:186de7ef977a20343ee9a5544073f081976a29e2d29ecf8379891e7bf177fbe9
name=bloem-storage-s3-fixture-20261006
if docker inspect "$name" >/dev/null 2>&1; then
 echo "Fixture already exists; inspect it or docker start $name" >&2
 exit 1
fi
umask 077
mkdir -p .fixture
chmod 700 .fixture
cat > .fixture/s3.json <<'JSON'
{"identities":[{"name":"fixture-writer","credentials":[{"accessKey":"fixture-writer","secretKey":"synthetic-writer-secret"}],"actions":["Admin","Read","Write","List","Tagging"]},{"name":"fixture-reader","credentials":[{"accessKey":"fixture-reader","secretKey":"synthetic-reader-secret"}],"actions":["Read:fixture-books","List:fixture-books"]}]}
JSON
chmod 600 .fixture/s3.json
docker run -d --name "$name" --label bloem.task=storage-s3-20261006 \
 -p 127.0.0.1:19340:8333 \
 -v "$PWD/.fixture/s3.json:/etc/seaweedfs/s3.json:ro" "$image" \
 server -s3 -s3.config=/etc/seaweedfs/s3.json -dir=/tmp \
 -volume.max=8 -master.volumeSizeLimitMB=32
