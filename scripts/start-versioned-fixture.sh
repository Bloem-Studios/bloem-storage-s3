#!/bin/bash
# Disposable official-source fixture only; this is not a production backend recipe.
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."
# The fixture is pinned to Go 1.26.8. Set GO to that go binary (default: the
# "go" found on PATH). GO_SHA256 overrides the pinned SHA-256 of that binary,
# for a different build of the same release.
go_bin=$(command -v "${GO:-go}") && go_bin=$(readlink -f "$go_bin") || { echo 'go not found; set GO to a go1.26.8 binary' >&2; exit 1; }
go_sha256=${GO_SHA256:-d9a2fa19c7ef8b57f420012c21f49f235c46f08a68c12077d9c753dbb6ccdc34}
export PATH="${go_bin%/*}:$PATH"
export GOTOOLCHAIN=local GOWORK=off GOMAXPROCS=2 CGO_ENABLED=0 GOFLAGS=
proof="$PWD/.superpowers/sdd/versioned-proof"
source="$proof/minio-source"
commit=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
release=RELEASE.2025-10-15T17-29-55Z
label=storage-s3-versioned-proof-20261006
mkdir -p "$proof"
chmod 700 "$proof"
# Caller must retain this private directory to manage exactly this fixture.
if [[ ${1:-start} == start ]]; then
  [[ -z ${S3_VERSIONED_DIR:-} ]] || { echo 'start creates a new fixture directory' >&2; exit 1; }
  dir=$(mktemp -d "$proof/run.XXXXXXXX")
else
  dir=${S3_VERSIONED_DIR:?set S3_VERSIONED_DIR to the task-owned private fixture directory}
fi
[[ "$dir" == "$proof"/run.* && "${dir#"$proof"/}" != */* && -d "$dir" && ! -L "$dir" ]] || exit 1
name=""
erase_private() {
  rm -f "$dir/env" "$dir/fixture.json"
  rm -rf "$dir/data"
}
remove_owned_container() {
  local actual container_id
  [[ -n "$name" ]] || return 0
  # Inspect even when docker run failed/timed out before returning an ID.
  # An unavailable inspect is ambiguous: report failure, never remove blindly.
  if ! actual=$(timeout 10 docker inspect --format '{{.Id}} {{index .Config.Labels "bloem.task"}}' "$name" 2>/dev/null); then
    echo 'task container absent or inspect unavailable; no blind removal' >&2
    return 1
  fi
  container_id=${actual%% *}
  actual=${actual#* }
  [[ "$container_id" =~ ^[0-9a-f]{64}$ && "$actual" == "$label" ]] || { echo 'task label mismatch or invalid container ID; container retained' >&2; return 1; }
  timeout 30 docker rm -f "$container_id" >/dev/null
}
failed_cleanup() {
  local code=$?
  trap - EXIT TERM INT
  if (( code != 0 )); then
    set +e
    remove_owned_container
    local cleanup_code=$?
    erase_private
    echo "failed-start private cleanup completed; container cleanup status=$cleanup_code" >&2
  fi
  exit "$code"
}
if [[ ${1:-start} == start ]]; then
  # Installed before any private data or credentials, including Python failures.
  trap failed_cleanup EXIT
  trap 'exit 143' TERM
  trap 'exit 130' INT
fi
check_label() {
  [[ $(timeout 10 docker inspect --format '{{index .Config.Labels "bloem.task"}}' "$name") == "$label" ]] || { echo 'task label mismatch' >&2; return 1; }
}
ready() {
  local deadline=$((SECONDS+45))
  until curl --silent --fail --max-time 1 http://127.0.0.1:19342/minio/health/ready >/dev/null; do
    (( SECONDS < deadline )) || { echo 'fixture readiness timeout' >&2; return 1; }
    sleep 0.2
  done
}
case ${1:-start} in
restart|stop|cleanup|status)
  if [[ "$1" == cleanup && ! -f "$dir/name" ]]; then erase_private; exit 0; fi
  name=$(cat "$dir/name")
  if [[ "$1" == cleanup ]]; then
    cleanup_code=0
    remove_owned_container || cleanup_code=$?
    erase_private
    echo "private fixture files erased; container cleanup status=$cleanup_code"
    exit "$cleanup_code"
  fi
  check_label
  case "$1" in
    restart) timeout 40 docker restart "$name" >/dev/null; ready ;;
    stop) timeout 25 docker stop -t 10 "$name" >/dev/null ;;
    status) docker inspect --format '{{.State.Status}} {{.Image}} {{json .HostConfig.PortBindings}} {{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}' "$name" ;;

  esac
  exit 0 ;;
start) ;;
*) echo 'usage: start-versioned-fixture.sh [start|restart|stop|status|cleanup]' >&2; exit 1 ;;
esac
python3 - <<'PY'
import socket
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(('127.0.0.1',19342)); s.listen(1); s.close()
print('127.0.0.1:19342 unused at preflight')
PY
# Reuse ONLY the independently reviewed local artifact. Missing/mismatched
# inputs fail closed; this launcher never downloads/rebuilds/blesses a cache.
pin_failure() { echo 'pinned fixture provenance mismatch; reuse refused' >&2; exit 1; }
hash_matches() {
  [[ -f "$1" && ! -L "$1" ]] || pin_failure
  local actual
  actual=$(sha256sum "$1") || pin_failure
  [[ "${actual%% *}" == "$2" ]] || pin_failure
}
[[ -d "$source/.git" && ! -L "$source" ]] || pin_failure
[[ $(git -C "$source" remote get-url origin) == https://github.com/minio/minio.git ]] || pin_failure
[[ $(git -C "$source" cat-file -t "$commit") == commit ]] || pin_failure
[[ $(git -C "$source" rev-parse HEAD) == "$commit" ]] || pin_failure
[[ $(git -C "$source" rev-parse "refs/tags/$release^{commit}") == "$commit" ]] || pin_failure
[[ $(git -C "$source" rev-parse 'HEAD^{tree}') == e4064317998bf2a12b0c74db0bf0de06f96ccb71 ]] || pin_failure
source_status=$(git -C "$source" status --porcelain --untracked-files=all) || pin_failure
[[ -z "$source_status" ]] || pin_failure
hash_matches "$go_bin" "$go_sha256"
[[ $("$go_bin" version) == 'go version go1.26.8 linux/amd64' ]] || pin_failure
hash_matches "$source/go.mod" 673f06144e90bc045f0a20050d2874c52e551be5bfbd70dff6e76b66da0db702
hash_matches "$source/go.sum" 86e062349c7abdce0465561bb409d410a95b00b0969ba05d5fb5f2e3550a5cd4
hash_matches "$source/buildscripts/gen-ldflags.go" aa7ecefcb05793bf4104fc0c1a08b783fe51eff7217baa358ac27c27fc7cdcb7
hash_matches "$proof/ldflags.txt" ff134101c5453288fe2121266af7c110adb58736ed70771b3d123f34386adfa5
hash_matches "$proof/minio" 4b3bb8d19348c01213aa0697e61afb5a8bfeb0883f2852fb2df37d3c0fecde37
[[ -x "$proof/minio" ]] || pin_failure
# These records are frozen evidence, never output destinations on reuse.
hash_matches "$proof/source-provenance.txt" 1b6c300af9d95d8dc0de2b8f3e0f49c6c1d805f37fb9e6588577c99b082fa436
hash_matches "$proof/binary-modules.txt" 961f923f162fd72138da4823c57f1e554685e2d15d230dd8f9381787a8836d79
hash_matches "$proof/backend-version.txt" 12943f9a272175a771b320f436564846139daa51a0c10b3a2b20ba1f1bf05c65
hash_matches "$proof/binary-hashes.txt" 2b3e632ad595a2dadb29747e18e91c30182f266128ad9cd6a626af3b49c5ee2e
go version -m "$proof/minio" > "$dir/binary-modules-current.txt"
# First line embeds the cache path; all build/dependency/settings lines must
# match exactly, including toolchain, vcs.revision and vcs.modified=false.
sed '1s|^.*: go|go|' "$dir/binary-modules-current.txt" > "$dir/binary-modules-normalized.txt"
sed '1s|^.*: go|go|' "$proof/binary-modules.txt" > "$dir/binary-modules-record-normalized.txt"
cmp -s "$dir/binary-modules-normalized.txt" "$dir/binary-modules-record-normalized.txt" || pin_failure
"$proof/minio" --version > "$dir/backend-version-current.txt"
cmp -s "$dir/backend-version-current.txt" "$proof/backend-version.txt" || pin_failure
# Static binary in scratch: no mutable base and no registry lookup/pull.
mkdir -p "$proof/image-context"
cp "$proof/minio" "$proof/image-context/minio"
cat > "$proof/image-context/Dockerfile" <<EOF
FROM scratch
LABEL bloem.task="$label" org.opencontainers.image.source="https://github.com/minio/minio" org.opencontainers.image.revision="$commit"
COPY minio /minio
USER 0:0
ENTRYPOINT ["/minio"]
EOF
image="bloem-versioned-proof-local:$commit"
timeout 120 docker build --network=none -t "$image" "$proof/image-context" > "$proof/image-build.log" 2>&1
imageid=$(docker image inspect --format '{{.Id}}' "$image")
printf '%s\n' "$imageid" > "$dir/imageid"
# Produce a real local OCI layout from the exact Docker config/layer blobs.
# Its manifest digest is NOT a published registry manifest pin.
timeout 60 docker save "$imageid" -o "$proof/local-image.tar"
python3 - "$proof" <<'PY'
import pathlib,tarfile,json,hashlib
p=pathlib.Path(__import__('sys').argv[1]); layout=p/'oci'; blobs=layout/'blobs'/'sha256'; blobs.mkdir(parents=True,exist_ok=True)
def blob(data,media):
 h=hashlib.sha256(data).hexdigest(); (blobs/h).write_bytes(data)
 return {'mediaType':media,'digest':'sha256:'+h,'size':len(data)}
with tarfile.open(p/'local-image.tar') as t:
 m=json.load(t.extractfile('manifest.json'))[0]
 config=blob(t.extractfile(m['Config']).read(),'application/vnd.oci.image.config.v1+json')
 layers=[blob(t.extractfile(x).read(),'application/vnd.oci.image.layer.v1.tar') for x in m['Layers']]
 manifest={'schemaVersion':2,'mediaType':'application/vnd.oci.image.manifest.v1+json','config':config,'layers':layers}
 d=blob(json.dumps(manifest,separators=(',',':')).encode(),manifest['mediaType'])
 (layout/'index.json').write_text(json.dumps({'schemaVersion':2,'manifests':[d]},separators=(',',':')))
 (layout/'oci-layout').write_text('{"imageLayoutVersion":"1.0.0"}')
 (p/'oci-digests.txt').write_text('local OCI manifest '+d['digest']+'\nDocker/OCI config ID '+config['digest']+'\n')
 print((p/'oci-digests.txt').read_text(),end='')
PY
suffix=${dir##*.}
name="bloem-versioned-proof-$suffix"
printf '%s\n' "$name" > "$dir/name"
mkdir "$dir/data"; chmod 700 "$dir/data"
python3 - "$dir" <<'PY'
import pathlib,secrets,json,sys
p=pathlib.Path(sys.argv[1]); suffix=p.name.split('.')[-1].lower()
w='writer'+secrets.token_hex(8); ws=secrets.token_hex(24); r='reader'+secrets.token_hex(8); rs=secrets.token_hex(24)
b='versioned-proof-'+suffix
(p/'env').write_text('MINIO_ROOT_USER='+w+'\nMINIO_ROOT_PASSWORD='+ws+'\nMINIO_BROWSER=off\nMINIO_UPDATE=off\nGOMAXPROCS=2\n')
(p/'fixture.json').write_text(json.dumps({'source_id':'versioned-proof','bucket':b,'negative_bucket':b+'-null','region':'us-east-1','endpoint':'http://127.0.0.1:19342','reader_key':r,'reader_secret':rs,'writer_key':w,'writer_secret':ws}))
for f in ('env','fixture.json'): (p/f).chmod(0o600)
PY
# Image config ID is the exact local artifact; no floating tag is run.
timeout 30 docker run -d --name "$name" --label "bloem.task=$label" --memory=768m --memory-swap=768m --cpus=2 --pids-limit=128 --user "1000:1000" --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m --env-file "$dir/env" -p 127.0.0.1:19342:9000 -v "$dir/data:/data" "$imageid" server /data --address :9000 > "$dir/container-id"
ready
cat > "$proof/provision.go" <<'GO'
package main
import("context";"encoding/json";"os";"fmt";"time"; admin "github.com/minio/madmin-go/v3"; minio "github.com/minio/minio-go/v7";"github.com/minio/minio-go/v7/pkg/credentials")
func main(){
 var f struct{Bucket string `json:"bucket"`;Negative string `json:"negative_bucket"`;Reader string `json:"reader_key"`;ReaderSecret string `json:"reader_secret"`;Writer string `json:"writer_key"`;WriterSecret string `json:"writer_secret"`}
 b,e:=os.ReadFile(os.Args[1]);must(e);must(json.Unmarshal(b,&f))
 ctx,cancel:=context.WithTimeout(context.Background(),45*time.Second);defer cancel()
 a,e:=admin.New("127.0.0.1:19342",f.Writer,f.WriterSecret,false);must(e)
 m,e:=minio.New("127.0.0.1:19342",&minio.Options{Creds:credentials.NewStaticV4(f.Writer,f.WriterSecret,""),Secure:false});must(e)
 for _,bucket:=range []string{f.Bucket,f.Negative}{must(m.MakeBucket(ctx,bucket,minio.MakeBucketOptions{Region:"us-east-1"}));must(m.SetBucketVersioning(ctx,bucket,minio.BucketVersioningConfiguration{Status:"Enabled"}))}
 must(m.SetBucketVersioning(ctx,f.Negative,minio.BucketVersioningConfiguration{Status:"Suspended"}))
 must(a.AddUser(ctx,f.Reader,f.ReaderSecret))
 policy:=fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket","s3:ListBucketVersions","s3:GetBucketLocation","s3:GetBucketVersioning"],"Resource":["arn:aws:s3:::%s","arn:aws:s3:::%s","arn:aws:s3:::%s-does-not-exist"]},{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":["arn:aws:s3:::%s/*","arn:aws:s3:::%s/*","arn:aws:s3:::%s-does-not-exist/*"]}]}`,f.Bucket,f.Negative,f.Bucket,f.Bucket,f.Negative,f.Bucket)
 must(a.AddCannedPolicy(ctx,"versioned-proof-reader",[]byte(policy)));must(a.SetPolicy(ctx,"versioned-proof-reader",f.Reader,false))
 fmt.Println("synthetic versioned and suspended buckets provisioned; distinct read-only provider principal")
}
func must(e error){if e!=nil{fmt.Fprintln(os.Stderr,"fixture provisioning failed (details withheld)");os.Exit(1)}}
GO
(cd "$source"; timeout 150 "$go_bin" run -p=1 -mod=readonly "$proof/provision.go" "$dir/fixture.json") > "$dir/provision.log" 2>&1
check_label
cat "$proof/backend-version.txt" "$proof/binary-hashes.txt"
printf 'S3_VERSIONED_DIR=%s\n' "$dir"
trap - EXIT
