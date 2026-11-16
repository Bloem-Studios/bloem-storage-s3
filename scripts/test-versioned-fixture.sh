#!/bin/bash
# Real launcher in isolated scratch; fake Docker never contacts the daemon.
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."
# Set GO to the go1.26.8 binary the fixture is pinned to (default: "go" on PATH).
GO_PATH=$(readlink -f "$(command -v "${GO:-go}")") || { echo 'go not found; set GO to a go1.26.8 binary' >&2; exit 1; }
export GO_PATH
export GOTOOLCHAIN=local GOMAXPROCS=2 GOWORK=off
root=$PWD
scratch=$(mktemp -d "$root/.superpowers/sdd/versioned-harness-test.XXXXXXXX")
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/repo/scripts" "$scratch/bin"
cp "${1:-scripts/start-versioned-fixture.sh}" "$scratch/repo/scripts/start-versioned-fixture.sh"
proof="$scratch/repo/.superpowers/sdd/versioned-proof"
original="$root/.superpowers/sdd/versioned-proof"
mkdir -p "$proof/minio-source/.git" "$proof/minio-source/buildscripts"
cp --reflink=auto "$original/minio" "$scratch/minio.saved"
for file in ldflags.txt binary-modules.txt backend-version.txt source-provenance.txt binary-hashes.txt; do cp "$original/$file" "$scratch/$file.saved"; done
cp "$original/minio-source/go.sum" "$proof/minio-source/"
cp "$original/minio-source/buildscripts/gen-ldflags.go" "$proof/minio-source/buildscripts/"
export HARNESS_STATE="$scratch/state" HARNESS_EVENTS="$scratch/events" HARNESS_EXECUTED="$scratch/executed"
cat > "$scratch/bin/git" <<'GIT'
#!/bin/bash
set -eu
[[ $1 == -C ]] && shift 2
case "$*" in
'cat-file -t '*) echo commit ;;
'rev-parse HEAD') echo "${HARNESS_HEAD:-9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a}" ;;
'rev-parse HEAD^{tree}') echo e4064317998bf2a12b0c74db0bf0de06f96ccb71 ;;
'rev-parse HEAD HEAD^{tree}') printf '%s\n' 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a e4064317998bf2a12b0c74db0bf0de06f96ccb71 ;;
'rev-parse refs/tags/'*) echo "${HARNESS_TAG:-9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a}" ;;
'status --porcelain'*) [[ ${HARNESS_STATUS_FAIL:-0} == 0 ]] || exit 94; printf '%s' "${HARNESS_DIRTY:-}" ;;
'remote get-url origin') echo "${HARNESS_ORIGIN:-https://github.com/minio/minio.git}" ;;
*) echo unexpected-git >&2; exit 98 ;;
esac
GIT
cat > "$scratch/bin/docker" <<'DOCKER'
#!/usr/bin/python3
import io,json,os,pathlib,sys,tarfile,time
args=sys.argv[1:]; state=pathlib.Path(os.environ['HARNESS_STATE']); events=pathlib.Path(os.environ['HARNESS_EVENTS'])
with events.open('a') as f: f.write(args[0]+'\n') # Never log private env values.
if args[0]=='build': sys.exit(0)
if args[:2]==['image','inspect']: print('sha256:'+'a'*64); sys.exit(0)
if args[0]=='save':
 with tarfile.open(args[args.index('-o')+1],'w') as tar:
  for name,data in [('manifest.json',json.dumps([{'Config':'config.json','Layers':['layer.tar']}]).encode()),('config.json',b'{}'),('layer.tar',b'fake-layer')]:
   info=tarfile.TarInfo(name); info.size=len(data); tar.addfile(info,io.BytesIO(data))
 sys.exit(0)
if args[0]=='run':
 mode=os.environ['HARNESS_MODE']
 if mode=='early': sys.exit(42)
 name=args[args.index('--name')+1]; label=args[args.index('--label')+1].split('=',1)[1]
 state.write_text(json.dumps({'name':name,'label':'foreign-task' if mode=='foreign' else label}))
 if mode=='foreign': sys.exit(43)
 if mode=='timeout': time.sleep(10); sys.exit(97)
 sys.exit(96)
if args[0]=='inspect':
 if not state.exists(): sys.exit(1)
 s=json.loads(state.read_text())
 if args[-1]!=s['name']: sys.exit(1)
 print(('b'*64+' ') if '{{.Id}}' in args[args.index('--format')+1] else '',end='')
 print(s['label']); sys.exit(0)
if args[0]=='rm':
 s=json.loads(state.read_text())
 if args[-1] not in (s['name'],'b'*64) or s['label']!='storage-s3-versioned-proof-20261006': sys.exit(99)
 failure=int(os.environ.get('HARNESS_RM_FAIL','0'))
 if failure: sys.exit(failure)
 state.unlink(); sys.exit(0)
sys.exit(95)
DOCKER
cat > "$scratch/bin/timeout" <<'TIMEOUT'
#!/bin/bash
set -eu
seconds=$1; shift
if [[ $1 == docker && ${2:-} == run && ${HARNESS_MODE:-} == timeout ]]; then exec /usr/bin/timeout 0.2 "$@"; fi
exec /usr/bin/timeout "$seconds" "$@"
TIMEOUT
cat > "$scratch/bin/sha256sum" <<'HASH'
#!/bin/bash
set -eu
if [[ ${HARNESS_BAD_TOOLCHAIN:-0} == 1 && $1 == "$GO_PATH" ]]; then
 printf '%064d  %s\n' 0 "$1"
else exec /usr/bin/sha256sum "$@"; fi
HASH
cat > "$scratch/bin/python3" <<'PYTHON'
#!/bin/bash
set -eu
if [[ ${HARNESS_MODE:-} == credentials && ${2:-} == */run.* ]]; then
 printf synthetic > "$2/env"
 printf synthetic > "$2/fixture.json"
 exit 44
fi
exec /usr/bin/python3 "$@"
PYTHON
chmod +x "$scratch/bin/"*
export PATH="$scratch/bin:$PATH"
failures=0
reset_case() {
 rm -rf "$proof"/run.*
 rm -f "$HARNESS_STATE" "$HARNESS_EVENTS" "$HARNESS_EXECUTED"
 cp --reflink=auto "$scratch/minio.saved" "$proof/minio"
 cp "$original/minio-source/go.mod" "$proof/minio-source/go.mod"
 for file in ldflags.txt binary-modules.txt backend-version.txt source-provenance.txt binary-hashes.txt; do cp "$scratch/$file.saved" "$proof/$file"; done
 unset HARNESS_HEAD HARNESS_TAG HARNESS_DIRTY HARNESS_ORIGIN HARNESS_BAD_TOOLCHAIN HARNESS_RM_FAIL HARNESS_STATUS_FAIL
 export HARNESS_MODE=early
}
assert_case() {
 local title=$1 expected=$2 collision=${3:-0} rc=0 bad=0
 bash "$scratch/repo/scripts/start-versioned-fixture.sh" start > "$scratch/output" 2>&1 || rc=$?
 [[ $rc == "$expected" ]] || { echo "FAIL $title exit=$rc expected=$expected"; bad=1; }
 for dir in "$proof"/run.*; do
  [[ -d $dir ]] || continue
  [[ ! -e $dir/env && ! -e $dir/fixture.json && ! -e $dir/data ]] || { echo "FAIL $title private material retained"; bad=1; }
 done
 if [[ $collision == 1 ]]; then
  [[ -f $HARNESS_STATE ]] || { echo "FAIL $title foreign resource removed"; bad=1; }
  ! grep -q '^rm$' "$HARNESS_EVENTS" || { echo "FAIL $title attempted foreign removal"; bad=1; }
 elif [[ $title == cleanup-error-preserves-timeout ]]; then
  [[ -f $HARNESS_STATE ]] && grep -q '^rm$' "$HARNESS_EVENTS" || { echo "FAIL $title no bounded removal attempt"; bad=1; }
 else
  [[ ! -f $HARNESS_STATE ]] || { echo "FAIL $title owned container retained"; bad=1; }
 fi
 if [[ $title == pin-* ]]; then
  [[ ! -e $HARNESS_EVENTS && ! -e $HARNESS_EXECUTED ]] || { echo "FAIL $title executed substituted cache or reached Docker"; bad=1; }
  for file in binary-modules.txt backend-version.txt source-provenance.txt binary-hashes.txt; do
   cmp -s "$scratch/$file.before" "$proof/$file" || { echo "FAIL $title rewrote $file"; bad=1; }
  done
 else
  local dir
  dir=$(echo "$proof"/run.*)
  [[ -f $dir/name && -f $proof/oci-digests.txt && ( $title == credentials-creation-failure || -f $dir/container-id ) ]] || { echo "FAIL $title public evidence lost"; bad=1; }
 fi
 if ((bad)); then failures=$((failures+1)); else echo "PASS $title"; fi
}
for mode in early timeout foreign; do
 reset_case; export HARNESS_MODE=$mode
 case $mode in early) assert_case launch-early 42;; timeout) assert_case launch-timeout-created 124;; foreign) assert_case launch-name-collision 43 1;; esac
done
reset_case; export HARNESS_MODE=timeout HARNESS_RM_FAIL=7
assert_case cleanup-error-preserves-timeout 124
reset_case; export HARNESS_MODE=credentials
assert_case credentials-creation-failure 44
for kind in binary source head tag dirty origin toolchain record flags status-error; do
 reset_case
 case $kind in
 binary) printf '#!/bin/bash\ntouch "$HARNESS_EXECUTED"\nprintf "substituted cache\\n"\n' > "$proof/minio"; chmod +x "$proof/minio" ;;
 source) echo '// substituted' >> "$proof/minio-source/go.mod" ;;
 head) export HARNESS_HEAD=bad ;;
 tag) export HARNESS_TAG=bad ;;
 dirty) export HARNESS_DIRTY=' M go.mod' ;;
 origin) export HARNESS_ORIGIN=https://example.invalid/minio.git ;;
 toolchain) export HARNESS_BAD_TOOLCHAIN=1 ;;
 record) echo substituted >> "$proof/binary-modules.txt" ;;
 flags) echo substituted >> "$proof/ldflags.txt" ;;
 status-error) export HARNESS_STATUS_FAIL=1 ;;
 esac
 for file in binary-modules.txt backend-version.txt source-provenance.txt binary-hashes.txt; do cp "$proof/$file" "$scratch/$file.before"; done
 assert_case "pin-$kind" 1
done
for label in storage-s3-versioned-proof-20261006 foreign-task; do
 reset_case
 dir="$proof/run.control"; mkdir -p "$dir/data"; printf control > "$dir/name"
 printf synthetic > "$dir/env"; printf synthetic > "$dir/fixture.json"; printf public > "$dir/provision.log"
 printf '{"name":"control","label":"%s"}' "$label" > "$HARNESS_STATE"
 rc=0; S3_VERSIONED_DIR="$dir" bash "$scratch/repo/scripts/start-versioned-fixture.sh" cleanup > "$scratch/output" 2>&1 || rc=$?
 bad=0
 [[ ! -e $dir/env && ! -e $dir/fixture.json && ! -e $dir/data && -f $dir/provision.log && -f $dir/name ]] || bad=1
 if [[ $label == foreign-task ]]; then
  [[ $rc != 0 && -f $HARNESS_STATE ]] || bad=1
  ! grep -q '^rm$' "$HARNESS_EVENTS" || bad=1
 else [[ $rc == 0 && ! -f $HARNESS_STATE ]] || bad=1; fi
 if ((bad)); then echo "FAIL local-cleanup-$label"; failures=$((failures+1)); else echo "PASS local-cleanup-$label"; fi
done
printf 'harness failures=%d\n' "$failures"
((failures==0))
