# Bloem S3 storage plugin

> [!CAUTION]
> **Offered without support.** The S3 storage plugin is provided as-is, with no support of any kind. Issues are turned off, questions go unanswered, and pull requests are not accepted: they are closed without review. You are free to fork it under the AGPL v3.

A storage plugin for [Bloem Server](https://github.com/Bloem-Studios/bloem-server). It lets a Bloem library read its
files from Amazon S3 or from another service that speaks the S3 protocol, such as Backblaze B2, Wasabi, Cloudflare R2,
DigitalOcean Spaces, Hetzner Object Storage, MinIO or Garage. It is meant for people who run their own Bloem Server and
keep their media in object storage instead of on the server's disks.

The plugin is a small program that Bloem Server starts and talks to over the plugin protocol (the
`bloem.plugin.v1.StorageProvider` service). Bloem asks it for two things only: lists of files, and exact byte ranges of
a file. It then treats every file like any other. The plugin passes on every file whatever its type; Bloem decides what
is an ebook, a movie or an audiobook track.

What it does and does not do:

- It only reads. It never writes, changes or deletes anything in your storage.
- It takes its keys only from the source's own settings, never from environment variables, AWS config files or
  instance metadata. It does not log them.
- It never follows redirects, never talks to an address other than the one configured, never returns signed links to
  your files, and never follows web addresses written inside files (such as `.strm` shortcuts).
- It passes on plain outcomes ("unavailable", "access denied", "changed") instead of the storage service's own error
  messages or addresses.

## Install

The plugin is published as a release in the [Bloem plugin catalog](https://github.com/Bloem-Studios/bloem-plugins), for
Linux on amd64 and arm64. Its plugin ID is `bloem.storage.s3`.

1. **Approve the release (server operator, once per release).** Bloem Server installs a native storage plugin only
   from a release whose manifest and SHA-256 checksum are listed in its approvals file. This is a JSON file whose path
   is set by the `BLOEM_NATIVE_STORAGE_APPROVALS` environment variable of Bloem Server. Add the release's entry
   (`manifest`, `checksum`, `os`, `arch`) and restart Bloem Server. The file must be an absolute path without
   symlinks, a regular file of at most 1 MiB owned by root or the server's user, with no group or world write
   permission.
2. **Add a source (administrator).** In Bloem's admin, open the storage sources page (for a platform administrator:
   **Plugins**, **Storage**, **Add source**), choose **S3-compatible storage**, pick your service, enter its region,
   and enter an access key and a secret key. For a service that is not listed, choose **Other** and type its
   address (the endpoint).
3. **Connect.** Bloem lists the buckets the keys can see. Choose **All buckets these keys can see** for one source
   that shows each bucket as a top-level folder, or choose individual buckets, or type a bucket name.
4. **Create a library** from the source and pick the folders it should use. Bloem then scans them and the titles
   appear.

Notes:

- Organization administrators can add sources only for their own organization, and only at public internet addresses
  on a short list of ports. Platform administrators install and upgrade the plugin.
- Keys can be replaced later without touching libraries. Any other setting, including the revision mode below, can be
  changed only while no library uses the source and nothing has been scanned from it.
- Settings are fixed while the plugin runs. Bloem restarts the plugin to apply new ones.
- The plugin keeps a private state file (a bbolt database) that remembers which entry ID belongs to which file. Bloem
  Server chooses its location and must keep it for as long as the source exists; deleting it loses the lookup that
  existing titles depend on.
- The keys need to list the bucket, check that it exists, and read files and their versions. A source that covers
  every bucket also needs permission to list buckets (`ListAllMyBuckets`).

### Revision mode

Every listed file carries a revision that says exactly which bytes Bloem scanned, and every read names it. The plugin
delivers those bytes or fails the read; it never mixes old and new bytes. The `revision_mode` setting decides how:

- `versioned` (default): needs a bucket that keeps every version of a file. Bloem reads exactly the version it scanned,
  even after the file is replaced. Files without a version are refused. Amazon S3 buckets have versioning off until
  you switch it on.
- `checked-etag`: for buckets without versioning. The plugin compares the file's ETag and size before and after each
  read and fails the read on any difference. It catches most changes but cannot prove the bytes are unchanged, for
  example if the service reuses an ETag. When a file does have a real version, this mode still reads that version.
  The server must be configured to accept this mode. Bloem's Add source form selects it by default.

## Configuration

The plugin has one configuration object, `connection` ("S3 storage connection"), defined in `manifest.json`. Unknown
fields are refused.

| Field | Required | Meaning |
| --- | --- | --- |
| `source_id` | yes | Stable source ID: 1 to 128 letters, digits, `_` or `-`. |
| `source_name` | no | Display name, at most 256 bytes, no control characters. Defaults to the source ID. |
| `bucket` | no | An S3 bucket name (`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`). Empty means every bucket the keys can list. |
| `prefix` | no | A key prefix inside the bucket: empty, or ending in `/`, at most 1,024 bytes, with no control characters, backslashes or `.`/`..` segments. Needs a bucket. |
| `region` | yes | The signing region, such as `us-east-1` (`^[a-z0-9-]{1,64}$`). |
| `endpoint` | yes | The service's HTTPS address: a host, an optional port, and no path (or `/`). No credentials, query or fragment. Requests use path-style addressing. |
| `access_key` | yes | Access key (secret, at most 1,024 bytes). |
| `secret_key` | yes | Secret key (secret, at most 4,096 bytes). |
| `session_token` | no | Session token for temporary credentials (secret, at most 8,192 bytes). |
| `revision_mode` | no | `versioned` (default) or `checked-etag`. Any other value is refused. |
| `state_path` | host | Absolute path of the state file. Chosen by Bloem Server, not by administrators. |
| `allow_loopback_http` | no | Default `false`. Allows plain `http` only for a numeric loopback address such as `127.0.0.1`, for test fixtures. |
| `probe_key` | no | Optional key (must start with the prefix) that is validated but used for nothing else. |

The form in Bloem's admin shows `source_id`, `source_name`, `bucket`, `prefix`, `region`, `endpoint`, the three
credentials and `revision_mode`. A configuration that is empty or incomplete leaves the plugin unconfigured: storage
calls then fail with `source not configured`. A configuration cannot be changed in a running process; the host restarts
the plugin instead.

### How files appear in Bloem

- The plugin lists one folder level at a time, page by page, never the whole tree. Names and extensions pass through
  unchanged. When a source starts inside a folder, that folder's name stays at the front of each path, so a source
  rooted at `movies/Example Film (2013)/` shows `Example Film (2013)/file.mkv`.
- Each file gets a permanent 64-character entry ID, a SHA-256 of the source, the kind and the full key. Changing the
  source ID, endpoint, bucket or prefix makes a different source that cannot reuse the old state file. Rotating keys
  or the region keeps the IDs.
- A source without a bucket shows each bucket as a folder; a prefix does not apply. Bucket names that do not follow
  current naming rules (old names with capitals or underscores) are skipped. On Amazon S3 such a source reads only
  buckets in the configured region.
- S3 offers no change feed, so every scan lists the folders again.

### When something goes wrong

- A down service, a deleted bucket or refused keys are reported as unavailable or access denied. They are never taken
  as "the file is gone", so an outage does not remove titles.
- A file is reported gone only when the storage says so for that file. An unclear "not found" counts only after the
  plugin has just reached the file's bucket successfully.
- A listing that is out of order, nested, or repeats itself stops the scan instead of guessing. Amazon S3 directory
  buckets (whose listings are unordered) and services that drop a file sharing its name with a folder are not
  supported.
- Each source remembers up to 4,000,000 files and folders, with no automatic clean-up. Past that, scans stop with an
  error.

Other bounds: 512 entries and 1 MiB per list reply, 8 MiB per read in chunks of at most 128 KiB, 8 MiB of listing XML,
and 30 seconds per HTTP request.

## Build and test

You need Go 1.26 or later (`go.mod` declares `go 1.26.0` and `toolchain go1.27.4`). The module depends on the public
[Bloem plugin SDK](https://github.com/Bloem-Studios/bloem-plugin-sdk) (Apache-2.0).

```sh
make build        # writes bin/bloem-storage-s3
make test         # go test -mod=readonly -p 2 -count=1 ./...
make test-race
make vet
bin/bloem-storage-s3 manifest   # prints the plugin manifest
```

The Makefile uses `go` from your `PATH`; set `GO=/path/to/go` to use another binary. It sets `GOTOOLCHAIN` to the
version in `go.mod`, so Go downloads that toolchain if yours is older; override it with `make GOTOOLCHAIN=local test`.

The default tests use local HTTP fixtures and synthetic credentials. Tests against real S3 servers are opt-in and
skipped by default. They are switched on by environment variables and need a local Docker container:

| Variable | Fixture |
| --- | --- |
| `S3_FIXTURE=1` | `scripts/start-fixture.sh`: a pinned SeaweedFS container on `127.0.0.1:19340` |
| `S3_FIXTURE=1 S3_BACKEND=garage` | `scripts/start-garage-fixture.sh`: a pinned Garage container on `127.0.0.1:19341` |
| `S3_CONFORMANCE=1`, `S3_RAW_DELIMITER=1`, `S3_OUTAGE=1` | the same containers |
| `S3_VERSIONED_FIXTURE=1`, `S3_VERSIONED_DIR` | `scripts/start-versioned-fixture.sh`: a MinIO build on `127.0.0.1:19342`. It reuses only a pinned, previously built local copy (see the script), so it works for the maintainer's setup but needs that prepared copy. Set `GO` to a Go 1.26.8 binary. |

All credentials in these fixtures are synthetic.

## Security

Please report vulnerabilities privately through GitHub's private vulnerability reporting:
<https://github.com/Bloem-Studios/bloem-storage-s3/security/advisories/new>. Do not open public issues for them.

## Licence

GNU Affero General Public License v3.0. See [LICENSE](LICENSE). Releases made before the AGPL v3 licence was added were
published under a proprietary licence and stay under it.
