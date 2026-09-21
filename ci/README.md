# Reproducible qualification (redemeine-cwvo)

From a trusted checkout, run **`./ci/qualify.sh`**. Jenkins runs this exact command;
there is no separate CI-only sequence. No publication or credentials are needed.
The host needs Linux, Bash, Git, GNU coreutils, and Docker CLI plus access to a
Linux Docker daemon. Registry/npm/Mongo binary download access and enough disk
for clean builds are required. Docker access is effectively host-root access:
use dedicated trusted agents, not agents that hold production credentials while
running untrusted PR code. The default Jenkins label is `lynx`; operators must
configure that capability (or override `DOCKER_AGENT_LABEL`). This repository
does not provision a Jenkins controller or agent.

## Toolchain and gates

`.nvmrc` pins Node **26.9.0**, and the root manifest pins npm **11.12.1**. The
Debian `ci/Dockerfile` installs npm inside the runner and checks `libcurl4` for
mongodb-memory-server. No host npm installation is changed. Production remains
Alpine, with both stages pinned to the same Node/npm versions. Base tags are
pulled, actual IDs/digests are logged, and the resulting production image ID—not
a mutable tag—is smoked and subsequently eligible for publication.

The runner executes install, pristine generated-output cleanup, forced serial root build, root check, strict CI
helper check/policy tests, forced serial root tests, packed-consumer compilation,
verified service connectivity, and the **unfiltered** real integration suite.
The host then performs a no-cache production build and real image smoke. No
failed return is ignored and no tests are filtered or retried to obtain green.
Existing root test skips/warnings remain visible. CI shell syntax is checked;
`ci/pipeline.test.mjs` verifies static Jenkins/gate policy, not the Jenkins DSL
runtime. Controller-side Declarative validation and a live job still require
the operator/Auditor; local execution must not be reported as live Jenkins proof.

Each run owns a unique private network, Mongo **8.3.9** `rs0`, separate expiry
Mongo **8.3.9** `rs0` (1 MiB oplog, `syncdelay=1`), and Rabbit **4.3.6**. No host
ports are published. Actual primary/server settings and AMQP connection are
checked. Rabbit uses a fresh UID-999 tmpfs home, not inherited cookie data.
The runner ignores host test URI variables and supplies only these isolated
test endpoints. Fsync-locking tests run serially; never point them at user data.

Node runs as the host UID/GID with a job-owned writable HOME/cache/tmp. The
workspace is mounted at its quoted original absolute path, including spaces.
Linked worktrees mount only required Git common/admin directories read-only at
their original paths; `GIT_OPTIONAL_LOCKS=0` avoids index refresh writes. No Docker
socket, host home, credentials directory, or arbitrary parent directory is
mounted in Node. A normal Jenkins checkout needs only its workspace `.git`.
The packed consumer uses OS `tmpdir()` plus a unique directory and removes that
directory in `finally`, printing child diagnostics on failure.

## Shipped image and shutdown contract

Only core/dispatcher workspace dependencies are installed. The runtime omits
development/root tools and copies their freshly compiled output, retaining the
core postbuild shim and declaration dependencies. Recursive ignore rules exclude
host dependencies/output, Git, caches, env/auth files and CI artifacts. A benign
generated sentinel checks that host `dist` did not enter the image.

### Pristine npm/image artifacts (redemeine-b3qo)

Turbo `--force` forces compilation but **does not remove deleted-module output**.
The shared qualifier therefore removes declared workspace `dist` and configured
`dist-worker` directories after install and before build. This intentionally
discards local generated results. It first validates the root `packages/*`
workspace contract, real package directories, known TypeScript output paths and
all deletion candidates, and refuses any tracked file under those outputs.
Symlinked roots/packages/output roots are rejected before deletion; nested output
symlinks are unlinked without following their targets. Source, `node_modules`,
unrelated ignored files/secrets, and caches are not cleaned. Current TypeScript
configs have no incremental/composite state; enabling it requires an explicit
cleanup contract rather than silently deleting generic `*.tsbuildinfo` files.

After qualification, npm dry-run pack metadata must include exactly each runtime
package's built `dist` paths and byte sizes. The receipt records every relative
runtime file path, byte count and SHA-256, plus an all-workspace build hash. The
actual image must match both runtime inventories **and every byte hash**, not
just package versions. Extra, missing or changed files fail. The temporary
Docker-context sentinel is inserted only for the build and removed before the
post-build artifact check; no filename is exempt from identity hashing. Adding
even `ci-host-sentinel` after qualification invalidates release preflight.

For a sequential end-to-end regression in a clean-source worktree, use the pinned
Node/npm toolchain to run `node ci/seeded-artifacts.mjs qualify`. This test-only
harness seeds an ignored obsolete module, invokes the real `./ci/qualify.sh`, then
checks its absence from npm and the actual image, verifies exact inventories,
and temporarily adds/tampers with output to prove rejection before restoring it.
It does not publish, bind credentials, or introduce a qualification bypass.
Normal `./ci/qualify.sh` always performs the pristine-output gate. Filesystem
safety and same-size byte mutation regressions run in private temporary fixture
repositories (including paths with spaces), never concurrent live build output.

Smoke verifies package imports/versions, Node/npm, non-root UID, tini PID 1 with
direct Node child, argument-over-environment precedence, real Rabbit bodies and
stable IDs, and majority checkpoint advancement. `docker stop --time 30` must
yield graceful exit **0**, invalid database initialization must exit **1**, and
a genuinely blocked checkpoint must exit **124** near the unchanged **10-second**
CLI deadline **while Mongo is still locked**. Only then does the helper unlock,
wait for outstanding writes plus a majority barrier, restart, and prove the next
commit progresses. Stable-ID duplicates remain permitted; exit does not prove
write cancellation. Production orchestrators must configure **30 seconds external
stop grace**, not Docker's default 10 seconds. This adds no readiness healthcheck,
runtime CLI flags, quarantine adapter, or distribution/claim semantics.

## Cleanup, diagnostics and recovery

`RUN_ID` defaults to a random UUID; Jenkins supplies one and reuses it in `post`.
Do not reuse a live run's ID. EXIT/INT/TERM cleanup and Jenkins `always` remove
only resources with **both** `org.tapeworm.ci.project=qualification` and
`org.tapeworm.ci.run=<RUN_ID>`. Named data volumes carry the same labels. Failure
diagnostics retain the last 200 service log lines and state (not full environment
inspection); qualification logs retain the last 2 MiB. Local evidence lives in
`.ci-artifacts/<RUN_ID>/`. Jenkins archives logs/identity then deletes its
workspace. Local built images and build cache remain for independent audit;
there is no global prune. Remove only known run image tags after audit.

Lost daemon/agent or SIGKILL cannot guarantee automatic teardown. On recovery,
inspect the two labels, then run `RUN_ID=<recorded-id> ./ci/qualify.sh cleanup` in
the same checkout. This is idempotent; cleanup failure fails a successful run but
does not replace its original failure status. Never use global Docker prune.
`TMPDIR=/your/private/temp ./ci/cleanup.test.sh` exercises an actual injected
service-start failure and SIGTERM, including preservation of unrelated owned
sentinel resources. It creates no publication credentials.

## Git Flow qualification and master-only publication

Changes integrate and qualify on **`develop`**. A **`release/*`** branch prepares
and qualifies reviewed versions before those changes merge to **`master`**.
Neither `develop` nor `release/*` receives publication credentials or publishes;
they run the same credential-free qualification used by other branch contexts.
Prepare versions in a **reviewed release commit outside CI**. Master publication
rejects pending changeset `.md` files and any tracked/untracked source dirt
(ignored build outputs are allowed). There is **no** `changeset:version`, Git
commit, or Git push in Jenkins. The credential-free preflight checks clean master,
already-committed versions, revision, qualified image receipt, and hashes of the
same built npm outputs; publication does not rebuild anything.

Only after qualification and preflight does master bind existing credentials
`npm-token`, `docker-creds`, and `docker-registry`. The registry value is an
explicit prefix such as `ghcr.io/org` or `registry.example:5000/team`; login uses
only the host, while image tags preserve the namespace. Temporary npm/Docker
configuration is outside the build context and removed on exit; npm auth uses
literal environment substitution and shell tracing is off. The tested image gets
revision/run and package-version tags first; **`:latest` is master-only and last**.
Git tags do not bypass the branch gate: tag builds, including a tag named
`master`, qualify but cannot preflight or publish. Jenkins excludes tag builds
from both release stages, and the credential-free policy independently rejects
tag metadata. Master is the sole publication branch.
Npm and image publication are not atomic. On partial failure inspect what was
published and recover manually against the same reviewed versions—never
automatically rerun versioning. Local qualification/tests never login or publish.
