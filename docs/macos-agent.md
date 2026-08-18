# macOS agent installation

Build the optimized agent on the same architecture as the target Mac:

```sh
rustup toolchain install 1.96.1
cargo build --release -p gitzero-agent
```

Install Git and Node.js 24 on the target. Git 2.28 or newer is required by workflows that use `actions/checkout` sparse patterns, and Git 2.42 or newer is required for repositories that use the stable SHA-256 object format. JavaScript actions execute with the machine's `node` binary; the launchd service searches `/opt/homebrew/bin`, `/usr/local/bin`, `/usr/bin`, and `/bin`.

Create or choose a dedicated non-administrator macOS user. Then run the installer from the repository checkout:

```sh
print -rn -- "$GITZERO_AGENT_TOKEN" | sudo packaging/macos/install.sh \
  --binary target/release/gitzero-agent \
  --control-plane https://<worker-host> \
  --workspace <github-installation-id> \
  --token-stdin \
  --user <dedicated-user> \
  --agent-id <unique-mac-id> \
  --labels xcode-16,signing \
  --runner-group release-minis \
  --parallelism 1 \
  --cache-max-bytes 10737418240 \
  --cache-max-entry-bytes 2147483648 \
  --cache-mode write \
  --artifact-max-bytes 10737418240 \
  --artifact-max-entry-bytes 2147483648
```

The token must be minted for the same installation ID and agent ID using the administrator-authenticated endpoint described in [github-app.md](github-app.md). The Worker's `AGENT_SHARED_TOKEN` signing key must never be copied to an agent.

The installer places the root-owned binary in `/usr/local/libexec`, writes a mode-0600 launchd plist, and creates the work and log directories for the chosen user. Reading the credential from standard input keeps it out of the installer process arguments; the launched process also receives it only through its protected launchd environment.

Verify the service:

```sh
sudo launchctl print system/com.gitzero.agent
tail -f /Library/Logs/GitZero/agent.log
curl -fsS https://<worker-host>/healthz
```

Use a unique agent ID per Mac. `--parallelism` caps both the PR runs admitted to that machine and the total workflow-job slots shared across those runs, so nested job and matrix parallelism cannot multiply the configured host load. Dependency-ready jobs receive empty isolated workspaces and run concurrently when slots are available; `actions/checkout` populates each workspace independently, and a workflow's matrix `max-parallel` may set a stricter limit. Start with one on machines that run signing, Xcode, or other stateful toolchains.

Within one admitted job, unchanged workflows may use GitHub's `background` or `parallel` steps. GitZero enforces GitHub's separate limit of ten active background steps per job and queues additional entries until a slot opens. These subprocesses share the job workspace and are intentionally not additional scheduler jobs, so size `--parallelism` with the possibility of up to ten concurrent step processes in each admitted job. `wait`, `wait-all`, and `cancel` synchronize them; unconsumed tasks are always drained before action post entrypoints or job cleanup.

Every agent automatically advertises GitHub's `self-hosted`, `macOS`, and native `ARM64` or `X64` labels. `--labels` adds a comma-separated custom set and `--runner-group` sets the optional group used by object-form `runs-on`. String, list, expression, and `{group, labels}` selectors are evaluated per job; every requested self-hosted label and the requested group must match case-insensitively. A lone GitHub-hosted `macos-*` label remains compatible so repositories do not need workflow edits. After exact-SHA workflow discovery, an agent evaluates job conditions and selectors whose values are fixed before execution. This includes literal values plus expressions based on the webhook-backed `github` context, repository/organization `vars`, static `matrix` and `strategy` values, and statically bound reusable-workflow `inputs`. If one does not match, the agent returns the complete bounded requirement set, removes its temporary checkout, and the control plane requeues the run without consuming an infrastructure retry. The rejecting Mac remains in a draining state until its next heartbeat confirms the local task has released its slot. The queue then selects the least-loaded matching Mac and continues dispatching compatible jobs behind an otherwise blocked entry. Conditions or selectors involving `needs`, dynamic matrices, status functions, step/job/runner/environment state, secrets, or `hashFiles` remain runtime-only; all Macs eligible for a repository must support their possible values, and a mismatch fails visibly instead of running on the wrong host. Explicit GitHub-hosted Linux or Windows selectors are not negotiated because no Mac can reproduce those platforms; they fail through the executor's existing unsupported-runner path.

Use a different work root for every agent process. The work root contains the shared runner tool cache, a persistent remote-source Git object cache, and `_workflow-cache` for unchanged `actions/cache` and setup-action clients. Public and owner-shared private action/reusable-workflow refs are refreshed once per PR run while unchanged objects are reused locally. Private targets must be included in the same GitHub App installation and explicitly shared through their native Actions access setting. The control plane sends only a target-scoped contents-read token after checking that policy; the agent masks it, uses it only for the matching Git fetch, and clears its run-local cache at completion. Do not delete or modify `_action-cache` or `_workflow-cache` while the agent is running because active jobs may reference their objects or archives. Remote-source caches never store checkout credentials or replace their canonical HTTPS origin with a workflow-selected SSH remote. An `actions/checkout` SSH private key and known-host file exist only as mode-0600 files inside that job's fresh `RUNNER_TEMP`; `persist-credentials` controls whether later steps inherit their `GIT_SSH_COMMAND`, and the agent deletes the credential directory on every job exit. Workflow-cache entries are isolated by repository and pull-request ref; their per-run loopback bearer token is masked from logs. They remain local to one Mac, so a job load-balanced to a different Mac receives a normal cache miss.

Workflow and job `permissions` are enforced per job. The no-declaration baseline is repository contents and pull-request read access; explicit read/write mappings, `read-all`, `{}`, and reusable-workflow downgrades receive their exact scope. Any non-baseline token is requested over the agent WebSocket only after assignment, masked immediately, cached only for the current run, and cleared when the run ends. Every grant carries GitHub's authoritative expiry; the agent rejects tokens within five minutes of expiry and refreshes cached source and workflow credentials before reuse, including each top-level step boundary. The Worker reconstructs the signed webhook before granting writes and reduces every fork or Dependabot PR write to read. OIDC and `write-all` requests stop the workflow as unsupported instead of silently granting incomplete authority. A single shell/action step and its registered post entrypoint still keep the credential with which that action lifecycle began, so they must complete within its remaining lifetime.

For a job environment whose deployment branch policy allows the workflow ref and which has no reviewer, wait-timer, or custom protection gate, the agent reports start and terminal lifecycle events around the workflow job, including a validated HTTP(S) environment URL after step outputs resolve. It never receives the Worker's deployment-write credential. The control plane durably mirrors those events into GitHub Deployment statuses. Object-form `environment.deployment: false` keeps metadata and branch-policy validation plus environment-scoped variables but suppresses those lifecycle events.

The installer defaults to a 10 GiB total workflow-cache budget and a 2 GiB per-entry limit. `--cache-max-bytes` and `--cache-max-entry-bytes` write the matching protected launchd environment values; the total must be at least the per-entry limit. `--cache-mode` selects the current runner's effective `none`, `read`, `write`, or `write-only` policy and defaults to `write`, which permits both restore and save. The agent logs that mode at job start and exports it as `ACTIONS_CACHE_MODE` only to JavaScript action phases, matching the runner handler boundary; ordinary shell and composite script steps do not receive it. Current toolkit clients skip disallowed operations locally, while the authenticated cache endpoint separately rejects forbidden lookups/downloads or reservations/uploads/commits so older clients cannot bypass the policy. Entries idle for seven days expire, and least-recently-used entries are evicted before a new immutable entry is committed.

Current-run artifacts have a separate 10 GiB total budget and 2 GiB per-artifact default. `--artifact-max-bytes` and `--artifact-max-entry-bytes` set those bounds. The agent exposes the current GitHub artifact Twirp and block-blob contracts on IPv4 loopback, so unchanged `actions/upload-artifact@v4+` and `actions/download-artifact@v4+` jobs can transfer archived or direct-file artifacts between jobs in the same assigned run. These artifacts are transient and disappear during normal run cleanup; they are not copied to another Mac, retained after the run, listed in GitHub's artifact UI, or available to cross-run/cross-repository downloads.

GitHub runner 2.336's artifact-subject environment files remain off by default upstream. Pass `--allow-artifacts-file` to the installer to opt this Mac into the same `ACTIONS_RUNNER_ALLOW_ARTIFACTS_FILE=true` gate. Every shell and JavaScript action phase then receives fresh `GITHUB_ARTIFACTS` and `GITHUB_ARTIFACTS_LIST` files. File subjects are resolved from `GITHUB_WORKSPACE` and SHA-256 hashed; explicit `oci://` subjects retain their validated SHA-256, SHA-384, or SHA-512 digest. The read-only list is regenerated in ordinal name order from the job-wide aggregate before each process. Declarations are limited to 1 MiB per step and 500 unique subject names per job, and a same-name digest conflict fails that step. These subjects are provenance metadata only: they do not upload bytes, persist beyond the job, or create a GitHub artifact record. Omit the flag to expose the two empty compatibility files while ignoring declarations, matching the disabled upstream feature.

GitZero executes repository-controlled shell and action code. A dedicated user protects the rest of the host only partially; it is not a sandbox. Persistent machines should accept trusted branches and pull requests only. Use disposable hosts or a real sandbox for untrusted fork code.
