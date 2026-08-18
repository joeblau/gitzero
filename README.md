# GitZero

GitZero runs pull-request CI on a pool of Mac Minis while GitHub remains the source of truth for repositories and pull-request checks.

The repository contains two cooperating planes:

- `apps/control-plane`: a Cloudflare Worker and Durable Object that accepts GitHub App webhooks, coordinates agents over WebSockets, and publishes GitHub Checks.
- `crates/gitzero-agent`: a Rust daemon for each Mac Mini. It checks out the exact PR commit and executes compatible jobs from the repository's existing GitHub Actions workflow files.

The project is intentionally independent of GitHub's hosted runner routing. Repositories onboarded to GitZero disable native GitHub Actions in repository settings, preventing duplicate hosted runs while keeping `.github/workflows/*.yml` unchanged.

Production setup is fail-closed and uses one validated command to upload the Worker configuration and required secrets atomically; see [GitHub App setup](docs/github-app.md).

## Current compatibility

GitZero executes `pull_request` workflows at the exact webhook head SHA. The implemented surface includes:

- concurrently scheduled workflow files and macOS jobs, GitHub-compatible string/list/group `runs-on` selection with all self-hosted labels enforced, negotiated routing of deterministically resolvable macOS/self-hosted selectors to the least-loaded matching Mac, repository-scoped workflow/job concurrency groups with case-insensitive keys, conditional cancellation, single-pending replacement, and FIFO `queue: max`, `needs`, static and expression-driven matrices with `include`/`exclude`, `max-parallel`, and fail-fast cancellation, plus pull-request activity/base-branch/path filters, job outputs, conditions, defaults, and tolerated failures;
- the complete authenticated pull-request webhook through `github.event` / `GITHUB_EVENT_PATH`, standard actor/repository/run/workflow/job/action identity in `github.*` and `GITHUB_*`, repository, accessible organization, and unprotected job-environment configuration through `vars`, typed `matrix` plus `strategy` index/total/parallel/fail-fast contexts, and GitHub expression functions, including the masked repository-scoped `github.token` / `secrets.GITHUB_TOKEN`, runtime `add-mask` and `stop-commands` processing, workspace-scoped `hashFiles`, step outputs, protected environment files and last-added-first path files, secret-masked `GITHUB_STEP_SUMMARY` Markdown surfaced in the PR Check, and action-wide step/job `timeout-minutes` with process-tree termination;
- durable, deduplicated webhook acknowledgement before external GitHub API work, followed by leased and retried variable/Check initialization that cannot dispatch an incomplete job to a Mac;
- shell steps with GitHub's distinct file-based default `bash -e {0}` and explicit `bash --noprofile --norc -eo pipefail {0}` behavior, custom shell templates, local, public, or owner-shared private GitHub-hosted JavaScript actions, composite actions with expression-driven nested `continue-on-error` and live `github.action_status`, environment-file and still-supported legacy command action outputs/saved state, and LIFO post entrypoints;
- nested same-repository plus public or owner-shared private remote reusable workflow calls up to ten total workflow levels, including per-run ref pinning, typed `workflow_call` inputs and defaults, static and output-driven call matrices with `max-parallel`/fail-fast, internal job dependencies, workflow outputs, caller conditions, cycle detection, and direct-call `secrets: inherit` or named aliases for the built-in token;
- empty, isolated workspaces and fresh `RUNNER_TEMP` roots for every matrix/job instance; exact authenticated head/base-snapshot `actions/checkout` plus anonymous public cross-repository checkout with per-run ref pinning, at the workspace root or a contained relative `path`, including cone/non-cone sparse working trees and partial-clone filters for large monorepos; unchanged `actions/cache` and setup-action cache clients through a repository/PR-scoped Mac-local cache service; unchanged current-run `actions/upload-artifact@v4+` and `actions/download-artifact@v4+` transfers through a bounded loopback artifact service; shared runner tool and remote-action object caches; and a per-Mac execution limit shared across concurrent PRs.

The compiler or executor fails visibly for behavior it cannot reproduce. Current explicit gaps include Docker actions and container/service jobs, repository/organization/environment secrets other than named aliases of the built-in token, retained/GitHub-hosted/cross-run artifacts and cluster-shared workflow caches, protected-environment approval/branch rules, private cross-repository checkout and custom checkout credentials, and pre-dispatch routing for selectors that depend on runtime state such as `needs` outputs or dynamic matrices. GitZero renders an environment URL into the PR Check but does not create GitHub Deployment records. Private cross-repository actions and reusable workflows follow GitHub's native sharing boundary: the caller and target must have the same user or organization owner, the App installation must include the target, and the target repository's Actions access setting must authorize that owner scope. The Worker checks that policy with an internal administration-read token, then returns a different contents-read token scoped only to the target; the agent masks it immediately and never exposes it as `github.token` or a workflow secret. GitHub's empty-string behavior for missing properties remains present, so unchanged fallback expressions remain valid. Same-repository checkout supports root or safe relative paths, safe aliases for the webhook-authenticated PR head and base snapshots, `ref`/`commit` outputs, repeat-clean behavior, the built-in token or anonymous access, depth, tags, progress control, sparse checkout, partial-clone filters, LFS, submodules, and ephemeral persisted credentials while always verifying the selected exact SHA. Public cross-repository checkout validates `owner/repository` and ref inputs, fetches without sending the source repository token, pins a moving ref once per PR run, and verifies the resolved commit after materialization. Workflow caches are immutable per key/version, support exact and ordered prefix restore matching, remain isolated to one repository and pull-request ref, and are best-effort: a run dispatched to another Mac starts cold. Current-run artifacts are immutable by default, support the official overwrite/delete flow, archived and direct-file transfer, digest verification, and exact name/ID lookup, but are transient to the assigned run and Mac; the action's GitHub `artifact-url` output is not backed by a GitHub artifact record. See [architecture.md](docs/architecture.md) for the exact boundary.

## Development

Requirements: Rust 1.96.1+, Node.js 24+, and npm 11+. Production agents also require Git and Node.js 24 on every Mac because JavaScript actions use the machine's Node runtime; sparse checkout requires Git 2.28 or newer.

```sh
npm install
npm test
npm run check
npm run test:e2e
```

`npm run test:e2e` is a macOS-only, account-free acceptance test. It starts the Worker and its SQLite Durable Object through local Wrangler, creates a temporary repository with an exact pull-request ref, connects two real Rust agents and an observer WebSocket, and submits two overlapping jobs. The test requires both jobs to execute the fixture workflow successfully on different agents, verifies the relayed lifecycle/log events and final summaries, and removes every process, repository, secret file, work root, and local Durable Object database when it exits. The fixture's production-shaped `github.com` URL is mapped to its temporary bare repository only inside the spawned test agents, so production repository validation is unchanged and the test does not contact GitHub or a Cloudflare account.

Run the local control plane with `npm run dev --workspace @gitzero/control-plane`. Run an agent with:

```sh
cargo run -p gitzero-agent -- \
  --control-plane ws://127.0.0.1:8787 \
  --workspace-id local \
  --agent-token "$GITZERO_AGENT_TOKEN"
```

`GITZERO_AGENT_TOKEN` is minted by the authenticated control-plane agent-token endpoint for one workspace and one agent ID. It is not the Worker's agent-token signing key.

Secrets and GitHub App setup are described in [github-app.md](docs/github-app.md).

Repository onboarding uses an administrator-authenticated two-step gate. The read-only readiness endpoint confirms a connected compatible Mac, verifies a successful GitZero Check back from GitHub for the exact PR head, and reads the repository's native Actions state. An operator can then submit those exact evidence identifiers plus an explicit confirmation to the onboarding endpoint. The endpoint revalidates everything, disables native Actions with a separate repository-scoped token, and reads the setting back before reporting completion. Webhook delivery never invokes this administrative path.

The Worker exposes public `/healthz` liveness separately from authenticated `/readyz`, which locally validates the GitHub App credentials and secret hygiene without contacting GitHub.

For a release build and launchd installation on a Mac Mini, see [macos-agent.md](docs/macos-agent.md).
