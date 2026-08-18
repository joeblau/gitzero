# GitHub App setup

Create a GitHub App with:

- Repository permissions for maximum workflow compatibility: `Actions: write`, `Artifact metadata: write`, `Attestations: write`, `Checks: write`, `Code quality: write`, `Contents: write`, `Deployments: write`, `Discussions: write`, `Environments: read`, `Issues: write`, `Packages: write`, `Pages: write`, `Pull requests: write`, `Code scanning alerts: write`, `Commit statuses: write`, `Dependabot alerts: read`, `Variables: read`, `Metadata: read`, and `Administration: write`. Every installation token still contains only the exact subset and access levels for one job and repository; granting these capabilities to the App does not place them all in a workflow token. Administrative write is used only by the explicitly confirmed repository-onboarding endpoint. Readiness and private shared-source policy checks request only `administration: read`; webhook processing never requests or uses administrative write access.
- Subscribe to `Pull request` events.
- Webhook URL: `https://<control-plane>/webhooks/github`.
- A high-entropy webhook secret.

When adding these maximum permissions to an existing GitHub App, its installations must approve the updated permission request before workflows can mint tokens that use the new write scopes. Until approval, exact write-token issuance fails visibly.

Create a JSON secrets file outside the repository with exactly these keys:

```json
{
  "GITHUB_WEBHOOK_SECRET": "<at-least-32-byte-random-value>",
  "GITHUB_APP_PRIVATE_KEY": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
  "AGENT_SHARED_TOKEN": "<different-at-least-32-byte-random-value>",
  "ADMIN_TOKEN": "<different-at-least-32-byte-random-value>"
}
```

Use a local editor or secret manager that does not expose the values in shell history. On macOS or Linux, restrict the file with `chmod 600 /absolute/path/gitzero.production.secrets.json`. Never commit it. Git also ignores files ending in `.secrets.json` as a final safeguard.

`AGENT_SHARED_TOKEN` is a high-entropy signing key held only by the Worker. Do not install it on a Mac. After deployment, mint a credential scoped to one installation/workspace and one agent ID:

```sh
curl -fsS -X POST \
  -H "Authorization: Bearer $GITZERO_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"agent_id":"mini-1"}' \
  "https://<control-plane>/v1/workspaces/<installation-id>/agent-token"
```

Use the returned `token` as that Mac's `GITZERO_AGENT_TOKEN`. A credential cannot connect to a different workspace or under a different agent ID. Rotating `AGENT_SHARED_TOKEN` invalidates every issued agent credential.

Pass the numeric GitHub App ID to the deployment command below. GitZero uses a signed app JWT to mint one-hour installation tokens only when required. With no workflow `permissions` declaration, the agent uses a single-repository baseline token with `contents: read` and `pull_requests: read`. Top-level and job-level declarations select the exact named read/write scopes for `github.token` / `secrets.GITHUB_TOKEN`; omitted mapping entries become none, `{}` exposes no token, `read-all` selects all supported GitHub read scopes, and a called reusable workflow receives the per-scope minimum of its own request and its caller's maximum. `write-all` and `id-token: write` fail visibly because GitZero does not issue GitHub OIDC tokens. Non-baseline tokens are requested only by the Mac that owns the active run, scoped to one repository and the exact permission set, cached only for that run, masked before use, and cleared at completion. The Durable Object reconstructs the authenticated webhook before minting writes: fork and Dependabot PR requests are always downgraded to read, even if GitHub's optional send-write-tokens-to-forks setting is enabled. A reusable-workflow call may inherit the token or pass it under declared names, but each nested call must pass or inherit the alias again and cannot broaden the token or introduce a repository secret. Public cross-repository checkout is anonymous and never sends this token to the other repository; private cross-repository and custom-token checkout remain unsupported. When an action or reusable workflow source is private, the App installation must also include that target repository and its Settings → Actions → General access policy must permit repositories owned by the same user or organization. GitZero first reads that native policy with a target-scoped `administration: read` token retained inside the Worker, then sends the Mac a separate target-only `contents: read` token. That source token is injected only into the relevant Git fetch, masked before output can be relayed, and never becomes a workflow token or secret. Separate `checks: write` and `variables: read` tokens remain in the Worker. At authenticated webhook enqueue, the Worker snapshots repository variables and organization variables shared with that repository, applies GitHub's repository precedence and 256 KiB run limit, then sends only the resulting non-sensitive `vars` values with the job.

Store the downloaded GitHub App PEM unchanged in `GITHUB_APP_PRIVATE_KEY`, including its header and footer. GitHub downloads PKCS#1 `RSA PRIVATE KEY` files; the Worker wraps that DER key in PKCS#8 in memory for Web Crypto and also accepts an already converted PKCS#8 `PRIVATE KEY`. No local OpenSSL conversion or rewritten secret is required.

When dispatching a GitHub-backed run, the Worker also mints a separate single-repository token with `actions: read` and `environments: read`. The Mac uses it internally to retrieve metadata and up to 100 variables for the exact environment selected by a runnable job. It is redacted from diagnostics and logs, is never inserted into `github.token`, `secrets`, or a workflow process environment, and is discarded when the run ends. Environment responses are cached per run. An environment with reviewer, wait-timer, custom protection, or deployment-branch rules fails closed before any job step starts because GitZero cannot safely bypass or reproduce GitHub's approval gate. Workflow `permissions` declarations affect only the separate built-in workflow token.

Install dependencies and validate the repository:

```sh
npm ci
npm test
npm run check
```

Then validate the production configuration and build locally without changing Cloudflare:

```sh
npm run deploy:control-plane -- \
  --account-id <cloudflare-account-id> \
  --github-app-id <github-app-id> \
  --secrets-file /absolute/path/gitzero.production.secrets.json
```

The command verifies the account and App IDs, exact secret set, minimum secret lengths and separation, RSA key material, private file permissions, and that the secret file is not Git-tracked. It then runs a strict Wrangler dry build. Secret values are read from the file and are never placed in process arguments or printed.

After reviewing the dry run, deploy the Worker, Durable Object migration, bindings, variables, and all four secrets together by adding the exact confirmation:

```sh
npm run deploy:control-plane -- \
  --account-id <cloudflare-account-id> \
  --github-app-id <github-app-id> \
  --secrets-file /absolute/path/gitzero.production.secrets.json \
  --confirm gitzero-control-plane
```

On success, the command discovers the `workers.dev` origin and checks both `/healthz` and authenticated `/readyz`. Pass `--url https://<control-plane>` when using a custom hostname. The committed Wrangler configuration declares all four secrets as required, so a raw production deployment fails closed when any secret is absent; use the bootstrap command so configuration and secrets are uploaded atomically.

`GET /healthz` is a public, dependency-free liveness check. The deployment command performs the authenticated local credential preflight automatically. It can also be called directly before accepting real deliveries:

```sh
curl -fsS \
  -H "Authorization: Bearer $GITZERO_ADMIN_TOKEN" \
  "https://<control-plane>/readyz"
```

It returns `200` only when the App ID and either supported PEM format can mint an RS256 JWT locally, the configured API version is date-shaped, and the webhook, agent-signing, and administrator secrets are at least 32 bytes and pairwise distinct. It makes no external request and never returns a secret. A failed preflight returns `503` with only boolean check results.

For no workflow changes and no duplicate hosted execution, native Actions must be disabled on each onboarded repository. Keep Actions enabled until `/healthz`, `/readyz`, an agent connection, and a test Check Run have all been verified. One duplicate native Actions run during this validation is expected. Changing repository Actions policy is potentially disruptive, so GitZero performs it only through the administrator-authenticated onboarding endpoint described below; the webhook handler never invokes that endpoint or changes repository policy.

A new signed delivery returns after durable persistence, before repository-variable reads and Check creation. The workspace snapshot reports `initialization_status` as `pending`, `running`, or `ready`, plus the attempt count and last bounded error. A job is never assigned before it is `ready`; transient initialization failures retry from Durable Object alarms and become a visible terminal failure after eight attempts.

After the test pull request finishes, use the administrator-authenticated, read-only readiness endpoint:

```sh
curl -fsS \
  -H "Authorization: Bearer $GITZERO_ADMIN_TOKEN" \
  "https://<control-plane>/v1/workspaces/<installation-id>/readiness?owner=<owner>&repository=<repository>"
```

The endpoint mints one short-lived, single-repository installation token with `administration: read` and, when a local candidate exists, `checks: read`. It reads the repository Actions policy and verifies the successful Check Run back from GitHub against GitZero's local job ID and exact pull-request head SHA. It never changes repository state or returns the GitHub token. Continue only when `safe_to_disable_native_actions` is `true` and `next_action` is `disable_native_actions`.

Copy the three exact values from `successful_check` into an explicit onboarding request:

```sh
curl -fsS -X POST \
  -H "Authorization: Bearer $GITZERO_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "owner": "<owner>",
    "repository": "<repository>",
    "expected_job_id": "<successful_check.job_id>",
    "expected_check_run_id": <successful_check.check_run_id>,
    "expected_head_sha": "<successful_check.head_sha>",
    "confirmation": "disable_native_actions"
  }' \
  "https://<control-plane>/v1/workspaces/<installation-id>/onboard"
```

The endpoint rejects missing confirmation or stale identifiers, then independently repeats the agent, local-result, GitHub Check, head-SHA, and repository-policy checks. Only after all checks pass does it mint a separate short-lived token restricted to that repository with `administration: write`, set `enabled: false`, and read the policy back. A successful first request returns `changed: true`, `onboarded: true`, and `next_action: complete`. Repeating the same request is idempotent and returns `changed: false` without another write. The possible next actions are `connect_compatible_agent`, `run_test_pull_request`, `wait_for_check_sync`, `disable_native_actions`, and `complete`.

The control plane exposes:

- `POST /webhooks/github` for GitHub deliveries.
- `GET /v1/workspaces/:installation_id/connect?role=agent&agent_id=...` for agent WebSockets.
- `GET /v1/workspaces/:installation_id/connect?role=observer` for read-only status WebSockets.
- `POST /v1/workspaces/:installation_id/agent-token` for administrator-authenticated, agent-scoped credential minting.
- `POST /v1/workspaces/:installation_id/jobs` for authenticated development/manual dispatch.
- `GET /v1/workspaces/:installation_id` for an authenticated fleet/job snapshot.
- `GET /v1/workspaces/:installation_id/readiness?owner=...&repository=...` for the administrator-authenticated, read-only onboarding gate.
- `POST /v1/workspaces/:installation_id/onboard` for the administrator-authenticated, confirmed, and evidence-bound native Actions shutdown.
- `GET /healthz` for health checks.
- `GET /readyz` for the administrator-authenticated, local credential/configuration preflight.

The Worker and Durable Object hold only coordination state, leases, deduplication records, and bounded log/check summaries; they do not store source, cache payloads, or artifact payloads. Each Mac keeps its own bounded workflow cache on disk and removes current-run artifact files with that run's isolated workspace. Source code and pull-request history remain in GitHub.
