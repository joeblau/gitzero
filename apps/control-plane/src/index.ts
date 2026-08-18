import { z } from "zod";
import {
  createAgentToken,
  timingSafeSecretEqual,
  verifyAgentBearer,
  verifyBearer,
  verifyWebhookSignature,
} from "./auth";
import { queuedJobSchema, type QueuedJob } from "./protocol";
import {
  disableRepositoryActions,
  fetchRepositoryOnboardingEvidence,
  validateGitHubAppCredentials,
} from "./github";
import {
  buildRepositoryReadiness,
  findSuccessfulCheckCandidate,
} from "./readiness";
import {
  managedSecretIdentitySchema,
  managedSecretInputSchema,
} from "./secrets";
export { Workspace } from "./workspace";

const MAX_WEBHOOK_BYTES = 5 * 1024 * 1024;
const MAX_ONBOARDING_BODY_BYTES = 16 * 1024;
const MAX_MANAGED_SECRET_BODY_BYTES = 64 * 1024;
export const SUPPORTED_PULL_REQUEST_ACTIONS = [
  "assigned",
  "unassigned",
  "labeled",
  "unlabeled",
  "opened",
  "edited",
  "closed",
  "reopened",
  "synchronize",
  "converted_to_draft",
  "locked",
  "unlocked",
  "enqueued",
  "dequeued",
  "milestoned",
  "demilestoned",
  "ready_for_review",
  "review_requested",
  "review_request_removed",
  "auto_merge_enabled",
  "auto_merge_disabled",
] as const;
const acceptedPullRequestActions = new Set<string>(
  SUPPORTED_PULL_REQUEST_ACTIONS,
);

const webhookSchema = z.object({
  action: z.string(),
  installation: z.object({ id: z.number().int().positive() }),
  repository: z.object({
    id: z.number().int().positive(),
    name: z.string().min(1),
    clone_url: z.url(),
    owner: z.object({
      id: z.number().int().positive(),
      login: z.string().min(1),
      type: z.enum(["User", "Organization"]),
    }),
  }),
  pull_request: z.object({
    number: z.number().int().positive(),
    draft: z.boolean().nullable(),
    merged: z.boolean(),
    merge_commit_sha: z
      .string()
      .regex(/^[0-9a-fA-F]{40}$/)
      .nullable()
      .optional(),
    head: z.object({ sha: z.string(), ref: z.string().min(1) }),
    base: z.object({ sha: z.string(), ref: z.string().min(1) }),
  }),
  sender: z.object({
    id: z.number().int().positive(),
    login: z.string().min(1),
  }),
});

const manualJobSchema = queuedJobSchema
  .omit({
    id: true,
    workspace_id: true,
    run_number: true,
    check_run_id: true,
    event: true,
    requires_github_token: true,
    report_to_github: true,
  })
  .extend({
    installation_id: z.number().int().nonnegative().default(0),
    environment: z.record(z.string(), z.string()).default({}),
    requires_github_token: z.boolean().default(false),
    report_to_github: z.boolean().default(false),
  });

const agentTokenSchema = z.object({
  agent_id: z.string().min(1).max(128),
});

const readinessQuerySchema = z.object({
  owner: z
    .string()
    .min(1)
    .max(39)
    .regex(/^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/),
  repository: z
    .string()
    .min(1)
    .max(100)
    .regex(/^[A-Za-z0-9._-]+$/),
});

const onboardingBodySchema = readinessQuerySchema.extend({
  expected_job_id: z.uuid(),
  expected_check_run_id: z.number().int().positive(),
  expected_head_sha: z.string().regex(/^[0-9a-fA-F]{40}$/),
  confirmation: z.literal("disable_native_actions"),
});

export default {
  async fetch(request, env): Promise<Response> {
    const requestId = crypto.randomUUID();
    const url = new URL(request.url);
    try {
      if (request.method === "GET" && url.pathname === "/healthz") {
        return Response.json({
          status: "ok",
          service: "gitzero-control-plane",
        });
      }
      if (request.method === "GET" && url.pathname === "/readyz") {
        return handleServiceReadiness(request, env);
      }
      if (request.method === "POST" && url.pathname === "/webhooks/github") {
        return handleGitHubWebhook(request, env);
      }

      const connect = url.pathname.match(
        /^\/v1\/workspaces\/([^/]+)\/connect$/,
      );
      if (request.method === "GET" && connect) {
        return handleWorkspaceConnect(
          request,
          env,
          decodeURIComponent(requiredMatch(connect[1])),
        );
      }
      const manual = url.pathname.match(/^\/v1\/workspaces\/([^/]+)\/jobs$/);
      if (request.method === "POST" && manual) {
        return handleManualJob(
          request,
          env,
          decodeURIComponent(requiredMatch(manual[1])),
        );
      }
      const agentToken = url.pathname.match(
        /^\/v1\/workspaces\/([^/]+)\/agent-token$/,
      );
      if (request.method === "POST" && agentToken) {
        return handleAgentToken(
          request,
          env,
          decodeURIComponent(requiredMatch(agentToken[1])),
        );
      }
      const readiness = url.pathname.match(
        /^\/v1\/workspaces\/([^/]+)\/readiness$/,
      );
      if (request.method === "GET" && readiness) {
        return handleRepositoryReadiness(
          request,
          env,
          decodeURIComponent(requiredMatch(readiness[1])),
        );
      }
      const secrets = url.pathname.match(
        /^\/v1\/workspaces\/([^/]+)\/secrets$/,
      );
      if (["GET", "PUT", "DELETE"].includes(request.method) && secrets) {
        return handleManagedSecrets(
          request,
          env,
          decodeURIComponent(requiredMatch(secrets[1])),
        );
      }
      const onboard = url.pathname.match(
        /^\/v1\/workspaces\/([^/]+)\/onboard$/,
      );
      if (request.method === "POST" && onboard) {
        return handleRepositoryOnboarding(
          request,
          env,
          decodeURIComponent(requiredMatch(onboard[1])),
        );
      }
      const workspace = url.pathname.match(/^\/v1\/workspaces\/([^/]+)$/);
      if (request.method === "GET" && workspace) {
        return handleWorkspaceStatus(
          request,
          env,
          decodeURIComponent(requiredMatch(workspace[1])),
        );
      }
      return Response.json(
        { error: "not_found", request_id: requestId },
        { status: 404 },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        JSON.stringify({
          message: "request failed",
          requestId,
          path: url.pathname,
          error: message,
        }),
      );
      return Response.json(
        {
          error: "internal_error",
          message: "The request could not be processed.",
          request_id: requestId,
        },
        { status: 500 },
      );
    }
  },
} satisfies ExportedHandler<Cloudflare.Env>;

async function handleGitHubWebhook(
  request: Request,
  env: Cloudflare.Env,
): Promise<Response> {
  const body = await readBoundedBody(request, MAX_WEBHOOK_BYTES);
  if (
    !(await verifyWebhookSignature(
      body,
      request.headers.get("X-Hub-Signature-256"),
      env.GITHUB_WEBHOOK_SECRET,
    ))
  ) {
    return Response.json({ error: "invalid_signature" }, { status: 401 });
  }
  const event = request.headers.get("X-GitHub-Event");
  if (event !== "pull_request") {
    return Response.json(
      { accepted: false, reason: "event_not_supported" },
      { status: 202 },
    );
  }
  const deliveryId = request.headers.get("X-GitHub-Delivery");
  if (!deliveryId) {
    return Response.json({ error: "missing_delivery_id" }, { status: 400 });
  }

  const parsed = createGitHubWebhookJob(
    JSON.parse(new TextDecoder().decode(body)),
  );
  if (!acceptedPullRequestActions.has(parsed.action)) {
    return Response.json(
      { accepted: false, reason: "action_not_runnable" },
      { status: 202 },
    );
  }

  const workspaceId = String(parsed.job.installation_id);
  const job = queuedJobSchema.parse({
    ...parsed.job,
    workspace_id: workspaceId,
  });
  const result = await env.WORKSPACES.getByName(workspaceId).enqueue(
    job,
    deliveryId,
  );
  return Response.json(result, { status: result.duplicate ? 200 : 202 });
}

async function handleServiceReadiness(
  request: Request,
  env: Cloudflare.Env,
): Promise<Response> {
  if (!(await verifyBearer(request, env.ADMIN_TOKEN))) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  let githubApp = true;
  try {
    await validateGitHubAppCredentials(env);
  } catch {
    githubApp = false;
  }
  const [
    webhookMatchesAgent,
    webhookMatchesAdmin,
    webhookMatchesEncryption,
    agentMatchesAdmin,
    agentMatchesEncryption,
    adminMatchesEncryption,
  ] = await Promise.all([
    timingSafeSecretEqual(env.GITHUB_WEBHOOK_SECRET, env.AGENT_SHARED_TOKEN),
    timingSafeSecretEqual(env.GITHUB_WEBHOOK_SECRET, env.ADMIN_TOKEN),
    timingSafeSecretEqual(
      env.GITHUB_WEBHOOK_SECRET,
      env.SECRETS_ENCRYPTION_KEY,
    ),
    timingSafeSecretEqual(env.AGENT_SHARED_TOKEN, env.ADMIN_TOKEN),
    timingSafeSecretEqual(env.AGENT_SHARED_TOKEN, env.SECRETS_ENCRYPTION_KEY),
    timingSafeSecretEqual(env.ADMIN_TOKEN, env.SECRETS_ENCRYPTION_KEY),
  ]);
  const checks = {
    github_app_credentials: githubApp,
    github_api_version: /^\d{4}-\d{2}-\d{2}$/.test(env.GITHUB_API_VERSION),
    webhook_secret: hasMinimumSecretEntropy(env.GITHUB_WEBHOOK_SECRET),
    agent_signing_key: hasMinimumSecretEntropy(env.AGENT_SHARED_TOKEN),
    admin_token: hasMinimumSecretEntropy(env.ADMIN_TOKEN),
    secrets_encryption_key: hasMinimumSecretEntropy(env.SECRETS_ENCRYPTION_KEY),
    secrets_are_distinct:
      !webhookMatchesAgent &&
      !webhookMatchesAdmin &&
      !webhookMatchesEncryption &&
      !agentMatchesAdmin &&
      !agentMatchesEncryption &&
      !adminMatchesEncryption,
  };
  const ready = Object.values(checks).every(Boolean);
  return Response.json(
    { status: ready ? "ready" : "not_ready", checks },
    { status: ready ? 200 : 503 },
  );
}

export function createGitHubWebhookJob(input: unknown): {
  action: string;
  draft: boolean | null;
  job: QueuedJob;
} {
  const event = z.json().parse(input);
  const payload = webhookSchema.parse(event);
  const workspaceId = String(payload.installation.id);
  const job = queuedJobSchema.parse({
    id: crypto.randomUUID(),
    workspace_id: workspaceId,
    installation_id: payload.installation.id,
    run_number: 0,
    repository: {
      owner: payload.repository.owner.login,
      name: payload.repository.name,
      clone_url: payload.repository.clone_url,
    },
    pull_request: {
      number: payload.pull_request.number,
      action: payload.action,
      head_sha: payload.pull_request.head.sha,
      base_sha: payload.pull_request.base.sha,
      merge_sha: payload.pull_request.merge_commit_sha ?? null,
      execution_ref:
        payload.action === "closed" && payload.pull_request.merged
          ? `refs/heads/${payload.pull_request.base.ref}`
          : `refs/pull/${payload.pull_request.number}/merge`,
      head_ref: payload.pull_request.head.ref,
      base_ref: payload.pull_request.base.ref,
    },
    check_run_id: null,
    event,
    environment: {},
    variables: {},
    requires_github_token: true,
    report_to_github: true,
  });
  return { action: payload.action, draft: payload.pull_request.draft, job };
}

async function handleWorkspaceConnect(
  request: Request,
  env: Cloudflare.Env,
  workspaceId: string,
): Promise<Response> {
  if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
    return Response.json(
      { error: "websocket_upgrade_required" },
      { status: 426 },
    );
  }
  const url = new URL(request.url);
  const role = url.searchParams.get("role");
  if (role !== "agent" && role !== "observer") {
    return Response.json({ error: "invalid_role" }, { status: 400 });
  }
  const agentId = url.searchParams.get("agent_id");
  if (role === "agent" && !agentId) {
    return Response.json({ error: "missing_agent_id" }, { status: 400 });
  }
  const authenticated =
    role === "agent"
      ? await verifyAgentBearer(
          request,
          env.AGENT_SHARED_TOKEN,
          workspaceId,
          requiredValue(agentId),
        )
      : await verifyBearer(request, env.ADMIN_TOKEN);
  if (!authenticated) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  return env.WORKSPACES.getByName(workspaceId).fetch(request);
}

async function handleAgentToken(
  request: Request,
  env: Cloudflare.Env,
  workspaceId: string,
): Promise<Response> {
  if (!(await verifyBearer(request, env.ADMIN_TOKEN))) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const { agent_id: agentId } = agentTokenSchema.parse(await request.json());
  return Response.json({
    workspace_id: workspaceId,
    agent_id: agentId,
    token: await createAgentToken(env.AGENT_SHARED_TOKEN, workspaceId, agentId),
  });
}

async function handleManualJob(
  request: Request,
  env: Cloudflare.Env,
  workspaceId: string,
): Promise<Response> {
  if (!(await verifyBearer(request, env.ADMIN_TOKEN))) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const input = manualJobSchema.parse(await request.json());
  const job = queuedJobSchema.parse({
    ...input,
    id: crypto.randomUUID(),
    workspace_id: workspaceId,
    check_run_id: null,
  });
  const result = await env.WORKSPACES.getByName(workspaceId).enqueue(
    job,
    `manual:${job.id}`,
  );
  return Response.json(result, { status: 202 });
}

async function handleWorkspaceStatus(
  request: Request,
  env: Cloudflare.Env,
  workspaceId: string,
): Promise<Response> {
  if (!(await verifyBearer(request, env.ADMIN_TOKEN))) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const snapshot = await env.WORKSPACES.getByName(workspaceId).getSnapshot();
  return Response.json(snapshot);
}

async function handleManagedSecrets(
  request: Request,
  env: Cloudflare.Env,
  workspaceId: string,
): Promise<Response> {
  if (!(await verifyBearer(request, env.ADMIN_TOKEN))) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const workspace = env.WORKSPACES.getByName(workspaceId);
  if (request.method === "GET") {
    return Response.json({
      secrets: await workspace.listManagedSecrets(workspaceId),
    });
  }
  let body: unknown;
  try {
    const bytes = await readBoundedBody(request, MAX_MANAGED_SECRET_BODY_BYTES);
    body = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return Response.json(
      {
        error: "invalid_secret_request",
        message: "a bounded JSON managed-secret request is required.",
      },
      { status: 400 },
    );
  }
  if (request.method === "PUT") {
    const parsed = managedSecretInputSchema.safeParse(body);
    if (!parsed.success) {
      return Response.json(
        {
          error: "invalid_secret_request",
          message:
            "managed-secret scope, identity, name, and value are invalid.",
        },
        { status: 400 },
      );
    }
    try {
      const result = await workspace.putManagedSecret(workspaceId, parsed.data);
      return Response.json(result, { status: result.created ? 201 : 200 });
    } catch {
      return Response.json(
        {
          error: "secret_update_rejected",
          message: "the managed secret could not be stored in this scope.",
        },
        { status: 409 },
      );
    }
  }
  const parsed = managedSecretIdentitySchema.safeParse(body);
  if (!parsed.success) {
    return Response.json(
      {
        error: "invalid_secret_request",
        message: "managed-secret scope, identity, and name are invalid.",
      },
      { status: 400 },
    );
  }
  const result = await workspace.deleteManagedSecret(workspaceId, parsed.data);
  return Response.json(result, { status: result.deleted ? 200 : 404 });
}

async function handleRepositoryReadiness(
  request: Request,
  env: Cloudflare.Env,
  workspaceId: string,
): Promise<Response> {
  if (!(await verifyBearer(request, env.ADMIN_TOKEN))) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const installationId = parseInstallationId(workspaceId);
  if (installationId === null) {
    return Response.json(
      {
        error: "invalid_workspace",
        message: "workspace ID must be a GitHub App installation ID.",
      },
      { status: 400 },
    );
  }
  const url = new URL(request.url);
  const parsed = readinessQuerySchema.safeParse({
    owner: url.searchParams.get("owner"),
    repository: url.searchParams.get("repository"),
  });
  if (!parsed.success) {
    return Response.json(
      {
        error: "invalid_repository",
        message: "owner and repository query parameters are required.",
      },
      { status: 400 },
    );
  }
  const snapshot = await env.WORKSPACES.getByName(workspaceId).getSnapshot();
  const candidate = findSuccessfulCheckCandidate(
    parsed.data.owner,
    parsed.data.repository,
    snapshot,
  );
  const evidence = await fetchRepositoryOnboardingEvidence(
    env,
    installationId,
    parsed.data.owner,
    parsed.data.repository,
    candidate?.check_run_id ?? null,
  );
  return Response.json(
    buildRepositoryReadiness(
      installationId,
      parsed.data.owner,
      parsed.data.repository,
      evidence.actions,
      snapshot,
      evidence.check_run,
    ),
  );
}

async function handleRepositoryOnboarding(
  request: Request,
  env: Cloudflare.Env,
  workspaceId: string,
): Promise<Response> {
  if (!(await verifyBearer(request, env.ADMIN_TOKEN))) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const installationId = parseInstallationId(workspaceId);
  if (installationId === null) {
    return Response.json(
      {
        error: "invalid_workspace",
        message: "workspace ID must be a GitHub App installation ID.",
      },
      { status: 400 },
    );
  }

  let body: unknown;
  try {
    const bytes = await readBoundedBody(request, MAX_ONBOARDING_BODY_BYTES);
    body = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return Response.json(
      {
        error: "invalid_onboarding_request",
        message: "a bounded JSON onboarding request is required.",
      },
      { status: 400 },
    );
  }
  const parsed = onboardingBodySchema.safeParse(body);
  if (!parsed.success) {
    return Response.json(
      {
        error: "invalid_onboarding_request",
        message:
          "owner, repository, exact readiness evidence, and explicit confirmation are required.",
      },
      { status: 400 },
    );
  }

  const snapshot = await env.WORKSPACES.getByName(workspaceId).getSnapshot();
  const candidate = findSuccessfulCheckCandidate(
    parsed.data.owner,
    parsed.data.repository,
    snapshot,
  );
  if (
    candidate === null ||
    candidate.job_id !== parsed.data.expected_job_id ||
    candidate.check_run_id !== parsed.data.expected_check_run_id ||
    candidate.head_sha.toLowerCase() !==
      parsed.data.expected_head_sha.toLowerCase()
  ) {
    return Response.json(
      {
        error: "readiness_changed",
        message:
          "the supplied successful Check evidence is no longer the current local candidate; fetch readiness again.",
      },
      { status: 409 },
    );
  }

  const evidence = await fetchRepositoryOnboardingEvidence(
    env,
    installationId,
    parsed.data.owner,
    parsed.data.repository,
    candidate.check_run_id,
  );
  const readiness = buildRepositoryReadiness(
    installationId,
    parsed.data.owner,
    parsed.data.repository,
    evidence.actions,
    snapshot,
    evidence.check_run,
  );
  if (readiness.onboarded) {
    return Response.json({ ...readiness, changed: false });
  }
  if (!readiness.safe_to_disable_native_actions) {
    return Response.json(
      {
        error: "repository_not_ready",
        message:
          "native Actions remains enabled because current GitZero readiness evidence is insufficient.",
        readiness,
      },
      { status: 409 },
    );
  }

  const disabled = await disableRepositoryActions(
    env,
    installationId,
    parsed.data.owner,
    parsed.data.repository,
  );
  const completed = buildRepositoryReadiness(
    installationId,
    parsed.data.owner,
    parsed.data.repository,
    disabled,
    snapshot,
    evidence.check_run,
  );
  if (!completed.onboarded) {
    throw new Error("repository onboarding verification failed after mutation");
  }
  return Response.json({ ...completed, changed: true });
}

async function readBoundedBody(
  request: Request,
  maximumBytes: number,
): Promise<ArrayBuffer> {
  const contentLength = request.headers.get("Content-Length");
  if (contentLength !== null && Number(contentLength) > maximumBytes) {
    throw new Error(`request body exceeds ${maximumBytes} bytes`);
  }
  if (!request.body) {
    return new ArrayBuffer(0);
  }
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maximumBytes) {
      await reader.cancel("body too large");
      throw new Error(`request body exceeds ${maximumBytes} bytes`);
    }
    chunks.push(value);
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body.buffer;
}

function requiredMatch(value: string | undefined): string {
  if (value === undefined) throw new Error("invalid route match");
  return value;
}

function requiredValue(value: string | null): string {
  if (value === null) throw new Error("required value is missing");
  return value;
}

function parseInstallationId(workspaceId: string): number | null {
  if (!/^[1-9][0-9]*$/.test(workspaceId)) {
    return null;
  }
  const installationId = Number(workspaceId);
  if (!Number.isSafeInteger(installationId)) {
    return null;
  }
  return installationId;
}

function hasMinimumSecretEntropy(value: string): boolean {
  return new TextEncoder().encode(value).byteLength >= 32;
}
