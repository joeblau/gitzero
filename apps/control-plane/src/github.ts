import { SignJWT, importPKCS8 } from "jose";
import { z } from "zod";
import type { Conclusion, QueuedJob } from "./protocol";

const VARIABLE_PAGE_SIZE = 30;
const MAX_REPOSITORY_VARIABLES = 500;
const MAX_ORGANIZATION_VARIABLES = 1_000;
const MAX_COMBINED_VARIABLE_BYTES = 256 * 1_024;

const actionsVariablePageSchema = z.object({
  total_count: z.number().int().nonnegative(),
  variables: z.array(
    z.object({
      name: z.string().min(1),
      value: z.string(),
    }),
  ),
});

const repositoryActionsPermissionsSchema = z.object({
  enabled: z.boolean(),
  allowed_actions: z.enum(["all", "local_only", "selected"]),
  selected_actions_url: z.url().optional(),
  sha_pinning_required: z.boolean().optional(),
});

const repositoryCheckRunSchema = z.object({
  id: z.number().int().positive(),
  name: z.string(),
  head_sha: z.string(),
  external_id: z.string().nullable(),
  status: z.string(),
  conclusion: z.string().nullable(),
});

const checkRunRecoveryPageSchema = z.object({
  total_count: z.number().int().nonnegative(),
  check_runs: z.array(
    z.object({
      id: z.number().int().positive(),
      name: z.string(),
      head_sha: z.string(),
      external_id: z.string().nullable(),
    }),
  ),
});

type ActionsVariable = z.infer<
  typeof actionsVariablePageSchema
>["variables"][number];

export type RepositoryActionsPermissions = z.infer<
  typeof repositoryActionsPermissionsSchema
>;

export type RepositoryCheckRun = z.infer<typeof repositoryCheckRunSchema>;

export interface RepositoryOnboardingEvidence {
  actions: RepositoryActionsPermissions;
  check_run: RepositoryCheckRun | null;
}

export async function validateGitHubAppCredentials(
  env: GitHubEnvironment,
): Promise<void> {
  await createAppJwt(env);
}

interface GitHubEnvironment {
  GITHUB_API_VERSION: string;
  GITHUB_APP_ID: string;
  GITHUB_APP_PRIVATE_KEY: string;
}

type GitHubApiEnvironment = Pick<GitHubEnvironment, "GITHUB_API_VERSION">;

export async function createCheckRun(
  env: GitHubEnvironment,
  job: QueuedJob,
  recoverExisting = false,
): Promise<number> {
  const token = await createCheckToken(
    env,
    job.installation_id,
    job.repository.name,
  );
  const repositoryPath = `/repos/${encodeURIComponent(job.repository.owner)}/${encodeURIComponent(job.repository.name)}`;
  if (recoverExisting) {
    const query = new URLSearchParams({
      check_name: "GitZero",
      filter: "all",
      per_page: "100",
      app_id: env.GITHUB_APP_ID,
    });
    const page = checkRunRecoveryPageSchema.parse(
      await githubRequest(
        env,
        token,
        `${repositoryPath}/commits/${encodeURIComponent(job.pull_request.head_sha)}/check-runs?${query.toString()}`,
        { method: "GET" },
      ),
    );
    const existing = page.check_runs.find(
      (checkRun) =>
        checkRun.name === "GitZero" &&
        checkRun.external_id === job.id &&
        checkRun.head_sha.toLowerCase() ===
          job.pull_request.head_sha.toLowerCase(),
    );
    if (existing) return existing.id;
  }
  const response = await githubRequest<{
    id: number;
  }>(env, token, `${repositoryPath}/check-runs`, {
    method: "POST",
    body: JSON.stringify({
      name: "GitZero",
      head_sha: job.pull_request.head_sha,
      status: "queued",
      external_id: job.id,
      output: {
        title: "Waiting for a GitZero Mac",
        summary:
          "The pull request is queued for execution on the GitZero macOS cluster.",
      },
    }),
  });
  return response.id;
}

export async function updateCheckRun(
  env: GitHubEnvironment,
  job: QueuedJob,
  update:
    | { status: "in_progress"; title: string; summary: string }
    | {
        status: "completed";
        conclusion: Conclusion;
        title: string;
        summary: string;
      },
): Promise<void> {
  if (job.check_run_id === null) {
    return;
  }
  const token = await createCheckToken(
    env,
    job.installation_id,
    job.repository.name,
  );
  const body =
    update.status === "completed"
      ? {
          status: update.status,
          conclusion: githubConclusion(update.conclusion),
          completed_at: new Date().toISOString(),
          output: {
            title: update.title,
            summary: truncate(update.summary, 65_535),
          },
        }
      : {
          status: update.status,
          started_at: new Date().toISOString(),
          output: {
            title: update.title,
            summary: truncate(update.summary, 65_535),
          },
        };
  await githubRequest(
    env,
    token,
    `/repos/${encodeURIComponent(job.repository.owner)}/${encodeURIComponent(job.repository.name)}/check-runs/${job.check_run_id}`,
    { method: "PATCH", body: JSON.stringify(body) },
  );
}

export async function createAgentTokens(
  env: GitHubEnvironment,
  installationId: number,
  repository: string,
): Promise<{ checkoutToken: string; environmentToken: string }> {
  const appJwt = await createAppJwt(env);
  const [checkoutToken, environmentToken] = await Promise.all([
    createInstallationTokenWithJwt(env, appJwt, installationId, repository, {
      contents: "read",
      pull_requests: "read",
    }),
    createInstallationTokenWithJwt(env, appJwt, installationId, repository, {
      actions: "read",
      environments: "read",
    }),
  ]);
  return { checkoutToken, environmentToken };
}

export async function fetchActionsVariables(
  env: GitHubEnvironment,
  installationId: number,
  owner: string,
  repository: string,
  includeOrganizationVariables: boolean,
): Promise<Record<string, string>> {
  const token = await createInstallationToken(env, installationId, repository, {
    variables: "read",
  });
  return fetchActionsVariablesWithToken(
    env,
    token,
    owner,
    repository,
    includeOrganizationVariables,
  );
}

export async function fetchActionsVariablesWithToken(
  env: GitHubApiEnvironment,
  token: string,
  owner: string,
  repository: string,
  includeOrganizationVariables: boolean,
): Promise<Record<string, string>> {
  const repositoryPath = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}`;
  const repositoryVariables = listActionsVariables(
    env,
    token,
    `${repositoryPath}/actions/variables`,
    MAX_REPOSITORY_VARIABLES,
  );
  const organizationVariables = includeOrganizationVariables
    ? listActionsVariables(
        env,
        token,
        `${repositoryPath}/actions/organization-variables`,
        MAX_ORGANIZATION_VARIABLES,
      )
    : Promise.resolve([]);
  const [organization, repositoryScoped] = await Promise.all([
    organizationVariables,
    repositoryVariables,
  ]);
  return selectActionsVariables(organization, repositoryScoped);
}

export async function fetchRepositoryOnboardingEvidence(
  env: GitHubEnvironment,
  installationId: number,
  owner: string,
  repository: string,
  checkRunId: number | null,
): Promise<RepositoryOnboardingEvidence> {
  const token = await createInstallationToken(
    env,
    installationId,
    repository,
    checkRunId === null
      ? { administration: "read" }
      : { administration: "read", checks: "read" },
  );
  const repositoryPath = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}`;
  const actionsRequest = githubRequest(
    env,
    token,
    `${repositoryPath}/actions/permissions`,
    { method: "GET" },
  );
  const checkRequest =
    checkRunId === null
      ? Promise.resolve(null)
      : githubRequest(
          env,
          token,
          `${repositoryPath}/check-runs/${checkRunId}`,
          { method: "GET" },
        );
  const [actions, checkRun] = await Promise.all([actionsRequest, checkRequest]);
  return {
    actions: repositoryActionsPermissionsSchema.parse(actions),
    check_run:
      checkRun === null ? null : repositoryCheckRunSchema.parse(checkRun),
  };
}

export async function disableRepositoryActions(
  env: GitHubEnvironment,
  installationId: number,
  owner: string,
  repository: string,
): Promise<RepositoryActionsPermissions> {
  const token = await createInstallationToken(env, installationId, repository, {
    administration: "write",
  });
  const repositoryPath = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}`;
  await githubRequest<void>(
    env,
    token,
    `${repositoryPath}/actions/permissions`,
    {
      method: "PUT",
      body: JSON.stringify({ enabled: false }),
    },
  );
  const permissions = repositoryActionsPermissionsSchema.parse(
    await githubRequest(env, token, `${repositoryPath}/actions/permissions`, {
      method: "GET",
    }),
  );
  if (permissions.enabled) {
    throw new Error("GitHub did not disable repository Actions");
  }
  return permissions;
}

async function createCheckToken(
  env: GitHubEnvironment,
  installationId: number,
  repository: string,
): Promise<string> {
  return createInstallationToken(env, installationId, repository, {
    checks: "write",
  });
}

async function createInstallationToken(
  env: GitHubEnvironment,
  installationId: number,
  repository: string,
  permissions: Record<string, "read" | "write">,
): Promise<string> {
  if (!Number.isSafeInteger(installationId) || installationId <= 0) {
    throw new Error("a positive GitHub App installation ID is required");
  }
  const appJwt = await createAppJwt(env);
  return createInstallationTokenWithJwt(
    env,
    appJwt,
    installationId,
    repository,
    permissions,
  );
}

async function createInstallationTokenWithJwt(
  env: GitHubApiEnvironment,
  appJwt: string,
  installationId: number,
  repository: string,
  permissions: Record<string, "read" | "write">,
): Promise<string> {
  if (!Number.isSafeInteger(installationId) || installationId <= 0) {
    throw new Error("a positive GitHub App installation ID is required");
  }
  const response = await githubRequest<{ token: string }>(
    env,
    appJwt,
    `/app/installations/${installationId}/access_tokens`,
    {
      method: "POST",
      body: JSON.stringify({
        repositories: [repository],
        permissions,
      }),
    },
  );
  return response.token;
}

async function createAppJwt(env: GitHubEnvironment): Promise<string> {
  const appId = Number(env.GITHUB_APP_ID);
  if (!Number.isSafeInteger(appId) || appId <= 0) {
    throw new Error("GITHUB_APP_ID is not configured");
  }
  const privateKey = await importPKCS8(
    normalizePrivateKeyPem(env.GITHUB_APP_PRIVATE_KEY),
    "RS256",
  );
  const now = Math.floor(Date.now() / 1_000);
  return new SignJWT({})
    .setProtectedHeader({ alg: "RS256" })
    .setIssuer(String(appId))
    .setIssuedAt(now - 60)
    .setExpirationTime(now + 9 * 60)
    .sign(privateKey);
}

async function githubRequest<T = unknown>(
  env: GitHubApiEnvironment,
  token: string,
  path: string,
  init: RequestInit,
): Promise<T> {
  const response = await fetch(`https://api.github.com${path}`, {
    ...init,
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "User-Agent": "gitzero-control-plane",
      "X-GitHub-Api-Version": env.GITHUB_API_VERSION,
      ...init.headers,
    },
  });
  if (!response.ok) {
    const body = await readResponsePrefix(response, 4_096);
    throw new Error(
      `GitHub API ${init.method ?? "GET"} ${path} failed (${response.status}): ${body}`,
    );
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return response.json<T>();
}

async function readResponsePrefix(
  response: Response,
  maximumBytes: number,
): Promise<string> {
  if (!response.body) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let truncated = false;
  while (total < maximumBytes) {
    const { done, value } = await reader.read();
    if (done) break;
    const remaining = maximumBytes - total;
    if (value.byteLength > remaining) {
      chunks.push(value.slice(0, remaining));
      total += remaining;
      truncated = true;
      await reader.cancel("error response limit reached");
      break;
    }
    chunks.push(value);
    total += value.byteLength;
  }
  if (total === maximumBytes && !truncated) {
    truncated = true;
    await reader.cancel("error response limit reached");
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return `${new TextDecoder().decode(bytes)}${truncated ? "…" : ""}`;
}

async function listActionsVariables(
  env: GitHubApiEnvironment,
  token: string,
  path: string,
  maximumCount: number,
): Promise<ActionsVariable[]> {
  const variables: ActionsVariable[] = [];
  const maximumPages = Math.ceil(maximumCount / VARIABLE_PAGE_SIZE);
  for (let page = 1; page <= maximumPages; page += 1) {
    const separator = path.includes("?") ? "&" : "?";
    const response = actionsVariablePageSchema.parse(
      await githubRequest(
        env,
        token,
        `${path}${separator}per_page=${VARIABLE_PAGE_SIZE}&page=${page}`,
        {
          method: "GET",
        },
      ),
    );
    variables.push(...response.variables);
    if (
      response.variables.length < VARIABLE_PAGE_SIZE ||
      variables.length >= response.total_count
    ) {
      return variables.slice(0, maximumCount);
    }
  }
  return variables.slice(0, maximumCount);
}

function selectActionsVariables(
  organization: ActionsVariable[],
  repository: ActionsVariable[],
): Record<string, string> {
  const selected = new Map<string, ActionsVariable>();
  let byteCount = 0;
  const addLevel = (variables: ActionsVariable[]) => {
    for (const variable of variables.toSorted(compareVariableNames)) {
      const normalizedName = variable.name.toUpperCase();
      if (selected.has(normalizedName)) continue;
      const variableBytes = new TextEncoder().encode(
        `${variable.name}${variable.value}`,
      ).byteLength;
      if (byteCount + variableBytes > MAX_COMBINED_VARIABLE_BYTES) break;
      selected.set(normalizedName, variable);
      byteCount += variableBytes;
    }
  };

  // GitHub accounts for the more-specific repository scope before adding
  // alphabetically sorted organization variables to the shared 256 KiB limit.
  addLevel(repository);
  addLevel(organization);
  return Object.fromEntries(
    [...selected.values()].map((variable) => [variable.name, variable.value]),
  );
}

function compareVariableNames(
  left: ActionsVariable,
  right: ActionsVariable,
): number {
  const a = left.name.toUpperCase();
  const b = right.name.toUpperCase();
  return a < b ? -1 : a > b ? 1 : 0;
}

function githubConclusion(conclusion: Conclusion): string {
  switch (conclusion) {
    case "success":
    case "failure":
    case "cancelled":
    case "neutral":
      return conclusion;
    case "timed_out":
      return "timed_out";
  }
}

function normalizePrivateKeyPem(value: string): string {
  const normalized = value.includes("\\n")
    ? value.replaceAll("\\n", "\n")
    : value;
  if (normalized.includes("-----BEGIN PRIVATE KEY-----")) {
    return normalized;
  }
  const match = normalized.match(
    /^-----BEGIN RSA PRIVATE KEY-----\s+([A-Za-z0-9+/=\s]+)-----END RSA PRIVATE KEY-----\s*$/,
  );
  if (!match?.[1]) {
    throw new Error(
      "GITHUB_APP_PRIVATE_KEY must be an unencrypted PKCS#1 or PKCS#8 PEM key",
    );
  }
  const encoded = match[1].replaceAll(/\s/g, "");
  if (encoded.length > 64 * 1_024) {
    throw new Error("GITHUB_APP_PRIVATE_KEY exceeds the supported PEM size");
  }
  let pkcs1: Uint8Array;
  try {
    pkcs1 = Uint8Array.from(atob(encoded), (character) =>
      character.charCodeAt(0),
    );
  } catch {
    throw new Error("GITHUB_APP_PRIVATE_KEY contains invalid PEM encoding");
  }
  const rsaAlgorithmIdentifier = new Uint8Array([
    0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01,
    0x01, 0x05, 0x00,
  ]);
  const privateKeyInfo = derElement(
    0x30,
    concatenateBytes(
      new Uint8Array([0x02, 0x01, 0x00]),
      rsaAlgorithmIdentifier,
      derElement(0x04, pkcs1),
    ),
  );
  let binary = "";
  for (const byte of privateKeyInfo) binary += String.fromCharCode(byte);
  const base64 = btoa(binary);
  const lines = base64.match(/.{1,64}/g);
  if (!lines) throw new Error("failed to normalize GITHUB_APP_PRIVATE_KEY");
  return `-----BEGIN PRIVATE KEY-----\n${lines.join("\n")}\n-----END PRIVATE KEY-----`;
}

function derElement(tag: number, contents: Uint8Array): Uint8Array {
  return concatenateBytes(
    new Uint8Array([tag]),
    derLength(contents.length),
    contents,
  );
}

function derLength(length: number): Uint8Array {
  if (length < 0x80) return new Uint8Array([length]);
  const bytes: number[] = [];
  for (let remaining = length; remaining > 0; remaining >>>= 8) {
    bytes.unshift(remaining & 0xff);
  }
  return new Uint8Array([0x80 | bytes.length, ...bytes]);
}

function concatenateBytes(...values: Uint8Array[]): Uint8Array {
  const combined = new Uint8Array(
    values.reduce((total, value) => total + value.byteLength, 0),
  );
  let offset = 0;
  for (const value of values) {
    combined.set(value, offset);
    offset += value.byteLength;
  }
  return combined;
}

function truncate(value: string, maximum: number): string {
  return value.length <= maximum ? value : `${value.slice(0, maximum - 1)}…`;
}

export function cloneTokenForJob(token: string | null): string {
  return token ?? "";
}
