import { SignJWT, importPKCS8 } from "jose";
import { z } from "zod";
import {
  WORKFLOW_TOKEN_PERMISSIONS,
  WORKFLOW_WRITE_PERMISSIONS,
  checkAnnotationSchema,
  type CheckAnnotation,
  type Conclusion,
  type QueuedJob,
} from "./protocol";
import { fullGitObjectIdSchema } from "./git";

const VARIABLE_PAGE_SIZE = 30;
const MAX_REPOSITORY_VARIABLES = 500;
const MAX_ORGANIZATION_VARIABLES = 1_000;
const MAX_COMBINED_VARIABLE_BYTES = 256 * 1_024;
const workflowTokenPermissions = new Set<string>(WORKFLOW_TOKEN_PERMISSIONS);
const workflowWritePermissions = new Set<string>(WORKFLOW_WRITE_PERMISSIONS);

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

const repositoryActionsAccessSchema = z.object({
  access_level: z.enum(["none", "user", "organization", "enterprise"]),
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

const checkRunAnnotationsSchema = z.array(
  z.object({
    path: z.string(),
    start_line: z.number().int(),
    end_line: z.number().int(),
    start_column: z.number().int().nullable().optional(),
    end_column: z.number().int().nullable().optional(),
    annotation_level: z.enum(["notice", "warning", "failure"]),
    message: z.string(),
    title: z.string().nullable().optional(),
  }),
);

const pullRequestMergeSchema = z.object({
  head: z.object({ sha: fullGitObjectIdSchema }),
  base: z.object({ sha: fullGitObjectIdSchema }),
  merged: z.boolean(),
  mergeable: z.boolean().nullable(),
  merge_commit_sha: fullGitObjectIdSchema.nullable(),
});

const installationAccessTokenSchema = z.object({
  token: z.string().min(1),
  expires_at: z.iso.datetime({ offset: true }),
});

const deploymentSchema = z.object({
  id: z.number().int().positive(),
  payload: z.unknown().optional(),
});

const deploymentStatusSchema = z.object({
  state: z.enum([
    "error",
    "failure",
    "inactive",
    "in_progress",
    "queued",
    "pending",
    "success",
  ]),
  description: z.string().nullable().optional(),
  environment: z.string(),
  environment_url: z.string().nullable().optional(),
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

export type PullRequestMergeSnapshot =
  | { status: "ready"; merge_sha: string }
  | { status: "pending" }
  | { status: "conflicted" }
  | {
      status: "changed";
      current_head_sha: string;
      current_base_sha: string;
    };

export interface InstallationAccessToken {
  token: string;
  expiresAtEpochSeconds: number;
}

export type GitHubDeploymentState =
  | "in_progress"
  | "success"
  | "failure"
  | "error";

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
  const executionSha = jobExecutionSha(job);
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
        `${repositoryPath}/commits/${encodeURIComponent(executionSha)}/check-runs?${query.toString()}`,
        { method: "GET" },
      ),
    );
    const existing = page.check_runs.find(
      (checkRun) =>
        checkRun.name === "GitZero" &&
        checkRun.external_id === job.id &&
        checkRun.head_sha.toLowerCase() === executionSha.toLowerCase(),
    );
    if (existing) return existing.id;
  }
  const response = await githubRequest<{
    id: number;
  }>(env, token, `${repositoryPath}/check-runs`, {
    method: "POST",
    body: JSON.stringify({
      name: "GitZero",
      head_sha: executionSha,
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
        annotations: CheckAnnotation[];
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
  const repositoryPath = `/repos/${encodeURIComponent(job.repository.owner)}/${encodeURIComponent(job.repository.name)}`;
  const annotations =
    update.status === "completed" && update.annotations.length > 0
      ? missingCheckAnnotations(
          update.annotations,
          await fetchCheckAnnotations(
            env,
            token,
            repositoryPath,
            job.check_run_id,
          ),
        )
      : [];
  const body =
    update.status === "completed"
      ? {
          status: update.status,
          conclusion: githubConclusion(update.conclusion),
          completed_at: new Date().toISOString(),
          output: {
            title: update.title,
            summary: truncate(update.summary, 65_535),
            ...(annotations.length > 0
              ? { annotations: annotations.map(githubCheckAnnotation) }
              : {}),
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
    `${repositoryPath}/check-runs/${job.check_run_id}`,
    { method: "PATCH", body: JSON.stringify(body) },
  );
}

async function fetchCheckAnnotations(
  env: GitHubApiEnvironment,
  token: string,
  repositoryPath: string,
  checkRunId: number,
): Promise<CheckAnnotation[]> {
  const response = checkRunAnnotationsSchema.parse(
    await githubRequest(
      env,
      token,
      `${repositoryPath}/check-runs/${checkRunId}/annotations?per_page=100`,
      { method: "GET" },
    ),
  );
  return response.map((annotation) =>
    checkAnnotationSchema.parse({
      ...annotation,
      start_column: annotation.start_column ?? null,
      end_column: annotation.end_column ?? null,
      title: annotation.title ?? null,
    }),
  );
}

function missingCheckAnnotations(
  desired: readonly CheckAnnotation[],
  existing: readonly CheckAnnotation[],
): CheckAnnotation[] {
  const existingCounts = new Map<string, number>();
  for (const annotation of existing) {
    const key = checkAnnotationKey(annotation);
    existingCounts.set(key, (existingCounts.get(key) ?? 0) + 1);
  }
  return desired.filter((annotation) => {
    const key = checkAnnotationKey(annotation);
    const remaining = existingCounts.get(key) ?? 0;
    if (remaining === 0) return true;
    existingCounts.set(key, remaining - 1);
    return false;
  });
}

function checkAnnotationKey(annotation: CheckAnnotation): string {
  return JSON.stringify([
    annotation.path,
    annotation.start_line,
    annotation.end_line,
    annotation.start_column,
    annotation.end_column,
    annotation.annotation_level,
    annotation.message,
    annotation.title,
  ]);
}

function githubCheckAnnotation(annotation: CheckAnnotation): object {
  return {
    path: annotation.path,
    start_line: annotation.start_line,
    end_line: annotation.end_line,
    ...(annotation.start_column === null
      ? {}
      : {
          start_column: annotation.start_column,
          end_column: annotation.end_column,
        }),
    annotation_level: annotation.annotation_level,
    message: annotation.message,
    ...(annotation.title === null ? {} : { title: annotation.title }),
  };
}

export async function createAgentToken(
  env: GitHubEnvironment,
  installationId: number,
  repository: string,
): Promise<InstallationAccessToken> {
  return createInstallationAccessToken(env, installationId, repository, {
    contents: "read",
    pull_requests: "read",
  });
}

export async function createEnvironmentToken(
  env: GitHubEnvironment,
  installationId: number,
  repository: string,
): Promise<InstallationAccessToken> {
  return createInstallationAccessToken(env, installationId, repository, {
    actions: "read",
    contents: "read",
    environments: "read",
  });
}

export async function syncGitHubDeployment(
  env: GitHubEnvironment,
  job: QueuedJob,
  unitId: string,
  environment: string,
  state: GitHubDeploymentState,
  environmentUrl: string | null,
  deploymentId: number | null,
  recoverAmbiguousWrite: boolean,
): Promise<number> {
  const token = await createInstallationToken(
    env,
    job.installation_id,
    job.repository.name,
    { deployments: "write" },
  );
  const repositoryPath = `/repos/${encodeURIComponent(job.repository.owner)}/${encodeURIComponent(job.repository.name)}`;
  let resolvedDeploymentId = deploymentId;
  if (resolvedDeploymentId === null && recoverAmbiguousWrite) {
    const query = new URLSearchParams({
      sha: jobExecutionSha(job),
      environment,
      task: "deploy",
      per_page: "100",
    });
    const deployments = z
      .array(deploymentSchema)
      .parse(
        await githubRequest(
          env,
          token,
          `${repositoryPath}/deployments?${query.toString()}`,
          { method: "GET" },
        ),
      );
    resolvedDeploymentId =
      deployments.find((deployment) =>
        isGitZeroDeploymentPayload(deployment.payload, job.id, unitId),
      )?.id ?? null;
  }
  if (resolvedDeploymentId === null) {
    const deployment = deploymentSchema.parse(
      await githubRequest(env, token, `${repositoryPath}/deployments`, {
        method: "POST",
        body: JSON.stringify({
          ref: jobExecutionSha(job),
          task: "deploy",
          auto_merge: false,
          required_contexts: [],
          environment,
          description: "GitZero workflow deployment.",
          payload: {
            gitzero: {
              job_id: job.id,
              unit_id: unitId,
            },
          },
        }),
      }),
    );
    resolvedDeploymentId = deployment.id;
  }

  const description = deploymentStatusDescription(state);
  if (recoverAmbiguousWrite) {
    const statuses = z
      .array(deploymentStatusSchema)
      .parse(
        await githubRequest(
          env,
          token,
          `${repositoryPath}/deployments/${resolvedDeploymentId}/statuses?per_page=100`,
          { method: "GET" },
        ),
      );
    if (
      statuses.some(
        (status) =>
          status.state === state &&
          status.environment === environment &&
          (status.environment_url ?? null) === environmentUrl &&
          (status.description ?? "") === description,
      )
    ) {
      return resolvedDeploymentId;
    }
  }
  await githubRequest(
    env,
    token,
    `${repositoryPath}/deployments/${resolvedDeploymentId}/statuses`,
    {
      method: "POST",
      body: JSON.stringify({
        state,
        environment,
        description,
        ...(environmentUrl === null ? {} : { environment_url: environmentUrl }),
        auto_inactive: state === "success",
      }),
    },
  );
  return resolvedDeploymentId;
}

export async function createWorkflowToken(
  env: GitHubEnvironment,
  installationId: number,
  repository: string,
  readPermissions: readonly string[],
  writePermissions: readonly string[],
): Promise<InstallationAccessToken> {
  const combined = [...readPermissions, ...writePermissions];
  if (
    combined.length === 0 ||
    combined.length > WORKFLOW_TOKEN_PERMISSIONS.length ||
    new Set(combined).size !== combined.length ||
    readPermissions.some(
      (permission) => !workflowTokenPermissions.has(permission),
    ) ||
    writePermissions.some(
      (permission) => !workflowWritePermissions.has(permission),
    )
  ) {
    throw new Error(
      "workflow token permissions must be a nonempty disjoint set of supported read/write scopes",
    );
  }
  return createInstallationAccessToken(
    env,
    installationId,
    repository,
    Object.fromEntries([
      ...readPermissions.map(
        (permission) => [permission.replaceAll("-", "_"), "read"] as const,
      ),
      ...writePermissions.map(
        (permission) => [permission.replaceAll("-", "_"), "write"] as const,
      ),
    ]),
  );
}

export async function createSharedRepositoryToken(
  env: GitHubEnvironment,
  installationId: number,
  callerOwner: string,
  callerRepository: string,
  callerOwnerType: string,
  targetOwner: string,
  targetRepository: string,
): Promise<InstallationAccessToken> {
  validateCrossRepositoryTarget(
    callerOwner,
    callerRepository,
    targetOwner,
    targetRepository,
  );
  const allowedLevels =
    callerOwnerType === "Organization"
      ? new Set(["organization", "enterprise"])
      : callerOwnerType === "User"
        ? new Set(["user"])
        : null;
  if (allowedLevels === null) {
    throw new Error("the caller repository owner type is unavailable");
  }

  const appJwt = await createAppJwt(env);
  const policyToken = await createInstallationTokenWithJwt(
    env,
    appJwt,
    installationId,
    targetRepository,
    { administration: "read" },
  );
  const repositoryPath = `/repos/${encodeURIComponent(targetOwner)}/${encodeURIComponent(targetRepository)}`;
  const access = repositoryActionsAccessSchema.parse(
    await githubRequest(
      env,
      policyToken,
      `${repositoryPath}/actions/permissions/access`,
      { method: "GET" },
    ),
  );
  if (!allowedLevels.has(access.access_level)) {
    throw new Error(
      `target repository Actions access policy '${access.access_level}' does not allow this caller`,
    );
  }
  return createInstallationAccessTokenWithJwt(
    env,
    appJwt,
    installationId,
    targetRepository,
    { contents: "read" },
  );
}

export async function createPrivateCheckoutToken(
  env: GitHubEnvironment,
  installationId: number,
  callerOwner: string,
  callerRepository: string,
  targetOwner: string,
  targetRepository: string,
): Promise<InstallationAccessToken> {
  validateCrossRepositoryTarget(
    callerOwner,
    callerRepository,
    targetOwner,
    targetRepository,
  );
  return createInstallationAccessToken(env, installationId, targetRepository, {
    contents: "read",
  });
}

export async function fetchPullRequestMergeSnapshot(
  env: GitHubEnvironment,
  installationId: number,
  owner: string,
  repository: string,
  pullRequestNumber: number,
  expectedHeadSha: string,
  expectedBaseSha: string,
): Promise<PullRequestMergeSnapshot> {
  if (!Number.isSafeInteger(pullRequestNumber) || pullRequestNumber <= 0) {
    throw new Error("a positive pull request number is required");
  }
  const token = await createInstallationToken(env, installationId, repository, {
    pull_requests: "read",
  });
  const repositoryPath = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}`;
  const pullRequest = pullRequestMergeSchema.parse(
    await githubRequest(
      env,
      token,
      `${repositoryPath}/pulls/${pullRequestNumber}`,
      { method: "GET" },
    ),
  );
  if (
    pullRequest.head.sha.toLowerCase() !== expectedHeadSha.toLowerCase() ||
    pullRequest.base.sha.toLowerCase() !== expectedBaseSha.toLowerCase()
  ) {
    return {
      status: "changed",
      current_head_sha: pullRequest.head.sha,
      current_base_sha: pullRequest.base.sha,
    };
  }
  if (pullRequest.merged && pullRequest.merge_commit_sha !== null) {
    return { status: "ready", merge_sha: pullRequest.merge_commit_sha };
  }
  if (pullRequest.mergeable === false) {
    return { status: "conflicted" };
  }
  if (pullRequest.mergeable === null || pullRequest.merge_commit_sha === null) {
    return { status: "pending" };
  }
  return { status: "ready", merge_sha: pullRequest.merge_commit_sha };
}

function validateCrossRepositoryTarget(
  callerOwner: string,
  callerRepository: string,
  targetOwner: string,
  targetRepository: string,
): void {
  if (
    callerOwner.toLowerCase() !== targetOwner.toLowerCase() ||
    callerRepository.toLowerCase() === targetRepository.toLowerCase()
  ) {
    throw new Error(
      "repository access requires a different repository under the caller owner",
    );
  }
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
  return (
    await createInstallationAccessToken(
      env,
      installationId,
      repository,
      permissions,
    )
  ).token;
}

async function createInstallationAccessToken(
  env: GitHubEnvironment,
  installationId: number,
  repository: string,
  permissions: Record<string, "read" | "write">,
): Promise<InstallationAccessToken> {
  if (!Number.isSafeInteger(installationId) || installationId <= 0) {
    throw new Error("a positive GitHub App installation ID is required");
  }
  const appJwt = await createAppJwt(env);
  return createInstallationAccessTokenWithJwt(
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
  return (
    await createInstallationAccessTokenWithJwt(
      env,
      appJwt,
      installationId,
      repository,
      permissions,
    )
  ).token;
}

async function createInstallationAccessTokenWithJwt(
  env: GitHubApiEnvironment,
  appJwt: string,
  installationId: number,
  repository: string,
  permissions: Record<string, "read" | "write">,
): Promise<InstallationAccessToken> {
  if (!Number.isSafeInteger(installationId) || installationId <= 0) {
    throw new Error("a positive GitHub App installation ID is required");
  }
  const response = installationAccessTokenSchema.parse(
    await githubRequest(
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
    ),
  );
  const expiresAtEpochSeconds = Math.floor(
    Date.parse(response.expires_at) / 1_000,
  );
  if (
    !Number.isSafeInteger(expiresAtEpochSeconds) ||
    expiresAtEpochSeconds <= 0
  ) {
    throw new Error("GitHub returned an invalid installation token expiry");
  }
  return { token: response.token, expiresAtEpochSeconds };
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

function jobExecutionSha(job: QueuedJob): string {
  const mergeSha = job.pull_request.merge_sha;
  if (mergeSha === null) {
    throw new Error("pull request merge snapshot is not initialized");
  }
  return mergeSha;
}

function isGitZeroDeploymentPayload(
  payload: unknown,
  jobId: string,
  unitId: string,
): boolean {
  if (!payload || typeof payload !== "object") return false;
  const gitzero = Reflect.get(payload, "gitzero");
  return (
    gitzero !== null &&
    typeof gitzero === "object" &&
    Reflect.get(gitzero, "job_id") === jobId &&
    Reflect.get(gitzero, "unit_id") === unitId
  );
}

function deploymentStatusDescription(state: GitHubDeploymentState): string {
  switch (state) {
    case "in_progress":
      return "GitZero deployment is running.";
    case "success":
      return "GitZero deployment completed successfully.";
    case "failure":
      return "GitZero deployment failed.";
    case "error":
      return "GitZero deployment was cancelled or timed out.";
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
