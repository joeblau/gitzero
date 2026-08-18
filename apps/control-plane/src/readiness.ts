import { z } from "zod";
import type {
  RepositoryActionsPermissions,
  RepositoryCheckRun,
} from "./github";
import { fullGitObjectIdSchema } from "./git";

const snapshotSchema = z.object({
  agents: z.array(
    z.object({
      agent_id: z.string().min(1),
      status: z.literal("online"),
      labels: z.array(z.string()),
      available_capacity: z.number().int().nonnegative(),
    }),
  ),
  jobs: z.array(
    z.object({
      id: z.string().min(1),
      repository: z.string().min(1),
      head_sha: fullGitObjectIdSchema,
      status: z.enum(["queued", "assigned", "running", "completed"]),
      conclusion: z
        .enum(["success", "failure", "cancelled", "timed_out", "neutral"])
        .nullable(),
      check_run_id: z.number().int().positive().nullable(),
      completed_at: z.string().nullable(),
    }),
  ),
});

export type OnboardingNextAction =
  | "connect_compatible_agent"
  | "run_test_pull_request"
  | "wait_for_check_sync"
  | "disable_native_actions"
  | "complete";

export interface SuccessfulCheckCandidate {
  job_id: string;
  check_run_id: number;
  head_sha: string;
  completed_at: string;
}

export interface RepositoryReadiness {
  installation_id: number;
  repository: string;
  native_actions: RepositoryActionsPermissions;
  compatible_agents_online: number;
  available_capacity: number;
  successful_check: {
    verified: boolean;
    job_id: string | null;
    check_run_id: number | null;
    head_sha: string | null;
    completed_at: string | null;
    github_status: string | null;
    github_conclusion: string | null;
  };
  safe_to_disable_native_actions: boolean;
  onboarded: boolean;
  next_action: OnboardingNextAction;
}

export function buildRepositoryReadiness(
  installationId: number,
  owner: string,
  repository: string,
  permissions: RepositoryActionsPermissions,
  snapshotInput: unknown,
  checkRun: RepositoryCheckRun | null,
): RepositoryReadiness {
  const snapshot = snapshotSchema.parse(snapshotInput);
  const repositoryName = `${owner}/${repository}`;
  const compatibleAgents = snapshot.agents.filter((agent) =>
    agent.labels.some((label) => label.toLowerCase() === "macos"),
  );
  const candidate = findSuccessfulCheckCandidateFromSnapshot(
    owner,
    repository,
    snapshot,
  );
  const hasCompatibleAgent = compatibleAgents.length > 0;
  const hasSuccessfulCheck =
    candidate !== null &&
    checkRun !== null &&
    checkRun.id === candidate.check_run_id &&
    checkRun.name === "GitZero" &&
    checkRun.external_id === candidate.job_id &&
    checkRun.head_sha.toLowerCase() === candidate.head_sha.toLowerCase() &&
    checkRun.status === "completed" &&
    checkRun.conclusion === "success";
  const safeToDisable =
    permissions.enabled && hasCompatibleAgent && hasSuccessfulCheck;
  const onboarded =
    !permissions.enabled && hasCompatibleAgent && hasSuccessfulCheck;

  let nextAction: OnboardingNextAction;
  if (!hasCompatibleAgent) {
    nextAction = "connect_compatible_agent";
  } else if (candidate === null) {
    nextAction = "run_test_pull_request";
  } else if (!hasSuccessfulCheck) {
    nextAction = "wait_for_check_sync";
  } else if (permissions.enabled) {
    nextAction = "disable_native_actions";
  } else {
    nextAction = "complete";
  }

  return {
    installation_id: installationId,
    repository: repositoryName,
    native_actions: permissions,
    compatible_agents_online: compatibleAgents.length,
    available_capacity: compatibleAgents.reduce(
      (total, agent) => total + agent.available_capacity,
      0,
    ),
    successful_check: {
      verified: hasSuccessfulCheck,
      job_id: candidate?.job_id ?? null,
      check_run_id: candidate?.check_run_id ?? null,
      head_sha: candidate?.head_sha ?? null,
      completed_at: candidate?.completed_at ?? null,
      github_status: checkRun?.status ?? null,
      github_conclusion: checkRun?.conclusion ?? null,
    },
    safe_to_disable_native_actions: safeToDisable,
    onboarded,
    next_action: nextAction,
  };
}

export function findSuccessfulCheckCandidate(
  owner: string,
  repository: string,
  snapshotInput: unknown,
): SuccessfulCheckCandidate | null {
  return findSuccessfulCheckCandidateFromSnapshot(
    owner,
    repository,
    snapshotSchema.parse(snapshotInput),
  );
}

function findSuccessfulCheckCandidateFromSnapshot(
  owner: string,
  repository: string,
  snapshot: z.infer<typeof snapshotSchema>,
): SuccessfulCheckCandidate | null {
  const matchingRepository = `${owner}/${repository}`.toLowerCase();
  const job = snapshot.jobs.find(
    (candidate) =>
      candidate.repository.toLowerCase() === matchingRepository &&
      candidate.status === "completed" &&
      candidate.conclusion === "success" &&
      candidate.check_run_id !== null &&
      candidate.completed_at !== null,
  );
  if (
    job?.check_run_id === null ||
    job?.check_run_id === undefined ||
    job.completed_at === null
  ) {
    return null;
  }
  return {
    job_id: job.id,
    check_run_id: job.check_run_id,
    head_sha: job.head_sha,
    completed_at: job.completed_at,
  };
}
