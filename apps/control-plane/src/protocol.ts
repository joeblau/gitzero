import { z } from "zod";
import { fullGitObjectIdSchema } from "./git";

export const PROTOCOL_VERSION = 14;

export const repositoryTokenPurposeSchema = z.enum([
  "source",
  "environment",
  "shared_source",
  "checkout",
]);

export const WORKFLOW_TOKEN_PERMISSIONS = [
  "actions",
  "artifact-metadata",
  "attestations",
  "checks",
  "code-quality",
  "contents",
  "deployments",
  "discussions",
  "issues",
  "packages",
  "pages",
  "pull-requests",
  "security-events",
  "statuses",
  "vulnerability-alerts",
] as const;
export const WORKFLOW_WRITE_PERMISSIONS = [
  "actions",
  "artifact-metadata",
  "attestations",
  "checks",
  "code-quality",
  "contents",
  "deployments",
  "discussions",
  "issues",
  "packages",
  "pages",
  "pull-requests",
  "security-events",
  "statuses",
] as const;
const workflowTokenPermission = z.enum(WORKFLOW_TOKEN_PERMISSIONS);
const workflowWritePermission = z.enum(WORKFLOW_WRITE_PERMISSIONS);
const workflowReadPermissions = z
  .array(workflowTokenPermission)
  .max(WORKFLOW_TOKEN_PERMISSIONS.length)
  .refine((permissions) => new Set(permissions).size === permissions.length, {
    message: "workflow read permissions must be unique",
  });
const workflowWritePermissions = z
  .array(workflowWritePermission)
  .max(WORKFLOW_WRITE_PERMISSIONS.length)
  .refine((permissions) => new Set(permissions).size === permissions.length, {
    message: "workflow write permissions must be unique",
  });
const workflowTokenRequestSchema = z
  .object({
    type: z.literal("workflow_token_request"),
    message_id: z.string().uuid(),
    job_id: z.string().uuid(),
    request_id: z.string().uuid(),
    read_permissions: workflowReadPermissions,
    write_permissions: workflowWritePermissions,
  })
  .refine(
    ({ read_permissions, write_permissions }) => {
      const combined = [...read_permissions, ...write_permissions];
      return (
        combined.length > 0 &&
        combined.length <= WORKFLOW_TOKEN_PERMISSIONS.length &&
        new Set(combined).size === combined.length
      );
    },
    { message: "workflow token permissions must be nonempty and disjoint" },
  );

const uuid = z.string().uuid();
const tokenExpiryEpochSeconds = z
  .number()
  .int()
  .positive()
  .max(Number.MAX_SAFE_INTEGER);
const gitExecutionRef = z
  .string()
  .min(1)
  .refine(
    (value) =>
      value.startsWith("refs/") &&
      new TextEncoder().encode(value).byteLength <= 4_096 &&
      !Array.from(value).some(
        (character) =>
          character <= " " ||
          character === "\u007f" ||
          "~^:?*[\\".includes(character),
      ) &&
      !value.includes("..") &&
      !value.includes("@{") &&
      !value.includes("//") &&
      !value.endsWith(".") &&
      !value.endsWith("/") &&
      value
        .split("/")
        .every(
          (component) =>
            component.length > 0 &&
            !component.startsWith(".") &&
            !component.endsWith(".lock"),
        ),
    { message: "execution ref must be a valid fully qualified Git ref" },
  );
const repositoryComponent = z
  .string()
  .min(1)
  .max(100)
  .regex(/^[A-Za-z0-9._-]+$/);
const deploymentUnitId = z
  .string()
  .trim()
  .min(1)
  .max(512)
  .refine((value) => !value.includes("\0") && !/[\r\n]/.test(value));
const deploymentEnvironment = z
  .string()
  .trim()
  .min(1)
  .max(255)
  .refine((value) => !value.includes("\0") && !/[\r\n]/.test(value));
const deploymentEnvironmentUrl = z
  .url()
  .max(2_048)
  .refine((value) => ["http:", "https:"].includes(new URL(value).protocol));

export const repositorySchema = z.object({
  owner: z.string().min(1),
  name: z.string().min(1),
  clone_url: z
    .url()
    .refine((value) => new URL(value).hostname === "github.com", {
      error: "clone_url must use github.com",
    }),
});

const pullRequestShape = {
  number: z.number().int().positive(),
  action: z.string().min(1),
  head_sha: fullGitObjectIdSchema,
  base_sha: fullGitObjectIdSchema,
  merge_sha: fullGitObjectIdSchema.nullable().default(null),
  execution_ref: gitExecutionRef.nullable().default(null),
  head_ref: z.string().min(1),
  base_ref: z.string().min(1),
};
const objectIdsUseOneFormat = (snapshot: {
  head_sha: string;
  base_sha: string;
  merge_sha: string | null;
}) =>
  snapshot.base_sha.length === snapshot.head_sha.length &&
  (snapshot.merge_sha === null ||
    snapshot.merge_sha.length === snapshot.head_sha.length);
const objectFormatIssue = {
  message: "pull request object IDs must use one Git object format",
};

export const pullRequestSchema = z
  .object(pullRequestShape)
  .refine(objectIdsUseOneFormat, objectFormatIssue);

export const queuedJobSchema = z.object({
  id: uuid,
  workspace_id: z.string().min(1),
  installation_id: z.number().int().nonnegative(),
  run_number: z.number().int().nonnegative().default(0),
  repository: repositorySchema,
  pull_request: pullRequestSchema,
  check_run_id: z.number().int().positive().nullable(),
  event: z.json().default({}),
  environment: z.record(z.string(), z.string()).default({}),
  variables: z.record(z.string(), z.string()).default({}),
  requires_github_token: z.boolean().default(true),
  report_to_github: z.boolean().default(true),
});

export type QueuedJob = z.infer<typeof queuedJobSchema>;

export const runSpecSchema = queuedJobSchema
  .omit({
    requires_github_token: true,
    report_to_github: true,
  })
  .extend({
    pull_request: z
      .object({
        ...pullRequestShape,
        merge_sha: fullGitObjectIdSchema,
        execution_ref: gitExecutionRef,
      })
      .refine(objectIdsUseOneFormat, objectFormatIssue),
    check_run_id: z.number().int().positive().nullable().optional(),
    checkout_token: z.string(),
    checkout_token_expires_at_epoch_seconds: tokenExpiryEpochSeconds
      .nullable()
      .default(null),
    github_api_version: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    managed_secrets: z.boolean().default(false),
    changed_paths: z.array(z.string().min(1).max(4_096)).max(3_000).optional(),
  })
  .superRefine((run, context) => {
    if (
      (run.checkout_token.length === 0) !==
      (run.checkout_token_expires_at_epoch_seconds === null)
    ) {
      context.addIssue({
        code: "custom",
        path: ["checkout_token_expires_at_epoch_seconds"],
        message:
          "checkout token and expiry must either both be present or absent",
      });
    }
  });

export type RunSpec = z.infer<typeof runSpecSchema>;

const conclusionSchema = z.enum([
  "success",
  "failure",
  "cancelled",
  "timed_out",
  "neutral",
]);

const annotationCoordinate = z.number().int().min(1).max(2_147_483_647);
const utf8Bytes = (value: string): number =>
  new TextEncoder().encode(value).byteLength;
export const checkAnnotationSchema = z
  .object({
    path: z
      .string()
      .min(1)
      .refine((value) => utf8Bytes(value) <= 4_096)
      .refine(
        (value) =>
          !value.startsWith("/") &&
          !/^[A-Za-z]:[\\/]/.test(value) &&
          !value.includes("\0") &&
          !/[\r\n]/.test(value) &&
          value
            .split(/[\\/]/)
            .every((component) => component !== ".." && component !== ""),
        { message: "annotation path must be repository relative" },
      ),
    start_line: annotationCoordinate,
    end_line: annotationCoordinate,
    start_column: annotationCoordinate.nullable(),
    end_column: annotationCoordinate.nullable(),
    annotation_level: z.enum(["notice", "warning", "failure"]),
    message: z
      .string()
      .min(1)
      .refine((value) => utf8Bytes(value) <= 65_536),
    title: z
      .string()
      .refine((value) => utf8Bytes(value) <= 255)
      .nullable(),
  })
  .superRefine((annotation, context) => {
    if (annotation.end_line < annotation.start_line) {
      context.addIssue({
        code: "custom",
        path: ["end_line"],
        message: "annotation end line precedes its start line",
      });
    }
    if (
      (annotation.start_column === null) !==
      (annotation.end_column === null)
    ) {
      context.addIssue({
        code: "custom",
        path: ["start_column"],
        message: "annotation columns must be both present or both absent",
      });
    }
    if (
      annotation.start_line !== annotation.end_line &&
      (annotation.start_column !== null || annotation.end_column !== null)
    ) {
      context.addIssue({
        code: "custom",
        path: ["start_column"],
        message: "multi-line annotations cannot include columns",
      });
    }
    if (
      annotation.start_column !== null &&
      annotation.end_column !== null &&
      annotation.end_column < annotation.start_column
    ) {
      context.addIssue({
        code: "custom",
        path: ["end_column"],
        message: "annotation end column precedes its start column",
      });
    }
  });

export type CheckAnnotation = z.infer<typeof checkAnnotationSchema>;

const runnerSelectorSchema = z
  .string()
  .trim()
  .min(1)
  .max(256)
  .refine((value) => !value.includes("\0") && !/[\r\n]/.test(value));

export const runnerRequirementSchema = z.object({
  labels: z.array(runnerSelectorSchema).max(32),
  runner_group: runnerSelectorSchema.nullable().default(null),
});

export type RunnerRequirement = z.infer<typeof runnerRequirementSchema>;
export const runnerRequirementsSchema = z
  .array(runnerRequirementSchema)
  .max(512);

export const concurrencyQueueSchema = z.enum(["single", "max"]);

export const agentMessageSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("hello"),
    hello: z.object({
      protocol_version: z.number().int(),
      agent_id: z.string().min(1),
      name: z.string().min(1),
      version: z.string().min(1),
      labels: z.array(runnerSelectorSchema).max(32),
      runner_group: runnerSelectorSchema.nullable().default(null),
      max_parallelism: z.number().int().min(1).max(64),
    }),
  }),
  z.object({
    type: z.literal("heartbeat"),
    message_id: uuid,
    running_job_ids: z.array(uuid).max(64),
  }),
  z.object({
    type: z.literal("job_started"),
    message_id: uuid,
    job_id: uuid,
  }),
  z.object({
    type: z.literal("job_rejected"),
    message_id: uuid,
    job_id: uuid,
    requirements: runnerRequirementsSchema.min(1),
    reason: z.string().min(1).max(4_096),
  }),
  z.object({
    type: z.literal("concurrency_acquire"),
    message_id: uuid,
    job_id: uuid,
    request_id: uuid,
    unit_id: z.string().trim().min(1).max(512),
    group: z
      .string()
      .trim()
      .min(1)
      .max(256)
      .refine((value) => !value.includes("\0") && !/[\r\n]/.test(value)),
    cancel_in_progress: z.boolean(),
    queue: concurrencyQueueSchema,
  }),
  z.object({
    type: z.literal("concurrency_release"),
    message_id: uuid,
    job_id: uuid,
    request_id: uuid,
  }),
  z.object({
    type: z.literal("repository_token_request"),
    message_id: uuid,
    job_id: uuid,
    request_id: uuid,
    purpose: repositoryTokenPurposeSchema,
    owner: repositoryComponent,
    repository: repositoryComponent,
  }),
  workflowTokenRequestSchema,
  z.object({
    type: z.literal("secret_request"),
    message_id: uuid,
    job_id: uuid,
    request_id: uuid,
    unit_id: deploymentUnitId,
    environment: deploymentEnvironment.nullable(),
  }),
  z.object({
    type: z.literal("deployment_started"),
    message_id: uuid,
    job_id: uuid,
    unit_id: deploymentUnitId,
    environment: deploymentEnvironment,
  }),
  z.object({
    type: z.literal("deployment_finished"),
    message_id: uuid,
    job_id: uuid,
    unit_id: deploymentUnitId,
    conclusion: z.enum(["success", "failure", "cancelled", "timed_out"]),
    environment_url: deploymentEnvironmentUrl.nullable(),
  }),
  z.object({
    type: z.literal("step_started"),
    message_id: uuid,
    job_id: uuid,
    step_id: z.string().min(1).max(256),
    name: z.string().min(1).max(512),
  }),
  z.object({
    type: z.literal("log_chunk"),
    message_id: uuid,
    job_id: uuid,
    step_id: z.string().min(1).max(256),
    sequence: z.number().int().nonnegative(),
    stream: z.enum(["stdout", "stderr", "system"]),
    data: z.string().max(65_536),
  }),
  z.object({
    type: z.literal("step_finished"),
    message_id: uuid,
    job_id: uuid,
    step_id: z.string().min(1).max(256),
    conclusion: conclusionSchema,
    exit_code: z.number().int().nullable(),
  }),
  z.object({
    type: z.literal("job_finished"),
    message_id: uuid,
    job_id: uuid,
    conclusion: conclusionSchema,
    summary: z.string().max(65_536),
    annotations: z.array(checkAnnotationSchema).max(50).default([]),
  }),
]);

export type AgentMessage = z.infer<typeof agentMessageSchema>;
export type AgentHello = Extract<AgentMessage, { type: "hello" }>["hello"];
export type Conclusion = z.infer<typeof conclusionSchema>;

export type ServerMessage =
  | {
      type: "welcome";
      protocol_version: number;
      heartbeat_interval_seconds: number;
    }
  | { type: "run_job"; job: RunSpec }
  | { type: "cancel_job"; job_id: string; reason: string }
  | { type: "concurrency_granted"; request_id: string }
  | { type: "concurrency_cancelled"; request_id: string; reason: string }
  | {
      type: "repository_token_granted";
      request_id: string;
      token: string;
      expires_at_epoch_seconds: number;
    }
  | { type: "repository_token_denied"; request_id: string; reason: string }
  | {
      type: "workflow_token_granted";
      request_id: string;
      token: string;
      expires_at_epoch_seconds: number;
    }
  | { type: "workflow_token_denied"; request_id: string; reason: string }
  | {
      type: "secret_granted";
      request_id: string;
      secrets: Record<string, string>;
    }
  | { type: "secret_denied"; request_id: string; reason: string }
  | { type: "ack"; message_id: string }
  | { type: "error"; code: string; message: string };

export interface SocketAttachment {
  role: "agent" | "observer";
  agentId?: string;
  hello?: AgentHello;
  drainingJobIds?: string[];
  connectedAt: number;
  lastSeenAt: number;
}

export interface ObserverEvent {
  type: string;
  workspace_id: string;
  timestamp: string;
  data: Record<string, unknown>;
}
