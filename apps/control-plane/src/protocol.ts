import { z } from "zod";

export const PROTOCOL_VERSION = 3;

const uuid = z.string().uuid();
const sha = z.string().regex(/^[0-9a-fA-F]{40}$/);

export const repositorySchema = z.object({
  owner: z.string().min(1),
  name: z.string().min(1),
  clone_url: z
    .url()
    .refine((value) => new URL(value).hostname === "github.com", {
      error: "clone_url must use github.com",
    }),
});

export const pullRequestSchema = z.object({
  number: z.number().int().positive(),
  action: z.string().min(1),
  head_sha: sha,
  base_sha: sha,
  head_ref: z.string().min(1),
  base_ref: z.string().min(1),
});

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
    check_run_id: z.number().int().positive().nullable().optional(),
    checkout_token: z.string(),
    environment_token: z.string().default(""),
    github_api_version: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    changed_paths: z.array(z.string().min(1).max(4_096)).max(3_000).optional(),
  });

export type RunSpec = z.infer<typeof runSpecSchema>;

const conclusionSchema = z.enum([
  "success",
  "failure",
  "cancelled",
  "timed_out",
  "neutral",
]);

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
