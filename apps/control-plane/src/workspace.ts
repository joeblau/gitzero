import { DurableObject } from "cloudflare:workers";
import {
  createAgentToken,
  createCheckRun,
  createEnvironmentToken,
  createPrivateCheckoutToken,
  createSharedRepositoryToken,
  createWorkflowToken,
  fetchActionsVariables,
  fetchPullRequestMergeSnapshot,
  updateCheckRun,
  type InstallationAccessToken,
} from "./github";
import {
  PROTOCOL_VERSION,
  agentMessageSchema,
  checkAnnotationSchema,
  queuedJobSchema,
  runnerRequirementsSchema,
  runSpecSchema,
  type AgentHello,
  type AgentMessage,
  type CheckAnnotation,
  type Conclusion,
  type ObserverEvent,
  type QueuedJob,
  type RunSpec,
  type RunnerRequirement,
  type ServerMessage,
  type SocketAttachment,
} from "./protocol";

const LEASE_MILLISECONDS = 60_000;
const CHECK_RETRY_MILLISECONDS = 30_000;
const QUEUE_TIMEOUT_MILLISECONDS = 24 * 60 * 60 * 1_000;
const RETENTION_MILLISECONDS = 30 * 24 * 60 * 60 * 1_000;
const MAX_ASSIGNMENT_ATTEMPTS = 3;
const MAX_INITIALIZATION_ATTEMPTS = 8;
const INITIALIZATION_LEASE_MILLISECONDS = 2 * 60 * 1_000;
const INITIALIZATION_RETRY_BASE_MILLISECONDS = 1_000;
const INITIALIZATION_RETRY_MAX_MILLISECONDS = 5 * 60 * 1_000;
const EVENT_CHUNK_CHARACTERS = 256 * 1024;

type JobStatus = "queued" | "assigned" | "running" | "completed";
type ConcurrencyStatus = "waiting" | "active" | "cancelling";

interface JobRow extends Record<string, SqlStorageValue> {
  id: string;
  job_json: string;
  status: JobStatus;
  agent_id: string | null;
  lease_expires_at: number | null;
  created_at: number;
  started_at: number | null;
  completed_at: number | null;
  conclusion: Conclusion | null;
  summary: string | null;
  check_sync_needed: number;
  attempt_count: number;
  queue_expires_at: number;
  last_error: string | null;
  initialization_needed: 0 | 1 | 2;
  initialization_attempt_count: number;
  initialization_retry_at: number | null;
  runner_requirements_json: string;
}

interface CountRow extends Record<string, SqlStorageValue> {
  count: number;
}

interface MinimumRow extends Record<string, SqlStorageValue> {
  value: number | null;
}

interface ColumnRow extends Record<string, SqlStorageValue> {
  name: string;
}

interface AnnotationRow extends Record<string, SqlStorageValue> {
  annotation_json: string;
}

interface ActiveJobRow extends Record<string, SqlStorageValue> {
  id: string;
  status: "assigned" | "running";
  lease_expires_at: number;
}

interface ConcurrencyRow extends Record<string, SqlStorageValue> {
  sequence: number;
  request_id: string;
  run_id: string;
  agent_id: string;
  repository_key: string;
  group_key: string;
  group_name: string;
  unit_id: string;
  queue_mode: "single" | "max";
  status: ConcurrencyStatus;
  created_at: number;
}

interface AgentCandidate {
  socket: WebSocket;
  hello: AgentHello;
  agentId: string;
  assigned: number;
  capacity: number;
  connectedAt: number;
}

export class Workspace extends DurableObject<Cloudflare.Env> {
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => this.migrate());
  }

  async enqueue(
    input: QueuedJob,
    deliveryId: string,
  ): Promise<{ duplicate: boolean; job_id: string }> {
    const job = queuedJobSchema.parse(input);
    this.ensureWorkspaceId(job.workspace_id);
    this.prune(Date.now());
    const inserted = this.ctx.storage.sql
      .exec<{ delivery_id: string }>(
        `INSERT INTO deliveries (delivery_id, job_id, received_at)
         VALUES (?, ?, ?)
         ON CONFLICT (delivery_id) DO NOTHING
         RETURNING delivery_id`,
        deliveryId,
        job.id,
        Date.now(),
      )
      .toArray();
    if (inserted.length === 0) {
      const existing = this.ctx.storage.sql
        .exec<{
          job_id: string;
        }>("SELECT job_id FROM deliveries WHERE delivery_id = ?", deliveryId)
        .one();
      return { duplicate: true, job_id: existing.job_id };
    }

    const persistedJob = {
      ...job,
      run_number: this.nextRunNumber(),
      event: {},
    } satisfies QueuedJob;
    const needsInitialization =
      job.requires_github_token || job.report_to_github;
    const now = Date.now();
    this.ctx.storage.sql.exec(
      `INSERT INTO jobs (
        id, job_json, status, agent_id, lease_expires_at, created_at,
        started_at, completed_at, conclusion, summary, check_sync_needed,
        attempt_count, queue_expires_at, last_error, initialization_needed,
        initialization_attempt_count, initialization_retry_at,
        runner_requirements_json
      ) VALUES (?, ?, 'queued', NULL, NULL, ?, NULL, NULL, NULL, NULL, 0, 0, ?, NULL, ?, 0, ?, '[]')`,
      job.id,
      JSON.stringify(persistedJob),
      now,
      now + QUEUE_TIMEOUT_MILLISECONDS,
      needsInitialization ? 1 : 0,
      needsInitialization ? now : null,
    );
    this.persistEvent(job.id, job.event);

    this.broadcast("job_queued", { job: publicJob(persistedJob) });
    if (needsInitialization) {
      this.ctx.waitUntil(this.initializeJob(job.id));
    } else {
      await this.tryDispatch();
    }
    await this.scheduleAlarm();
    return { duplicate: false, job_id: job.id };
  }

  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return Response.json(
        { error: "websocket_upgrade_required" },
        { status: 426 },
      );
    }
    const url = new URL(request.url);
    const workspaceId = workspaceIdFromPath(url.pathname);
    this.ensureWorkspaceId(workspaceId);
    const role = url.searchParams.get("role");
    if (role !== "agent" && role !== "observer") {
      return Response.json({ error: "invalid_role" }, { status: 400 });
    }
    const agentId =
      role === "agent"
        ? (url.searchParams.get("agent_id") ?? undefined)
        : undefined;
    if (role === "agent" && !agentId) {
      return Response.json({ error: "missing_agent_id" }, { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server, [
      `role:${role}`,
      ...(agentId ? [`agent:${agentId}`] : []),
    ]);
    const now = Date.now();
    server.serializeAttachment({
      role,
      agentId,
      drainingJobIds: [],
      connectedAt: now,
      lastSeenAt: now,
    } satisfies SocketAttachment);

    if (role === "observer") {
      server.send(
        JSON.stringify({
          type: "snapshot",
          workspace_id: workspaceId,
          timestamp: new Date().toISOString(),
          data: this.getSnapshot(),
        } satisfies ObserverEvent),
      );
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  override async webSocketMessage(
    socket: WebSocket,
    rawMessage: string | ArrayBuffer,
  ): Promise<void> {
    const attachment = readAttachment(socket);
    if (attachment.role !== "agent") {
      sendServer(socket, {
        type: "error",
        code: "read_only",
        message: "Observers cannot send messages.",
      });
      return;
    }
    if (typeof rawMessage !== "string") {
      sendServer(socket, {
        type: "error",
        code: "invalid_message",
        message: "Binary messages are not supported.",
      });
      return;
    }

    let message: AgentMessage;
    try {
      message = agentMessageSchema.parse(JSON.parse(rawMessage));
    } catch {
      sendServer(socket, {
        type: "error",
        code: "invalid_message",
        message: "Message validation failed.",
      });
      return;
    }
    attachment.lastSeenAt = Date.now();

    if (message.type === "hello") {
      if (message.hello.agent_id !== attachment.agentId) {
        socket.close(1008, "agent ID does not match connection");
        return;
      }
      if (message.hello.protocol_version !== PROTOCOL_VERSION) {
        sendServer(socket, {
          type: "error",
          code: "protocol_mismatch",
          message: `Control plane requires protocol ${PROTOCOL_VERSION}.`,
        });
        socket.close(1002, "protocol mismatch");
        return;
      }
      const duplicate = this.ctx
        .getWebSockets(`agent:${message.hello.agent_id}`)
        .some(
          (candidate) =>
            candidate !== socket &&
            candidate.readyState === WebSocket.OPEN &&
            readAttachment(candidate).hello !== undefined,
        );
      if (duplicate) {
        sendServer(socket, {
          type: "error",
          code: "duplicate_agent_id",
          message: "Another agent is already connected with this agent ID.",
        });
        socket.close(1008, "duplicate agent ID");
        return;
      }
      attachment.hello = message.hello;
      attachment.drainingJobIds = [];
      socket.serializeAttachment(attachment);
      sendServer(socket, {
        type: "welcome",
        protocol_version: PROTOCOL_VERSION,
        heartbeat_interval_seconds: 15,
      });
      this.broadcast("agent_online", { agent: this.agentStatus(socket) });
      await this.tryDispatch();
      return;
    }
    if (!attachment.hello || !attachment.agentId) {
      sendServer(socket, {
        type: "error",
        code: "hello_required",
        message: "Send hello before job events.",
      });
      return;
    }
    socket.serializeAttachment(attachment);

    if (!this.reserveMessage(message.message_id)) {
      sendServer(socket, { type: "ack", message_id: message.message_id });
      return;
    }

    await this.handleAgentEvent(socket, attachment.agentId, message);
    sendServer(socket, { type: "ack", message_id: message.message_id });
  }

  override async webSocketClose(
    socket: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean,
  ): Promise<void> {
    const attachment = readAttachment(socket);
    if (attachment.role === "agent" && attachment.agentId && attachment.hello) {
      await this.handleAgentDisconnect(
        attachment.agentId,
        code,
        reason,
        wasClean,
      );
    }
  }

  override async webSocketError(
    socket: WebSocket,
    error: unknown,
  ): Promise<void> {
    const attachment = readAttachment(socket);
    console.error(
      JSON.stringify({
        message: "workspace websocket error",
        role: attachment.role,
        error: String(error),
      }),
    );
    if (attachment.role === "agent" && attachment.agentId && attachment.hello) {
      await this.handleAgentDisconnect(
        attachment.agentId,
        1011,
        "websocket error",
        false,
      );
    }
  }

  override async alarm(): Promise<void> {
    const initializationDueAt = Date.now();
    const initializationDue = this.ctx.storage.sql
      .exec<{ id: string }>(
        `SELECT id FROM jobs
         WHERE status = 'queued' AND initialization_needed != 0
           AND initialization_retry_at IS NOT NULL AND initialization_retry_at <= ?
         ORDER BY initialization_retry_at LIMIT 5`,
        initializationDueAt,
      )
      .toArray();
    for (const row of initializationDue) {
      await this.initializeJob(row.id);
    }

    const now = Date.now();
    const expired = this.ctx.storage.sql
      .exec<JobRow>(
        `SELECT * FROM jobs
         WHERE status IN ('assigned', 'running') AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`,
        now,
      )
      .toArray();
    for (const row of expired) {
      this.cancelOnAgent(row, "agent lease expired");
      this.requeue(row.id, "agent lease expired");
    }

    const queueExpired = this.ctx.storage.sql
      .exec<JobRow>(
        `SELECT * FROM jobs
         WHERE status = 'queued' AND queue_expires_at <= ?`,
        now,
      )
      .toArray();
    for (const row of queueExpired) {
      this.completeInfrastructureJob(
        row,
        "timed_out",
        "No compatible GitZero agent became available before the queue deadline.",
      );
    }

    const unsynced = this.ctx.storage.sql
      .exec<JobRow>(
        "SELECT * FROM jobs WHERE check_sync_needed = 1 ORDER BY completed_at LIMIT 20",
      )
      .toArray();
    for (const row of unsynced) {
      await this.syncCheck(row);
    }
    this.prune(now);
    await this.tryDispatch();
    await this.scheduleAlarm();
  }

  private migrate(): void {
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS deliveries (
        delivery_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        received_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        job_json TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('queued', 'assigned', 'running', 'completed')),
        agent_id TEXT,
        lease_expires_at INTEGER,
        created_at INTEGER NOT NULL,
        started_at INTEGER,
        completed_at INTEGER,
        conclusion TEXT,
        summary TEXT,
        check_sync_needed INTEGER NOT NULL DEFAULT 0,
        initialization_needed INTEGER NOT NULL DEFAULT 0
          CHECK (initialization_needed IN (0, 1, 2)),
        initialization_attempt_count INTEGER NOT NULL DEFAULT 0,
        initialization_retry_at INTEGER,
        runner_requirements_json TEXT NOT NULL DEFAULT '[]'
      );
      CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, created_at);
      CREATE INDEX IF NOT EXISTS jobs_agent ON jobs(agent_id, status);
      CREATE INDEX IF NOT EXISTS jobs_lease ON jobs(lease_expires_at) WHERE lease_expires_at IS NOT NULL;
      CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        received_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS job_event_chunks (
        job_id TEXT NOT NULL,
        chunk_index INTEGER NOT NULL,
        content TEXT NOT NULL,
        PRIMARY KEY (job_id, chunk_index)
      );
      CREATE TABLE IF NOT EXISTS job_annotations (
        job_id TEXT NOT NULL,
        annotation_index INTEGER NOT NULL,
        annotation_json TEXT NOT NULL,
        PRIMARY KEY (job_id, annotation_index)
      );
      CREATE TABLE IF NOT EXISTS concurrency_leases (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL UNIQUE,
        run_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        repository_key TEXT NOT NULL,
        group_key TEXT NOT NULL,
        group_name TEXT NOT NULL,
        unit_id TEXT NOT NULL,
        queue_mode TEXT NOT NULL CHECK (queue_mode IN ('single', 'max')),
        status TEXT NOT NULL CHECK (status IN ('waiting', 'active', 'cancelling')),
        created_at INTEGER NOT NULL
      );
      CREATE UNIQUE INDEX IF NOT EXISTS concurrency_active
        ON concurrency_leases(repository_key, group_key)
        WHERE status IN ('active', 'cancelling');
      CREATE INDEX IF NOT EXISTS concurrency_waiting
        ON concurrency_leases(repository_key, group_key, status, sequence);
      CREATE INDEX IF NOT EXISTS concurrency_run
        ON concurrency_leases(run_id);
      CREATE TABLE IF NOT EXISTS _sql_schema_migrations (
        id INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (1);
    `);

    const columns = new Set(
      this.ctx.storage.sql
        .exec<ColumnRow>("PRAGMA table_info(jobs)")
        .toArray()
        .map((column) => column.name),
    );
    if (!columns.has("attempt_count")) {
      this.ctx.storage.sql.exec(
        "ALTER TABLE jobs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0",
      );
    }
    if (!columns.has("queue_expires_at")) {
      this.ctx.storage.sql.exec(
        "ALTER TABLE jobs ADD COLUMN queue_expires_at INTEGER NOT NULL DEFAULT 0",
      );
      this.ctx.storage.sql.exec(
        "UPDATE jobs SET queue_expires_at = created_at + ? WHERE queue_expires_at = 0",
        QUEUE_TIMEOUT_MILLISECONDS,
      );
    }
    if (!columns.has("last_error")) {
      this.ctx.storage.sql.exec("ALTER TABLE jobs ADD COLUMN last_error TEXT");
    }
    if (!columns.has("initialization_needed")) {
      this.ctx.storage.sql.exec(
        `ALTER TABLE jobs ADD COLUMN initialization_needed INTEGER NOT NULL DEFAULT 0
         CHECK (initialization_needed IN (0, 1, 2))`,
      );
    }
    if (!columns.has("initialization_attempt_count")) {
      this.ctx.storage.sql.exec(
        "ALTER TABLE jobs ADD COLUMN initialization_attempt_count INTEGER NOT NULL DEFAULT 0",
      );
    }
    if (!columns.has("initialization_retry_at")) {
      this.ctx.storage.sql.exec(
        "ALTER TABLE jobs ADD COLUMN initialization_retry_at INTEGER",
      );
    }
    if (!columns.has("runner_requirements_json")) {
      this.ctx.storage.sql.exec(
        "ALTER TABLE jobs ADD COLUMN runner_requirements_json TEXT NOT NULL DEFAULT '[]'",
      );
    }
    this.ctx.storage.sql.exec(`
      CREATE INDEX IF NOT EXISTS jobs_queue_deadline ON jobs(status, queue_expires_at);
      CREATE INDEX IF NOT EXISTS jobs_completed ON jobs(completed_at) WHERE completed_at IS NOT NULL;
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (2);
      CREATE INDEX IF NOT EXISTS job_event_chunks_job ON job_event_chunks(job_id, chunk_index);
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (3);
      CREATE INDEX IF NOT EXISTS jobs_initialization ON jobs(initialization_retry_at)
        WHERE status = 'queued' AND initialization_needed != 0;
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (4);
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (5);
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (6);
      CREATE INDEX IF NOT EXISTS job_annotations_job
        ON job_annotations(job_id, annotation_index);
      INSERT OR IGNORE INTO _sql_schema_migrations (id) VALUES (7);
    `);
  }

  private async initializeJob(jobId: string): Promise<void> {
    const claim = this.ctx.storage.sql
      .exec<JobRow>(
        `UPDATE jobs
         SET initialization_needed = 2,
             initialization_attempt_count = initialization_attempt_count + 1,
             initialization_retry_at = ?
         WHERE id = ? AND status = 'queued' AND initialization_needed != 0
           AND initialization_retry_at IS NOT NULL AND initialization_retry_at <= ?
         RETURNING *`,
        Date.now() + INITIALIZATION_LEASE_MILLISECONDS,
        jobId,
        Date.now(),
      )
      .toArray()[0];
    if (!claim) return;

    const job = parseJob(claim.job_json);
    try {
      let initializedJob = job;
      if (initializedJob.pull_request.execution_ref === null) {
        initializedJob = {
          ...initializedJob,
          pull_request: {
            ...initializedJob.pull_request,
            execution_ref: `refs/pull/${initializedJob.pull_request.number}/merge`,
          },
        };
      }
      if (initializedJob.pull_request.merge_sha === null) {
        if (!initializedJob.requires_github_token) {
          initializedJob = {
            ...initializedJob,
            pull_request: {
              ...initializedJob.pull_request,
              merge_sha: initializedJob.pull_request.head_sha,
            },
          };
        } else {
          const snapshot = await fetchPullRequestMergeSnapshot(
            this.env,
            initializedJob.installation_id,
            initializedJob.repository.owner,
            initializedJob.repository.name,
            initializedJob.pull_request.number,
            initializedJob.pull_request.head_sha,
            initializedJob.pull_request.base_sha,
          );
          if (snapshot.status === "pending") {
            throw new Error(
              "GitHub is still computing the pull request merge snapshot",
            );
          }
          if (snapshot.status === "conflicted") {
            this.completeInitializingJob(
              claim,
              "neutral",
              "GitZero did not create a run because GitHub reports that the pull request cannot be merged into its base branch.",
            );
            return;
          }
          if (snapshot.status === "changed") {
            this.completeInitializingJob(
              claim,
              "failure",
              "GitZero could not pin the webhook's merge snapshot because the pull request head or base changed before GitHub finished computing it.",
            );
            return;
          }
          initializedJob = {
            ...initializedJob,
            pull_request: {
              ...initializedJob.pull_request,
              merge_sha: snapshot.merge_sha,
            },
          };
        }
      }
      const variables = initializedJob.requires_github_token
        ? await fetchActionsVariables(
            this.env,
            initializedJob.installation_id,
            initializedJob.repository.owner,
            initializedJob.repository.name,
            eventRepositoryOwnerType(this.eventPayload(initializedJob.id)) ===
              "Organization",
          )
        : initializedJob.variables;
      initializedJob = { ...initializedJob, variables };
      if (initializedJob.report_to_github) {
        const checkRunId = await createCheckRun(
          this.env,
          initializedJob,
          claim.initialization_attempt_count > 1,
        );
        initializedJob = { ...initializedJob, check_run_id: checkRunId };
      }
      const updated = this.ctx.storage.sql
        .exec<{ id: string }>(
          `UPDATE jobs SET job_json = ?, initialization_needed = 0,
             initialization_retry_at = NULL, last_error = NULL
           WHERE id = ? AND status = 'queued' AND initialization_needed = 2
           RETURNING id`,
          JSON.stringify(initializedJob),
          job.id,
        )
        .toArray();
      if (updated.length === 0) return;
      this.broadcast("job_initialized", { job: publicJob(initializedJob) });
      await this.tryDispatch();
    } catch (error) {
      const detail = truncateDiagnostic(error);
      if (claim.initialization_attempt_count >= MAX_INITIALIZATION_ATTEMPTS) {
        this.ctx.storage.sql.exec(
          `UPDATE jobs SET initialization_needed = 0,
             initialization_retry_at = NULL, last_error = ?
           WHERE id = ? AND status = 'queued' AND initialization_needed = 2`,
          detail,
          job.id,
        );
        const terminal = this.jobRow(job.id);
        this.completeInfrastructureJob(
          terminal,
          "failure",
          `GitZero could not initialize the run after ${claim.initialization_attempt_count} attempts. Last error: ${detail}`,
        );
        await this.syncCheck(this.jobRow(job.id));
      } else {
        const retryAt =
          Date.now() +
          initializationRetryDelay(claim.initialization_attempt_count);
        this.ctx.storage.sql.exec(
          `UPDATE jobs SET initialization_needed = 1,
             initialization_retry_at = ?, last_error = ?
           WHERE id = ? AND status = 'queued' AND initialization_needed = 2`,
          retryAt,
          detail,
          job.id,
        );
        this.broadcast("job_initialization_retry", {
          job_id: job.id,
          attempt: claim.initialization_attempt_count,
          retry_at: new Date(retryAt).toISOString(),
          error: detail,
        });
      }
      console.error(
        JSON.stringify({
          message: "GitHub job initialization failed",
          jobId: job.id,
          attempt: claim.initialization_attempt_count,
          error: detail,
        }),
      );
    } finally {
      await this.scheduleAlarm();
    }
  }

  private ensureWorkspaceId(workspaceId: string): void {
    this.ctx.storage.sql.exec(
      "INSERT INTO metadata (key, value) VALUES ('workspace_id', ?) ON CONFLICT (key) DO NOTHING",
      workspaceId,
    );
    const stored = this.ctx.storage.sql
      .exec<{
        value: string;
      }>("SELECT value FROM metadata WHERE key = 'workspace_id'")
      .one().value;
    if (stored !== workspaceId) {
      throw new Error("workspace ID does not match Durable Object identity");
    }
  }

  private completeInitializingJob(
    row: JobRow,
    conclusion: Extract<Conclusion, "failure" | "neutral">,
    summary: string,
  ): void {
    const completed = this.ctx.storage.sql
      .exec<{ id: string }>(
        `UPDATE jobs SET status = 'completed', completed_at = ?,
           conclusion = ?, summary = ?, check_sync_needed = 0,
           last_error = ?, initialization_needed = 0,
           initialization_retry_at = NULL
         WHERE id = ? AND status = 'queued' AND initialization_needed = 2
         RETURNING id`,
        Date.now(),
        conclusion,
        summary,
        summary,
        row.id,
      )
      .toArray();
    if (completed.length === 0) return;
    this.broadcast("job_finished", {
      job_id: row.id,
      agent_id: null,
      conclusion,
      summary,
    });
  }

  private workspaceId(): string {
    return this.ctx.storage.sql
      .exec<{
        value: string;
      }>("SELECT value FROM metadata WHERE key = 'workspace_id'")
      .one().value;
  }

  private nextRunNumber(): number {
    this.ctx.storage.sql.exec(
      "INSERT OR IGNORE INTO metadata (key, value) VALUES ('run_number', '0')",
    );
    return this.ctx.storage.sql
      .exec<{ run_number: number }>(
        `UPDATE metadata SET value = CAST(value AS INTEGER) + 1
         WHERE key = 'run_number'
         RETURNING CAST(value AS INTEGER) AS run_number`,
      )
      .one().run_number;
  }

  private persistEvent(jobId: string, event: QueuedJob["event"]): void {
    const encoded = JSON.stringify(event);
    for (
      let offset = 0, index = 0;
      offset < encoded.length;
      offset += EVENT_CHUNK_CHARACTERS, index += 1
    ) {
      this.ctx.storage.sql.exec(
        "INSERT INTO job_event_chunks (job_id, chunk_index, content) VALUES (?, ?, ?)",
        jobId,
        index,
        encoded.slice(offset, offset + EVENT_CHUNK_CHARACTERS),
      );
    }
  }

  private eventPayload(jobId: string): unknown {
    const encoded = this.ctx.storage.sql
      .exec<{ content: string }>(
        "SELECT content FROM job_event_chunks WHERE job_id = ? ORDER BY chunk_index",
        jobId,
      )
      .toArray()
      .map((row) => row.content)
      .join("");
    return encoded.length === 0 ? {} : JSON.parse(encoded);
  }

  private reserveMessage(messageId: string): boolean {
    const rows = this.ctx.storage.sql
      .exec<{ message_id: string }>(
        `INSERT INTO messages (message_id, received_at) VALUES (?, ?)
         ON CONFLICT (message_id) DO NOTHING RETURNING message_id`,
        messageId,
        Date.now(),
      )
      .toArray();
    return rows.length === 1;
  }

  private acquireConcurrency(
    socket: WebSocket,
    agentId: string,
    row: JobRow,
    message: Extract<AgentMessage, { type: "concurrency_acquire" }>,
  ): void {
    const existing = this.ctx.storage.sql
      .exec<ConcurrencyRow>(
        "SELECT * FROM concurrency_leases WHERE request_id = ?",
        message.request_id,
      )
      .toArray()[0];
    if (existing) {
      if (existing.run_id !== row.id || existing.agent_id !== agentId) {
        sendServer(socket, {
          type: "concurrency_cancelled",
          request_id: message.request_id,
          reason: "Concurrency request ID is already owned by another run.",
        });
      } else if (existing.status === "active") {
        sendServer(socket, {
          type: "concurrency_granted",
          request_id: message.request_id,
        });
      } else if (existing.status === "cancelling") {
        sendServer(socket, {
          type: "concurrency_cancelled",
          request_id: message.request_id,
          reason: "Concurrency unit was superseded by a newer request.",
        });
      }
      return;
    }

    const job = parseJob(row.job_json);
    const repositoryKey =
      `${job.repository.owner}/${job.repository.name}`.toLowerCase();
    const groupName = message.group.trim();
    const groupKey = groupName.toLowerCase();
    const cancelledWaiters: ConcurrencyRow[] = [];
    let activeToCancel: ConcurrencyRow | undefined;
    let insertedStatus:
      | Extract<ConcurrencyStatus, "waiting" | "active">
      | undefined;
    let rejection: string | undefined;

    this.ctx.storage.transactionSync(() => {
      if (message.queue === "single") {
        cancelledWaiters.push(
          ...this.ctx.storage.sql
            .exec<ConcurrencyRow>(
              `SELECT * FROM concurrency_leases
               WHERE repository_key = ? AND group_key = ? AND status = 'waiting'
               ORDER BY sequence`,
              repositoryKey,
              groupKey,
            )
            .toArray(),
        );
        this.ctx.storage.sql.exec(
          `DELETE FROM concurrency_leases
           WHERE repository_key = ? AND group_key = ? AND status = 'waiting'`,
          repositoryKey,
          groupKey,
        );
      } else {
        const waiting = this.ctx.storage.sql
          .exec<CountRow>(
            `SELECT COUNT(*) AS count FROM concurrency_leases
             WHERE repository_key = ? AND group_key = ? AND status = 'waiting'`,
            repositoryKey,
            groupKey,
          )
          .one().count;
        if (waiting >= 100) {
          rejection =
            "The concurrency group already has GitHub's maximum of 100 pending units.";
          return;
        }
      }

      const active = this.ctx.storage.sql
        .exec<ConcurrencyRow>(
          `SELECT * FROM concurrency_leases
           WHERE repository_key = ? AND group_key = ?
             AND status IN ('active', 'cancelling')
           LIMIT 1`,
          repositoryKey,
          groupKey,
        )
        .toArray()[0];
      if (active && message.cancel_in_progress) {
        activeToCancel = active;
        this.ctx.storage.sql.exec(
          "UPDATE concurrency_leases SET status = 'cancelling' WHERE request_id = ?",
          active.request_id,
        );
      }
      insertedStatus = active ? "waiting" : "active";
      this.ctx.storage.sql.exec(
        `INSERT INTO concurrency_leases (
          request_id, run_id, agent_id, repository_key, group_key,
          group_name, unit_id, queue_mode, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        message.request_id,
        row.id,
        agentId,
        repositoryKey,
        groupKey,
        groupName,
        message.unit_id,
        message.queue,
        insertedStatus,
        Date.now(),
      );
    });

    for (const cancelled of cancelledWaiters) {
      this.sendConcurrencyCancellation(
        cancelled,
        "A newer concurrency request replaced this pending unit.",
      );
    }
    if (rejection) {
      sendServer(socket, {
        type: "concurrency_cancelled",
        request_id: message.request_id,
        reason: rejection,
      });
      return;
    }
    if (activeToCancel) {
      this.sendConcurrencyCancellation(
        activeToCancel,
        `A newer unit in concurrency group '${groupName}' requested cancel-in-progress.`,
      );
    }
    if (insertedStatus === "active") {
      sendServer(socket, {
        type: "concurrency_granted",
        request_id: message.request_id,
      });
      this.broadcast("concurrency_started", {
        request_id: message.request_id,
        run_id: row.id,
        agent_id: agentId,
        unit_id: message.unit_id,
        group: groupName,
      });
    } else {
      this.broadcast("concurrency_waiting", {
        request_id: message.request_id,
        run_id: row.id,
        agent_id: agentId,
        unit_id: message.unit_id,
        group: groupName,
        queue: message.queue,
      });
    }
  }

  private releaseConcurrency(
    requestId: string,
    runId: string,
    agentId: string,
    reason: string,
  ): void {
    const row = this.ctx.storage.sql
      .exec<ConcurrencyRow>(
        `DELETE FROM concurrency_leases
         WHERE request_id = ? AND run_id = ? AND agent_id = ?
         RETURNING *`,
        requestId,
        runId,
        agentId,
      )
      .toArray()[0];
    if (!row) return;
    this.broadcast("concurrency_released", {
      request_id: row.request_id,
      run_id: row.run_id,
      agent_id: row.agent_id,
      unit_id: row.unit_id,
      group: row.group_name,
      reason,
    });
    if (row.status !== "waiting") {
      this.promoteConcurrency(row.repository_key, row.group_key);
    }
  }

  private releaseConcurrencyForRun(runId: string, reason: string): void {
    const rows = this.ctx.storage.sql
      .exec<ConcurrencyRow>(
        "DELETE FROM concurrency_leases WHERE run_id = ? RETURNING *",
        runId,
      )
      .toArray();
    const groups = new Map<string, [string, string]>();
    for (const row of rows) {
      this.sendConcurrencyCancellation(row, reason);
      this.broadcast("concurrency_released", {
        request_id: row.request_id,
        run_id: row.run_id,
        agent_id: row.agent_id,
        unit_id: row.unit_id,
        group: row.group_name,
        reason,
      });
      if (row.status !== "waiting") {
        groups.set(`${row.repository_key}\0${row.group_key}`, [
          row.repository_key,
          row.group_key,
        ]);
      }
    }
    for (const [repositoryKey, groupKey] of groups.values()) {
      this.promoteConcurrency(repositoryKey, groupKey);
    }
  }

  private promoteConcurrency(repositoryKey: string, groupKey: string): void {
    while (true) {
      const next = this.ctx.storage.transactionSync(() => {
        const active = this.ctx.storage.sql
          .exec<CountRow>(
            `SELECT COUNT(*) AS count FROM concurrency_leases
             WHERE repository_key = ? AND group_key = ?
               AND status IN ('active', 'cancelling')`,
            repositoryKey,
            groupKey,
          )
          .one().count;
        if (active > 0) return undefined;
        const candidate = this.ctx.storage.sql
          .exec<ConcurrencyRow>(
            `SELECT concurrency_leases.* FROM concurrency_leases
             JOIN jobs ON jobs.id = concurrency_leases.run_id
             WHERE concurrency_leases.repository_key = ?
               AND concurrency_leases.group_key = ?
               AND concurrency_leases.status = 'waiting'
               AND jobs.status IN ('assigned', 'running')
               AND jobs.agent_id = concurrency_leases.agent_id
             ORDER BY concurrency_leases.sequence
             LIMIT 1`,
            repositoryKey,
            groupKey,
          )
          .toArray()[0];
        if (!candidate) return undefined;
        return this.ctx.storage.sql
          .exec<ConcurrencyRow>(
            `UPDATE concurrency_leases SET status = 'active'
             WHERE request_id = ? AND status = 'waiting'
             RETURNING *`,
            candidate.request_id,
          )
          .toArray()[0];
      });
      if (!next) return;
      if (
        this.sendToAgent(next.agent_id, {
          type: "concurrency_granted",
          request_id: next.request_id,
        })
      ) {
        this.broadcast("concurrency_started", {
          request_id: next.request_id,
          run_id: next.run_id,
          agent_id: next.agent_id,
          unit_id: next.unit_id,
          group: next.group_name,
        });
        return;
      }
      this.ctx.storage.sql.exec(
        "DELETE FROM concurrency_leases WHERE request_id = ?",
        next.request_id,
      );
    }
  }

  private sendConcurrencyCancellation(
    row: ConcurrencyRow,
    reason: string,
  ): void {
    this.sendToAgent(row.agent_id, {
      type: "concurrency_cancelled",
      request_id: row.request_id,
      reason,
    });
    this.broadcast("concurrency_cancelled", {
      request_id: row.request_id,
      run_id: row.run_id,
      agent_id: row.agent_id,
      unit_id: row.unit_id,
      group: row.group_name,
      reason,
    });
  }

  private sendToAgent(agentId: string, message: ServerMessage): boolean {
    for (const socket of this.ctx.getWebSockets(`agent:${agentId}`)) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      sendServer(socket, message);
      return true;
    }
    return false;
  }

  private async handleAgentEvent(
    socket: WebSocket,
    agentId: string,
    message: Exclude<AgentMessage, { type: "hello" }>,
  ): Promise<void> {
    switch (message.type) {
      case "heartbeat": {
        const expiresAt = Date.now() + LEASE_MILLISECONDS;
        const reportedRunningJobIds = [...new Set(message.running_job_ids)];
        for (const jobId of reportedRunningJobIds) {
          const updated = this.ctx.storage.sql
            .exec<{ id: string }>(
              `UPDATE jobs SET lease_expires_at = ?
               WHERE id = ? AND agent_id = ? AND status IN ('assigned', 'running')
               RETURNING id`,
              expiresAt,
              jobId,
              agentId,
            )
            .toArray();
          if (updated.length === 0) {
            sendServer(socket, {
              type: "cancel_job",
              job_id: jobId,
              reason: "The control plane no longer owns this job lease.",
            });
          }
        }
        const attachment = readAttachment(socket);
        const reported = new Set(reportedRunningJobIds);
        attachment.drainingJobIds = (attachment.drainingJobIds ?? []).filter(
          (jobId) => reported.has(jobId),
        );
        socket.serializeAttachment(attachment);
        this.broadcast("agent_status", {
          agent: this.agentStatus(socket),
          reported_running_job_ids: reportedRunningJobIds,
        });
        await this.scheduleAlarm();
        await this.tryDispatch();
        return;
      }
      case "job_started": {
        const row = this.assignedJob(message.job_id, agentId);
        if (row.status === "assigned") {
          this.ctx.storage.sql.exec(
            `UPDATE jobs SET status = 'running', started_at = ?, lease_expires_at = ?, check_sync_needed = 1
             WHERE id = ? AND agent_id = ? AND status = 'assigned'`,
            Date.now(),
            Date.now() + LEASE_MILLISECONDS,
            row.id,
            agentId,
          );
          const running = this.jobRow(row.id);
          this.broadcast("job_started", { job_id: row.id, agent_id: agentId });
          await this.syncCheck(running);
        }
        return;
      }
      case "job_rejected": {
        const row = this.assignedJob(message.job_id, agentId);
        const attachment = readAttachment(socket);
        if (
          row.status !== "assigned" ||
          !attachment.hello ||
          agentSatisfiesRequirements(attachment.hello, message.requirements)
        ) {
          sendServer(socket, {
            type: "error",
            code: "invalid_job_rejection",
            message:
              "A job may be rejected only before it starts and only for runner requirements this agent does not satisfy.",
          });
          return;
        }
        const requirements = normalizeRunnerRequirements(message.requirements);
        attachment.drainingJobIds = [
          ...new Set([...(attachment.drainingJobIds ?? []), row.id]),
        ];
        socket.serializeAttachment(attachment);
        this.ctx.storage.sql.exec(
          `UPDATE jobs SET status = 'queued', agent_id = NULL,
             lease_expires_at = NULL, started_at = NULL, last_error = ?,
             runner_requirements_json = ?,
             attempt_count = CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END
           WHERE id = ? AND agent_id = ? AND status = 'assigned'`,
          message.reason,
          JSON.stringify(requirements),
          row.id,
          agentId,
        );
        this.broadcast("job_rejected", {
          job_id: row.id,
          agent_id: agentId,
          reason: message.reason,
          runner_requirements: requirements,
        });
        this.broadcast("job_requeued", {
          job_id: row.id,
          reason: message.reason,
          runner_requirements: requirements,
        });
        await this.tryDispatch();
        await this.scheduleAlarm();
        return;
      }
      case "concurrency_acquire": {
        const row = this.assignedJob(message.job_id, agentId);
        if (message.queue === "max" && message.cancel_in_progress) {
          sendServer(socket, {
            type: "concurrency_cancelled",
            request_id: message.request_id,
            reason:
              "GitHub Actions does not allow queue: max with cancel-in-progress: true.",
          });
          return;
        }
        this.acquireConcurrency(socket, agentId, row, message);
        return;
      }
      case "concurrency_release": {
        this.releaseConcurrency(
          message.request_id,
          message.job_id,
          agentId,
          "Concurrency unit completed.",
        );
        return;
      }
      case "repository_token_request": {
        const row = this.assignedJob(message.job_id, agentId);
        if (row.status === "completed") {
          sendServer(socket, {
            type: "repository_token_denied",
            request_id: message.request_id,
            reason: "The parent GitZero run is no longer active.",
          });
          return;
        }
        const job = parseJob(row.job_json);
        if (!job.requires_github_token) {
          sendServer(socket, {
            type: "repository_token_denied",
            request_id: message.request_id,
            reason:
              "The parent GitZero run is not authorized for GitHub tokens.",
          });
          return;
        }
        try {
          let credential: InstallationAccessToken;
          switch (message.purpose) {
            case "source":
            case "environment": {
              if (
                message.owner.toLowerCase() ===
                  job.repository.owner.toLowerCase() &&
                message.repository.toLowerCase() ===
                  job.repository.name.toLowerCase()
              ) {
                credential =
                  message.purpose === "source"
                    ? await createAgentToken(
                        this.env,
                        job.installation_id,
                        job.repository.name,
                      )
                    : await createEnvironmentToken(
                        this.env,
                        job.installation_id,
                        job.repository.name,
                      );
                break;
              }
              throw new Error(
                `${message.purpose} token target does not match the assigned repository`,
              );
            }
            case "shared_source":
              credential = await createSharedRepositoryToken(
                this.env,
                job.installation_id,
                job.repository.owner,
                job.repository.name,
                eventRepositoryOwnerType(this.eventPayload(job.id)),
                message.owner,
                message.repository,
              );
              break;
            case "checkout":
              credential = await createPrivateCheckoutToken(
                this.env,
                job.installation_id,
                job.repository.owner,
                job.repository.name,
                message.owner,
                message.repository,
              );
              break;
          }
          const stillOwned = this.ctx.storage.sql
            .exec<{ id: string }>(
              `SELECT id FROM jobs
               WHERE id = ? AND agent_id = ? AND status IN ('assigned', 'running')`,
              job.id,
              agentId,
            )
            .toArray().length;
          if (stillOwned === 0 || socket.readyState !== WebSocket.OPEN) {
            return;
          }
          sendServer(socket, {
            type: "repository_token_granted",
            request_id: message.request_id,
            token: credential.token,
            expires_at_epoch_seconds: credential.expiresAtEpochSeconds,
          });
        } catch (error) {
          console.error(
            JSON.stringify({
              message: "repository token request denied",
              jobId: job.id,
              agentId,
              purpose: message.purpose,
              target: `${message.owner}/${message.repository}`,
              error: truncateDiagnostic(error),
            }),
          );
          if (socket.readyState === WebSocket.OPEN) {
            const reason =
              message.purpose === "source" || message.purpose === "environment"
                ? "Internal repository credential access was denied because the target does not match the active run."
                : message.purpose === "shared_source"
                  ? "Private source access was denied. Confirm the target Actions sharing policy and GitHub App installation include this repository."
                  : "Private checkout access was denied. Confirm the target has the same owner and the GitHub App installation includes this repository.";
            sendServer(socket, {
              type: "repository_token_denied",
              request_id: message.request_id,
              reason,
            });
          }
        }
        return;
      }
      case "workflow_token_request": {
        const row = this.assignedJob(message.job_id, agentId);
        if (row.status === "completed") {
          sendServer(socket, {
            type: "workflow_token_denied",
            request_id: message.request_id,
            reason: "The parent GitZero run is no longer active.",
          });
          return;
        }
        const job = parseJob(row.job_json);
        if (!job.requires_github_token) {
          sendServer(socket, {
            type: "workflow_token_denied",
            request_id: message.request_id,
            reason:
              "The parent GitZero run is not authorized for GitHub tokens.",
          });
          return;
        }
        let effectivePermissions: { read: string[]; write: string[] } = {
          read: [...message.read_permissions],
          write: [],
        };
        try {
          effectivePermissions =
            message.write_permissions.length === 0
              ? {
                  read: [...message.read_permissions],
                  write: [],
                }
              : effectiveWorkflowTokenPermissions(
                  job,
                  this.eventPayload(job.id),
                  message.read_permissions,
                  message.write_permissions,
                );
          const credential = await createWorkflowToken(
            this.env,
            job.installation_id,
            job.repository.name,
            effectivePermissions.read,
            effectivePermissions.write,
          );
          const stillOwned = this.ctx.storage.sql
            .exec<{ id: string }>(
              `SELECT id FROM jobs
               WHERE id = ? AND agent_id = ? AND status IN ('assigned', 'running')`,
              job.id,
              agentId,
            )
            .toArray().length;
          if (stillOwned === 0 || socket.readyState !== WebSocket.OPEN) {
            return;
          }
          sendServer(socket, {
            type: "workflow_token_granted",
            request_id: message.request_id,
            token: credential.token,
            expires_at_epoch_seconds: credential.expiresAtEpochSeconds,
          });
        } catch (error) {
          console.error(
            JSON.stringify({
              message: "workflow token request denied",
              jobId: job.id,
              agentId,
              readPermissions: effectivePermissions.read,
              writePermissions: effectivePermissions.write,
              writePermissionsDowngraded:
                message.write_permissions.length > 0 &&
                effectivePermissions.write.length === 0,
              error: truncateDiagnostic(error),
            }),
          );
          if (socket.readyState === WebSocket.OPEN) {
            sendServer(socket, {
              type: "workflow_token_denied",
              request_id: message.request_id,
              reason:
                "The requested workflow token could not be issued. Confirm the GitHub App has every requested repository permission.",
            });
          }
        }
        return;
      }
      case "step_started":
      case "step_finished":
      case "log_chunk": {
        this.assignedJob(message.job_id, agentId);
        this.broadcast(message.type, {
          ...redactMessage(message),
          agent_id: agentId,
        });
        return;
      }
      case "job_finished": {
        const row = this.assignedJob(message.job_id, agentId);
        if (row.status !== "completed") {
          this.releaseConcurrencyForRun(
            row.id,
            "The parent GitZero run completed.",
          );
          this.ctx.storage.sql.exec(
            `UPDATE jobs SET status = 'completed', completed_at = ?, conclusion = ?, summary = ?,
              lease_expires_at = NULL, check_sync_needed = 1
             WHERE id = ? AND agent_id = ? AND status IN ('assigned', 'running')`,
            Date.now(),
            message.conclusion,
            message.summary,
            message.job_id,
            agentId,
          );
          this.ctx.storage.sql.exec(
            "DELETE FROM job_annotations WHERE job_id = ?",
            message.job_id,
          );
          for (const [index, annotation] of message.annotations.entries()) {
            this.ctx.storage.sql.exec(
              `INSERT INTO job_annotations (job_id, annotation_index, annotation_json)
               VALUES (?, ?, ?)`,
              message.job_id,
              index,
              JSON.stringify(annotation),
            );
          }
          const completed = this.jobRow(message.job_id);
          this.broadcast("job_finished", {
            job_id: message.job_id,
            agent_id: agentId,
            conclusion: message.conclusion,
            summary: message.summary,
            annotation_count: message.annotations.length,
          });
          await this.syncCheck(completed);
          await this.tryDispatch();
          await this.scheduleAlarm();
        }
        return;
      }
    }
  }

  private async handleAgentDisconnect(
    agentId: string,
    code: number,
    reason: string,
    wasClean: boolean,
  ): Promise<void> {
    const rows = this.ctx.storage.sql
      .exec<JobRow>(
        "SELECT * FROM jobs WHERE agent_id = ? AND status IN ('assigned', 'running')",
        agentId,
      )
      .toArray();
    for (const row of rows) {
      const terminal = this.requeue(row.id, "agent disconnected");
      if (terminal) await this.syncCheck(terminal);
    }
    this.broadcast("agent_offline", {
      agent_id: agentId,
      code,
      reason,
      was_clean: wasClean,
    });
    await this.tryDispatch();
    await this.scheduleAlarm();
  }

  private requeue(jobId: string, reason: string): JobRow | null {
    const row = this.jobRow(jobId);
    if (!["assigned", "running"].includes(row.status)) {
      return null;
    }
    this.releaseConcurrencyForRun(
      row.id,
      `The parent GitZero run was requeued: ${reason}`,
    );
    if (row.attempt_count >= MAX_ASSIGNMENT_ATTEMPTS) {
      this.completeInfrastructureJob(
        row,
        "failure",
        `GitZero could not complete the run after ${row.attempt_count} agent assignment attempts. Last error: ${reason}`,
      );
      return this.jobRow(jobId);
    }
    this.ctx.storage.sql.exec(
      `UPDATE jobs SET status = 'queued', agent_id = NULL, lease_expires_at = NULL,
       started_at = NULL, last_error = ?
       WHERE id = ? AND status IN ('assigned', 'running')`,
      reason,
      jobId,
    );
    this.broadcast("job_requeued", { job_id: jobId, reason });
    return null;
  }

  private async tryDispatch(): Promise<void> {
    while (true) {
      const candidates = this.availableAgents();
      if (candidates.length === 0) return;
      let selected: { queued: JobRow; candidate: AgentCandidate } | undefined;
      const queuedJobs = this.ctx.storage.sql.exec<JobRow>(
        `SELECT * FROM jobs
         WHERE status = 'queued' AND initialization_needed = 0
         ORDER BY created_at`,
      );
      for (const queued of queuedJobs) {
        const requirements = parseRunnerRequirements(
          queued.runner_requirements_json,
        );
        const candidate = leastLoadedCompatibleAgent(candidates, requirements);
        if (candidate) {
          selected = { queued, candidate };
          break;
        }
      }
      if (!selected) return;
      const { queued, candidate } = selected;
      const leaseExpiresAt = Date.now() + LEASE_MILLISECONDS;
      this.ctx.storage.sql.exec(
        `UPDATE jobs SET status = 'assigned', agent_id = ?, lease_expires_at = ?,
         attempt_count = attempt_count + 1, last_error = NULL
         WHERE id = ? AND status = 'queued'`,
        candidate.agentId,
        leaseExpiresAt,
        queued.id,
      );
      const job = parseJob(queued.job_json);

      let checkoutToken = "";
      let checkoutTokenExpiresAtEpochSeconds: number | null = null;
      try {
        const event = this.eventPayload(job.id);
        if (job.requires_github_token) {
          const credential = await createAgentToken(
            this.env,
            job.installation_id,
            job.repository.name,
          );
          checkoutToken = credential.token;
          checkoutTokenExpiresAtEpochSeconds = credential.expiresAtEpochSeconds;
        }
        const runSpec: RunSpec = runSpecSchema.parse({
          id: job.id,
          workspace_id: job.workspace_id,
          installation_id: job.installation_id,
          run_number: job.run_number,
          repository: job.repository,
          pull_request: job.pull_request,
          check_run_id: job.check_run_id ?? undefined,
          event,
          checkout_token: checkoutToken,
          checkout_token_expires_at_epoch_seconds:
            checkoutTokenExpiresAtEpochSeconds,
          github_api_version: this.env.GITHUB_API_VERSION,
          environment: job.environment,
          variables: job.variables,
        });
        sendServer(candidate.socket, { type: "run_job", job: runSpec });
        this.broadcast("job_assigned", {
          job_id: job.id,
          agent_id: candidate.agentId,
        });
      } catch (error) {
        const terminal = this.requeue(
          job.id,
          "assignment could not be delivered",
        );
        console.error(
          JSON.stringify({
            message: "job dispatch failed",
            jobId: job.id,
            error: String(error),
          }),
        );
        if (terminal) await this.syncCheck(terminal);
        await this.ctx.storage.setAlarm(Date.now() + CHECK_RETRY_MILLISECONDS);
        return;
      }
      await this.scheduleAlarm();
    }
  }

  private availableAgents(): AgentCandidate[] {
    const candidates: AgentCandidate[] = [];
    for (const socket of this.ctx.getWebSockets("role:agent")) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      const attachment = readAttachment(socket);
      if (
        !attachment.agentId ||
        !attachment.hello ||
        (attachment.drainingJobIds?.length ?? 0) > 0 ||
        !supportsMacos(attachment.hello)
      )
        continue;
      const assigned = this.ctx.storage.sql
        .exec<CountRow>(
          "SELECT COUNT(*) AS count FROM jobs WHERE agent_id = ? AND status IN ('assigned', 'running')",
          attachment.agentId,
        )
        .one().count;
      const capacity = attachment.hello.max_parallelism;
      if (assigned >= capacity) continue;
      candidates.push({
        socket,
        hello: attachment.hello,
        agentId: attachment.agentId,
        assigned,
        capacity,
        connectedAt: attachment.connectedAt,
      });
    }
    return candidates;
  }

  private assignedJob(jobId: string, agentId: string): JobRow {
    const row = this.jobRow(jobId);
    if (
      row.agent_id !== agentId ||
      !["assigned", "running", "completed"].includes(row.status)
    ) {
      throw new Error(`agent ${agentId} does not own job ${jobId}`);
    }
    return row;
  }

  private jobRow(jobId: string): JobRow {
    const rows = this.ctx.storage.sql
      .exec<JobRow>("SELECT * FROM jobs WHERE id = ?", jobId)
      .toArray();
    const row = rows[0];
    if (!row) throw new Error(`unknown job ${jobId}`);
    return row;
  }

  private jobAnnotations(jobId: string): CheckAnnotation[] {
    return this.ctx.storage.sql
      .exec<AnnotationRow>(
        `SELECT annotation_json FROM job_annotations
         WHERE job_id = ? ORDER BY annotation_index`,
        jobId,
      )
      .toArray()
      .map((row) =>
        checkAnnotationSchema.parse(JSON.parse(row.annotation_json)),
      );
  }

  private async syncCheck(row: JobRow): Promise<void> {
    const job = parseJob(row.job_json);
    if (!job.report_to_github || job.check_run_id === null) {
      this.ctx.storage.sql.exec(
        "UPDATE jobs SET check_sync_needed = 0 WHERE id = ?",
        row.id,
      );
      return;
    }
    try {
      if (row.status === "running") {
        await updateCheckRun(this.env, job, {
          status: "in_progress",
          title: "Running on GitZero",
          summary: `The workflow is running on agent ${row.agent_id ?? "unknown"}.`,
        });
      } else if (row.status === "completed" && row.conclusion) {
        await updateCheckRun(this.env, job, {
          status: "completed",
          conclusion: row.conclusion,
          title: checkTitle(row.conclusion),
          summary: row.summary ?? "GitZero completed the run.",
          annotations: this.jobAnnotations(row.id),
        });
      } else {
        return;
      }
      this.ctx.storage.sql.exec(
        "UPDATE jobs SET check_sync_needed = 0 WHERE id = ? AND status = ?",
        row.id,
        row.status,
      );
    } catch (error) {
      console.error(
        JSON.stringify({
          message: "GitHub check sync failed",
          jobId: row.id,
          error: String(error),
        }),
      );
      await this.ctx.storage.setAlarm(Date.now() + CHECK_RETRY_MILLISECONDS);
    }
  }

  private async scheduleAlarm(): Promise<void> {
    const lease = this.ctx.storage.sql
      .exec<MinimumRow>(
        "SELECT MIN(lease_expires_at) AS value FROM jobs WHERE status IN ('assigned', 'running')",
      )
      .one().value;
    const queueDeadline = this.ctx.storage.sql
      .exec<MinimumRow>(
        "SELECT MIN(queue_expires_at) AS value FROM jobs WHERE status = 'queued'",
      )
      .one().value;
    const pendingChecks = this.ctx.storage.sql
      .exec<CountRow>(
        "SELECT COUNT(*) AS count FROM jobs WHERE check_sync_needed = 1",
      )
      .one().count;
    const checkRetry =
      pendingChecks > 0 ? Date.now() + CHECK_RETRY_MILLISECONDS : null;
    const initializationRetry = this.ctx.storage.sql
      .exec<MinimumRow>(
        `SELECT MIN(initialization_retry_at) AS value FROM jobs
         WHERE status = 'queued' AND initialization_needed != 0`,
      )
      .one().value;
    const next = [lease, queueDeadline, checkRetry, initializationRetry]
      .filter((value): value is number => value !== null)
      .sort((a, b) => a - b)[0];
    if (next === undefined) {
      await this.ctx.storage.deleteAlarm();
    } else {
      await this.ctx.storage.setAlarm(next);
    }
  }

  getSnapshot(): Record<string, unknown> {
    const jobs = this.ctx.storage.sql
      .exec<JobRow>("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 100")
      .toArray()
      .map((row) => ({
        ...publicJob(parseJob(row.job_json)),
        status: row.status,
        agent_id: row.agent_id,
        created_at: new Date(row.created_at).toISOString(),
        started_at: row.started_at
          ? new Date(row.started_at).toISOString()
          : null,
        completed_at: row.completed_at
          ? new Date(row.completed_at).toISOString()
          : null,
        conclusion: row.conclusion,
        summary: row.summary,
        attempt_count: row.attempt_count,
        queue_expires_at: new Date(row.queue_expires_at).toISOString(),
        last_error: row.last_error,
        initialization_status: initializationStatus(row.initialization_needed),
        initialization_attempt_count: row.initialization_attempt_count,
        runner_requirements: parseRunnerRequirements(
          row.runner_requirements_json,
        ),
      }));
    const agents = this.ctx
      .getWebSockets("role:agent")
      .filter((socket) => readAttachment(socket).hello !== undefined)
      .map((socket) => this.agentStatus(socket));
    const concurrency = this.ctx.storage.sql
      .exec<ConcurrencyRow>(
        "SELECT * FROM concurrency_leases ORDER BY sequence LIMIT 200",
      )
      .toArray()
      .map((row) => ({
        request_id: row.request_id,
        run_id: row.run_id,
        agent_id: row.agent_id,
        unit_id: row.unit_id,
        group: row.group_name,
        queue: row.queue_mode,
        status: row.status,
        created_at: new Date(row.created_at).toISOString(),
      }));
    return { jobs, agents, concurrency };
  }

  private agentStatus(socket: WebSocket): Record<string, unknown> {
    const attachment = readAttachment(socket);
    if (!attachment.hello || !attachment.agentId) {
      throw new Error("agent status requested before protocol handshake");
    }
    const activeJobs = this.ctx.storage.sql
      .exec<ActiveJobRow>(
        `SELECT id, status, lease_expires_at FROM jobs
         WHERE agent_id = ? AND status IN ('assigned', 'running')
         ORDER BY created_at`,
        attachment.agentId,
      )
      .toArray()
      .map((job) => ({
        job_id: job.id,
        status: job.status,
        lease_expires_at: new Date(job.lease_expires_at).toISOString(),
      }));
    return {
      ...publicAgent(attachment.hello),
      status: "online",
      connected_at: new Date(attachment.connectedAt).toISOString(),
      last_seen_at: new Date(attachment.lastSeenAt).toISOString(),
      active_jobs: activeJobs,
      draining_job_ids: attachment.drainingJobIds ?? [],
      available_capacity: Math.max(
        0,
        attachment.hello.max_parallelism -
          activeJobs.length -
          (attachment.drainingJobIds?.length ?? 0),
      ),
    };
  }

  private broadcast(type: string, data: Record<string, unknown>): void {
    const event: ObserverEvent = {
      type,
      workspace_id: this.workspaceId(),
      timestamp: new Date().toISOString(),
      data,
    };
    const encoded = JSON.stringify(event);
    for (const observer of this.ctx.getWebSockets("role:observer")) {
      if (observer.readyState !== WebSocket.OPEN) continue;
      try {
        observer.send(encoded);
      } catch (error) {
        console.error(
          JSON.stringify({
            message: "observer broadcast failed",
            error: String(error),
          }),
        );
      }
    }
  }

  private cancelOnAgent(row: JobRow, reason: string): void {
    if (!row.agent_id) return;
    for (const socket of this.ctx.getWebSockets(`agent:${row.agent_id}`)) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      sendServer(socket, {
        type: "cancel_job",
        job_id: row.id,
        reason,
      });
    }
  }

  private completeInfrastructureJob(
    row: JobRow,
    conclusion: Extract<Conclusion, "failure" | "timed_out">,
    summary: string,
  ): void {
    this.ctx.storage.sql.exec(
      `UPDATE jobs SET status = 'completed', agent_id = NULL,
       lease_expires_at = NULL, completed_at = ?, conclusion = ?, summary = ?,
       check_sync_needed = 1, last_error = ?, initialization_needed = 0,
       initialization_retry_at = NULL
       WHERE id = ? AND status IN ('queued', 'assigned', 'running')`,
      Date.now(),
      conclusion,
      summary,
      summary,
      row.id,
    );
    this.broadcast("job_finished", {
      job_id: row.id,
      agent_id: row.agent_id,
      conclusion,
      summary,
    });
  }

  private prune(now: number): void {
    const cutoff = now - RETENTION_MILLISECONDS;
    this.ctx.storage.sql.exec(
      "DELETE FROM messages WHERE received_at < ?",
      cutoff,
    );
    this.ctx.storage.sql.exec(
      `DELETE FROM deliveries WHERE job_id IN (
         SELECT id FROM jobs WHERE status = 'completed' AND completed_at < ?
       )`,
      cutoff,
    );
    this.ctx.storage.sql.exec(
      `DELETE FROM job_event_chunks WHERE job_id IN (
         SELECT id FROM jobs WHERE status = 'completed' AND completed_at < ?
       )`,
      cutoff,
    );
    this.ctx.storage.sql.exec(
      `DELETE FROM job_annotations WHERE job_id IN (
         SELECT id FROM jobs WHERE status = 'completed' AND completed_at < ?
       )`,
      cutoff,
    );
    this.ctx.storage.sql.exec(
      "DELETE FROM jobs WHERE status = 'completed' AND completed_at < ?",
      cutoff,
    );
  }
}

function initializationRetryDelay(attempt: number): number {
  return Math.min(
    INITIALIZATION_RETRY_BASE_MILLISECONDS * 2 ** Math.max(0, attempt - 1),
    INITIALIZATION_RETRY_MAX_MILLISECONDS,
  );
}

function initializationStatus(value: 0 | 1 | 2): string {
  switch (value) {
    case 0:
      return "ready";
    case 1:
      return "pending";
    case 2:
      return "running";
  }
}

function truncateDiagnostic(error: unknown): string {
  const value = error instanceof Error ? error.message : String(error);
  return value.length <= 4_096 ? value : `${value.slice(0, 4_095)}…`;
}

function eventRepositoryOwnerType(event: unknown): string {
  if (!event || typeof event !== "object") return "";
  const repository = Reflect.get(event, "repository");
  if (!repository || typeof repository !== "object") return "";
  const owner = Reflect.get(repository, "owner");
  if (!owner || typeof owner !== "object") return "";
  const type = Reflect.get(owner, "type");
  return typeof type === "string" ? type : "";
}

function effectiveWorkflowTokenPermissions(
  job: QueuedJob,
  event: unknown,
  readPermissions: readonly string[],
  writePermissions: readonly string[],
): { read: string[]; write: string[] } {
  if (
    writePermissions.length === 0 ||
    workflowWritePermissionsAllowed(job, event)
  ) {
    return { read: [...readPermissions], write: [...writePermissions] };
  }
  return {
    read: [...new Set([...readPermissions, ...writePermissions])].sort(),
    write: [],
  };
}

function workflowWritePermissionsAllowed(
  job: QueuedJob,
  event: unknown,
): boolean {
  const repository =
    `${job.repository.owner}/${job.repository.name}`.toLowerCase();
  const eventRepository = eventString(event, ["repository", "full_name"]);
  const headRepository = eventString(event, [
    "pull_request",
    "head",
    "repo",
    "full_name",
  ]);
  const pullRequestAuthor = eventString(event, [
    "pull_request",
    "user",
    "login",
  ]);
  return (
    eventRepository.toLowerCase() === repository &&
    headRepository.toLowerCase() === repository &&
    pullRequestAuthor.length > 0 &&
    pullRequestAuthor.toLowerCase() !== "dependabot[bot]"
  );
}

function eventString(event: unknown, path: readonly string[]): string {
  let value = event;
  for (const component of path) {
    if (!value || typeof value !== "object") return "";
    value = Reflect.get(value, component);
  }
  return typeof value === "string" ? value : "";
}

function parseJob(value: string): QueuedJob {
  return queuedJobSchema.parse(JSON.parse(value));
}

function readAttachment(socket: WebSocket): SocketAttachment {
  const value: unknown = socket.deserializeAttachment();
  if (!value || typeof value !== "object" || !("role" in value)) {
    throw new Error("WebSocket attachment is missing");
  }
  const attachment = value as Partial<SocketAttachment>;
  if (attachment.role !== "agent" && attachment.role !== "observer") {
    throw new Error("WebSocket attachment role is invalid");
  }
  if (
    typeof attachment.connectedAt !== "number" ||
    typeof attachment.lastSeenAt !== "number"
  ) {
    throw new Error("WebSocket attachment timestamps are invalid");
  }
  if (
    attachment.drainingJobIds !== undefined &&
    (!Array.isArray(attachment.drainingJobIds) ||
      attachment.drainingJobIds.some((jobId) => typeof jobId !== "string"))
  ) {
    throw new Error("WebSocket attachment draining jobs are invalid");
  }
  return attachment as SocketAttachment;
}

function sendServer(socket: WebSocket, message: ServerMessage): void {
  socket.send(JSON.stringify(message));
}

function supportsMacos(hello: AgentHello): boolean {
  return hello.labels.some((label) => label.toLowerCase() === "macos");
}

function parseRunnerRequirements(value: string): RunnerRequirement[] {
  return runnerRequirementsSchema.parse(JSON.parse(value));
}

function normalizeRunnerRequirements(
  requirements: RunnerRequirement[],
): RunnerRequirement[] {
  const normalized = new Map<string, RunnerRequirement>();
  for (const requirement of requirements) {
    const labels = [
      ...new Set(requirement.labels.map((label) => label.trim().toLowerCase())),
    ].sort();
    const runnerRequirement = {
      labels,
      runner_group: requirement.runner_group?.trim().toLowerCase() ?? null,
    } satisfies RunnerRequirement;
    normalized.set(JSON.stringify(runnerRequirement), runnerRequirement);
  }
  return [...normalized.values()].sort((left, right) =>
    JSON.stringify(left).localeCompare(JSON.stringify(right)),
  );
}

function agentSatisfiesRequirements(
  hello: AgentHello,
  requirements: RunnerRequirement[],
): boolean {
  const labels = new Set(hello.labels.map((label) => label.toLowerCase()));
  return requirements.every((requirement) => {
    const requiredGroup = requirement.runner_group?.toLowerCase() ?? null;
    if (
      requiredGroup !== null &&
      hello.runner_group?.toLowerCase() !== requiredGroup
    ) {
      return false;
    }
    if (
      requiredGroup === null &&
      requirement.labels.length === 1 &&
      isHostedMacosLabel(requirement.labels[0] ?? "")
    ) {
      return true;
    }
    return requirement.labels.every((label) => labels.has(label.toLowerCase()));
  });
}

function isHostedMacosLabel(label: string): boolean {
  const normalized = label.toLowerCase();
  return normalized === "macos-latest" || normalized.startsWith("macos-");
}

function leastLoadedCompatibleAgent(
  candidates: AgentCandidate[],
  requirements: RunnerRequirement[],
): AgentCandidate | undefined {
  let best: AgentCandidate | undefined;
  for (const candidate of candidates) {
    if (!agentSatisfiesRequirements(candidate.hello, requirements)) continue;
    if (!best) {
      best = candidate;
      continue;
    }
    const candidateLoad = candidate.assigned * best.capacity;
    const bestLoad = best.assigned * candidate.capacity;
    if (
      candidateLoad < bestLoad ||
      (candidateLoad === bestLoad && candidate.assigned < best.assigned) ||
      (candidateLoad === bestLoad &&
        candidate.assigned === best.assigned &&
        candidate.connectedAt < best.connectedAt) ||
      (candidateLoad === bestLoad &&
        candidate.assigned === best.assigned &&
        candidate.connectedAt === best.connectedAt &&
        candidate.agentId.localeCompare(best.agentId) < 0)
    ) {
      best = candidate;
    }
  }
  return best;
}

function publicAgent(hello: AgentHello): Record<string, unknown> {
  return {
    agent_id: hello.agent_id,
    name: hello.name,
    version: hello.version,
    labels: hello.labels,
    runner_group: hello.runner_group,
    max_parallelism: hello.max_parallelism,
  };
}

function publicJob(job: QueuedJob): Record<string, unknown> {
  const executionSha = job.pull_request.merge_sha ?? job.pull_request.head_sha;
  return {
    id: job.id,
    workspace_id: job.workspace_id,
    repository: `${job.repository.owner}/${job.repository.name}`,
    pull_request: job.pull_request.number,
    head_sha: executionSha,
    execution_sha: executionSha,
    execution_ref: job.pull_request.execution_ref,
    pull_request_head_sha: job.pull_request.head_sha,
    check_run_id: job.check_run_id,
  };
}

function redactMessage(message: AgentMessage): Record<string, unknown> {
  return JSON.parse(JSON.stringify(message)) as Record<string, unknown>;
}

function workspaceIdFromPath(pathname: string): string {
  const match = pathname.match(/^\/v1\/workspaces\/([^/]+)\/connect$/);
  const value = match?.[1];
  if (!value) throw new Error("workspace ID is missing from WebSocket path");
  return decodeURIComponent(value);
}

function checkTitle(conclusion: Conclusion): string {
  switch (conclusion) {
    case "success":
      return "GitZero checks passed";
    case "failure":
      return "GitZero checks failed";
    case "cancelled":
      return "GitZero run cancelled";
    case "timed_out":
      return "GitZero run timed out";
    case "neutral":
      return "GitZero run completed";
  }
}
