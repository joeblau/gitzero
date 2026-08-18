import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { QueuedJob } from "../src/protocol";
import type { ManagedSecretInput } from "../src/secrets";

const INSTALLATION_TOKEN_EXPIRY = "2100-01-01T00:00:00Z";
const INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS = 4_102_444_800;

function installationToken(token: string): {
  token: string;
  expires_at: string;
} {
  return { token, expires_at: INSTALLATION_TOKEN_EXPIRY };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Workspace Durable Object", () => {
  it("durably acknowledges GitHub jobs before external initialization completes", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    let releaseCheck: (() => void) | undefined;
    const checkGate = new Promise<void>((resolve) => {
      releaseCheck = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        if (url.pathname.endsWith("/access_tokens")) {
          const permissions = JSON.parse(String(init?.body))
            .permissions as Record<string, string>;
          if (permissions.variables === "read") {
            return Response.json(installationToken("variables-token"));
          }
          if (permissions.checks === "write") {
            return Response.json(installationToken("check-token"));
          }
          return Response.json(installationToken("checkout-token"));
        }
        if (url.pathname.endsWith("/actions/variables")) {
          return Response.json({
            total_count: 1,
            variables: [{ name: "RUNTIME", value: "24" }],
          });
        }
        if (url.pathname.endsWith("/check-runs")) {
          await checkGate;
          return Response.json({ id: 44 }, { status: 201 });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    try {
      const workspaceId = "7001";
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const agent = await connectAgent(workspace, workspaceId, "mini-1", 1);
      const assignment = collectMessages(agent, 1);
      const job = fixtureJob(workspaceId);
      job.installation_id = 7001;
      job.requires_github_token = true;
      job.report_to_github = true;
      job.event = {
        repository: { owner: { type: "User" } },
      };

      await expect(workspace.enqueue(job, "fast-webhook")).resolves.toEqual({
        duplicate: false,
        job_id: job.id,
      });
      const initializing = await runInDurableObject(
        workspace,
        (_instance, state) =>
          state.storage.sql
            .exec<{
              status: string;
              initialization_needed: number;
            }>(
              "SELECT status, initialization_needed FROM jobs WHERE id = ?",
              job.id,
            )
            .one(),
      );
      expect(initializing).toEqual({
        status: "queued",
        initialization_needed: 2,
      });

      releaseCheck?.();
      await expect(assignment).resolves.toEqual([
        expect.objectContaining({
          type: "run_job",
          job: expect.objectContaining({
            id: job.id,
            check_run_id: 44,
            variables: { RUNTIME: "24" },
            checkout_token: "checkout-token",
            checkout_token_expires_at_epoch_seconds:
              INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
          }),
        }),
      ]);
      agent.close(1000, "test complete");
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
      releaseCheck?.();
    }
  });

  it("keeps initialization failures off agents and retries them from the alarm", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    let failVariables = true;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        if (url.pathname.endsWith("/access_tokens")) {
          const permissions = JSON.parse(String(init?.body))
            .permissions as Record<string, string>;
          return Response.json(
            installationToken(
              permissions.contents === "read"
                ? "checkout-token"
                : "variables-token",
            ),
          );
        }
        if (url.pathname.endsWith("/actions/variables")) {
          return failVariables
            ? new Response("temporary failure", { status: 503 })
            : Response.json({
                total_count: 1,
                variables: [{ name: "RETRIED", value: "true" }],
              });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    try {
      const workspaceId = "7002";
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const agent = await connectAgent(workspace, workspaceId, "mini-1", 1);
      const assignment = collectMessages(agent, 1);
      const job = fixtureJob(workspaceId);
      job.installation_id = 7002;
      job.requires_github_token = true;

      await workspace.enqueue(job, "retry-initialization");
      await vi.waitFor(async () => {
        const row = await runInDurableObject(workspace, (_instance, state) =>
          state.storage.sql
            .exec<{
              status: string;
              initialization_needed: number;
              initialization_attempt_count: number;
              last_error: string | null;
            }>(
              `SELECT status, initialization_needed,
                 initialization_attempt_count, last_error
               FROM jobs WHERE id = ?`,
              job.id,
            )
            .one(),
        );
        expect(row).toMatchObject({
          status: "queued",
          initialization_needed: 1,
          initialization_attempt_count: 1,
        });
        expect(row.last_error).toContain("503");
      });

      failVariables = false;
      await runInDurableObject(workspace, (_instance, state) => {
        state.storage.sql.exec(
          "UPDATE jobs SET initialization_retry_at = 1 WHERE id = ?",
          job.id,
        );
      });
      await runDurableObjectAlarm(workspace);
      await expect(assignment).resolves.toEqual([
        expect.objectContaining({
          type: "run_job",
          job: expect.objectContaining({
            id: job.id,
            variables: { RETRIED: "true" },
          }),
        }),
      ]);
      const ready = await runInDurableObject(workspace, (_instance, state) =>
        state.storage.sql
          .exec<{
            initialization_needed: number;
            initialization_attempt_count: number;
            last_error: string | null;
          }>(
            `SELECT initialization_needed, initialization_attempt_count,
               last_error FROM jobs WHERE id = ?`,
            job.id,
          )
          .one(),
      );
      expect(ready).toEqual({
        initialization_needed: 0,
        initialization_attempt_count: 2,
        last_error: null,
      });
      agent.close(1000, "test complete");
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
    }
  });

  it("pins a missing webhook merge snapshot before creating credentials or dispatching", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          const permissions = JSON.parse(String(init?.body))
            .permissions as Record<string, string>;
          if (permissions.variables === "read") {
            return Response.json(installationToken("variables-token"));
          }
          if (permissions.contents === "read") {
            return Response.json(installationToken("checkout-token"));
          }
          return Response.json(installationToken("merge-token"));
        }
        if (url.pathname.endsWith("/pulls/1")) {
          expect(new Headers(init?.headers).get("Authorization")).toBe(
            "Bearer merge-token",
          );
          return Response.json({
            head: { sha: "0".repeat(40) },
            base: { sha: "a".repeat(40) },
            merged: false,
            mergeable: true,
            merge_commit_sha: "b".repeat(40),
          });
        }
        if (url.pathname.endsWith("/actions/variables")) {
          return Response.json({ total_count: 0, variables: [] });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    try {
      const workspaceId = "7090";
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const agent = await connectAgent(workspace, workspaceId, "mini-merge", 1);
      const assignment = collectMessages(agent, 1);
      const job = fixtureJob(workspaceId);
      job.installation_id = 7090;
      job.pull_request.head_sha = "0".repeat(40);
      job.pull_request.base_sha = "a".repeat(40);
      job.pull_request.merge_sha = null;
      job.requires_github_token = true;
      job.event = { repository: { owner: { type: "User" } } };

      await workspace.enqueue(job, "resolve-merge-snapshot");
      await expect(assignment).resolves.toEqual([
        expect.objectContaining({
          type: "run_job",
          job: expect.objectContaining({
            id: job.id,
            pull_request: expect.objectContaining({
              head_sha: "0".repeat(40),
              base_sha: "a".repeat(40),
              merge_sha: "b".repeat(40),
              execution_ref: "refs/pull/1/merge",
            }),
          }),
        }),
      ]);
      const mergeAuthorization = requests.find((request) =>
        request.url.pathname.endsWith("/access_tokens"),
      );
      expect(JSON.parse(String(mergeAuthorization?.init?.body))).toEqual({
        repositories: ["Hello-World"],
        permissions: { pull_requests: "read" },
      });
      agent.close(1000, "test complete");
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
    }
  });

  it("finishes a conflicted pull request neutrally without creating a Check", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    const requests: URL[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input));
        requests.push(url);
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("merge-token"));
        }
        if (url.pathname.endsWith("/pulls/1")) {
          return Response.json({
            head: { sha: "0".repeat(40) },
            base: { sha: "a".repeat(40) },
            merged: false,
            mergeable: false,
            merge_commit_sha: null,
          });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    try {
      const workspaceId = "7091";
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const job = fixtureJob(workspaceId);
      job.installation_id = 7091;
      job.pull_request.head_sha = "0".repeat(40);
      job.pull_request.base_sha = "a".repeat(40);
      job.pull_request.merge_sha = null;
      job.requires_github_token = true;
      job.report_to_github = true;
      await workspace.enqueue(job, "conflicted-merge-snapshot");

      await vi.waitFor(async () => {
        await expect(workspace.getSnapshot()).resolves.toMatchObject({
          jobs: [
            expect.objectContaining({
              id: job.id,
              status: "completed",
              conclusion: "neutral",
              check_run_id: null,
              summary: expect.stringContaining("cannot be merged"),
            }),
          ],
        });
      });
      expect(requests.map((url) => url.pathname)).toEqual([
        "/app/installations/7091/access_tokens",
        "/repos/octocat/Hello-World/pulls/1",
      ]);
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
    }
  });

  it("deduplicates webhook delivery IDs", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const job = fixtureJob(workspaceId);

    await expect(workspace.enqueue(job, "delivery-1")).resolves.toEqual({
      duplicate: false,
      job_id: job.id,
    });
    await expect(
      workspace.enqueue({ ...job, id: crypto.randomUUID() }, "delivery-1"),
    ).resolves.toEqual({
      duplicate: true,
      job_id: job.id,
    });
  });

  it("assigns monotonic run numbers and reconstructs chunked webhook events", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const socket = await connectAgent(workspace, workspaceId, "mini-1", 2);
    const assignments = collectMessages(socket, 2);
    const largeBody = "metadata-".repeat(80_000);
    const first = fixtureJob(workspaceId);
    first.event = {
      action: "opened",
      pull_request: { number: 1, body: largeBody },
      sender: { id: 1, login: "octocat" },
    };
    const second = fixtureJob(workspaceId);
    second.event = { action: "synchronize", number: 1 };

    await workspace.enqueue(first, "event-chunks-1");
    await workspace.enqueue(second, "event-chunks-2");

    const stored = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{ chunks: number; total: number }>(
          `SELECT COUNT(*) AS chunks, SUM(LENGTH(content)) AS total
           FROM job_event_chunks WHERE job_id = ?`,
          first.id,
        )
        .one(),
    );
    expect(stored.chunks).toBeGreaterThan(1);
    expect(stored.total).toBeGreaterThan(largeBody.length);

    const messages = await assignments;
    const firstRun = messages[0]?.job as
      | { run_number?: number; event?: Record<string, unknown> }
      | undefined;
    const secondRun = messages[1]?.job as
      | { run_number?: number; event?: Record<string, unknown> }
      | undefined;
    expect(firstRun?.run_number).toBe(1);
    expect(secondRun?.run_number).toBe(2);
    expect(
      (firstRun?.event?.pull_request as { body?: string } | undefined)?.body,
    ).toBe(largeBody);
    expect(secondRun?.event).toEqual({ action: "synchronize", number: 1 });
    socket.close(1000, "test complete");
  });

  it("dispatches queued work after an agent completes the protocol handshake", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const job = fixtureJob(workspaceId);
    job.variables = { RUNTIME: "24" };
    await workspace.enqueue(job, "delivery-dispatch");

    const response = await workspace.fetch(
      new Request(
        `http://example.test/v1/workspaces/${workspaceId}/connect?role=agent&agent_id=mini-1`,
        {
          headers: { Upgrade: "websocket" },
        },
      ),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket;
    expect(socket).not.toBeNull();
    if (!socket) throw new Error("missing WebSocket");
    socket.accept();

    const messages = collectMessages(socket, 2);
    socket.send(
      JSON.stringify({
        type: "hello",
        hello: {
          protocol_version: 14,
          agent_id: "mini-1",
          name: "Test Mini",
          version: "0.1.0",
          labels: ["self-hosted", "macOS", "aarch64"],
          max_parallelism: 1,
        },
      }),
    );

    const [welcome, assignment] = await messages;
    expect(welcome).toMatchObject({ type: "welcome", protocol_version: 14 });
    expect(assignment).toMatchObject({
      type: "run_job",
      job: {
        id: job.id,
        checkout_token: "",
        github_api_version: "2026-03-10",
        variables: { RUNTIME: "24" },
      },
    });
    socket.close(1000, "test complete");
  });

  it("balances work toward the least-loaded compatible agent", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const first = await connectAgent(workspace, workspaceId, "mini-1", 2);
    const second = await connectAgent(workspace, workspaceId, "mini-2", 2);
    const firstAssignment = collectMessages(first, 1);
    const secondAssignment = collectMessages(second, 1);

    const firstJob = fixtureJob(workspaceId);
    const secondJob = fixtureJob(workspaceId);
    await workspace.enqueue(firstJob, "balanced-1");
    await workspace.enqueue(secondJob, "balanced-2");

    const assignments = [
      (await firstAssignment)[0],
      (await secondAssignment)[0],
    ];
    expect(assignments).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "run_job",
          job: expect.objectContaining({ id: firstJob.id }),
        }),
        expect.objectContaining({
          type: "run_job",
          job: expect.objectContaining({ id: secondJob.id }),
        }),
      ]),
    );
    first.close(1000, "test complete");
    second.close(1000, "test complete");
  });

  it("negotiates statically discovered runner requirements onto a matching Mac", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const basic = await connectAgentWithTargeting(
      workspace,
      workspaceId,
      "a-basic",
      1,
      ["self-hosted", "macOS", "ARM64"],
      null,
    );
    const specialized = await connectAgentWithTargeting(
      workspace,
      workspaceId,
      "z-specialized",
      1,
      ["self-hosted", "macOS", "ARM64", "xcode-16"],
      "release-minis",
    );
    const basicAssignment = collectMessages(basic, 1);
    const specializedAssignment = collectMessages(specialized, 1);
    const job = fixtureJob(workspaceId);
    await workspace.enqueue(job, "targeted-dispatch");
    await expect(basicAssignment).resolves.toEqual([
      expect.objectContaining({
        type: "run_job",
        job: expect.objectContaining({ id: job.id }),
      }),
    ]);

    const acknowledgement = collectMessages(basic, 1);
    basic.send(
      JSON.stringify({
        type: "job_rejected",
        message_id: crypto.randomUUID(),
        job_id: job.id,
        requirements: [
          {
            labels: ["self-hosted", "macOS", "xcode-16"],
            runner_group: "release-minis",
          },
        ],
        reason: "This Mac does not satisfy the static runner selector.",
      }),
    );
    await acknowledgement;
    await expect(specializedAssignment).resolves.toEqual([
      expect.objectContaining({
        type: "run_job",
        job: expect.objectContaining({ id: job.id }),
      }),
    ]);

    const assigned = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{
          status: string;
          agent_id: string;
          attempt_count: number;
          runner_requirements_json: string;
        }>(
          `SELECT status, agent_id, attempt_count, runner_requirements_json
           FROM jobs WHERE id = ?`,
          job.id,
        )
        .one(),
    );
    expect(assigned).toMatchObject({
      status: "assigned",
      agent_id: "z-specialized",
      attempt_count: 1,
    });
    expect(JSON.parse(assigned.runner_requirements_json)).toEqual([
      {
        labels: ["macos", "self-hosted", "xcode-16"],
        runner_group: "release-minis",
      },
    ]);
    basic.close(1000, "test complete");
    specialized.close(1000, "test complete");
  });

  it("does not let an incompatible queued job block later compatible work", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const basic = await connectAgent(workspace, workspaceId, "mini-basic", 1);
    const blocked = fixtureJob(workspaceId);
    const blockedAssignment = collectMessages(basic, 1);
    await workspace.enqueue(blocked, "blocked-target");
    await blockedAssignment;

    const acknowledgement = collectMessages(basic, 1);
    basic.send(
      JSON.stringify({
        type: "job_rejected",
        message_id: crypto.randomUUID(),
        job_id: blocked.id,
        requirements: [
          {
            labels: ["self-hosted", "macOS", "xcode-16"],
            runner_group: null,
          },
        ],
        reason: "Missing xcode-16.",
      }),
    );
    await acknowledgement;

    const compatible = fixtureJob(workspaceId);
    await workspace.enqueue(compatible, "compatible-behind-blocked");
    const beforeHeartbeat = await runInDurableObject(
      workspace,
      (_instance, state) =>
        state.storage.sql
          .exec<{
            id: string;
            status: string;
          }>("SELECT id, status FROM jobs ORDER BY created_at")
          .toArray(),
    );
    expect(beforeHeartbeat).toEqual(
      expect.arrayContaining([
        { id: blocked.id, status: "queued" },
        { id: compatible.id, status: "queued" },
      ]),
    );

    const recoveryMessages = collectMessages(basic, 2);
    basic.send(
      JSON.stringify({
        type: "heartbeat",
        message_id: crypto.randomUUID(),
        running_job_ids: [],
      }),
    );
    await expect(recoveryMessages).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "run_job",
          job: expect.objectContaining({ id: compatible.id }),
        }),
        expect.objectContaining({ type: "ack" }),
      ]),
    );
    const statuses = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{
          id: string;
          status: string;
          agent_id: string | null;
        }>("SELECT id, status, agent_id FROM jobs ORDER BY created_at")
        .toArray(),
    );
    expect(statuses).toEqual(
      expect.arrayContaining([
        { id: blocked.id, status: "queued", agent_id: null },
        { id: compatible.id, status: "assigned", agent_id: "mini-basic" },
      ]),
    );
    basic.close(1000, "test complete");
  });

  it("does not let an agent poison scheduling with requirements it satisfies", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const agent = await connectAgent(workspace, workspaceId, "mini-1", 1);
    const job = fixtureJob(workspaceId);
    const assignment = collectMessages(agent, 1);
    await workspace.enqueue(job, "invalid-rejection");
    await assignment;

    const response = collectMessages(agent, 2);
    agent.send(
      JSON.stringify({
        type: "job_rejected",
        message_id: crypto.randomUUID(),
        job_id: job.id,
        requirements: [
          {
            labels: ["macos-latest"],
            runner_group: null,
          },
        ],
        reason: "invalid rejection",
      }),
    );
    await expect(response).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "error",
          code: "invalid_job_rejection",
        }),
        expect.objectContaining({ type: "ack" }),
      ]),
    );
    const assigned = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{
          status: string;
          agent_id: string;
          attempt_count: number;
          runner_requirements_json: string;
        }>(
          `SELECT status, agent_id, attempt_count, runner_requirements_json
           FROM jobs WHERE id = ?`,
          job.id,
        )
        .one(),
    );
    expect(assigned).toEqual({
      status: "assigned",
      agent_id: "mini-1",
      attempt_count: 1,
      runner_requirements_json: "[]",
    });
    agent.close(1000, "test complete");
  });

  it("rejects duplicate agent IDs without releasing the active agent's jobs", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const first = await connectAgent(workspace, workspaceId, "mini-1", 1);
    const assignment = collectMessages(first, 1);
    const job = fixtureJob(workspaceId);
    await workspace.enqueue(job, "duplicate-agent");
    await assignment;

    const duplicateResponse = await workspace.fetch(
      new Request(
        `http://example.test/v1/workspaces/${workspaceId}/connect?role=agent&agent_id=mini-1`,
        { headers: { Upgrade: "websocket" } },
      ),
    );
    const duplicate = duplicateResponse.webSocket;
    if (!duplicate) throw new Error("missing duplicate WebSocket");
    duplicate.accept();
    const rejection = collectMessages(duplicate, 1);
    duplicate.send(
      JSON.stringify({
        type: "hello",
        hello: {
          protocol_version: 14,
          agent_id: "mini-1",
          name: "duplicate",
          version: "0.1.0",
          labels: ["macOS"],
          max_parallelism: 1,
        },
      }),
    );
    await expect(rejection).resolves.toEqual([
      expect.objectContaining({
        type: "error",
        code: "duplicate_agent_id",
      }),
    ]);

    const acknowledgement = collectMessages(first, 1);
    first.send(
      JSON.stringify({
        type: "heartbeat",
        message_id: crypto.randomUUID(),
        running_job_ids: [job.id],
      }),
    );
    await acknowledgement;
    const owner = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{
          status: string;
          agent_id: string;
        }>("SELECT status, agent_id FROM jobs WHERE id = ?", job.id)
        .one(),
    );
    expect(owner).toEqual({ status: "assigned", agent_id: "mini-1" });
    first.close(1000, "test complete");
  });

  it("renews leases only for jobs reported by the agent heartbeat", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const socket = await connectAgent(workspace, workspaceId, "mini-1", 2);
    const assignments = collectMessages(socket, 2);
    const firstJob = fixtureJob(workspaceId);
    const secondJob = fixtureJob(workspaceId);
    await workspace.enqueue(firstJob, "heartbeat-1");
    await workspace.enqueue(secondJob, "heartbeat-2");
    await assignments;

    await runInDurableObject(workspace, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE jobs SET lease_expires_at = 1 WHERE id IN (?, ?)",
        firstJob.id,
        secondJob.id,
      );
    });

    const acknowledgement = collectMessages(socket, 1);
    socket.send(
      JSON.stringify({
        type: "heartbeat",
        message_id: crypto.randomUUID(),
        running_job_ids: [firstJob.id],
      }),
    );
    await acknowledgement;

    await runInDurableObject(workspace, (_instance, state) => {
      const rows = state.storage.sql
        .exec<{
          id: string;
          lease_expires_at: number;
        }>("SELECT id, lease_expires_at FROM jobs ORDER BY id")
        .toArray();
      const first = rows.find((row) => row.id === firstJob.id);
      const second = rows.find((row) => row.id === secondJob.id);
      expect(first?.lease_expires_at).toBeGreaterThan(Date.now());
      expect(second?.lease_expires_at).toBe(1);
    });
    socket.close(1000, "test complete");
  });

  it("streams per-Mac heartbeat and active-job status to observers", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const observerResponse = await workspace.fetch(
      new Request(
        `http://example.test/v1/workspaces/${workspaceId}/connect?role=observer`,
        { headers: { Upgrade: "websocket" } },
      ),
    );
    const observer = observerResponse.webSocket;
    if (!observer) throw new Error("missing observer WebSocket");
    const initialSnapshot = collectMessages(observer, 1);
    observer.accept();
    await initialSnapshot;

    const online = collectMessageOfType(observer, "agent_online");
    const agent = await connectAgent(workspace, workspaceId, "mini-1", 2);
    await expect(online).resolves.toMatchObject({
      data: {
        agent: {
          agent_id: "mini-1",
          status: "online",
          active_jobs: [],
          available_capacity: 2,
        },
      },
    });

    const assignment = collectMessages(agent, 1);
    const job = fixtureJob(workspaceId);
    await workspace.enqueue(job, "observer-status");
    await assignment;
    const status = collectMessageOfType(observer, "agent_status");
    const acknowledgement = collectMessages(agent, 1);
    agent.send(
      JSON.stringify({
        type: "heartbeat",
        message_id: crypto.randomUUID(),
        running_job_ids: [job.id],
      }),
    );
    await acknowledgement;
    await expect(status).resolves.toMatchObject({
      type: "agent_status",
      data: {
        reported_running_job_ids: [job.id],
        agent: {
          agent_id: "mini-1",
          status: "online",
          active_jobs: [
            {
              job_id: job.id,
              status: "assigned",
            },
          ],
          available_capacity: 1,
        },
      },
    });
    agent.close(1000, "test complete");
    observer.close(1000, "test complete");
  });

  it("persists deployment intent and recovers ambiguous GitHub writes from the alarm", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    let runningStatusPosts = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          const permissions = JSON.parse(String(init?.body))
            .permissions as Record<string, string>;
          if (permissions.variables === "read") {
            return Response.json(installationToken("variables-token"));
          }
          if (permissions.checks === "write") {
            return Response.json(installationToken("check-token"));
          }
          if (permissions.deployments === "write") {
            return Response.json(installationToken("deployment-token"));
          }
          return Response.json(installationToken("checkout-token"));
        }
        if (url.pathname.endsWith("/actions/variables")) {
          return Response.json({ total_count: 0, variables: [] });
        }
        if (url.pathname.endsWith("/check-runs")) {
          return Response.json({ id: 44 }, { status: 201 });
        }
        if (url.pathname.endsWith("/deployments")) {
          if (init?.method === "GET") {
            return Response.json([
              {
                id: 91,
                payload: {
                  gitzero: {
                    job_id: currentJobId,
                    unit_id: "deploy / matrix[region=west]",
                  },
                },
              },
            ]);
          }
          return Response.json({ id: 91, payload: {} }, { status: 201 });
        }
        if (url.pathname.endsWith("/deployments/91/statuses")) {
          if (init?.method === "GET") {
            return Response.json([
              {
                state: "in_progress",
                description: "GitZero deployment is running.",
                environment: "production-west",
                environment_url: null,
              },
            ]);
          }
          const body = JSON.parse(String(init?.body)) as {
            state: string;
          };
          if (body.state === "in_progress") {
            runningStatusPosts += 1;
            return new Response("response lost", { status: 502 });
          }
          return Response.json({ id: 93 }, { status: 201 });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    let currentJobId = "";
    try {
      const workspaceId = crypto.randomUUID();
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const agent = await connectAgent(workspace, workspaceId, "mini-1", 1);
      const assignment = collectMessageOfType(agent, "run_job");
      const job = fixtureJob(workspaceId);
      currentJobId = job.id;
      job.installation_id = 7001;
      job.requires_github_token = true;
      job.report_to_github = true;
      job.event = { repository: { owner: { type: "User" } } };
      await workspace.enqueue(job, "deployment-lifecycle");
      await assignment;

      let acknowledgement = collectMessageOfType(agent, "ack");
      agent.send(
        JSON.stringify({
          type: "deployment_started",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          unit_id: "deploy / matrix[region=west]",
          environment: "production-west",
        }),
      );
      await acknowledgement;

      await vi.waitFor(async () => {
        const row = await runInDurableObject(workspace, (_instance, state) =>
          state.storage.sql
            .exec<{
              sync_needed: number;
              sync_attempt_count: number;
              last_error: string | null;
            }>(
              `SELECT sync_needed, sync_attempt_count, last_error
               FROM deployments WHERE job_id = ?`,
              job.id,
            )
            .one(),
        );
        expect(row).toMatchObject({
          sync_needed: 1,
          sync_attempt_count: 1,
        });
        expect(row.last_error).toContain("502");
      });
      await runInDurableObject(workspace, (_instance, state) => {
        state.storage.sql.exec(
          "UPDATE deployments SET sync_retry_at = 1 WHERE job_id = ?",
          job.id,
        );
      });
      await runDurableObjectAlarm(workspace);

      await vi.waitFor(async () => {
        const snapshot = (await workspace.getSnapshot()) as unknown as {
          deployments: Array<Record<string, unknown>>;
        };
        expect(snapshot.deployments).toEqual([
          expect.objectContaining({
            job_id: job.id,
            unit_id: "deploy / matrix[region=west]",
            desired_state: "in_progress",
            synced_state: "in_progress",
            github_deployment_id: 91,
            sync_status: "synced",
            sync_attempt_count: 2,
          }),
        ]);
      });

      acknowledgement = collectMessageOfType(agent, "ack");
      agent.send(
        JSON.stringify({
          type: "deployment_finished",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          unit_id: "deploy / matrix[region=west]",
          conclusion: "success",
          environment_url: "https://west.example.test/releases/42",
        }),
      );
      await acknowledgement;
      await vi.waitFor(async () => {
        const snapshot = (await workspace.getSnapshot()) as unknown as {
          deployments: Array<Record<string, unknown>>;
        };
        expect(snapshot.deployments[0]).toMatchObject({
          desired_state: "success",
          synced_state: "success",
          synced_environment_url: "https://west.example.test/releases/42",
          sync_status: "synced",
        });
      });

      expect(runningStatusPosts).toBe(1);
      expect(
        requests.filter(
          (request) =>
            request.url.pathname.endsWith("/deployments") &&
            request.init?.method === "POST",
        ),
      ).toHaveLength(1);
      const terminalStatus = requests.find((request) => {
        if (
          !request.url.pathname.endsWith("/deployments/91/statuses") ||
          request.init?.method !== "POST"
        ) {
          return false;
        }
        return (
          (JSON.parse(String(request.init.body)) as { state: string }).state ===
          "success"
        );
      });
      expect(JSON.parse(String(terminalStatus?.init?.body))).toEqual({
        state: "success",
        environment: "production-west",
        description: "GitZero deployment completed successfully.",
        environment_url: "https://west.example.test/releases/42",
        auto_inactive: true,
      });

      await runInDurableObject(workspace, (_instance, state) => {
        state.storage.sql.exec(
          "UPDATE jobs SET status = 'completed' WHERE id = ?",
          job.id,
        );
      });
      const rejected = collectMessageOfType(agent, "error");
      agent.send(
        JSON.stringify({
          type: "deployment_started",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          unit_id: "late-deploy",
          environment: "production-west",
        }),
      );
      await expect(rejected).resolves.toMatchObject({
        code: "deployment_run_completed",
      });
      const lateDeployments = await runInDurableObject(
        workspace,
        (_instance, state) =>
          state.storage.sql
            .exec<{
              count: number;
            }>(
              "SELECT COUNT(*) AS count FROM deployments WHERE job_id = ? AND unit_id = 'late-deploy'",
              job.id,
            )
            .one().count,
      );
      expect(lateDeployments).toBe(0);
      agent.close(1000, "test complete");
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
    }
  });

  it("persists and broadcasts the agent's final Markdown summary", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const observerResponse = await workspace.fetch(
      new Request(
        `http://example.test/v1/workspaces/${workspaceId}/connect?role=observer`,
        { headers: { Upgrade: "websocket" } },
      ),
    );
    const observer = observerResponse.webSocket;
    if (!observer) throw new Error("missing observer WebSocket");
    const snapshot = collectMessages(observer, 1);
    observer.accept();
    await snapshot;

    const agent = await connectAgent(workspace, workspaceId, "mini-1", 1);
    const assignment = collectMessages(agent, 1);
    const job = fixtureJob(workspaceId);
    await workspace.enqueue(job, "summary-completion");
    await assignment;

    let acknowledgement = collectMessages(agent, 1);
    agent.send(
      JSON.stringify({
        type: "job_started",
        message_id: crypto.randomUUID(),
        job_id: job.id,
      }),
    );
    await acknowledgement;

    const markdown =
      "GitZero completed 1 step successfully.\n\n## Tests\n\n| suite | result |\n| --- | --- |\n| unit | ✅ |";
    const annotation = {
      path: "src/lib.rs",
      start_line: 7,
      end_line: 7,
      start_column: 2,
      end_column: 5,
      annotation_level: "warning",
      message: "check this expression",
      title: "Compiler",
    };
    const finished = collectMessageOfType(observer, "job_finished");
    acknowledgement = collectMessages(agent, 1);
    agent.send(
      JSON.stringify({
        type: "job_finished",
        message_id: crypto.randomUUID(),
        job_id: job.id,
        conclusion: "success",
        summary: markdown,
        annotations: [annotation],
      }),
    );
    await acknowledgement;
    await expect(finished).resolves.toMatchObject({
      type: "job_finished",
      data: {
        job_id: job.id,
        conclusion: "success",
        summary: markdown,
        annotation_count: 1,
      },
    });

    const completed = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{
          status: string;
          conclusion: string;
          summary: string;
        }>("SELECT status, conclusion, summary FROM jobs WHERE id = ?", job.id)
        .one(),
    );
    expect(completed).toEqual({
      status: "completed",
      conclusion: "success",
      summary: markdown,
    });
    const annotations = await runInDurableObject(
      workspace,
      (_instance, state) =>
        state.storage.sql
          .exec<{ annotation_json: string }>(
            `SELECT annotation_json FROM job_annotations
             WHERE job_id = ? ORDER BY annotation_index`,
            job.id,
          )
          .toArray()
          .map((row) => JSON.parse(row.annotation_json)),
    );
    expect(annotations).toEqual([annotation]);
    agent.close(1000, "test complete");
    observer.close(1000, "test complete");
  });

  it("terminates a job after repeated infrastructure lease failures", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const socket = await connectAgent(workspace, workspaceId, "mini-1", 1);
    const assignment = collectMessages(socket, 1);
    const job = fixtureJob(workspaceId);
    await workspace.enqueue(job, "retry-limit");
    await assignment;

    await runInDurableObject(workspace, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE jobs SET attempt_count = 3, lease_expires_at = 1 WHERE id = ?",
        job.id,
      );
    });
    await expect(runDurableObjectAlarm(workspace)).resolves.toBe(true);

    const completed = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{
          id: string;
          status: string;
          conclusion: string;
          attempt_count: number;
          summary: string;
        }>(
          "SELECT id, status, conclusion, attempt_count, summary FROM jobs WHERE id = ?",
          job.id,
        )
        .one(),
    );
    expect(completed).toEqual(
      expect.objectContaining({
        id: job.id,
        status: "completed",
        conclusion: "failure",
        attempt_count: 3,
        summary: expect.stringContaining("3 agent assignment attempts"),
      }),
    );
    socket.close(1000, "test complete");
  });

  it("denies repository and workflow credentials for runs without token authority", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const agent = await connectAgent(workspace, workspaceId, "mini-1", 1);
    const job = fixtureJob(workspaceId);
    const assignment = collectMessageOfType(agent, "run_job");
    await workspace.enqueue(job, "no-token-authority");
    await assignment;

    const repositoryRequestId = crypto.randomUUID();
    const repositoryDenied = collectMessageOfType(
      agent,
      "repository_token_denied",
    );
    agent.send(
      JSON.stringify({
        type: "repository_token_request",
        message_id: crypto.randomUUID(),
        job_id: job.id,
        request_id: repositoryRequestId,
        purpose: "source",
        owner: job.repository.owner,
        repository: job.repository.name,
      }),
    );
    await expect(repositoryDenied).resolves.toEqual({
      type: "repository_token_denied",
      request_id: repositoryRequestId,
      reason: "The parent GitZero run is not authorized for GitHub tokens.",
    });

    const workflowRequestId = crypto.randomUUID();
    const workflowDenied = collectMessageOfType(agent, "workflow_token_denied");
    agent.send(
      JSON.stringify({
        type: "workflow_token_request",
        message_id: crypto.randomUUID(),
        job_id: job.id,
        request_id: workflowRequestId,
        read_permissions: ["contents"],
        write_permissions: [],
      }),
    );
    await expect(workflowDenied).resolves.toEqual({
      type: "workflow_token_denied",
      request_id: workflowRequestId,
      reason: "The parent GitZero run is not authorized for GitHub tokens.",
    });
    const secretRequestId = crypto.randomUUID();
    const secretDenied = collectMessageOfType(agent, "secret_denied");
    agent.send(
      JSON.stringify({
        type: "secret_request",
        message_id: crypto.randomUUID(),
        job_id: job.id,
        request_id: secretRequestId,
        unit_id: "manual-job",
        environment: null,
      }),
    );
    await expect(secretDenied).resolves.toEqual({
      type: "secret_denied",
      request_id: secretRequestId,
      reason:
        "Managed secrets are unavailable to manual, fork, Dependabot, or unverified pull request runs.",
    });
    agent.close(1000, "test complete");
  });

  it("returns target- and permission-scoped tokens only for authorized active runs", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          const body = JSON.parse(String(init?.body)) as {
            repositories: string[];
            permissions: Record<string, string>;
          };
          return Response.json(
            installationToken(
              body.permissions.administration === "read"
                ? "policy-token"
                : body.permissions.environments === "read"
                  ? "environment-token"
                  : body.repositories[0] === "caller" &&
                      body.permissions.pull_requests === "read"
                    ? "source-token"
                    : body.repositories[0] === "caller"
                      ? "workflow-write-token"
                      : "target-contents-token",
            ),
          );
        }
        if (
          url.pathname.endsWith("/actions/variables") ||
          url.pathname.endsWith("/actions/organization-variables")
        ) {
          return Response.json({ total_count: 0, variables: [] });
        }
        return Response.json({ access_level: "organization" });
      }),
    );

    try {
      const workspaceId = "7010";
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const agent = await connectAgent(workspace, workspaceId, "mini-1", 1);
      const assignment = collectMessageOfType(agent, "run_job");
      const job = fixtureJob(workspaceId);
      job.installation_id = 7010;
      job.requires_github_token = true;
      job.repository.owner = "acme";
      job.repository.name = "caller";
      job.repository.clone_url = "https://github.com/acme/caller.git";
      job.event = {
        repository: {
          full_name: "acme/caller",
          owner: { type: "Organization" },
        },
        pull_request: {
          head: { repo: { full_name: "acme/caller" } },
          user: { login: "trusted-author" },
        },
      };
      await workspace.enqueue(job, "shared-repository-token");
      await assignment;
      requests.length = 0;

      const requestId = crypto.randomUUID();
      const granted = collectMessageOfType(agent, "repository_token_granted");
      agent.send(
        JSON.stringify({
          type: "repository_token_request",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          request_id: requestId,
          purpose: "shared_source",
          owner: "ACME",
          repository: "shared-actions",
        }),
      );
      await expect(granted).resolves.toEqual({
        type: "repository_token_granted",
        request_id: requestId,
        token: "target-contents-token",
        expires_at_epoch_seconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
      });
      expect(requests.map((request) => request.url.pathname)).toEqual([
        "/app/installations/7010/access_tokens",
        "/repos/ACME/shared-actions/actions/permissions/access",
        "/app/installations/7010/access_tokens",
      ]);
      expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
        repositories: ["shared-actions"],
        permissions: { administration: "read" },
      });
      expect(JSON.parse(String(requests[2]?.init?.body))).toEqual({
        repositories: ["shared-actions"],
        permissions: { contents: "read" },
      });

      const checkoutRequestId = crypto.randomUUID();
      const checkoutGranted = collectMessageOfType(
        agent,
        "repository_token_granted",
      );
      agent.send(
        JSON.stringify({
          type: "repository_token_request",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          request_id: checkoutRequestId,
          purpose: "checkout",
          owner: "acme",
          repository: "private-dependency",
        }),
      );
      await expect(checkoutGranted).resolves.toEqual({
        type: "repository_token_granted",
        request_id: checkoutRequestId,
        token: "target-contents-token",
        expires_at_epoch_seconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
      });
      expect(JSON.parse(String(requests[3]?.init?.body))).toEqual({
        repositories: ["private-dependency"],
        permissions: { contents: "read" },
      });

      const workflowRequestId = crypto.randomUUID();
      const workflowGranted = collectMessageOfType(
        agent,
        "workflow_token_granted",
      );
      agent.send(
        JSON.stringify({
          type: "workflow_token_request",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          request_id: workflowRequestId,
          read_permissions: ["contents"],
          write_permissions: ["checks"],
        }),
      );
      await expect(workflowGranted).resolves.toEqual({
        type: "workflow_token_granted",
        request_id: workflowRequestId,
        token: "workflow-write-token",
        expires_at_epoch_seconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
      });
      expect(JSON.parse(String(requests[4]?.init?.body))).toEqual({
        repositories: ["caller"],
        permissions: { checks: "write", contents: "read" },
      });

      for (const [purpose, token, permissions] of [
        ["source", "source-token", { contents: "read", pull_requests: "read" }],
        [
          "environment",
          "environment-token",
          { actions: "read", environments: "read" },
        ],
      ] as const) {
        const internalRequestId = crypto.randomUUID();
        const internalGranted = collectMessageOfType(
          agent,
          "repository_token_granted",
        );
        agent.send(
          JSON.stringify({
            type: "repository_token_request",
            message_id: crypto.randomUUID(),
            job_id: job.id,
            request_id: internalRequestId,
            purpose,
            owner: "ACME",
            repository: "CALLER",
          }),
        );
        await expect(internalGranted).resolves.toEqual({
          type: "repository_token_granted",
          request_id: internalRequestId,
          token,
          expires_at_epoch_seconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
        });
        expect(JSON.parse(String(requests.at(-1)?.init?.body))).toEqual({
          repositories: ["caller"],
          permissions,
        });
      }

      const mismatchedSourceRequestId = crypto.randomUUID();
      const mismatchedSourceDenied = collectMessageOfType(
        agent,
        "repository_token_denied",
      );
      agent.send(
        JSON.stringify({
          type: "repository_token_request",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          request_id: mismatchedSourceRequestId,
          purpose: "source",
          owner: "acme",
          repository: "another-repository",
        }),
      );
      await expect(mismatchedSourceDenied).resolves.toEqual({
        type: "repository_token_denied",
        request_id: mismatchedSourceRequestId,
        reason:
          "Internal repository credential access was denied because the target does not match the active run.",
      });
      expect(requests).toHaveLength(7);

      const deniedRequestId = crypto.randomUUID();
      const denied = collectMessageOfType(agent, "repository_token_denied");
      agent.send(
        JSON.stringify({
          type: "repository_token_request",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          request_id: deniedRequestId,
          purpose: "checkout",
          owner: "another-owner",
          repository: "private-action",
        }),
      );
      await expect(denied).resolves.toMatchObject({
        type: "repository_token_denied",
        request_id: deniedRequestId,
        reason: expect.stringContaining("access was denied"),
      });
      expect(requests).toHaveLength(7);
      agent.close(1000, "test complete");
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
    }
  });

  it("downgrades untrusted or incomplete workflow writes before minting tokens", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/actions/variables")) {
          return Response.json({ total_count: 0, variables: [] });
        }
        if (url.pathname.endsWith("/check-runs")) {
          return Response.json({ id: 711 }, { status: 201 });
        }
        return Response.json(installationToken("downgraded-read-token"));
      }),
    );

    try {
      const workspaceId = crypto.randomUUID();
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const agent = await connectAgent(workspace, workspaceId, "mini-1", 3);
      const jobs = [
        {
          job: fixtureJob(workspaceId),
          headRepository: "contributor/caller",
          author: "contributor",
        },
        {
          job: fixtureJob(workspaceId),
          headRepository: "acme/caller",
          author: "dependabot[bot]",
        },
        {
          job: fixtureJob(workspaceId),
          headRepository: "acme/caller",
          author: "",
        },
      ];
      for (const { job, headRepository, author } of jobs) {
        job.installation_id = 7011;
        job.requires_github_token = true;
        job.report_to_github = true;
        job.repository.owner = "acme";
        job.repository.name = "caller";
        job.repository.clone_url = "https://github.com/acme/caller.git";
        job.event = {
          repository: { full_name: "acme/caller" },
          pull_request: {
            head: { repo: { full_name: headRepository } },
            user: { login: author },
          },
        };
        const assignment = collectMessageOfType(agent, "run_job");
        await workspace.enqueue(job, `write-downgrade:${author}`);
        await assignment;
      }
      requests.length = 0;

      for (const { job } of jobs) {
        const requestId = crypto.randomUUID();
        const granted = collectMessageOfType(agent, "workflow_token_granted");
        agent.send(
          JSON.stringify({
            type: "workflow_token_request",
            message_id: crypto.randomUUID(),
            job_id: job.id,
            request_id: requestId,
            read_permissions: ["contents"],
            write_permissions: ["checks"],
          }),
        );
        await expect(granted).resolves.toEqual({
          type: "workflow_token_granted",
          request_id: requestId,
          token: "downgraded-read-token",
          expires_at_epoch_seconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
        });
        const secretRequestId = crypto.randomUUID();
        const denied = collectMessageOfType(agent, "secret_denied");
        agent.send(
          JSON.stringify({
            type: "secret_request",
            message_id: crypto.randomUUID(),
            job_id: job.id,
            request_id: secretRequestId,
            unit_id: "untrusted-job",
            environment: null,
          }),
        );
        await expect(denied).resolves.toMatchObject({
          type: "secret_denied",
          request_id: secretRequestId,
          reason: expect.stringContaining("unavailable"),
        });
      }
      expect(requests).toHaveLength(3);
      for (const request of requests) {
        expect(JSON.parse(String(request.init?.body))).toEqual({
          repositories: ["caller"],
          permissions: { checks: "read", contents: "read" },
        });
      }
      agent.close(1000, "test complete");
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
    }
  });

  it("serializes case-insensitive concurrency groups and cancels superseded units", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const first = await connectAgent(workspace, workspaceId, "mini-1", 1);
    const second = await connectAgent(workspace, workspaceId, "mini-2", 1);
    const third = await connectAgent(workspace, workspaceId, "mini-3", 1);
    const firstJob = fixtureJob(workspaceId);
    const secondJob = fixtureJob(workspaceId);
    const thirdJob = fixtureJob(workspaceId);
    const firstAssignment = collectMessageOfType(first, "run_job");
    const secondAssignment = collectMessageOfType(second, "run_job");
    const thirdAssignment = collectMessageOfType(third, "run_job");
    await workspace.enqueue(firstJob, "concurrency-first");
    await workspace.enqueue(secondJob, "concurrency-second");
    await workspace.enqueue(thirdJob, "concurrency-third");
    await expect(firstAssignment).resolves.toMatchObject({
      job: { id: firstJob.id },
    });
    await expect(secondAssignment).resolves.toMatchObject({
      job: { id: secondJob.id },
    });
    await expect(thirdAssignment).resolves.toMatchObject({
      job: { id: thirdJob.id },
    });

    const firstRequest = crypto.randomUUID();
    const firstGranted = collectMessageOfType(first, "concurrency_granted");
    sendConcurrencyAcquire(first, firstJob.id, firstRequest, "Deploy", false);
    await expect(firstGranted).resolves.toMatchObject({
      request_id: firstRequest,
    });

    const secondRequest = crypto.randomUUID();
    const secondAcknowledged = collectMessageOfType(second, "ack");
    sendConcurrencyAcquire(
      second,
      secondJob.id,
      secondRequest,
      "deploy",
      false,
    );
    await secondAcknowledged;

    const thirdRequest = crypto.randomUUID();
    const secondCancelled = collectMessageOfType(
      second,
      "concurrency_cancelled",
    );
    const thirdAcknowledged = collectMessageOfType(third, "ack");
    sendConcurrencyAcquire(third, thirdJob.id, thirdRequest, "DEPLOY", false);
    await expect(secondCancelled).resolves.toMatchObject({
      request_id: secondRequest,
    });
    await thirdAcknowledged;

    const thirdGranted = collectMessageOfType(third, "concurrency_granted");
    sendConcurrencyRelease(first, firstJob.id, firstRequest);
    await expect(thirdGranted).resolves.toMatchObject({
      request_id: thirdRequest,
    });

    const replacementRequest = crypto.randomUUID();
    const thirdCancelled = collectMessageOfType(third, "concurrency_cancelled");
    sendConcurrencyAcquire(
      second,
      secondJob.id,
      replacementRequest,
      "deploy",
      true,
    );
    await expect(thirdCancelled).resolves.toMatchObject({
      request_id: thirdRequest,
    });
    const replacementGranted = collectMessageOfType(
      second,
      "concurrency_granted",
    );
    sendConcurrencyRelease(third, thirdJob.id, thirdRequest);
    await expect(replacementGranted).resolves.toMatchObject({
      request_id: replacementRequest,
    });

    const snapshot = (await workspace.getSnapshot()) as unknown as {
      concurrency: Array<Record<string, unknown>>;
    };
    expect(snapshot.concurrency).toEqual([
      expect.objectContaining({
        request_id: replacementRequest,
        status: "active",
        group: "deploy",
      }),
    ]);
    first.close(1000, "test complete");
    second.close(1000, "test complete");
    third.close(1000, "test complete");
  });

  it("promotes queue max concurrency waiters in FIFO order", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const agents = await Promise.all([
      connectAgent(workspace, workspaceId, "mini-a", 1),
      connectAgent(workspace, workspaceId, "mini-b", 1),
      connectAgent(workspace, workspaceId, "mini-c", 1),
    ]);
    const jobs = [
      fixtureJob(workspaceId),
      fixtureJob(workspaceId),
      fixtureJob(workspaceId),
    ];
    const assignments = agents.map((agent) =>
      collectMessageOfType(agent, "run_job"),
    );
    for (const [index, job] of jobs.entries()) {
      await workspace.enqueue(job, `fifo-${index}`);
    }
    const assignmentMessages = await Promise.all(assignments);
    const agentByJob = new Map(
      assignmentMessages.map((assignment, index) => [
        (assignment.job as { id: string }).id,
        agents[index] as WebSocket,
      ]),
    );
    const assignedAgents = jobs.map(
      (job) => agentByJob.get(job.id) as WebSocket,
    );

    const requests = jobs.map(() => crypto.randomUUID());
    const firstGranted = collectMessageOfType(
      assignedAgents[0] as WebSocket,
      "concurrency_granted",
    );
    for (const [index, agent] of assignedAgents.entries()) {
      sendConcurrencyAcquire(
        agent,
        (jobs[index] as QueuedJob).id,
        requests[index] as string,
        "release",
        false,
        "max",
      );
    }
    await expect(firstGranted).resolves.toMatchObject({
      request_id: requests[0],
    });

    const secondGranted = collectMessageOfType(
      assignedAgents[1] as WebSocket,
      "concurrency_granted",
    );
    sendConcurrencyRelease(
      assignedAgents[0] as WebSocket,
      (jobs[0] as QueuedJob).id,
      requests[0] as string,
    );
    await expect(secondGranted).resolves.toMatchObject({
      request_id: requests[1],
    });
    const thirdGranted = collectMessageOfType(
      assignedAgents[2] as WebSocket,
      "concurrency_granted",
    );
    sendConcurrencyRelease(
      assignedAgents[1] as WebSocket,
      (jobs[1] as QueuedJob).id,
      requests[1] as string,
    );
    await expect(thirdGranted).resolves.toMatchObject({
      request_id: requests[2],
    });
    for (const agent of agents) agent.close(1000, "test complete");
  });

  it("isolates identical concurrency groups between repositories", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const first = await connectAgent(workspace, workspaceId, "mini-a", 1);
    const second = await connectAgent(workspace, workspaceId, "mini-b", 1);
    const firstJob = fixtureJob(workspaceId);
    const secondJob = fixtureJob(workspaceId);
    secondJob.repository.name = "another-repository";
    const firstAssignment = collectMessageOfType(first, "run_job");
    const secondAssignment = collectMessageOfType(second, "run_job");
    await workspace.enqueue(firstJob, "repository-concurrency-first");
    await workspace.enqueue(secondJob, "repository-concurrency-second");
    const assignmentMessages = await Promise.all([
      firstAssignment,
      secondAssignment,
    ]);
    const agentByJob = new Map([
      [(assignmentMessages[0]?.job as { id: string }).id, first],
      [(assignmentMessages[1]?.job as { id: string }).id, second],
    ]);
    const firstOwner = agentByJob.get(firstJob.id) as WebSocket;
    const secondOwner = agentByJob.get(secondJob.id) as WebSocket;

    const firstRequest = crypto.randomUUID();
    const secondRequest = crypto.randomUUID();
    const firstGranted = collectMessageOfType(
      firstOwner,
      "concurrency_granted",
    );
    const secondGranted = collectMessageOfType(
      secondOwner,
      "concurrency_granted",
    );
    sendConcurrencyAcquire(
      firstOwner,
      firstJob.id,
      firstRequest,
      "deploy",
      false,
    );
    sendConcurrencyAcquire(
      secondOwner,
      secondJob.id,
      secondRequest,
      "DEPLOY",
      false,
    );
    await expect(firstGranted).resolves.toMatchObject({
      request_id: firstRequest,
    });
    await expect(secondGranted).resolves.toMatchObject({
      request_id: secondRequest,
    });

    sendConcurrencyRelease(firstOwner, firstJob.id, firstRequest);
    sendConcurrencyRelease(secondOwner, secondJob.id, secondRequest);
    first.close(1000, "test complete");
    second.close(1000, "test complete");
  });

  it("times out queued work that never receives a compatible agent", async () => {
    const workspaceId = crypto.randomUUID();
    const workspace = env.WORKSPACES.getByName(workspaceId);
    const job = fixtureJob(workspaceId);
    await workspace.enqueue(job, "queue-timeout");
    await runInDurableObject(workspace, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE jobs SET queue_expires_at = 1 WHERE id = ?",
        job.id,
      );
    });
    await expect(runDurableObjectAlarm(workspace)).resolves.toBe(true);

    const completed = await runInDurableObject(workspace, (_instance, state) =>
      state.storage.sql
        .exec<{
          id: string;
          status: string;
          conclusion: string;
          summary: string;
        }>(
          "SELECT id, status, conclusion, summary FROM jobs WHERE id = ?",
          job.id,
        )
        .one(),
    );
    expect(completed).toEqual(
      expect.objectContaining({
        id: job.id,
        status: "completed",
        conclusion: "timed_out",
        summary: expect.stringContaining("queue deadline"),
      }),
    );
  });

  it("snapshots trusted repository secrets and resolves current environment precedence on demand", async () => {
    const privateKey = await testPrivateKeyPem();
    const originals = {
      appId: env.GITHUB_APP_ID,
      privateKey: env.GITHUB_APP_PRIVATE_KEY,
      encryptionKey: env.SECRETS_ENCRYPTION_KEY,
    };
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    Reflect.set(
      env,
      "SECRETS_ENCRYPTION_KEY",
      "workspace-secret-encryption-".padEnd(48, "e"),
    );
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input));
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("managed-secret-test-token"));
        }
        if (
          url.pathname.endsWith("/actions/variables") ||
          url.pathname.endsWith("/actions/organization-variables")
        ) {
          return Response.json({ total_count: 0, variables: [] });
        }
        if (url.pathname.endsWith("/check-runs")) {
          return Response.json({ id: 412 }, { status: 201 });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    try {
      const workspaceId = "7412";
      const workspace = env.WORKSPACES.getByName(workspaceId);
      const put = (input: ManagedSecretInput) =>
        workspace.putManagedSecret(workspaceId, input);
      await put({
        scope: "organization",
        owner: "octocat",
        name: "ORG_ONLY",
        value: "organization-secret-value",
        visibility: "all",
        selected_repositories: [],
      });
      await put({
        scope: "organization",
        owner: "octocat",
        name: "SHARED",
        value: "organization-shared-value",
        visibility: "all",
        selected_repositories: [],
      });
      await put({
        scope: "organization",
        owner: "octocat",
        name: "PRIVATE_ONLY",
        value: "private-repository-secret-value",
        visibility: "private",
        selected_repositories: [],
      });
      await put({
        scope: "organization",
        owner: "octocat",
        name: "SELECTED_OTHER",
        value: "must-not-be-granted",
        visibility: "selected",
        selected_repositories: ["octocat/another-repository"],
      });
      await put({
        scope: "repository",
        owner: "octocat",
        repository: "Hello-World",
        name: "SHARED",
        value: "repository-shared-value",
      });
      await put({
        scope: "repository",
        owner: "octocat",
        repository: "Hello-World",
        name: "QUEUED",
        value: "repository-queued-value",
      });
      await put({
        scope: "environment",
        owner: "octocat",
        repository: "Hello-World",
        environment: "Production",
        name: "SHARED",
        value: "environment-original-value",
      });

      const agent = await connectAgent(
        workspace,
        workspaceId,
        "mini-secret",
        1,
      );
      const assignment = collectMessageOfType(agent, "run_job");
      const job = fixtureJob(workspaceId);
      job.installation_id = Number(workspaceId);
      job.requires_github_token = true;
      job.report_to_github = true;
      job.event = {
        repository: {
          full_name: "octocat/Hello-World",
          private: true,
          owner: { type: "Organization" },
        },
        pull_request: {
          head: { repo: { full_name: "octocat/Hello-World" } },
          user: { login: "trusted-author" },
        },
      };
      await workspace.enqueue(job, "managed-secret-snapshot");
      await put({
        scope: "repository",
        owner: "octocat",
        repository: "Hello-World",
        name: "QUEUED",
        value: "repository-updated-after-queue",
      });
      await put({
        scope: "environment",
        owner: "octocat",
        repository: "Hello-World",
        environment: "Production",
        name: "SHARED",
        value: "environment-current-value",
      });
      await expect(assignment).resolves.toMatchObject({
        type: "run_job",
        job: { id: job.id, managed_secrets: true },
      });

      const stored = await runInDurableObject(workspace, (_instance, state) =>
        state.storage.sql
          .exec<{ ciphertext: string; nonce: string }>(
            `SELECT ciphertext, nonce FROM managed_secrets
               UNION ALL SELECT ciphertext, nonce FROM job_secrets`,
          )
          .toArray(),
      );
      const serializedStorage = JSON.stringify(stored);
      for (const plaintext of [
        "organization-secret-value",
        "private-repository-secret-value",
        "must-not-be-granted",
        "repository-shared-value",
        "repository-queued-value",
        "repository-updated-after-queue",
        "environment-current-value",
      ]) {
        expect(serializedStorage).not.toContain(plaintext);
      }

      const requestId = crypto.randomUUID();
      const granted = collectMessageOfType(agent, "secret_granted");
      agent.send(
        JSON.stringify({
          type: "secret_request",
          message_id: crypto.randomUUID(),
          job_id: job.id,
          request_id: requestId,
          unit_id: "deploy-production",
          environment: "Production",
        }),
      );
      await expect(granted).resolves.toEqual({
        type: "secret_granted",
        request_id: requestId,
        secrets: {
          ORG_ONLY: "organization-secret-value",
          PRIVATE_ONLY: "private-repository-secret-value",
          QUEUED: "repository-queued-value",
          SHARED: "environment-current-value",
        },
      });
      agent.close(1000, "test complete");
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originals.appId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originals.privateKey);
      Reflect.set(env, "SECRETS_ENCRYPTION_KEY", originals.encryptionKey);
    }
  });
});

async function connectAgent(
  workspace: DurableObjectStub<import("../src/workspace").Workspace>,
  workspaceId: string,
  agentId: string,
  maxParallelism: number,
): Promise<WebSocket> {
  return connectAgentWithTargeting(
    workspace,
    workspaceId,
    agentId,
    maxParallelism,
    ["self-hosted", "macOS", "aarch64"],
    null,
  );
}

async function connectAgentWithTargeting(
  workspace: DurableObjectStub<import("../src/workspace").Workspace>,
  workspaceId: string,
  agentId: string,
  maxParallelism: number,
  labels: string[],
  runnerGroup: string | null,
): Promise<WebSocket> {
  const response = await workspace.fetch(
    new Request(
      `http://example.test/v1/workspaces/${workspaceId}/connect?role=agent&agent_id=${agentId}`,
      { headers: { Upgrade: "websocket" } },
    ),
  );
  const socket = response.webSocket;
  if (!socket) throw new Error("missing WebSocket");
  socket.accept();
  const welcome = collectMessages(socket, 1);
  socket.send(
    JSON.stringify({
      type: "hello",
      hello: {
        protocol_version: 14,
        agent_id: agentId,
        name: agentId,
        version: "0.1.0",
        labels,
        runner_group: runnerGroup,
        max_parallelism: maxParallelism,
      },
    }),
  );
  await welcome;
  return socket;
}

function collectMessages(
  socket: WebSocket,
  count: number,
): Promise<Record<string, unknown>[]> {
  return new Promise((resolve, reject) => {
    const messages: Record<string, unknown>[] = [];
    socket.addEventListener("message", (event) => {
      try {
        messages.push(
          JSON.parse(String(event.data)) as Record<string, unknown>,
        );
        if (messages.length === count) resolve(messages);
      } catch (error) {
        reject(error);
      }
    });
    socket.addEventListener("error", () =>
      reject(new Error("WebSocket failed")),
    );
  });
}

function collectMessageOfType(
  socket: WebSocket,
  type: string,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    socket.addEventListener("message", (event) => {
      try {
        const message = JSON.parse(String(event.data)) as Record<
          string,
          unknown
        >;
        if (message.type === type) resolve(message);
      } catch (error) {
        reject(error);
      }
    });
    socket.addEventListener("error", () =>
      reject(new Error("WebSocket failed")),
    );
  });
}

function sendConcurrencyAcquire(
  socket: WebSocket,
  jobId: string,
  requestId: string,
  group: string,
  cancelInProgress: boolean,
  queue: "single" | "max" = "single",
): void {
  socket.send(
    JSON.stringify({
      type: "concurrency_acquire",
      message_id: crypto.randomUUID(),
      job_id: jobId,
      request_id: requestId,
      unit_id: `unit-${requestId}`,
      group,
      cancel_in_progress: cancelInProgress,
      queue,
    }),
  );
}

function sendConcurrencyRelease(
  socket: WebSocket,
  jobId: string,
  requestId: string,
): void {
  socket.send(
    JSON.stringify({
      type: "concurrency_release",
      message_id: crypto.randomUUID(),
      job_id: jobId,
      request_id: requestId,
    }),
  );
}

function fixtureJob(workspaceId: string): QueuedJob {
  return {
    id: crypto.randomUUID(),
    workspace_id: workspaceId,
    installation_id: 0,
    run_number: 0,
    repository: {
      owner: "octocat",
      name: "Hello-World",
      clone_url: "https://github.com/octocat/Hello-World.git",
    },
    pull_request: {
      number: 1,
      action: "opened",
      head_sha: "0123456789012345678901234567890123456789",
      base_sha: "abcdefabcdefabcdefabcdefabcdefabcdefabcd",
      merge_sha: "1111111111111111111111111111111111111111",
      execution_ref: "refs/pull/1/merge",
      head_ref: "feature",
      base_ref: "main",
    },
    check_run_id: null,
    event: {},
    environment: {},
    variables: {},
    requires_github_token: false,
    report_to_github: false,
  };
}

async function testPrivateKeyPem(): Promise<string> {
  const pair = await crypto.subtle.generateKey(
    {
      name: "RSASSA-PKCS1-v1_5",
      modulusLength: 2_048,
      publicExponent: new Uint8Array([1, 0, 1]),
      hash: "SHA-256",
    },
    true,
    ["sign", "verify"],
  );
  if (!("privateKey" in pair)) throw new Error("expected an RSA key pair");
  const exported = await crypto.subtle.exportKey("pkcs8", pair.privateKey);
  if (!(exported instanceof ArrayBuffer)) {
    throw new Error("expected a PKCS8 ArrayBuffer");
  }
  const encoded = new Uint8Array(exported);
  let binary = "";
  for (const byte of encoded) binary += String.fromCharCode(byte);
  const base64 = btoa(binary);
  const lines = base64.match(/.{1,64}/g);
  if (!lines) throw new Error("failed to encode test private key");
  return `-----BEGIN PRIVATE KEY-----\n${lines.join("\n")}\n-----END PRIVATE KEY-----`;
}
