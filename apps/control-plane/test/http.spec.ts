import { env, exports } from "cloudflare:workers";
import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("HTTP entrypoint", () => {
  it("reports health", async () => {
    const response = await exports.default.fetch(
      new Request("http://example.test/healthz"),
    );
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      status: "ok",
      service: "gitzero-control-plane",
    });
  });

  it("keeps deployment readiness authenticated and validates local credentials", async () => {
    const unauthorized = await exports.default.fetch(
      new Request("http://example.test/readyz"),
    );
    expect(unauthorized.status).toBe(401);

    const privateKey = await testPrivateKeyPem();
    const originals = {
      appId: env.GITHUB_APP_ID,
      privateKey: env.GITHUB_APP_PRIVATE_KEY,
      webhookSecret: env.GITHUB_WEBHOOK_SECRET,
      agentSigningKey: env.AGENT_SHARED_TOKEN,
      adminToken: env.ADMIN_TOKEN,
    };
    const adminToken = "admin-ready-".padEnd(48, "a");
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    Reflect.set(env, "GITHUB_WEBHOOK_SECRET", "webhook-ready-".padEnd(48, "w"));
    Reflect.set(env, "AGENT_SHARED_TOKEN", "agent-ready-".padEnd(48, "g"));
    Reflect.set(env, "ADMIN_TOKEN", adminToken);
    try {
      const response = await exports.default.fetch(
        new Request("http://example.test/readyz", {
          headers: { Authorization: `Bearer ${adminToken}` },
        }),
      );
      expect(response.status).toBe(200);
      await expect(response.json()).resolves.toEqual({
        status: "ready",
        checks: {
          github_app_credentials: true,
          github_api_version: true,
          webhook_secret: true,
          agent_signing_key: true,
          admin_token: true,
          secrets_are_distinct: true,
        },
      });
    } finally {
      Reflect.set(env, "GITHUB_APP_ID", originals.appId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originals.privateKey);
      Reflect.set(env, "GITHUB_WEBHOOK_SECRET", originals.webhookSecret);
      Reflect.set(env, "AGENT_SHARED_TOKEN", originals.agentSigningKey);
      Reflect.set(env, "ADMIN_TOKEN", originals.adminToken);
    }
  });

  it("does not expose unknown routes", async () => {
    const response = await exports.default.fetch(
      new Request("http://example.test/private"),
    );
    expect(response.status).toBe(404);
  });

  it("acknowledges a signed webhook without waiting for GitHub API work", async () => {
    const privateKey = await testPrivateKeyPem();
    const originalAppId = env.GITHUB_APP_ID;
    const originalPrivateKey = env.GITHUB_APP_PRIVATE_KEY;
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);
    let releaseToken: (() => void) | undefined;
    const tokenGate = new Promise<void>((resolve) => {
      releaseToken = resolve;
    });
    let firstToken = true;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        if (url.pathname.endsWith("/access_tokens")) {
          const permissions = JSON.parse(String(init?.body))
            .permissions as Record<string, string>;
          if (firstToken) {
            firstToken = false;
            await tokenGate;
          }
          return Response.json({
            token:
              permissions.checks === "write"
                ? "check-token"
                : "variables-token",
          });
        }
        if (url.pathname.endsWith("/actions/variables")) {
          return Response.json({ total_count: 0, variables: [] });
        }
        if (url.pathname.endsWith("/check-runs")) {
          return Response.json({ id: 77 }, { status: 201 });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    const event = {
      action: "opened",
      installation: { id: 7101 },
      repository: {
        id: 9001,
        name: "widget",
        clone_url: "https://github.com/acme/widget.git",
        owner: { id: 8001, login: "acme", type: "User" },
      },
      pull_request: {
        number: 42,
        draft: false,
        head: { sha: "1".repeat(40), ref: "feature/fast-ack" },
        base: { sha: "2".repeat(40), ref: "main" },
      },
      sender: { id: 1234, login: "octocat" },
    };
    const body = JSON.stringify(event);
    const signature = await webhookSignature(body, env.GITHUB_WEBHOOK_SECRET);

    try {
      const responsePromise = exports.default.fetch(
        new Request("http://example.test/webhooks/github", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-GitHub-Delivery": "fast-http-delivery",
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": signature,
          },
          body,
        }),
      );
      const outcome = await Promise.race([
        responsePromise,
        new Promise<"timeout">((resolve) =>
          setTimeout(() => resolve("timeout"), 1_000),
        ),
      ]);
      expect(outcome).not.toBe("timeout");
      if (!(outcome instanceof Response)) {
        throw new Error("webhook acknowledgement timed out");
      }
      expect(outcome.status).toBe(202);
      const accepted = await outcome.json<{ job_id: string }>();

      releaseToken?.();
      const workspace = env.WORKSPACES.getByName("7101");
      await vi.waitFor(async () => {
        await expect(workspace.getSnapshot()).resolves.toMatchObject({
          jobs: [
            expect.objectContaining({
              id: accepted.job_id,
              check_run_id: 77,
              initialization_status: "ready",
            }),
          ],
        });
      });
    } finally {
      releaseToken?.();
      Reflect.set(env, "GITHUB_APP_ID", originalAppId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originalPrivateKey);
    }
  });

  it("protects and validates the onboarding routes", async () => {
    const unauthorized = await exports.default.fetch(
      new Request(
        "http://example.test/v1/workspaces/7001/readiness?owner=acme&repository=widget",
      ),
    );
    expect(unauthorized.status).toBe(401);

    const missingRepository = await exports.default.fetch(
      new Request("http://example.test/v1/workspaces/7001/readiness", {
        headers: { Authorization: `Bearer ${env.ADMIN_TOKEN}` },
      }),
    );
    expect(missingRepository.status).toBe(400);
    await expect(missingRepository.json()).resolves.toMatchObject({
      error: "invalid_repository",
    });

    const invalidWorkspace = await exports.default.fetch(
      new Request(
        "http://example.test/v1/workspaces/local/readiness?owner=acme&repository=widget",
        { headers: { Authorization: `Bearer ${env.ADMIN_TOKEN}` } },
      ),
    );
    expect(invalidWorkspace.status).toBe(400);
    await expect(invalidWorkspace.json()).resolves.toMatchObject({
      error: "invalid_workspace",
    });

    const unauthorizedOnboarding = await exports.default.fetch(
      new Request("http://example.test/v1/workspaces/7001/onboard", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(unauthorizedOnboarding.status).toBe(401);

    const invalidOnboarding = await exports.default.fetch(
      new Request("http://example.test/v1/workspaces/7001/onboard", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.ADMIN_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          owner: "acme",
          repository: "widget",
          expected_job_id: crypto.randomUUID(),
          expected_check_run_id: 44,
          expected_head_sha: "1".repeat(40),
          confirmation: "not-confirmed",
        }),
      }),
    );
    expect(invalidOnboarding.status).toBe(400);
    await expect(invalidOnboarding.json()).resolves.toMatchObject({
      error: "invalid_onboarding_request",
    });

    const staleOnboarding = await exports.default.fetch(
      new Request("http://example.test/v1/workspaces/7001/onboard", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.ADMIN_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          owner: "acme",
          repository: "widget",
          expected_job_id: crypto.randomUUID(),
          expected_check_run_id: 44,
          expected_head_sha: "1".repeat(40),
          confirmation: "disable_native_actions",
        }),
      }),
    );
    expect(staleOnboarding.status).toBe(409);
    await expect(staleOnboarding.json()).resolves.toMatchObject({
      error: "readiness_changed",
    });
  });

  it("verifies readiness, disables native Actions explicitly, and stays idempotent", async () => {
    const privateKey = await testPrivateKeyPem();
    const originals = {
      appId: env.GITHUB_APP_ID,
      privateKey: env.GITHUB_APP_PRIVATE_KEY,
    };
    Reflect.set(env, "GITHUB_APP_ID", "1234");
    Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", privateKey);

    const workspaceId = "7201";
    const jobId = crypto.randomUUID();
    const headSha = "3".repeat(40);
    const checkRunId = 8801;
    let actionsEnabled = true;
    let checkConclusion = "success";
    const actionWrites: Array<{ authorization: string | null; body: unknown }> =
      [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        const method = init?.method ?? "GET";
        if (url.pathname.endsWith("/access_tokens")) {
          const permissions = JSON.parse(String(init?.body))
            .permissions as Record<string, string>;
          return Response.json({
            token:
              permissions.administration === "write"
                ? "onboarding-token"
                : permissions.administration === "read"
                  ? "readiness-token"
                  : "check-token",
          });
        }
        if (
          method === "POST" &&
          url.pathname === "/repos/acme/widget/check-runs"
        ) {
          return Response.json({ id: checkRunId }, { status: 201 });
        }
        if (
          method === "PATCH" &&
          url.pathname === `/repos/acme/widget/check-runs/${checkRunId}`
        ) {
          return Response.json({ id: checkRunId });
        }
        if (
          url.pathname === "/repos/acme/widget/actions/permissions" &&
          method === "PUT"
        ) {
          actionWrites.push({
            authorization: new Headers(init?.headers).get("Authorization"),
            body: JSON.parse(String(init?.body)),
          });
          actionsEnabled = false;
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/repos/acme/widget/actions/permissions") {
          return Response.json({
            enabled: actionsEnabled,
            allowed_actions: "all",
          });
        }
        if (url.pathname === `/repos/acme/widget/check-runs/${checkRunId}`) {
          return Response.json({
            id: checkRunId,
            name: "GitZero",
            head_sha: headSha,
            external_id: jobId,
            status: "completed",
            conclusion: checkConclusion,
          });
        }
        throw new Error(`unexpected GitHub request: ${method} ${url.pathname}`);
      }),
    );

    const workspace = env.WORKSPACES.getByName(workspaceId);
    const connection = await workspace.fetch(
      new Request(
        `http://example.test/v1/workspaces/${workspaceId}/connect?role=agent&agent_id=readiness-mini`,
        { headers: { Upgrade: "websocket" } },
      ),
    );
    const agent = connection.webSocket;
    if (!agent) throw new Error("missing agent WebSocket");
    agent.accept();
    const welcome = collectSocketMessages(agent, 1);
    agent.send(
      JSON.stringify({
        type: "hello",
        hello: {
          protocol_version: 6,
          agent_id: "readiness-mini",
          name: "readiness-mini",
          version: "0.1.0",
          labels: ["self-hosted", "macOS", "aarch64"],
          max_parallelism: 1,
        },
      }),
    );
    await welcome;

    try {
      const assignment = collectSocketMessages(agent, 1);
      await workspace.enqueue(
        {
          id: jobId,
          workspace_id: workspaceId,
          installation_id: Number(workspaceId),
          run_number: 0,
          repository: {
            owner: "acme",
            name: "widget",
            clone_url: "https://github.com/acme/widget.git",
          },
          pull_request: {
            number: 42,
            action: "opened",
            head_sha: headSha,
            base_sha: "4".repeat(40),
            head_ref: "feature/readiness",
            base_ref: "main",
          },
          check_run_id: null,
          event: {},
          environment: {},
          variables: {},
          requires_github_token: false,
          report_to_github: true,
        },
        "readiness-route-success",
      );
      await expect(assignment).resolves.toEqual([
        expect.objectContaining({
          type: "run_job",
          job: expect.objectContaining({
            id: jobId,
            check_run_id: checkRunId,
          }),
        }),
      ]);

      let acknowledgement = collectSocketMessages(agent, 1);
      agent.send(
        JSON.stringify({
          type: "job_started",
          message_id: crypto.randomUUID(),
          job_id: jobId,
        }),
      );
      await acknowledgement;
      acknowledgement = collectSocketMessages(agent, 1);
      agent.send(
        JSON.stringify({
          type: "job_finished",
          message_id: crypto.randomUUID(),
          job_id: jobId,
          conclusion: "success",
          summary: "GitZero readiness validation passed.",
        }),
      );
      await acknowledgement;

      const response = await exports.default.fetch(
        new Request(
          `http://example.test/v1/workspaces/${workspaceId}/readiness?owner=acme&repository=widget`,
          { headers: { Authorization: `Bearer ${env.ADMIN_TOKEN}` } },
        ),
      );
      expect(response.status).toBe(200);
      await expect(response.json()).resolves.toMatchObject({
        installation_id: Number(workspaceId),
        repository: "acme/widget",
        compatible_agents_online: 1,
        available_capacity: 1,
        successful_check: {
          verified: true,
          job_id: jobId,
          check_run_id: checkRunId,
          head_sha: headSha,
          github_status: "completed",
          github_conclusion: "success",
        },
        safe_to_disable_native_actions: true,
        onboarded: false,
        next_action: "disable_native_actions",
      });

      const onboardingRequest = () =>
        new Request(
          `http://example.test/v1/workspaces/${workspaceId}/onboard`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${env.ADMIN_TOKEN}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              owner: "acme",
              repository: "widget",
              expected_job_id: jobId,
              expected_check_run_id: checkRunId,
              expected_head_sha: headSha,
              confirmation: "disable_native_actions",
            }),
          },
        );

      checkConclusion = "failure";
      const drifted = await exports.default.fetch(onboardingRequest());
      expect(drifted.status).toBe(409);
      await expect(drifted.json()).resolves.toMatchObject({
        error: "repository_not_ready",
        readiness: {
          successful_check: {
            verified: false,
            github_conclusion: "failure",
          },
          safe_to_disable_native_actions: false,
        },
      });
      expect(actionWrites).toHaveLength(0);

      checkConclusion = "success";
      const onboarding = await exports.default.fetch(onboardingRequest());
      expect(onboarding.status).toBe(200);
      await expect(onboarding.json()).resolves.toMatchObject({
        repository: "acme/widget",
        native_actions: { enabled: false },
        successful_check: {
          verified: true,
          job_id: jobId,
          check_run_id: checkRunId,
          head_sha: headSha,
        },
        safe_to_disable_native_actions: false,
        onboarded: true,
        next_action: "complete",
        changed: true,
      });
      expect(actionWrites).toEqual([
        {
          authorization: "Bearer onboarding-token",
          body: { enabled: false },
        },
      ]);

      const repeated = await exports.default.fetch(onboardingRequest());
      expect(repeated.status).toBe(200);
      await expect(repeated.json()).resolves.toMatchObject({
        native_actions: { enabled: false },
        onboarded: true,
        next_action: "complete",
        changed: false,
      });
      expect(actionWrites).toHaveLength(1);
    } finally {
      agent.close(1000, "test complete");
      Reflect.set(env, "GITHUB_APP_ID", originals.appId);
      Reflect.set(env, "GITHUB_APP_PRIVATE_KEY", originals.privateKey);
    }
  });

  it("mints workspace- and agent-scoped connection credentials", async () => {
    const workspaceId = crypto.randomUUID();
    const agentId = "mini-1";
    const tokenResponse = await exports.default.fetch(
      new Request(
        `http://example.test/v1/workspaces/${workspaceId}/agent-token`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${env.ADMIN_TOKEN}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ agent_id: agentId }),
        },
      ),
    );
    expect(tokenResponse.status).toBe(200);
    const credential = await tokenResponse.json<{ token: string }>();

    const wrongWorkspace = await exports.default.fetch(
      new Request(
        `http://example.test/v1/workspaces/${crypto.randomUUID()}/connect?role=agent&agent_id=${agentId}`,
        {
          headers: {
            Authorization: `Bearer ${credential.token}`,
            Upgrade: "websocket",
          },
        },
      ),
    );
    expect(wrongWorkspace.status).toBe(401);

    const connection = await exports.default.fetch(
      new Request(
        `http://example.test/v1/workspaces/${workspaceId}/connect?role=agent&agent_id=${agentId}`,
        {
          headers: {
            Authorization: `Bearer ${credential.token}`,
            Upgrade: "websocket",
          },
        },
      ),
    );
    expect(connection.status).toBe(101);
    connection.webSocket?.accept();
    connection.webSocket?.close(1000, "test complete");
  });
});

async function webhookSignature(body: string, secret: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const digest = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body)),
  );
  return `sha256=${[...digest]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("")}`;
}

function collectSocketMessages(
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
