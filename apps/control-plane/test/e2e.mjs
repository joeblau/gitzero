import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { once } from "node:events";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import WebSocket from "ws";

const testDirectory = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(testDirectory, "../../..");
const controlPlaneDirectory = path.join(repositoryRoot, "apps/control-plane");
const wranglerBinary = path.join(repositoryRoot, "node_modules/.bin/wrangler");
const agentBinary = path.join(repositoryRoot, "target/debug/gitzero-agent");
const cloneUrl = "https://github.com/gitzero/e2e-fixture.git";
const agentIds = ["acceptance-mini-a", "acceptance-mini-b"];
const workspaceId = `acceptance-${randomUUID()}`;
const processLogs = new Map();
const children = new Set();
const secrets = new Set();
let observer;

try {
  assert.equal(
    process.platform,
    "darwin",
    "the full acceptance test must run on a macOS agent",
  );
  await run("cargo", ["build", "-p", "gitzero-agent"], {
    cwd: repositoryRoot,
    name: "cargo build",
  });

  const fixtureRoot = await mkdtemp(
    path.join(os.tmpdir(), "gitzero-control-plane-e2e-"),
  );
  try {
    const fixture = await createRepositoryFixture(fixtureRoot);
    const gitWrapperDirectory = await createGitWrapper(fixtureRoot);
    await verifyGitRewrite(
      gitWrapperDirectory,
      fixture.remoteUrl,
      fixture.headSha,
    );

    const adminToken = randomSecret();
    const agentSigningKey = randomSecret();
    const webhookSecret = randomSecret();
    secrets.add(adminToken);
    secrets.add(agentSigningKey);
    secrets.add(webhookSecret);
    const envFile = path.join(fixtureRoot, "acceptance.vars");
    await writeFile(
      envFile,
      [
        `ADMIN_TOKEN=${adminToken}`,
        `AGENT_SHARED_TOKEN=${agentSigningKey}`,
        `GITHUB_WEBHOOK_SECRET=${webhookSecret}`,
        "GITHUB_APP_PRIVATE_KEY=unused-by-local-acceptance",
        "",
      ].join("\n"),
      { mode: 0o600 },
    );
    await chmod(envFile, 0o600);

    const port = await freePort();
    const origin = `http://127.0.0.1:${port}`;
    const wrangler = start(
      wranglerBinary,
      [
        "dev",
        "--local",
        "--config",
        "wrangler.jsonc",
        "--ip",
        "127.0.0.1",
        "--port",
        String(port),
        "--persist-to",
        path.join(fixtureRoot, "wrangler-state"),
        "--env-file",
        envFile,
        "--log-level",
        "warn",
      ],
      {
        cwd: controlPlaneDirectory,
        name: "wrangler dev",
      },
    );
    await waitFor(
      "local Worker health",
      async () => {
        assertRunning(wrangler);
        try {
          const response = await fetch(`${origin}/healthz`);
          return response.ok;
        } catch {
          return false;
        }
      },
      60_000,
    );

    const credentials = await Promise.all(
      agentIds.map(async (agentId) => {
        const credential = await requestJson(
          `${origin}/v1/workspaces/${encodeURIComponent(workspaceId)}/agent-token`,
          {
            method: "POST",
            headers: authenticatedJsonHeaders(adminToken),
            body: JSON.stringify({ agent_id: agentId }),
          },
        );
        assert.equal(credential.workspace_id, workspaceId);
        assert.equal(credential.agent_id, agentId);
        assert.equal(typeof credential.token, "string");
        secrets.add(credential.token);
        return credential;
      }),
    );
    const agents = credentials.map((credential) =>
      startAgent(
        origin,
        fixtureRoot,
        gitWrapperDirectory,
        fixture.remoteUrl,
        credential,
      ),
    );

    const statusUrl = `${origin}/v1/workspaces/${encodeURIComponent(workspaceId)}`;
    let initialSnapshot;
    await waitFor(
      "agent protocol handshake",
      async () => {
        agents.forEach(assertRunning);
        initialSnapshot = await requestJson(statusUrl, {
          headers: bearerHeaders(adminToken),
        });
        return agentIds.every((agentId) =>
          initialSnapshot.agents?.some(
            (candidate) => candidate.agent_id === agentId,
          ),
        );
      },
      30_000,
    );
    for (const agentId of agentIds) {
      const onlineAgent = initialSnapshot.agents.find(
        (candidate) => candidate.agent_id === agentId,
      );
      assert.equal(onlineAgent.status, "online");
      assert.equal(onlineAgent.max_parallelism, 1);
      assert.ok(onlineAgent.labels.includes("self-hosted"));
      assert.ok(onlineAgent.labels.includes("macOS"));
      assert.ok(onlineAgent.labels.includes("acceptance"));
      assert.ok(
        onlineAgent.labels.includes(
          process.arch === "arm64"
            ? "ARM64"
            : process.arch === "x64"
              ? "X64"
              : process.arch,
        ),
      );
      assert.equal(onlineAgent.runner_group, "acceptance-minis");
    }

    const observerEvents = [];
    observer = await connectObserver(origin, workspaceId, adminToken);
    observer.on("message", (raw) => {
      observerEvents.push(JSON.parse(raw.toString()));
    });

    const queuedJobs = await Promise.all(
      ["first", "second"].map((runLabel) =>
        requestJson(
          `${origin}/v1/workspaces/${encodeURIComponent(workspaceId)}/jobs`,
          {
            method: "POST",
            headers: authenticatedJsonHeaders(adminToken),
            body: JSON.stringify({
              repository: {
                owner: "gitzero",
                name: "e2e-fixture",
                clone_url: cloneUrl,
              },
              pull_request: {
                number: 1,
                action: "opened",
                head_sha: fixture.headSha,
                base_sha: fixture.baseSha,
                head_ref: "acceptance",
                base_ref: "main",
              },
              variables: {},
              environment: {
                GITZERO_E2E_SENTINEL: "worker-agent-roundtrip",
                GITZERO_E2E_HEAD_SHA: fixture.headSha,
                GITZERO_E2E_RUN: runLabel,
              },
              requires_github_token: false,
              report_to_github: false,
            }),
          },
        ),
      ),
    );
    for (const queued of queuedJobs) {
      assert.equal(queued.duplicate, false);
      assert.equal(typeof queued.job_id, "string");
    }

    let terminalSnapshot;
    let completedJobs;
    await waitFor(
      "terminal workflow status",
      async () => {
        agents.forEach(assertRunning);
        terminalSnapshot = await requestJson(statusUrl, {
          headers: bearerHeaders(adminToken),
        });
        completedJobs = queuedJobs.map((queued) =>
          terminalSnapshot.jobs?.find(
            (candidate) => candidate.id === queued.job_id,
          ),
        );
        return completedJobs.every((job) => job?.status === "completed");
      },
      120_000,
    );

    for (const completedJob of completedJobs) {
      assert.equal(completedJob.conclusion, "success");
      assert.equal(completedJob.attempt_count, 1);
      assert.match(
        completedJob.summary,
        /GitZero completed 1 step\(s\) successfully\./,
      );
      assert.match(completedJob.summary, /GitZero local E2E summary marker/);
    }
    assert.deepEqual(
      new Set(completedJobs.map((job) => job.agent_id)),
      new Set(agentIds),
      "overlapping jobs were not distributed across both available agents",
    );
    await waitFor(
      "observer terminal event",
      () =>
        queuedJobs.every((queued) =>
          observerEvents.some(
            (event) =>
              event.type === "job_finished" &&
              event.data?.job_id === queued.job_id,
          ),
        ),
      10_000,
    );
    for (const queued of queuedJobs) {
      assertObserverLifecycle(observerEvents, queued.job_id);
      const completion = observerEvents.find(
        (event) =>
          event.type === "job_finished" && event.data?.job_id === queued.job_id,
      );
      assert.equal(completion?.data?.annotation_count, 1);
    }

    for (const agentId of agentIds) {
      const terminalAgent = terminalSnapshot.agents.find(
        (candidate) => candidate.agent_id === agentId,
      );
      assert.equal(terminalAgent.available_capacity, 1);
      assert.deepEqual(terminalAgent.active_jobs, []);
    }

    console.log(
      `GitZero local E2E passed: ${queuedJobs.length} jobs completed across ${agentIds.join(
        " and ",
      )}.`,
    );
  } finally {
    observer?.close(1000, "acceptance complete");
    await stopChildren();
    await rm(fixtureRoot, { recursive: true, force: true });
  }
} catch (error) {
  observer?.close();
  await stopChildren();
  console.error(redact(String(error?.stack ?? error)));
  for (const [name, logs] of processLogs) {
    if (logs.trim().length > 0) {
      console.error(`\n[${name}]\n${redact(logs)}`);
    }
  }
  process.exitCode = 1;
}

async function createRepositoryFixture(root) {
  const source = path.join(root, "source");
  const remote = path.join(root, "remote.git");
  await mkdir(source);
  await git(source, "init", "--initial-branch=main");
  await git(source, "config", "user.email", "gitzero@example.test");
  await git(source, "config", "user.name", "GitZero Acceptance");
  await writeFile(path.join(source, "README.md"), "GitZero E2E fixture\n");
  await git(source, "add", "README.md");
  await git(source, "commit", "-m", "base fixture");
  const baseSha = (await gitOutput(source, "rev-parse", "HEAD")).trim();

  await git(root, "init", "--bare", remote);
  await git(source, "remote", "add", "origin", remote);
  await git(source, "push", "origin", "main");

  const workflowDirectory = path.join(source, ".github/workflows");
  await mkdir(workflowDirectory, { recursive: true });
  await writeFile(
    path.join(workflowDirectory, "acceptance.yml"),
    `name: Local end-to-end acceptance
on: pull_request
jobs:
  roundtrip:
    runs-on:
      group: acceptance-minis
      labels: [self-hosted, macOS, acceptance]
    steps:
      - name: Prove Worker-to-agent execution
        run: |
          test "$GITZERO_E2E_SENTINEL" = "worker-agent-roundtrip"
          test "$GITHUB_SHA" = "$GITZERO_E2E_HEAD_SHA"
          sleep 2
          printf 'gitzero-e2e-log\\n'
          printf '::warning file=README.md,line=1,title=GitZero E2E::GitZero local E2E annotation marker\\n'
          printf 'GitZero local E2E summary marker\\n' >> "$GITHUB_STEP_SUMMARY"
`,
  );
  await git(source, "add", ".github/workflows/acceptance.yml");
  await git(source, "commit", "-m", "add acceptance workflow");
  const headSha = (await gitOutput(source, "rev-parse", "HEAD")).trim();
  await git(source, "push", "origin", "HEAD:refs/pull/1/head");

  return {
    baseSha,
    headSha,
    remoteUrl: pathToFileURL(remote).href,
  };
}

async function createGitWrapper(root) {
  const directory = path.join(root, "bin");
  await mkdir(directory);
  const wrapper = path.join(directory, "git");
  await writeFile(
    wrapper,
    `#!/bin/sh
set -eu
: "\${GITZERO_E2E_REMOTE_URL:?missing local acceptance repository URL}"
exec /usr/bin/git \\
  -c protocol.file.allow=always \\
  -c "url.\${GITZERO_E2E_REMOTE_URL}.insteadOf=${cloneUrl}" \\
  "$@"
`,
    { mode: 0o700 },
  );
  await chmod(wrapper, 0o700);
  return directory;
}

async function verifyGitRewrite(directory, remoteUrl, headSha) {
  const output = await run(
    path.join(directory, "git"),
    ["ls-remote", cloneUrl],
    {
      cwd: repositoryRoot,
      name: "git rewrite probe",
      env: { ...process.env, GITZERO_E2E_REMOTE_URL: remoteUrl },
      returnOutput: true,
    },
  );
  assert.match(output, new RegExp(`^${headSha}\\s+refs/pull/1/head$`, "m"));
}

function assertObserverLifecycle(events, jobId) {
  const matching = events.filter(
    (event) => event.data?.job_id === jobId || event.data?.job?.id === jobId,
  );
  const types = matching.map((event) => event.type);
  for (const required of [
    "job_queued",
    "job_assigned",
    "job_started",
    "step_started",
    "log_chunk",
    "step_finished",
    "job_finished",
  ]) {
    assert.ok(
      types.includes(required),
      `observer missed ${required}: ${types}`,
    );
  }
  const ordered = ["job_queued", "job_assigned", "job_started", "job_finished"];
  let previous = -1;
  for (const type of ordered) {
    const index = types.indexOf(type);
    assert.ok(index > previous, `observer lifecycle is out of order: ${types}`);
    previous = index;
  }
  assert.ok(
    matching.some(
      (event) =>
        event.type === "log_chunk" && event.data?.data === "gitzero-e2e-log\n",
    ),
    "observer did not receive the workflow log chunk",
  );
}

async function connectObserver(origin, workspace, adminToken) {
  const url = new URL(
    `/v1/workspaces/${encodeURIComponent(workspace)}/connect?role=observer`,
    origin,
  );
  url.protocol = "ws:";
  const socket = new WebSocket(url, {
    headers: { Authorization: `Bearer ${adminToken}` },
  });
  await Promise.race([
    once(socket, "open"),
    once(socket, "error").then(([error]) => Promise.reject(error)),
    delay(10_000).then(() => Promise.reject(new Error("observer timed out"))),
  ]);
  return socket;
}

function startAgent(
  origin,
  fixtureRoot,
  gitWrapperDirectory,
  remoteUrl,
  credential,
) {
  const agentId = credential.agent_id;
  return start(agentBinary, [], {
    cwd: repositoryRoot,
    name: `gitzero-agent ${agentId}`,
    env: {
      ...process.env,
      PATH: `${gitWrapperDirectory}:${process.env.PATH ?? ""}`,
      GITZERO_E2E_REMOTE_URL: remoteUrl,
      GITZERO_CONTROL_PLANE: origin,
      GITZERO_WORKSPACE_ID: workspaceId,
      GITZERO_AGENT_TOKEN: credential.token,
      GITZERO_AGENT_ID: agentId,
      GITZERO_AGENT_NAME: `Acceptance Mac Mini ${agentId}`,
      GITZERO_LABELS: "acceptance",
      GITZERO_RUNNER_GROUP: "acceptance-minis",
      GITZERO_WORK_ROOT: path.join(fixtureRoot, `agent-work-${agentId}`),
      GITZERO_MAX_PARALLELISM: "1",
      GITZERO_CACHE_MAX_BYTES: String(10 * 1024 * 1024),
      GITZERO_CACHE_MAX_ENTRY_BYTES: String(5 * 1024 * 1024),
      GITZERO_ARTIFACT_MAX_BYTES: String(10 * 1024 * 1024),
      GITZERO_ARTIFACT_MAX_ENTRY_BYTES: String(5 * 1024 * 1024),
      RUST_LOG: "gitzero_agent=info",
    },
  });
}

async function requestJson(url, init = {}) {
  const response = await fetch(url, init);
  const body = await response.text();
  if (!response.ok) {
    throw new Error(
      `HTTP ${response.status} from ${new URL(url).pathname}: ${body}`,
    );
  }
  return JSON.parse(body);
}

function authenticatedJsonHeaders(token) {
  return {
    ...bearerHeaders(token),
    "Content-Type": "application/json",
  };
}

function bearerHeaders(token) {
  return { Authorization: `Bearer ${token}` };
}

async function waitFor(label, operation, timeoutMilliseconds) {
  const deadline = Date.now() + timeoutMilliseconds;
  let lastError;
  while (Date.now() < deadline) {
    try {
      if (await operation()) return;
    } catch (error) {
      lastError = error;
    }
    await delay(200);
  }
  throw new Error(
    `${label} timed out after ${timeoutMilliseconds}ms${
      lastError ? `: ${lastError}` : ""
    }`,
  );
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function freePort() {
  const server = net.createServer();
  server.unref();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const port = address.port;
  const closed = once(server, "close");
  server.close();
  await closed;
  return port;
}

async function git(cwd, ...arguments_) {
  await run("/usr/bin/git", arguments_, { cwd, name: "fixture git" });
}

async function gitOutput(cwd, ...arguments_) {
  return run("/usr/bin/git", arguments_, {
    cwd,
    name: "fixture git",
    returnOutput: true,
  });
}

async function run(command, arguments_, options) {
  const child = spawn(command, arguments_, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const chunks = [];
  child.stdout.on("data", (chunk) => chunks.push(chunk));
  child.stderr.on("data", (chunk) => chunks.push(chunk));
  const [code, signal] = await once(child, "close");
  const output = Buffer.concat(chunks).toString("utf8");
  if (code !== 0) {
    throw new Error(
      `${options.name} exited with ${code ?? signal}: ${redact(output)}`,
    );
  }
  return options.returnOutput ? output : undefined;
}

function start(command, arguments_, options) {
  const child = spawn(command, arguments_, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.add(child);
  processLogs.set(options.name, "");
  const capture = (chunk) => {
    const previous = processLogs.get(options.name) ?? "";
    processLogs.set(options.name, `${previous}${chunk}`.slice(-64 * 1024));
  };
  child.stdout.on("data", capture);
  child.stderr.on("data", capture);
  child.once("close", () => children.delete(child));
  return child;
}

function assertRunning(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    throw new Error(
      `required process exited with ${child.exitCode ?? child.signalCode}`,
    );
  }
}

async function stopChildren() {
  await Promise.all([...children].map(stopChild));
}

async function stopChild(child) {
  if (child.exitCode !== null || child.signalCode !== null || !child.pid)
    return;
  const closed = once(child, "close");
  try {
    process.kill(-child.pid, "SIGTERM");
  } catch (error) {
    if (error.code !== "ESRCH") throw error;
  }
  const stopped = await Promise.race([closed.then(() => true), delay(5_000)]);
  if (stopped === true) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch (error) {
    if (error.code !== "ESRCH") throw error;
  }
  await closed;
}

function randomSecret() {
  return randomBytes(32).toString("hex");
}

function redact(value) {
  let redacted = value;
  for (const secret of secrets) redacted = redacted.replaceAll(secret, "***");
  return redacted;
}
