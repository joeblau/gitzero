import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { generateKeyPairSync } from "node:crypto";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  REQUIRED_SECRETS,
  WORKER_NAME,
  deploymentUrlFromWranglerOutput,
  parseAndValidateSecrets,
  parseArguments,
  validateDeploymentOptions,
  verifyDeployment,
  wranglerArguments,
} from "./deploy.mjs";

const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const privateKeyPem = privateKey.export({ type: "pkcs8", format: "pem" });
const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));

function validSecrets() {
  return {
    GITHUB_WEBHOOK_SECRET: "webhook-".padEnd(48, "w"),
    GITHUB_APP_PRIVATE_KEY: privateKeyPem,
    AGENT_SHARED_TOKEN: "agent-".padEnd(48, "a"),
    ADMIN_TOKEN: "admin-".padEnd(48, "d"),
  };
}

test("parses an explicit production confirmation", () => {
  const parsed = parseArguments([
    "--account-id",
    "a".repeat(32),
    "--github-app-id",
    "1234",
    "--secrets-file",
    "/tmp/gitzero.secrets.json",
    "--url",
    "https://gitzero.example.test/",
    "--confirm",
    WORKER_NAME,
  ]);
  assert.deepEqual(parsed, {
    accountId: "a".repeat(32),
    githubAppId: "1234",
    secretsFile: "/tmp/gitzero.secrets.json",
    url: "https://gitzero.example.test/",
    confirm: true,
    help: false,
  });
  assert.throws(
    () => parseArguments(["--confirm", "another-worker"]),
    /must equal gitzero-control-plane/,
  );
});

test("validates secret structure, entropy, distinction, and RSA key material", () => {
  const secrets = validSecrets();
  assert.deepEqual(parseAndValidateSecrets(JSON.stringify(secrets)), secrets);

  const missing = { ...secrets };
  delete missing.ADMIN_TOKEN;
  assert.throws(
    () => parseAndValidateSecrets(JSON.stringify(missing)),
    /exactly/,
  );

  assert.throws(
    () =>
      parseAndValidateSecrets(
        JSON.stringify({ ...secrets, ADMIN_TOKEN: secrets.AGENT_SHARED_TOKEN }),
      ),
    /must be distinct/,
  );
  assert.throws(
    () =>
      parseAndValidateSecrets(
        JSON.stringify({ ...secrets, GITHUB_APP_PRIVATE_KEY: "not a key" }),
      ),
    /not a valid private key/,
  );
});

test("requires a private untracked secrets file and valid deployment identifiers", () => {
  const directory = mkdtempSync(join(tmpdir(), "gitzero-deploy-test-"));
  try {
    const secretsFile = join(directory, "production.secrets.json");
    writeFileSync(secretsFile, JSON.stringify(validSecrets()), { mode: 0o600 });
    chmodSync(secretsFile, 0o600);
    const validated = validateDeploymentOptions(
      {
        accountId: "b".repeat(32),
        githubAppId: "42",
        secretsFile,
        url: "https://gitzero.example.test/",
        confirm: false,
        help: false,
      },
      { repositoryRoot: directory },
    );
    assert.equal(validated.secretsFile, secretsFile);
    assert.equal(validated.url, "https://gitzero.example.test");
    assert.deepEqual(
      Object.keys(validated.secrets).sort(),
      [...REQUIRED_SECRETS].sort(),
    );

    assert.throws(
      () =>
        validateDeploymentOptions(
          {
            accountId: "short",
            githubAppId: "42",
            secretsFile,
            confirm: false,
            help: false,
          },
          { repositoryRoot: directory },
        ),
      /32-character/,
    );
    if (process.platform !== "win32") {
      chmodSync(secretsFile, 0o644);
      assert.throws(
        () =>
          validateDeploymentOptions(
            {
              accountId: "b".repeat(32),
              githubAppId: "42",
              secretsFile,
              confirm: false,
              help: false,
            },
            { repositoryRoot: directory },
          ),
        /permissions/,
      );
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("builds Wrangler arguments without placing secret values in argv", () => {
  const options = {
    githubAppId: "1234",
    secretsFile: "/secure/gitzero.secrets.json",
  };
  const arguments_ = wranglerArguments(options, true);
  assert.deepEqual(arguments_.slice(0, 2), ["deploy", "--strict"]);
  assert(arguments_.includes("GITHUB_APP_ID:1234"));
  assert(arguments_.includes("/secure/gitzero.secrets.json"));
  assert(arguments_.includes("--dry-run"));
  for (const secret of Object.values(validSecrets())) {
    assert(!arguments_.includes(secret));
  }
});

test("extracts the deployed workers.dev URL", () => {
  assert.equal(
    deploymentUrlFromWranglerOutput(
      "Uploaded\n https://gitzero-control-plane.example.workers.dev\n",
    ),
    "https://gitzero-control-plane.example.workers.dev",
  );
  assert.equal(deploymentUrlFromWranglerOutput("dry run only"), undefined);
});

test("verifies public health and authenticated readiness without exposing the token", async () => {
  const requests = [];
  const fetchImplementation = async (url, init = {}) => {
    requests.push({ url, authorization: init.headers?.Authorization });
    if (url.endsWith("/healthz")) {
      return Response.json({ status: "ok", service: WORKER_NAME });
    }
    return Response.json({
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
  };
  await verifyDeployment(
    "https://gitzero.example.test",
    "administrator-secret",
    fetchImplementation,
  );
  assert.deepEqual(requests, [
    {
      url: "https://gitzero.example.test/healthz",
      authorization: undefined,
    },
    {
      url: "https://gitzero.example.test/readyz",
      authorization: "Bearer administrator-secret",
    },
  ]);
});

test(
  "root deployment command completes a strict Wrangler dry run",
  { timeout: 30_000 },
  () => {
    const directory = mkdtempSync(join(tmpdir(), "gitzero-deploy-cli-test-"));
    try {
      const secrets = validSecrets();
      const secretsFile = join(directory, "production.secrets.json");
      writeFileSync(secretsFile, JSON.stringify(secrets), { mode: 0o600 });
      chmodSync(secretsFile, 0o600);
      const result = spawnSync(
        "npm",
        [
          "run",
          "deploy:control-plane",
          "--",
          "--account-id",
          "b".repeat(32),
          "--github-app-id",
          "1234",
          "--secrets-file",
          secretsFile,
        ],
        {
          cwd: repositoryRoot,
          encoding: "utf8",
          env: { ...process.env, FORCE_COLOR: "0" },
        },
      );
      const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
      assert.equal(result.status, 0, output);
      assert.match(output, /--dry-run: exiting now/);
      assert.match(output, /Dry run complete/);
      for (const secret of Object.values(secrets)) {
        assert(!output.includes(secret));
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  },
);
