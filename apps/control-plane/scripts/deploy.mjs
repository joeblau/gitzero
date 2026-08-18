#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { createPrivateKey, sign } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const WORKER_NAME = "gitzero-control-plane";
export const REQUIRED_SECRETS = [
  "GITHUB_WEBHOOK_SECRET",
  "GITHUB_APP_PRIVATE_KEY",
  "AGENT_SHARED_TOKEN",
  "ADMIN_TOKEN",
  "SECRETS_ENCRYPTION_KEY",
];

const SCRIPT_DIRECTORY = dirname(fileURLToPath(import.meta.url));
const CONTROL_PLANE_ROOT = resolve(SCRIPT_DIRECTORY, "..");
const REPOSITORY_ROOT = resolve(CONTROL_PLANE_ROOT, "../..");
const GITHUB_API_VERSION = "2026-03-10";
const MAX_SECRETS_FILE_BYTES = 128 * 1024;

export function parseArguments(arguments_) {
  const options = {
    accountId: undefined,
    githubAppId: undefined,
    secretsFile: undefined,
    url: undefined,
    confirm: false,
    help: false,
  };
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === "--help" || argument === "-h") {
      options.help = true;
      continue;
    }
    if (argument === "--confirm") {
      const value = requiredArgumentValue(arguments_, index, argument);
      if (value !== WORKER_NAME) {
        throw new Error(`--confirm must equal ${WORKER_NAME}`);
      }
      options.confirm = true;
      index += 1;
      continue;
    }
    const destinations = {
      "--account-id": "accountId",
      "--github-app-id": "githubAppId",
      "--secrets-file": "secretsFile",
      "--url": "url",
    };
    const destination = destinations[argument];
    if (destination === undefined) {
      throw new Error(`unknown argument '${argument}'`);
    }
    options[destination] = requiredArgumentValue(arguments_, index, argument);
    index += 1;
  }
  return options;
}

function requiredArgumentValue(arguments_, index, option) {
  const value = arguments_[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${option} requires a value`);
  }
  return value;
}

export function validateDeploymentOptions(options, roots = {}) {
  if (!/^[a-f0-9]{32}$/i.test(options.accountId ?? "")) {
    throw new Error(
      "--account-id must be a 32-character Cloudflare account ID",
    );
  }
  if (!/^[1-9][0-9]*$/.test(options.githubAppId ?? "")) {
    throw new Error("--github-app-id must be a positive integer");
  }
  if (options.secretsFile === undefined) {
    throw new Error("--secrets-file is required");
  }
  const secretsFile = resolve(options.secretsFile);
  const repositoryRoot = roots.repositoryRoot ?? REPOSITORY_ROOT;
  const metadata = statSync(secretsFile);
  if (!metadata.isFile()) {
    throw new Error("--secrets-file must name a regular file");
  }
  if (metadata.size > MAX_SECRETS_FILE_BYTES) {
    throw new Error(`secrets file exceeds ${MAX_SECRETS_FILE_BYTES} bytes`);
  }
  if (process.platform !== "win32" && (metadata.mode & 0o077) !== 0) {
    throw new Error(
      "secrets file permissions must not grant group or other access",
    );
  }
  ensureSecretsFileIsNotTracked(secretsFile, repositoryRoot);
  const secrets = parseAndValidateSecrets(readFileSync(secretsFile, "utf8"));
  const url =
    options.url === undefined ? undefined : normalizeDeploymentUrl(options.url);
  return { ...options, secretsFile, secrets, url };
}

function ensureSecretsFileIsNotTracked(secretsFile, repositoryRoot) {
  const repositoryRelative = relative(repositoryRoot, secretsFile);
  if (
    repositoryRelative === "" ||
    repositoryRelative === ".." ||
    repositoryRelative.startsWith(
      `..${process.platform === "win32" ? "\\" : "/"}`,
    )
  ) {
    return;
  }
  const result = spawnSync(
    "git",
    [
      "-C",
      repositoryRoot,
      "ls-files",
      "--error-unmatch",
      "--",
      repositoryRelative,
    ],
    { stdio: "ignore" },
  );
  if (result.status === 0) {
    throw new Error("secrets file must not be tracked by Git");
  }
}

export function parseAndValidateSecrets(source) {
  let parsed;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new Error("secrets file must contain a JSON object");
  }
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new Error("secrets file must contain a JSON object");
  }
  const keys = Object.keys(parsed).sort();
  const expected = [...REQUIRED_SECRETS].sort();
  if (
    keys.length !== expected.length ||
    keys.some((key, index) => key !== expected[index])
  ) {
    throw new Error(
      `secrets file must contain exactly: ${REQUIRED_SECRETS.join(", ")}`,
    );
  }
  for (const name of REQUIRED_SECRETS) {
    if (typeof parsed[name] !== "string" || parsed[name].length === 0) {
      throw new Error(`secret ${name} must be a non-empty string`);
    }
  }
  for (const name of [
    "GITHUB_WEBHOOK_SECRET",
    "AGENT_SHARED_TOKEN",
    "ADMIN_TOKEN",
    "SECRETS_ENCRYPTION_KEY",
  ]) {
    if (Buffer.byteLength(parsed[name], "utf8") < 32) {
      throw new Error(`secret ${name} must contain at least 32 UTF-8 bytes`);
    }
  }
  const distinct = new Set([
    parsed.GITHUB_WEBHOOK_SECRET,
    parsed.AGENT_SHARED_TOKEN,
    parsed.ADMIN_TOKEN,
    parsed.SECRETS_ENCRYPTION_KEY,
  ]);
  if (distinct.size !== 4) {
    throw new Error(
      "webhook, agent-signing, administrator, and encryption secrets must be distinct",
    );
  }
  let privateKey;
  try {
    privateKey = createPrivateKey(parsed.GITHUB_APP_PRIVATE_KEY);
  } catch {
    throw new Error("GITHUB_APP_PRIVATE_KEY is not a valid private key");
  }
  if (privateKey.asymmetricKeyType !== "rsa") {
    throw new Error("GITHUB_APP_PRIVATE_KEY must be an RSA private key");
  }
  sign(
    "RSA-SHA256",
    Buffer.from("gitzero deployment preflight", "utf8"),
    privateKey,
  );
  return parsed;
}

function normalizeDeploymentUrl(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error("--url must be an absolute HTTPS URL");
  }
  if (
    url.protocol !== "https:" ||
    url.username !== "" ||
    url.password !== "" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new Error(
      "--url must be an HTTPS origin without credentials, query, or fragment",
    );
  }
  url.pathname = url.pathname.replace(/\/+$/, "") || "/";
  return url.href.replace(/\/$/, "");
}

export function wranglerArguments(options, dryRun) {
  const arguments_ = [
    "deploy",
    "--strict",
    "--var",
    "ENVIRONMENT:production",
    "--var",
    `GITHUB_API_VERSION:${GITHUB_API_VERSION}`,
    "--var",
    `GITHUB_APP_ID:${options.githubAppId}`,
    "--secrets-file",
    options.secretsFile,
  ];
  if (dryRun) {
    arguments_.push("--dry-run");
  }
  return arguments_;
}

async function runWrangler(options, dryRun) {
  const arguments_ = wranglerArguments(options, dryRun);
  const child = spawn("wrangler", arguments_, {
    cwd: CONTROL_PLANE_ROOT,
    env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: options.accountId },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  for (const stream of [child.stdout, child.stderr]) {
    stream.on("data", (chunk) => {
      const text = chunk.toString();
      output += text;
      if (stream === child.stdout) {
        process.stdout.write(text);
      } else {
        process.stderr.write(text);
      }
    });
  }
  const exitCode = await new Promise((resolveExit, reject) => {
    child.once("error", reject);
    child.once("close", resolveExit);
  });
  if (exitCode !== 0) {
    throw new Error(`Wrangler exited with status ${exitCode}`);
  }
  return output;
}

export function deploymentUrlFromWranglerOutput(output) {
  const plain = output.replace(/\u001B\[[0-9;]*m/g, "");
  const urls = plain.match(/https:\/\/[A-Za-z0-9.-]+\.workers\.dev\b/g) ?? [];
  return urls.at(-1);
}

export async function verifyDeployment(
  baseUrl,
  adminToken,
  fetchImplementation = fetch,
) {
  let lastError;
  for (let attempt = 1; attempt <= 8; attempt += 1) {
    try {
      const health = await fetchImplementation(`${baseUrl}/healthz`, {
        redirect: "error",
      });
      const healthBody = await health.json();
      if (
        health.status !== 200 ||
        healthBody.status !== "ok" ||
        healthBody.service !== WORKER_NAME
      ) {
        throw new Error(`health check returned HTTP ${health.status}`);
      }
      const readiness = await fetchImplementation(`${baseUrl}/readyz`, {
        redirect: "error",
        headers: { Authorization: `Bearer ${adminToken}` },
      });
      const readinessBody = await readiness.json();
      const checks = readinessBody?.checks;
      if (
        readiness.status !== 200 ||
        readinessBody?.status !== "ready" ||
        checks === null ||
        typeof checks !== "object" ||
        !Object.values(checks).every(Boolean)
      ) {
        const failedChecks =
          checks !== null && typeof checks === "object"
            ? Object.entries(checks)
                .filter(([, passed]) => !passed)
                .map(([name]) => name)
                .join(", ")
            : "unavailable";
        throw new Error(
          `readiness check returned HTTP ${readiness.status}; failed checks: ${failedChecks}`,
        );
      }
      return;
    } catch (error) {
      lastError = error;
      if (attempt < 8) {
        await new Promise((resolveDelay) =>
          setTimeout(resolveDelay, attempt * 500),
        );
      }
    }
  }
  throw lastError;
}

function usage() {
  return `Usage:
  npm run deploy:control-plane -- --account-id ACCOUNT_ID --github-app-id APP_ID --secrets-file PATH [--url HTTPS_URL]
  npm run deploy:control-plane -- --account-id ACCOUNT_ID --github-app-id APP_ID --secrets-file PATH [--url HTTPS_URL] --confirm ${WORKER_NAME}

Without --confirm, the command validates configuration and performs only a Wrangler dry run.`;
}

async function main() {
  const parsed = parseArguments(process.argv.slice(2));
  if (parsed.help) {
    console.log(usage());
    return;
  }
  const options = validateDeploymentOptions(parsed);
  console.log(
    `Validated ${REQUIRED_SECRETS.length} required secrets without printing their values.`,
  );
  await runWrangler(options, true);
  if (!options.confirm) {
    console.log(`Dry run complete. Add --confirm ${WORKER_NAME} to deploy.`);
    return;
  }
  const output = await runWrangler(options, false);
  const deploymentUrl = options.url ?? deploymentUrlFromWranglerOutput(output);
  if (deploymentUrl === undefined) {
    throw new Error(
      "deployment succeeded but its URL was not detected; rerun with --url",
    );
  }
  await verifyDeployment(deploymentUrl, options.secrets.ADMIN_TOKEN);
  console.log(`Deployment is healthy and ready at ${deploymentUrl}.`);
}

const invokedPath =
  process.argv[1] === undefined
    ? undefined
    : pathToFileURL(resolve(process.argv[1])).href;
if (invokedPath === import.meta.url) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
