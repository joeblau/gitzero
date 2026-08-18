const encoder = new TextEncoder();
const AGENT_TOKEN_VERSION = "gzagt1";

export async function timingSafeSecretEqual(
  provided: string,
  expected: string,
): Promise<boolean> {
  const [providedHash, expectedHash] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(provided)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  return crypto.subtle.timingSafeEqual(providedHash, expectedHash);
}

export async function verifyBearer(
  request: Request,
  expected: string,
): Promise<boolean> {
  const provided = bearerToken(request);
  if (provided === null) {
    return false;
  }
  return timingSafeSecretEqual(provided, expected);
}

export async function createAgentToken(
  signingKey: string,
  workspaceId: string,
  agentId: string,
): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(signingKey),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(JSON.stringify([AGENT_TOKEN_VERSION, workspaceId, agentId])),
  );
  return `${AGENT_TOKEN_VERSION}.${hex(new Uint8Array(signature))}`;
}

export async function verifyAgentBearer(
  request: Request,
  signingKey: string,
  workspaceId: string,
  agentId: string,
): Promise<boolean> {
  const provided = bearerToken(request);
  if (provided === null) {
    return false;
  }
  const expected = await createAgentToken(signingKey, workspaceId, agentId);
  return timingSafeSecretEqual(provided, expected);
}

export async function verifyWebhookSignature(
  body: ArrayBufferLike,
  signature: string | null,
  secret: string,
): Promise<boolean> {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const expected = await crypto.subtle.sign(
    "HMAC",
    key,
    Uint8Array.from(new Uint8Array(body)),
  );
  const supplied = parseSignature(signature);
  return crypto.subtle.timingSafeEqual(expected, supplied);
}

function parseSignature(signature: string | null): Uint8Array {
  const fallback = new Uint8Array(32);
  if (!signature?.startsWith("sha256=")) {
    return fallback;
  }
  const hex = signature.slice("sha256=".length);
  if (!/^[0-9a-fA-F]{64}$/.test(hex)) {
    return fallback;
  }
  return Uint8Array.from({ length: 32 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

function bearerToken(request: Request): string | null {
  const authorization = request.headers.get("Authorization");
  if (!authorization?.startsWith("Bearer ")) {
    return null;
  }
  return authorization.slice("Bearer ".length);
}

function hex(value: Uint8Array): string {
  return Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}
