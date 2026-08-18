import { describe, expect, it } from "vitest";
import {
  createAgentToken,
  timingSafeSecretEqual,
  verifyAgentBearer,
  verifyWebhookSignature,
} from "../src/auth";

describe("authentication", () => {
  it("matches GitHub's published HMAC-SHA256 test vector", async () => {
    const body = new TextEncoder().encode("Hello, World!").buffer;
    await expect(
      verifyWebhookSignature(
        body,
        "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
        "It's a Secret to Everybody",
      ),
    ).resolves.toBe(true);
  });

  it("rejects malformed and incorrect signatures", async () => {
    const body = new TextEncoder().encode("Hello, World!").buffer;
    await expect(verifyWebhookSignature(body, null, "secret")).resolves.toBe(
      false,
    );
    await expect(
      verifyWebhookSignature(body, "sha256=xyz", "secret"),
    ).resolves.toBe(false);
  });

  it("compares bearer secrets without a direct string comparison", async () => {
    await expect(timingSafeSecretEqual("same", "same")).resolves.toBe(true);
    await expect(timingSafeSecretEqual("same", "different")).resolves.toBe(
      false,
    );
  });

  it("scopes agent credentials to one workspace and agent ID", async () => {
    const token = await createAgentToken(
      "signing-key",
      "workspace-1",
      "mini-1",
    );
    const request = new Request("https://example.test", {
      headers: { Authorization: `Bearer ${token}` },
    });

    await expect(
      verifyAgentBearer(request, "signing-key", "workspace-1", "mini-1"),
    ).resolves.toBe(true);
    await expect(
      verifyAgentBearer(request, "signing-key", "workspace-2", "mini-1"),
    ).resolves.toBe(false);
    await expect(
      verifyAgentBearer(request, "signing-key", "workspace-1", "mini-2"),
    ).resolves.toBe(false);
  });
});
