import { describe, expect, it } from "vitest";
import {
  decryptManagedSecret,
  deriveManagedSecretKey,
  encryptManagedSecret,
  managedSecretInputSchema,
  type SecretBinding,
} from "../src/secrets";

describe("managed secrets", () => {
  it("normalizes GitHub-compatible identities and rejects reserved or oversized values", () => {
    expect(
      managedSecretInputSchema.parse({
        scope: "organization",
        owner: "Acme",
        name: "release_token",
        value: "configured-value",
        visibility: "selected",
        selected_repositories: ["Acme/Widget", "acme/widget"],
      }),
    ).toEqual({
      scope: "organization",
      owner: "Acme",
      name: "RELEASE_TOKEN",
      value: "configured-value",
      visibility: "selected",
      selected_repositories: ["acme/widget"],
    });
    expect(() =>
      managedSecretInputSchema.parse({
        scope: "repository",
        owner: "acme",
        repository: "widget",
        name: "GITHUB_OVERRIDE",
        value: "value",
      }),
    ).toThrow();
    expect(() =>
      managedSecretInputSchema.parse({
        scope: "repository",
        owner: "acme",
        repository: "widget",
        name: "TOKEN",
        value: "x".repeat(48 * 1024 + 1),
      }),
    ).toThrow();
  });

  it("encrypts values with randomized, scope-bound authenticated metadata", async () => {
    const plaintext = "not-present-in-durable-storage";
    const key = await deriveManagedSecretKey("encryption-key-".padEnd(48, "k"));
    const binding: SecretBinding = {
      workspaceId: "installation-42",
      scope: "repository",
      owner: "acme",
      repository: "widget",
      environment: "",
      name: "API_TOKEN",
    };
    const first = await encryptManagedSecret(key, binding, plaintext);
    const second = await encryptManagedSecret(key, binding, plaintext);
    expect(first).not.toEqual(second);
    expect(JSON.stringify(first)).not.toContain(plaintext);
    await expect(decryptManagedSecret(key, binding, first)).resolves.toBe(
      plaintext,
    );
    await expect(
      decryptManagedSecret(
        key,
        { ...binding, repository: "another-repository" },
        first,
      ),
    ).rejects.toThrow();
  });
});
