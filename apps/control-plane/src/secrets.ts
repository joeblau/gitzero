import { z } from "zod";

export const MAX_MANAGED_SECRET_BYTES = 48 * 1024;
export const MAX_ORGANIZATION_SECRETS = 1_000;
export const MAX_REPOSITORY_SECRETS = 100;
export const MAX_ENVIRONMENT_SECRETS = 100;
export const MAX_JOB_ORGANIZATION_SECRETS = 100;

const utf8Bytes = (value: string): number =>
  new TextEncoder().encode(value).byteLength;
const secretName = z
  .string()
  .min(1)
  .max(255)
  .regex(/^[A-Za-z_][A-Za-z0-9_]*$/)
  .refine((value) => !value.toUpperCase().startsWith("GITHUB_"), {
    message: "secret names must not use the reserved GITHUB_ prefix",
  })
  .transform((value) => value.toUpperCase());
const owner = z
  .string()
  .trim()
  .min(1)
  .max(100)
  .regex(/^[A-Za-z0-9._-]+$/);
const repository = z
  .string()
  .trim()
  .min(1)
  .max(100)
  .regex(/^[A-Za-z0-9._-]+$/);
const environment = z
  .string()
  .trim()
  .min(1)
  .max(255)
  .refine((value) => !value.includes("\0") && !/[\r\n]/.test(value));
const value = z
  .string()
  .refine((secret) => utf8Bytes(secret) <= MAX_MANAGED_SECRET_BYTES, {
    message: `secret values must not exceed ${MAX_MANAGED_SECRET_BYTES} UTF-8 bytes`,
  });
const selectedRepository = z
  .string()
  .trim()
  .max(201)
  .regex(/^[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+$/)
  .transform((entry) => entry.toLowerCase());

export const managedSecretInputSchema = z.discriminatedUnion("scope", [
  z.object({
    scope: z.literal("organization"),
    owner,
    name: secretName,
    value,
    visibility: z.enum(["all", "private", "selected"]).default("all"),
    selected_repositories: z
      .array(selectedRepository)
      .max(1_000)
      .default([])
      .transform((entries) => [...new Set(entries)].sort()),
  }),
  z.object({
    scope: z.literal("repository"),
    owner,
    repository,
    name: secretName,
    value,
  }),
  z.object({
    scope: z.literal("environment"),
    owner,
    repository,
    environment,
    name: secretName,
    value,
  }),
]);

export const managedSecretIdentitySchema = z.discriminatedUnion("scope", [
  z.object({ scope: z.literal("organization"), owner, name: secretName }),
  z.object({
    scope: z.literal("repository"),
    owner,
    repository,
    name: secretName,
  }),
  z.object({
    scope: z.literal("environment"),
    owner,
    repository,
    environment,
    name: secretName,
  }),
]);

export type ManagedSecretInput = z.infer<typeof managedSecretInputSchema>;
export type ManagedSecretIdentity = z.infer<typeof managedSecretIdentitySchema>;
export type ManagedSecretScope = ManagedSecretIdentity["scope"];

export interface ManagedSecretMetadata {
  scope: ManagedSecretScope;
  owner: string;
  repository?: string;
  environment?: string;
  name: string;
  visibility?: "all" | "private" | "selected";
  selected_repositories?: string[];
  created_at: string;
  updated_at: string;
}

export interface EncryptedSecret {
  ciphertext: string;
  nonce: string;
}

export interface SecretBinding {
  workspaceId: string;
  scope: ManagedSecretScope;
  owner: string;
  repository: string;
  environment: string;
  name: string;
}

export function normalizedSecretIdentity(identity: ManagedSecretIdentity): {
  scope: ManagedSecretScope;
  owner: string;
  repository: string;
  environment: string;
  name: string;
} {
  return {
    scope: identity.scope,
    owner: identity.owner.toLowerCase(),
    repository:
      identity.scope === "organization"
        ? ""
        : identity.repository.toLowerCase(),
    environment:
      identity.scope === "environment"
        ? identity.environment.toLowerCase()
        : "",
    name: identity.name.toUpperCase(),
  };
}

export async function deriveManagedSecretKey(
  encryptionSecret: string,
): Promise<CryptoKey> {
  const material = new TextEncoder().encode(
    `GitZero managed secrets encryption v1\0${encryptionSecret}`,
  );
  const digest = await crypto.subtle.digest("SHA-256", material);
  material.fill(0);
  return crypto.subtle.importKey("raw", digest, "AES-GCM", false, [
    "encrypt",
    "decrypt",
  ]);
}

export async function encryptManagedSecret(
  key: CryptoKey,
  binding: SecretBinding,
  plaintext: string,
): Promise<EncryptedSecret> {
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const plaintextBytes = new TextEncoder().encode(plaintext);
  try {
    const encrypted = await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv: nonce,
        additionalData: secretAdditionalData(binding),
      },
      key,
      plaintextBytes,
    );
    return {
      ciphertext: encodeBase64(new Uint8Array(encrypted)),
      nonce: encodeBase64(nonce),
    };
  } finally {
    plaintextBytes.fill(0);
  }
}

export async function decryptManagedSecret(
  key: CryptoKey,
  binding: SecretBinding,
  encrypted: EncryptedSecret,
): Promise<string> {
  const decrypted = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: decodeBase64(encrypted.nonce),
      additionalData: secretAdditionalData(binding),
    },
    key,
    decodeBase64(encrypted.ciphertext),
  );
  const plaintext = new Uint8Array(decrypted);
  try {
    return new TextDecoder("utf-8", {
      fatal: true,
      ignoreBOM: false,
    }).decode(plaintext);
  } finally {
    plaintext.fill(0);
  }
}

function secretAdditionalData(binding: SecretBinding): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify([
      "gitzero-managed-secret",
      1,
      binding.workspaceId,
      binding.scope,
      binding.owner,
      binding.repository,
      binding.environment,
      binding.name,
    ]),
  );
}

function encodeBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}
