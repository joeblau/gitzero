import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createAgentToken,
  createCheckRun,
  createEnvironmentToken,
  createPrivateCheckoutToken,
  createSharedRepositoryToken,
  createWorkflowToken,
  disableRepositoryActions,
  fetchActionsVariables,
  fetchActionsVariablesWithToken,
  fetchPullRequestMergeSnapshot,
  fetchRepositoryOnboardingEvidence,
  syncGitHubDeployment,
  updateCheckRun,
} from "../src/github";
import type { CheckAnnotation, QueuedJob } from "../src/protocol";

const githubEnvironment = { GITHUB_API_VERSION: "2026-03-10" } as const;
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

describe("GitHub Actions variables", () => {
  it("mints separately scoped source and environment tokens with authoritative expiry", async () => {
    const privateKey = await testPrivateKeyPem("pkcs1");
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        requests.push({ url: new URL(String(input)), init });
        const permissions = JSON.parse(String(init?.body))
          .permissions as Record<string, string>;
        return Response.json(
          installationToken(
            permissions.environments === "read"
              ? "environment-token"
              : "checkout-token",
          ),
        );
      }),
    );

    await expect(
      Promise.all([
        createAgentToken(
          {
            GITHUB_API_VERSION: "2026-03-10",
            GITHUB_APP_ID: "1234",
            GITHUB_APP_PRIVATE_KEY: privateKey,
          },
          7001,
          "hello-world",
        ),
        createEnvironmentToken(
          {
            GITHUB_API_VERSION: "2026-03-10",
            GITHUB_APP_ID: "1234",
            GITHUB_APP_PRIVATE_KEY: privateKey,
          },
          7001,
          "hello-world",
        ),
      ]),
    ).resolves.toEqual([
      {
        token: "checkout-token",
        expiresAtEpochSeconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
      },
      {
        token: "environment-token",
        expiresAtEpochSeconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
      },
    ]);

    expect(requests).toHaveLength(2);
    expect(
      requests.map((request) => JSON.parse(String(request.init?.body))),
    ).toEqual(
      expect.arrayContaining([
        {
          repositories: ["hello-world"],
          permissions: { contents: "read", pull_requests: "read" },
        },
        {
          repositories: ["hello-world"],
          permissions: { actions: "read", environments: "read" },
        },
      ]),
    );
    const authorizations = requests.map((request) =>
      new Headers(request.init?.headers).get("Authorization"),
    );
    expect(authorizations[0]).toMatch(/^Bearer eyJ/);
    expect(authorizations[1]).toMatch(/^Bearer eyJ/);
  });

  it("rejects installation credentials without a valid authoritative expiry", async () => {
    const privateKey = await testPrivateKeyPem();
    let response: unknown = { token: "missing-expiry" };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json(response)),
    );
    const create = () =>
      createAgentToken(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "hello-world",
      );

    await expect(create()).rejects.toThrow();
    response = { token: "malformed-expiry", expires_at: "not-a-date" };
    await expect(create()).rejects.toThrow();
  });

  it("mints a separate repository-scoped Variables read token", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("variables-token"));
        }
        return Response.json({ total_count: 0, variables: [] });
      }),
    );

    await expect(
      fetchActionsVariables(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "octocat",
        "hello-world",
        false,
      ),
    ).resolves.toEqual({});

    expect(requests).toHaveLength(2);
    expect(requests[0]?.url.pathname).toBe(
      "/app/installations/7001/access_tokens",
    );
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["hello-world"],
      permissions: { variables: "read" },
    });
    expect(new Headers(requests[1]?.init?.headers).get("Authorization")).toBe(
      "Bearer variables-token",
    );
  });

  it("paginates accessible organization variables and applies repository precedence", async () => {
    const organizationPage = Array.from({ length: 30 }, (_, index) => ({
      name: index === 0 ? "CHANNEL" : `ORG_${String(index).padStart(2, "0")}`,
      value: index === 0 ? "organization" : `org-${index}`,
    }));
    const requestUrls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requestUrls.push(`${url.pathname}${url.search}`);
        expect(new Headers(init?.headers).get("Authorization")).toBe(
          "Bearer variables-token",
        );
        expect(new Headers(init?.headers).get("X-GitHub-Api-Version")).toBe(
          "2026-03-10",
        );
        if (url.pathname.endsWith("/actions/organization-variables")) {
          const page = Number(url.searchParams.get("page"));
          return Response.json(
            page === 1
              ? { total_count: 31, variables: organizationPage }
              : {
                  total_count: 31,
                  variables: [{ name: "ORG_LAST", value: "last" }],
                },
          );
        }
        return Response.json({
          total_count: 2,
          variables: [
            { name: "REPO_ONLY", value: "repository" },
            { name: "channel", value: "repository-override" },
          ],
        });
      }),
    );

    const variables = await fetchActionsVariablesWithToken(
      githubEnvironment,
      "variables-token",
      "acme",
      "widget",
      true,
    );

    expect(variables.channel).toBe("repository-override");
    expect(variables).not.toHaveProperty("CHANNEL");
    expect(variables.REPO_ONLY).toBe("repository");
    expect(variables.ORG_01).toBe("org-1");
    expect(variables.ORG_LAST).toBe("last");
    expect(requestUrls).toEqual(
      expect.arrayContaining([
        "/repos/acme/widget/actions/variables?per_page=30&page=1",
        "/repos/acme/widget/actions/organization-variables?per_page=30&page=1",
        "/repos/acme/widget/actions/organization-variables?per_page=30&page=2",
      ]),
    );
  });

  it("skips organization lookup for user-owned repositories", async () => {
    const fetchMock = vi.fn(async () =>
      Response.json({
        total_count: 1,
        variables: [{ name: "RUNTIME", value: "24" }],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      fetchActionsVariablesWithToken(
        githubEnvironment,
        "variables-token",
        "octocat",
        "hello-world",
        false,
      ),
    ).resolves.toEqual({ RUNTIME: "24" });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("matches GitHub's alphabetical 256 KiB run boundary", async () => {
    const value = "x".repeat(48 * 1_024 - 16);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          total_count: 6,
          variables: ["F", "E", "D", "C", "B", "A"].map((name) => ({
            name,
            value,
          })),
        }),
      ),
    );

    const variables = await fetchActionsVariablesWithToken(
      githubEnvironment,
      "variables-token",
      "octocat",
      "hello-world",
      false,
    );

    expect(Object.keys(variables)).toEqual(["A", "B", "C", "D", "E"]);
    expect(variables).not.toHaveProperty("F");
  });
});

describe("pull request merge snapshots", () => {
  it("resolves the exact tested merge commit with a pull-requests-read token", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("pull-request-token"));
        }
        return Response.json({
          head: { sha: "A".repeat(40) },
          base: { sha: "B".repeat(40) },
          merged: false,
          mergeable: true,
          merge_commit_sha: "C".repeat(40),
        });
      }),
    );

    await expect(
      fetchPullRequestMergeSnapshot(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme corp",
        "hello world",
        42,
        "a".repeat(40),
        "b".repeat(40),
      ),
    ).resolves.toEqual({ status: "ready", merge_sha: "C".repeat(40) });

    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["hello world"],
      permissions: { pull_requests: "read" },
    });
    expect(requests[1]?.url.pathname).toBe(
      "/repos/acme%20corp/hello%20world/pulls/42",
    );
    expect(new Headers(requests[1]?.init?.headers).get("Authorization")).toBe(
      "Bearer pull-request-token",
    );
  });

  it("distinguishes pending, conflicted, and changed snapshots", async () => {
    const privateKey = await testPrivateKeyPem();
    let response = {
      head: { sha: "1".repeat(40) },
      base: { sha: "2".repeat(40) },
      merged: false,
      mergeable: null as boolean | null,
      merge_commit_sha: null as string | null,
    };
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) =>
        new URL(String(input)).pathname.endsWith("/access_tokens")
          ? Response.json(installationToken("pull-request-token"))
          : Response.json(response),
      ),
    );
    const resolve = () =>
      fetchPullRequestMergeSnapshot(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme",
        "widget",
        42,
        "1".repeat(40),
        "2".repeat(40),
      );

    await expect(resolve()).resolves.toEqual({ status: "pending" });
    response = { ...response, mergeable: false };
    await expect(resolve()).resolves.toEqual({ status: "conflicted" });
    response = {
      ...response,
      merged: true,
      mergeable: null,
      merge_commit_sha: "5".repeat(40),
    };
    await expect(resolve()).resolves.toEqual({
      status: "ready",
      merge_sha: "5".repeat(40),
    });
    response = {
      ...response,
      head: { sha: "4".repeat(40) },
      merged: false,
      mergeable: true,
      merge_commit_sha: "3".repeat(40),
    };
    await expect(resolve()).resolves.toEqual({
      status: "changed",
      current_head_sha: "4".repeat(40),
      current_base_sha: "2".repeat(40),
    });
  });
});

describe("GitHub production API contracts", () => {
  it("creates a merge-snapshot deployment and publishes its running status", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("deployment-token"));
        }
        if (url.pathname.endsWith("/deployments")) {
          return Response.json({ id: 91, payload: {} }, { status: 201 });
        }
        if (url.pathname.endsWith("/deployments/91/statuses")) {
          return Response.json({ id: 92 }, { status: 201 });
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    await expect(
      syncGitHubDeployment(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        fixtureJob(),
        "deploy / matrix[region=west]",
        "production-west",
        "in_progress",
        null,
        null,
        false,
      ),
    ).resolves.toBe(91);

    expect(requests).toHaveLength(3);
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["widget"],
      permissions: { deployments: "write" },
    });
    expect(JSON.parse(String(requests[1]?.init?.body))).toEqual({
      ref: "3".repeat(40),
      task: "deploy",
      auto_merge: false,
      required_contexts: [],
      environment: "production-west",
      description: "GitZero workflow deployment.",
      payload: {
        gitzero: {
          job_id: "11111111-1111-4111-8111-111111111111",
          unit_id: "deploy / matrix[region=west]",
        },
      },
    });
    expect(JSON.parse(String(requests[2]?.init?.body))).toEqual({
      state: "in_progress",
      environment: "production-west",
      description: "GitZero deployment is running.",
      auto_inactive: false,
    });
    expect(
      requests
        .slice(1)
        .map((request) =>
          new Headers(request.init?.headers).get("Authorization"),
        ),
    ).toEqual(["Bearer deployment-token", "Bearer deployment-token"]);
  });

  it("recovers ambiguous deployment writes without creating duplicate records or statuses", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("deployment-token"));
        }
        if (url.pathname.endsWith("/deployments")) {
          return Response.json([
            {
              id: 90,
              payload: {
                gitzero: {
                  job_id: "another-job",
                  unit_id: "deploy / matrix[region=west]",
                },
              },
            },
            {
              id: 91,
              payload: {
                gitzero: {
                  job_id: "11111111-1111-4111-8111-111111111111",
                  unit_id: "deploy / matrix[region=west]",
                },
              },
            },
          ]);
        }
        if (url.pathname.endsWith("/deployments/91/statuses")) {
          return Response.json([
            {
              state: "inactive",
              description: "Superseded elsewhere.",
              environment: "production-west",
              environment_url: null,
            },
            {
              state: "success",
              description: "GitZero deployment completed successfully.",
              environment: "production-west",
              environment_url: "https://west.example.test/releases/42",
            },
          ]);
        }
        throw new Error(`unexpected GitHub request: ${url.pathname}`);
      }),
    );

    await expect(
      syncGitHubDeployment(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        fixtureJob(),
        "deploy / matrix[region=west]",
        "production-west",
        "success",
        "https://west.example.test/releases/42",
        null,
        true,
      ),
    ).resolves.toBe(91);

    expect(requests).toHaveLength(3);
    expect(requests[1]?.init?.method).toBe("GET");
    expect(Object.fromEntries(requests[1]?.url.searchParams ?? [])).toEqual({
      sha: "3".repeat(40),
      environment: "production-west",
      task: "deploy",
      per_page: "100",
    });
    expect(requests[2]?.init?.method).toBe("GET");
  });

  it("mints workflow tokens with only the requested read and write permissions", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        requests.push({ url: new URL(String(input)), init });
        return Response.json(installationToken("exact-scoped-token"));
      }),
    );

    await expect(
      createWorkflowToken(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "widget",
        ["artifact-metadata", "security-events"],
        ["pull-requests"],
      ),
    ).resolves.toEqual({
      token: "exact-scoped-token",
      expiresAtEpochSeconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
    });
    expect(requests).toHaveLength(1);
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["widget"],
      permissions: {
        artifact_metadata: "read",
        pull_requests: "write",
        security_events: "read",
      },
    });

    await expect(
      createWorkflowToken(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "widget",
        ["contents"],
        ["contents"],
      ),
    ).rejects.toThrow("nonempty disjoint set");
    expect(requests).toHaveLength(1);
  });

  it("mints a contents-only token after the target sharing policy authorizes the caller", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          const permissions = JSON.parse(String(init?.body))
            .permissions as Record<string, string>;
          return Response.json(
            installationToken(
              permissions.administration === "read"
                ? "policy-token"
                : "shared-contents-token",
            ),
          );
        }
        expect(url.pathname).toBe(
          "/repos/ACME/shared-actions/actions/permissions/access",
        );
        expect(new Headers(init?.headers).get("Authorization")).toBe(
          "Bearer policy-token",
        );
        return Response.json({ access_level: "organization" });
      }),
    );

    await expect(
      createSharedRepositoryToken(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme",
        "widget",
        "Organization",
        "ACME",
        "shared-actions",
      ),
    ).resolves.toEqual({
      token: "shared-contents-token",
      expiresAtEpochSeconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
    });

    expect(requests).toHaveLength(3);
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["shared-actions"],
      permissions: { administration: "read" },
    });
    expect(requests[1]?.init?.method).toBe("GET");
    expect(JSON.parse(String(requests[2]?.init?.body))).toEqual({
      repositories: ["shared-actions"],
      permissions: { contents: "read" },
    });
    expect(JSON.parse(String(requests[2]?.init?.body))).not.toMatchObject({
      permissions: { administration: expect.anything() },
    });
  });

  it("fails closed before minting a contents token when sharing is disabled", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        return url.pathname.endsWith("/access_tokens")
          ? Response.json(installationToken("policy-token"))
          : Response.json({ access_level: "none" });
      }),
    );

    await expect(
      createSharedRepositoryToken(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "octocat",
        "caller",
        "User",
        "octocat",
        "private-action",
      ),
    ).rejects.toThrow("does not allow this caller");
    expect(requests).toHaveLength(2);
  });

  it("mints a checkout token scoped only to a same-owner installation repository", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        requests.push({ url: new URL(String(input)), init });
        return Response.json(installationToken("private-checkout-token"));
      }),
    );

    await expect(
      createPrivateCheckoutToken(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme",
        "widget",
        "ACME",
        "private-dependency",
      ),
    ).resolves.toEqual({
      token: "private-checkout-token",
      expiresAtEpochSeconds: INSTALLATION_TOKEN_EXPIRY_EPOCH_SECONDS,
    });

    expect(requests).toHaveLength(1);
    expect(requests[0]?.url.pathname).toBe(
      "/app/installations/7001/access_tokens",
    );
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["private-dependency"],
      permissions: { contents: "read" },
    });
  });

  it("rejects cross-owner and same-repository token requests without calling GitHub", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const environment = {
      GITHUB_API_VERSION: "2026-03-10",
      GITHUB_APP_ID: "1234",
      GITHUB_APP_PRIVATE_KEY: "unused",
    };

    await expect(
      createSharedRepositoryToken(
        environment,
        7001,
        "acme",
        "widget",
        "Organization",
        "another-owner",
        "shared-actions",
      ),
    ).rejects.toThrow("under the caller owner");
    await expect(
      createSharedRepositoryToken(
        environment,
        7001,
        "acme",
        "widget",
        "Organization",
        "ACME",
        "WIDGET",
      ),
    ).rejects.toThrow("different repository");
    await expect(
      createPrivateCheckoutToken(
        environment,
        7001,
        "acme",
        "widget",
        "another-owner",
        "private-dependency",
      ),
    ).rejects.toThrow("under the caller owner");
    await expect(
      createPrivateCheckoutToken(
        environment,
        7001,
        "acme",
        "widget",
        "ACME",
        "WIDGET",
      ),
    ).rejects.toThrow("different repository");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("reads repository Actions state with a repository-scoped Administration token", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("readiness-token"));
        }
        return Response.json({
          enabled: true,
          allowed_actions: "selected",
          selected_actions_url:
            "https://api.github.com/repositories/42/actions/permissions/selected-actions",
          sha_pinning_required: true,
        });
      }),
    );

    await expect(
      fetchRepositoryOnboardingEvidence(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme corp",
        "hello world",
        null,
      ),
    ).resolves.toEqual({
      actions: {
        enabled: true,
        allowed_actions: "selected",
        selected_actions_url:
          "https://api.github.com/repositories/42/actions/permissions/selected-actions",
        sha_pinning_required: true,
      },
      check_run: null,
    });

    expect(requests).toHaveLength(2);
    expect(requests[0]?.url.pathname).toBe(
      "/app/installations/7001/access_tokens",
    );
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["hello world"],
      permissions: { administration: "read" },
    });
    expect(requests[1]?.url.pathname).toBe(
      "/repos/acme%20corp/hello%20world/actions/permissions",
    );
    expect(requests[1]?.init?.method).toBe("GET");
    expect(new Headers(requests[1]?.init?.headers).get("Authorization")).toBe(
      "Bearer readiness-token",
    );
    expect(
      new Headers(requests[1]?.init?.headers).get("X-GitHub-Api-Version"),
    ).toBe("2026-03-10");
  });

  it("reads the candidate Check back with the same least-privilege token", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("readiness-token"));
        }
        if (url.pathname.endsWith("/actions/permissions")) {
          return Response.json({ enabled: true, allowed_actions: "all" });
        }
        return Response.json({
          id: 44,
          name: "GitZero",
          head_sha: "1".repeat(40),
          external_id: "11111111-1111-4111-8111-111111111111",
          status: "completed",
          conclusion: "success",
        });
      }),
    );

    await expect(
      fetchRepositoryOnboardingEvidence(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme",
        "widget",
        44,
      ),
    ).resolves.toMatchObject({
      actions: { enabled: true, allowed_actions: "all" },
      check_run: {
        id: 44,
        name: "GitZero",
        status: "completed",
        conclusion: "success",
      },
    });

    expect(requests).toHaveLength(3);
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["widget"],
      permissions: { administration: "read", checks: "read" },
    });
    expect(requests.slice(1).map((request) => request.url.pathname)).toEqual(
      expect.arrayContaining([
        "/repos/acme/widget/actions/permissions",
        "/repos/acme/widget/check-runs/44",
      ]),
    );
    for (const request of requests.slice(1)) {
      expect(new Headers(request.init?.headers).get("Authorization")).toBe(
        "Bearer readiness-token",
      );
    }
  });

  it("disables native Actions with one repository-scoped Administration write token and verifies the result", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("onboarding-token"));
        }
        if (init?.method === "PUT") {
          return new Response(null, { status: 204 });
        }
        return Response.json({ enabled: false, allowed_actions: "all" });
      }),
    );

    await expect(
      disableRepositoryActions(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme corp",
        "hello world",
      ),
    ).resolves.toEqual({ enabled: false, allowed_actions: "all" });

    expect(requests).toHaveLength(3);
    expect(requests[0]?.url.pathname).toBe(
      "/app/installations/7001/access_tokens",
    );
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["hello world"],
      permissions: { administration: "write" },
    });
    expect(requests.slice(1).map((request) => request.url.pathname)).toEqual([
      "/repos/acme%20corp/hello%20world/actions/permissions",
      "/repos/acme%20corp/hello%20world/actions/permissions",
    ]);
    expect(requests[1]?.init?.method).toBe("PUT");
    expect(JSON.parse(String(requests[1]?.init?.body))).toEqual({
      enabled: false,
    });
    expect(requests[2]?.init?.method).toBe("GET");
    for (const request of requests.slice(1)) {
      expect(new Headers(request.init?.headers).get("Authorization")).toBe(
        "Bearer onboarding-token",
      );
      expect(
        new Headers(request.init?.headers).get("X-GitHub-Api-Version"),
      ).toBe("2026-03-10");
    }
  });

  it("creates Check Runs with only Checks write permission and the exact PR merge snapshot", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        return url.pathname.endsWith("/access_tokens")
          ? Response.json(installationToken("check-token"))
          : Response.json({ id: 44 }, { status: 201 });
      }),
    );

    await expect(
      createCheckRun(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        fixtureJob(),
      ),
    ).resolves.toBe(44);

    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      repositories: ["widget"],
      permissions: { checks: "write" },
    });
    expect(requests[1]?.url.pathname).toBe("/repos/acme/widget/check-runs");
    expect(JSON.parse(String(requests[1]?.init?.body))).toMatchObject({
      name: "GitZero",
      head_sha: "3".repeat(40),
      status: "queued",
      external_id: "11111111-1111-4111-8111-111111111111",
    });
  });

  it("reconciles existing annotations before retrying a completed Check update", async () => {
    const privateKey = await testPrivateKeyPem();
    const duplicate = {
      path: "src/lib.rs",
      start_line: 7,
      end_line: 7,
      start_column: 2,
      end_column: 5,
      annotation_level: "warning" as const,
      message: "check this expression",
      title: "Compiler",
    };
    const generic = {
      path: ".github",
      start_line: 1,
      end_line: 1,
      start_column: null,
      end_column: null,
      annotation_level: "failure" as const,
      message: "tests failed",
      title: null,
    };
    const desired: CheckAnnotation[] = [duplicate, duplicate, generic];
    let existing: CheckAnnotation[] = [duplicate];
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("check-token"));
        }
        if (url.pathname.endsWith("/annotations")) {
          return Response.json(existing);
        }
        return Response.json({});
      }),
    );
    const job = fixtureJob();
    job.check_run_id = 44;
    const environment = {
      GITHUB_API_VERSION: "2026-03-10",
      GITHUB_APP_ID: "1234",
      GITHUB_APP_PRIVATE_KEY: privateKey,
    };

    await updateCheckRun(environment, job, {
      status: "completed",
      conclusion: "failure",
      title: "GitZero failed",
      summary: "lint failed",
      annotations: desired,
    });

    expect(requests).toHaveLength(3);
    expect(requests[1]?.url.pathname).toBe(
      "/repos/acme/widget/check-runs/44/annotations",
    );
    expect(requests[1]?.url.searchParams.get("per_page")).toBe("100");
    const firstUpdate = JSON.parse(String(requests[2]?.init?.body));
    expect(firstUpdate.output.annotations).toEqual([
      {
        path: "src/lib.rs",
        start_line: 7,
        end_line: 7,
        start_column: 2,
        end_column: 5,
        annotation_level: "warning",
        message: "check this expression",
        title: "Compiler",
      },
      {
        path: ".github",
        start_line: 1,
        end_line: 1,
        annotation_level: "failure",
        message: "tests failed",
      },
    ]);

    existing = desired;
    requests.length = 0;
    await updateCheckRun(environment, job, {
      status: "completed",
      conclusion: "failure",
      title: "GitZero failed",
      summary: "lint failed",
      annotations: desired,
    });
    expect(requests).toHaveLength(3);
    const retry = JSON.parse(String(requests[2]?.init?.body));
    expect(retry.output).not.toHaveProperty("annotations");
  });

  it("recovers an exact existing Check before a retry can create a duplicate", async () => {
    const privateKey = await testPrivateKeyPem();
    const requests: Array<{ url: URL; init?: RequestInit }> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(String(input));
        requests.push({ url, init });
        if (url.pathname.endsWith("/access_tokens")) {
          return Response.json(installationToken("check-token"));
        }
        return Response.json({
          total_count: 2,
          check_runs: [
            {
              id: 43,
              name: "GitZero",
              head_sha: "1".repeat(40),
              external_id: "a-different-job",
            },
            {
              id: 44,
              name: "GitZero",
              head_sha: "3".repeat(40),
              external_id: "11111111-1111-4111-8111-111111111111",
            },
          ],
        });
      }),
    );

    await expect(
      createCheckRun(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        fixtureJob(),
        true,
      ),
    ).resolves.toBe(44);

    expect(requests).toHaveLength(2);
    expect(requests[1]?.init?.method).toBe("GET");
    expect(requests[1]?.url.pathname).toBe(
      `/repos/acme/widget/commits/${"3".repeat(40)}/check-runs`,
    );
    expect(Object.fromEntries(requests[1]?.url.searchParams ?? [])).toEqual({
      check_name: "GitZero",
      filter: "all",
      per_page: "100",
      app_id: "1234",
    });
  });

  it("bounds untrusted GitHub error bodies before diagnostics", async () => {
    const privateKey = await testPrivateKeyPem();
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("x".repeat(20_000), { status: 502 })),
    );

    let message = "";
    try {
      await fetchActionsVariables(
        {
          GITHUB_API_VERSION: "2026-03-10",
          GITHUB_APP_ID: "1234",
          GITHUB_APP_PRIVATE_KEY: privateKey,
        },
        7001,
        "acme",
        "widget",
        false,
      );
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toContain("failed (502)");
    expect(message).toContain("…");
    expect(message.length).toBeLessThan(4_300);
  });
});

function fixtureJob(): QueuedJob {
  return {
    id: "11111111-1111-4111-8111-111111111111",
    workspace_id: "7001",
    installation_id: 7001,
    run_number: 1,
    repository: {
      owner: "acme",
      name: "widget",
      clone_url: "https://github.com/acme/widget.git",
    },
    pull_request: {
      number: 42,
      action: "synchronize",
      head_sha: "1".repeat(40),
      base_sha: "2".repeat(40),
      merge_sha: "3".repeat(40),
      execution_ref: "refs/pull/42/merge",
      head_ref: "feature/readiness",
      base_ref: "main",
    },
    check_run_id: null,
    event: {},
    environment: {},
    variables: {},
    requires_github_token: true,
    report_to_github: true,
  };
}

async function testPrivateKeyPem(
  format: "pkcs1" | "pkcs8" = "pkcs8",
): Promise<string> {
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
  const pkcs8 = `-----BEGIN PRIVATE KEY-----\n${lines.join("\n")}\n-----END PRIVATE KEY-----`;
  return format === "pkcs1" ? pkcs8ToPkcs1Pem(pkcs8) : pkcs8;
}

function pkcs8ToPkcs1Pem(pkcs8: string): string {
  const encoded = pkcs8
    .replace("-----BEGIN PRIVATE KEY-----", "")
    .replace("-----END PRIVATE KEY-----", "")
    .replaceAll(/\s/g, "");
  const bytes = Uint8Array.from(atob(encoded), (character) =>
    character.charCodeAt(0),
  );
  const outer = readDerElement(bytes, 0, 0x30);
  const version = readDerElement(bytes, outer.contentsOffset, 0x02);
  const algorithm = readDerElement(bytes, version.nextOffset, 0x30);
  const privateKey = readDerElement(bytes, algorithm.nextOffset, 0x04);
  if (privateKey.nextOffset !== outer.nextOffset) {
    throw new Error("unexpected PKCS#8 test key structure");
  }
  const pkcs1 = bytes.slice(privateKey.contentsOffset, privateKey.nextOffset);
  let binary = "";
  for (const byte of pkcs1) binary += String.fromCharCode(byte);
  const base64 = btoa(binary);
  const lines = base64.match(/.{1,64}/g);
  if (!lines) throw new Error("failed to encode PKCS#1 test key");
  return `-----BEGIN RSA PRIVATE KEY-----\n${lines.join("\n")}\n-----END RSA PRIVATE KEY-----`;
}

function readDerElement(
  bytes: Uint8Array,
  offset: number,
  expectedTag: number,
): { contentsOffset: number; nextOffset: number } {
  if (bytes[offset] !== expectedTag) throw new Error("unexpected DER tag");
  const firstLength = bytes[offset + 1];
  if (firstLength === undefined) throw new Error("missing DER length");
  let contentsOffset = offset + 2;
  let length = firstLength;
  if ((firstLength & 0x80) !== 0) {
    const byteCount = firstLength & 0x7f;
    if (byteCount === 0 || byteCount > 4) throw new Error("invalid DER length");
    length = 0;
    for (let index = 0; index < byteCount; index += 1) {
      const byte = bytes[contentsOffset + index];
      if (byte === undefined) throw new Error("truncated DER length");
      length = length * 256 + byte;
    }
    contentsOffset += byteCount;
  }
  const nextOffset = contentsOffset + length;
  if (nextOffset > bytes.byteLength) throw new Error("truncated DER element");
  return { contentsOffset, nextOffset };
}
