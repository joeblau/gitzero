import { describe, expect, it } from "vitest";
import { buildRepositoryReadiness } from "../src/readiness";

const actionsEnabled = {
  enabled: true,
  allowed_actions: "all" as const,
  sha_pinning_required: false,
};

describe("repository onboarding readiness", () => {
  it("requires an online macOS agent and a successful reported Check before disable", () => {
    const readiness = buildRepositoryReadiness(
      7001,
      "acme",
      "widget",
      actionsEnabled,
      snapshot(),
      githubCheck(),
    );

    expect(readiness).toMatchObject({
      installation_id: 7001,
      repository: "acme/widget",
      compatible_agents_online: 1,
      available_capacity: 2,
      successful_check: {
        verified: true,
        job_id: "job-success",
        check_run_id: 44,
        head_sha: "1".repeat(40),
      },
      safe_to_disable_native_actions: true,
      onboarded: false,
      next_action: "disable_native_actions",
    });
  });

  it("reports completion only after native Actions is disabled", () => {
    const readiness = buildRepositoryReadiness(
      7001,
      "ACME",
      "WIDGET",
      { ...actionsEnabled, enabled: false },
      snapshot(),
      githubCheck(),
    );

    expect(readiness.safe_to_disable_native_actions).toBe(false);
    expect(readiness.onboarded).toBe(true);
    expect(readiness.next_action).toBe("complete");
  });

  it("verifies SHA-256 repository checks", () => {
    const value = snapshot();
    value.jobs[0]!.head_sha = "a".repeat(64);
    const check = { ...githubCheck(), head_sha: "a".repeat(64) };

    const readiness = buildRepositoryReadiness(
      7001,
      "acme",
      "widget",
      actionsEnabled,
      value,
      check,
    );

    expect(readiness.successful_check).toMatchObject({
      verified: true,
      head_sha: "a".repeat(64),
    });
  });

  it("does not accept manual jobs or Checks from another repository", () => {
    const value = snapshot();
    value.jobs[0]!.check_run_id = null;
    value.jobs.push({
      ...value.jobs[0]!,
      id: "other-repository",
      repository: "acme/other",
      check_run_id: 99,
    });
    const readiness = buildRepositoryReadiness(
      7001,
      "acme",
      "widget",
      actionsEnabled,
      value,
      githubCheck(),
    );

    expect(readiness.successful_check.verified).toBe(false);
    expect(readiness.safe_to_disable_native_actions).toBe(false);
    expect(readiness.next_action).toBe("run_test_pull_request");
  });

  it("waits when GitHub has not confirmed the exact local Check", () => {
    const readiness = buildRepositoryReadiness(
      7001,
      "acme",
      "widget",
      actionsEnabled,
      snapshot(),
      { ...githubCheck(), head_sha: "2".repeat(40) },
    );

    expect(readiness.successful_check).toMatchObject({
      verified: false,
      job_id: "job-success",
      check_run_id: 44,
      github_status: "completed",
      github_conclusion: "success",
    });
    expect(readiness.safe_to_disable_native_actions).toBe(false);
    expect(readiness.next_action).toBe("wait_for_check_sync");
  });

  it("asks for a compatible agent before a test run", () => {
    const value = snapshot();
    value.agents[0]!.labels = ["linux", "x64"];
    const readiness = buildRepositoryReadiness(
      7001,
      "acme",
      "widget",
      actionsEnabled,
      value,
      githubCheck(),
    );

    expect(readiness.compatible_agents_online).toBe(0);
    expect(readiness.available_capacity).toBe(0);
    expect(readiness.next_action).toBe("connect_compatible_agent");
  });
});

function snapshot() {
  return {
    agents: [
      {
        agent_id: "mini-1",
        status: "online" as const,
        labels: ["self-hosted", "macOS", "aarch64"],
        available_capacity: 2,
      },
    ],
    jobs: [
      {
        id: "job-success",
        repository: "acme/widget",
        head_sha: "1".repeat(40),
        status: "completed" as const,
        conclusion: "success" as const,
        check_run_id: 44 as number | null,
        completed_at: "2026-08-17T12:00:00.000Z" as string | null,
      },
    ],
  };
}

function githubCheck() {
  return {
    id: 44,
    name: "GitZero",
    head_sha: "1".repeat(40),
    external_id: "job-success",
    status: "completed",
    conclusion: "success",
  };
}
