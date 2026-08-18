import { describe, expect, it } from "vitest";
import {
  PROTOCOL_VERSION,
  agentMessageSchema,
  queuedJobSchema,
  runSpecSchema,
} from "../src/protocol";

const baseRequest = {
  type: "workflow_token_request" as const,
  message_id: "11111111-1111-4111-8111-111111111111",
  job_id: "22222222-2222-4222-8222-222222222222",
  request_id: "33333333-3333-4333-8333-333333333333",
};

describe("agent protocol", () => {
  it("requires protocol v11 run assignments to carry an exact execution snapshot", () => {
    expect(PROTOCOL_VERSION).toBe(11);
    const queued = queuedJobSchema.parse({
      id: "11111111-1111-4111-8111-111111111111",
      workspace_id: "7001",
      installation_id: 7001,
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
        head_ref: "feature/merge-snapshot",
        base_ref: "main",
      },
      check_run_id: null,
    });
    expect(queued.pull_request.merge_sha).toBeNull();
    expect(queued.pull_request.execution_ref).toBeNull();
    const run = {
      ...queued,
      pull_request: {
        ...queued.pull_request,
        merge_sha: "3".repeat(40),
        execution_ref: "refs/pull/42/merge",
      },
      checkout_token: "checkout-token",
      github_api_version: "2022-11-28",
    };
    expect(runSpecSchema.parse(run).pull_request.merge_sha).toBe(
      "3".repeat(40),
    );
    expect(runSpecSchema.parse(run).pull_request.execution_ref).toBe(
      "refs/pull/42/merge",
    );
    expect(runSpecSchema.safeParse(queued).success).toBe(false);
    expect(
      runSpecSchema.safeParse({
        ...run,
        pull_request: {
          ...run.pull_request,
          execution_ref: "refs/heads/main\nforged",
        },
      }).success,
    ).toBe(false);
    for (const executionRef of [
      "refs/heads/.hidden",
      "refs/heads/topic.lock/child",
    ]) {
      expect(
        runSpecSchema.safeParse({
          ...run,
          pull_request: { ...run.pull_request, execution_ref: executionRef },
        }).success,
      ).toBe(false);
    }
  });

  it("requires an explicit bounded repository token purpose", () => {
    const request = {
      type: "repository_token_request",
      message_id: baseRequest.message_id,
      job_id: baseRequest.job_id,
      request_id: baseRequest.request_id,
      owner: "acme",
      repository: "shared-source",
    };
    for (const purpose of ["shared_source", "checkout"] as const) {
      expect(agentMessageSchema.parse({ ...request, purpose })).toMatchObject({
        purpose,
      });
    }
    expect(agentMessageSchema.safeParse(request).success).toBe(false);
    expect(
      agentMessageSchema.safeParse({ ...request, purpose: "workflow" }).success,
    ).toBe(false);
  });

  it("accepts disjoint exact workflow read and write permissions", () => {
    expect(
      agentMessageSchema.parse({
        ...baseRequest,
        read_permissions: ["contents", "vulnerability-alerts"],
        write_permissions: ["checks", "pull-requests"],
      }),
    ).toMatchObject({
      read_permissions: ["contents", "vulnerability-alerts"],
      write_permissions: ["checks", "pull-requests"],
    });
  });

  it("rejects empty, overlapping, and read-only write permission sets", () => {
    for (const permissions of [
      { read_permissions: [], write_permissions: [] },
      {
        read_permissions: ["contents"],
        write_permissions: ["contents"],
      },
      {
        read_permissions: [],
        write_permissions: ["vulnerability-alerts"],
      },
    ]) {
      expect(
        agentMessageSchema.safeParse({ ...baseRequest, ...permissions })
          .success,
      ).toBe(false);
    }
  });

  it("accepts bounded repository-relative Check annotations", () => {
    expect(
      agentMessageSchema.parse({
        type: "job_finished",
        message_id: baseRequest.message_id,
        job_id: baseRequest.job_id,
        conclusion: "failure",
        summary: "lint failed",
        annotations: [
          {
            path: "src/main.ts",
            start_line: 4,
            end_line: 4,
            start_column: 2,
            end_column: 8,
            annotation_level: "failure",
            message: "invalid syntax",
            title: "Compiler",
          },
        ],
      }),
    ).toMatchObject({
      annotations: [
        {
          path: "src/main.ts",
          annotation_level: "failure",
        },
      ],
    });
  });

  it("rejects unsafe annotation paths and invalid ranges", () => {
    const annotation = {
      path: "src/main.ts",
      start_line: 4,
      end_line: 4,
      start_column: null,
      end_column: null,
      annotation_level: "warning",
      message: "check this",
      title: null,
    };
    for (const invalid of [
      { ...annotation, path: "../secret" },
      { ...annotation, end_line: 3 },
      { ...annotation, start_column: 2 },
      { ...annotation, end_line: 5, start_column: 2, end_column: 3 },
      { ...annotation, start_column: 8, end_column: 2 },
    ]) {
      expect(
        agentMessageSchema.safeParse({
          type: "job_finished",
          message_id: baseRequest.message_id,
          job_id: baseRequest.job_id,
          conclusion: "failure",
          summary: "lint failed",
          annotations: [invalid],
        }).success,
      ).toBe(false);
    }
  });
});
