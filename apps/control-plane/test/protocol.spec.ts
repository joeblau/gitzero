import { describe, expect, it } from "vitest";
import { agentMessageSchema } from "../src/protocol";

const baseRequest = {
  type: "workflow_token_request" as const,
  message_id: "11111111-1111-4111-8111-111111111111",
  job_id: "22222222-2222-4222-8222-222222222222",
  request_id: "33333333-3333-4333-8333-333333333333",
};

describe("agent protocol", () => {
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
