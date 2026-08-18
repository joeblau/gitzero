import { describe, expect, it } from "vitest";
import { agentMessageSchema } from "../src/protocol";

const baseRequest = {
  type: "workflow_token_request" as const,
  message_id: "11111111-1111-4111-8111-111111111111",
  job_id: "22222222-2222-4222-8222-222222222222",
  request_id: "33333333-3333-4333-8333-333333333333",
};

describe("agent protocol", () => {
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
});
