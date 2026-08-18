import { describe, expect, it } from "vitest";
import {
  SUPPORTED_PULL_REQUEST_ACTIONS,
  createGitHubWebhookJob,
} from "../src/index";

describe("GitHub webhook ingestion", () => {
  it("preserves the complete signed event for the assigned runner", () => {
    const event = {
      action: "synchronize",
      installation: { id: 7001 },
      number: 42,
      repository: {
        id: 9001,
        name: "widget",
        full_name: "acme/widget",
        clone_url: "https://github.com/acme/widget.git",
        owner: { id: 8001, login: "acme", type: "Organization" },
      },
      pull_request: {
        number: 42,
        draft: false,
        merged: false,
        merge_commit_sha: "3".repeat(64),
        title: "Preserve webhook metadata",
        labels: [{ name: "ci" }],
        head: {
          sha: "1".repeat(64),
          ref: "feature/metadata",
          repo: { full_name: "acme/widget" },
        },
        base: { sha: "2".repeat(64), ref: "main" },
        user: { login: "octocat" },
      },
      sender: { id: 1234, login: "octocat" },
    };

    const parsed = createGitHubWebhookJob(event);

    expect(parsed).toMatchObject({
      action: "synchronize",
      draft: false,
      job: {
        workspace_id: "7001",
        installation_id: 7001,
        run_number: 0,
        repository: {
          owner: "acme",
          name: "widget",
          clone_url: "https://github.com/acme/widget.git",
        },
        pull_request: expect.objectContaining({
          merge_sha: "3".repeat(64),
          execution_ref: "refs/pull/42/merge",
        }),
      },
    });
    expect(parsed.job.event).toEqual(event);
    expect(parsed.job.variables).toEqual({});
  });

  it("covers every current GitHub pull_request activity and uses the base ref after merge", () => {
    expect(SUPPORTED_PULL_REQUEST_ACTIONS).toEqual([
      "assigned",
      "unassigned",
      "labeled",
      "unlabeled",
      "opened",
      "edited",
      "closed",
      "reopened",
      "synchronize",
      "converted_to_draft",
      "locked",
      "unlocked",
      "enqueued",
      "dequeued",
      "milestoned",
      "demilestoned",
      "ready_for_review",
      "review_requested",
      "review_request_removed",
      "auto_merge_enabled",
      "auto_merge_disabled",
    ]);
    const closed = createGitHubWebhookJob({
      action: "closed",
      installation: { id: 7002 },
      repository: {
        id: 9001,
        name: "widget",
        clone_url: "https://github.com/acme/widget.git",
        owner: { id: 8001, login: "acme", type: "Organization" },
      },
      pull_request: {
        number: 42,
        draft: false,
        merged: true,
        merge_commit_sha: "3".repeat(40),
        head: { sha: "1".repeat(40), ref: "feature/merged" },
        base: { sha: "2".repeat(40), ref: "main" },
      },
      sender: { id: 1234, login: "octocat" },
    });
    expect(closed.job.pull_request.execution_ref).toBe("refs/heads/main");
  });
});
