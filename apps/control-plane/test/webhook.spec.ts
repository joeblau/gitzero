import { describe, expect, it } from "vitest";
import { createGitHubWebhookJob } from "../src/index";

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
        merge_commit_sha: "3".repeat(40),
        title: "Preserve webhook metadata",
        labels: [{ name: "ci" }],
        head: {
          sha: "1".repeat(40),
          ref: "feature/metadata",
          repo: { full_name: "acme/widget" },
        },
        base: { sha: "2".repeat(40), ref: "main" },
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
        pull_request: expect.objectContaining({ merge_sha: "3".repeat(40) }),
      },
    });
    expect(parsed.job.event).toEqual(event);
    expect(parsed.job.variables).toEqual({});
  });
});
