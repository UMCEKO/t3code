import {
  isProviderNativeSubagentThread,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  makeWorkflowCoordinatorArtifacts,
  subagentThreadTitle,
  WORKFLOW_SCRIPT_OPENING_BYTE_CAP,
} from "./subagentProjection.ts";

describe("subagentThreadTitle", () => {
  it("clips a long prompt without splitting a surrogate pair", () => {
    const title = (prompt: string) =>
      subagentThreadTitle({ parentTitle: "Parent", prompt, ordinal: 1 });

    expect(title(`${"a".repeat(68)}😀 and more work.`)).toBe(`${"a".repeat(68)}...`);
    expect(title(`${"a".repeat(67)}😀 and more work.`)).toBe(`${"a".repeat(67)}😀...`);
  });
});

describe("makeWorkflowCoordinatorArtifacts", () => {
  const now = DateTime.makeUnsafe("2026-11-22T09:00:00.000Z");
  const parentThread: OrchestrationV2AppThread = {
    id: ThreadId.make("thread-parent"),
    projectId: ProjectId.make("project-1"),
    title: "Parent task",
    titleRegeneration: null,
    titleRenamed: false,
    linkedPullRequest: null,
    pullRequests: [],
    providerInstanceId: ProviderInstanceId.make("claudeAgent"),
    modelSelection: {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-sonnet-4-6",
    },
    activeProviderThreadId: null,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "main",
    worktreePath: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: ThreadId.make("thread-parent"),
    },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    pinnedAt: null,
    pinOrderKey: null,
    autoSettleDisabledAt: null,
    guestShare: null,
    lastVisitedAt: null,
    deletedAt: null,
  };

  it("builds C's thread, runless root node, and workflowScript opening message", () => {
    const coordinatorThreadId = ThreadId.make("thread-coordinator");
    const workflowNodeId = NodeId.make("node-workflow");
    const scriptText =
      'export const meta = { name: "Audit", description: "Audit code" };\nreturn 1;';

    const artifacts = makeWorkflowCoordinatorArtifacts({
      parentThread,
      coordinatorThreadId,
      workflowNodeId,
      scriptText,
      title: "Workflow · Audit",
      status: "running",
      now,
    });

    expect(artifacts.thread.id).toBe(coordinatorThreadId);
    expect(artifacts.thread.creationSource).toBe("server");
    expect(artifacts.thread.createdBy).toBe("agent");
    expect(artifacts.thread.title).toBe("Workflow · Audit");
    expect(artifacts.thread.lineage).toEqual({
      parentThreadId: parentThread.id,
      relationshipToParent: "subagent",
      rootThreadId: parentThread.id,
    });
    expect(artifacts.thread.forkedFrom).toEqual({ type: "node", nodeId: workflowNodeId });
    expect(isProviderNativeSubagentThread(artifacts.thread)).toBe(true);

    expect(artifacts.rootNode.threadId).toBe(coordinatorThreadId);
    expect(artifacts.rootNode.kind).toBe("root_turn");
    expect(artifacts.rootNode.runId).toBeNull();
    expect(artifacts.rootNode.parentNodeId).toBeNull();
    expect(artifacts.rootNode.rootNodeId).toBe(artifacts.rootNode.id);
    expect(artifacts.rootNode.status).toBe("running");
    expect(artifacts.rootNode.completedAt).toBeNull();

    expect(artifacts.scriptMessage.role).toBe("user");
    expect(artifacts.scriptMessage.threadId).toBe(coordinatorThreadId);
    expect(artifacts.scriptMessage.nodeId).toBe(artifacts.rootNode.id);
    expect(artifacts.scriptMessage.runId).toBeNull();
    expect(artifacts.scriptMessage.senderThreadId).toBe(parentThread.id);
    expect(artifacts.scriptMessage.workflowScript).toBe(true);
    expect(artifacts.scriptMessage.creationSource).toBe("server");
    expect(artifacts.scriptMessage.text).toBe(scriptText);

    expect(artifacts.scriptTurnItem.type).toBe("user_message");
    if (artifacts.scriptTurnItem.type === "user_message") {
      expect(artifacts.scriptTurnItem.workflowScript).toBe(true);
      expect(artifacts.scriptTurnItem.creationSource).toBe("server");
      expect(artifacts.scriptTurnItem.text).toBe(scriptText);
      expect(artifacts.scriptTurnItem.ordinal).toBe(100);
    }
    expect(artifacts.scriptTruncated).toBe(false);
  });

  it("sets completedAt on terminal root nodes and bounds scripts above 256 KiB safely", () => {
    expect(WORKFLOW_SCRIPT_OPENING_BYTE_CAP).toBe(256 * 1024);
    const over = `${"a".repeat(256 * 1024 - 2)}😀trailing`;
    const artifacts = makeWorkflowCoordinatorArtifacts({
      parentThread,
      coordinatorThreadId: ThreadId.make("thread-coordinator-2"),
      workflowNodeId: NodeId.make("node-workflow-2"),
      scriptText: over,
      title: "Workflow · Big",
      status: "completed",
      now,
    });

    expect(artifacts.rootNode.completedAt).toEqual(now);
    expect(artifacts.scriptTruncated).toBe(true);
    expect(artifacts.scriptMessage.workflowScript).toBe(true);
    expect(artifacts.scriptMessage.text).toContain("Truncated");
    // Must not split a UTF-16 surrogate pair at the boundary.
    expect(artifacts.scriptMessage.text).not.toMatch(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/);
    expect(artifacts.scriptMessage.text.startsWith("a".repeat(100))).toBe(true);
  });
});
