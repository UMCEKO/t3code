import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpThreadWaitResult,
  OrchestratorMcpWorkflowMeta,
  OrchestratorMcpWorkflowRunInput,
} from "./orchestratorMcp.ts";

const decodeDelegateTaskInput = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
const decodeDelegateTaskResult = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskResult);
const decodeThreadWaitResult = Schema.decodeUnknownSync(OrchestratorMcpThreadWaitResult);
const decodeWorkflowRunInput = Schema.decodeUnknownSync(OrchestratorMcpWorkflowRunInput);
const decodeWorkflowMeta = Schema.decodeUnknownSync(OrchestratorMcpWorkflowMeta);

describe("orchestrator MCP contracts", () => {
  it("decodes cross-provider delegated task requests and durable results", () => {
    const request = decodeDelegateTaskInput({
      task: "Inspect the workspace and report the result.",
      target: {
        providerInstanceId: "claudeAgent",
        model: "claude-sonnet-4-6",
      },
      mode: "wait",
      timeoutMs: 5_000,
      clientRequestId: "delegate-1",
      runtimeMode: "inherit",
      interactionMode: "inherit",
    });
    const result = decodeDelegateTaskResult({
      taskId: "node-task-1",
      childThreadId: "thread-child-1",
      childRunId: "run-child-1",
      childNodeId: "node-task-1",
      status: "completed",
      workState: "result_available",
      hasPendingChildRuns: false,
      providerInstanceId: "claudeAgent",
      model: "claude-sonnet-4-6",
      summary: "Workspace inspected.",
      resultContextTransferId: "context-transfer-result-1",
      latestTerminalRunId: "run-child-1",
      latestTerminalStatus: "completed",
      latestTerminalSummary: "Workspace inspected.",
      latestTerminalResultContextTransferId: "context-transfer-result-1",
      hasPendingQuestion: false,
      pendingApproval: null,
      waitTimedOut: false,
    });

    expect(request.target?.providerInstanceId).toBe("claudeAgent");
    expect(result.status).toBe("completed");
    expect(result.summary).toBe("Workspace inspected.");
    expect(result.pendingApproval).toBeNull();
    const pendingApproval = {
      requestId: "request-approval-1",
      kind: "command",
      runId: "run-child-1",
    };
    expect(
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, input: { command: "private command" } },
      }).pendingApproval,
    ).toEqual(pendingApproval);
    expect(() =>
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, kind: "user_input" },
      }),
    ).toThrow();
  });

  it("decodes a running-task next written by another version", () => {
    const next = "Completion wakes this thread, so end your turn. Do not wait on it or poll it.";
    expect(
      decodeThreadWaitResult({
        threadId: "thread-child-1",
        runId: "run-child-1",
        status: "running",
        timedOut: true,
        next,
      }).next,
    ).toBe(next);
  });

  it("decodes optional concurrency on workflow meta and t3_workflow_run input", () => {
    const meta = decodeWorkflowMeta({
      name: "Audit",
      description: "Audit files",
      concurrency: 3,
      phases: [{ title: "Scan" }],
    });
    expect(meta.concurrency).toBe(3);

    const run = decodeWorkflowRunInput({
      script: 'export const meta = { name: "Audit", description: "Audit files", concurrency: 3 };',
      concurrency: 2,
      tokenBudget: 10_000,
    });
    expect(run.concurrency).toBe(2);

    expect(() => decodeWorkflowMeta({ name: "Audit", description: "d", concurrency: 0 })).toThrow();
    expect(() => decodeWorkflowRunInput({ script: "x", concurrency: 0 })).toThrow();
  });
});
