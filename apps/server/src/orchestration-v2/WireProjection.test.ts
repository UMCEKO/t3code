import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  MessageId,
  ContextHandoffId,
  ProviderSessionId,
  ProviderThreadId,
  NodeId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2Subagent,
  type OrchestrationV2SubagentWorkflow,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadStreamItem,
  OrchestrationV2TurnItem,
  OrchestrationV2TurnItemJson,
  type OrchestrationV2WorkflowAgent,
} from "@t3tools/contracts";
import {
  dedupeSnapshotBodies,
  restoreSnapshotBodies,
  restoreSubagentBodies,
} from "@t3tools/shared/orchestrationV2DedupedBodies";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import { subscribeOrchestrationV2Thread } from "../ws.ts";
import { coalesceLiveUpdatedEvents } from "./ThreadLiveEventCoalescer.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import {
  makeLiveBodyDeduper,
  projectTurnItemForWire,
  projectTurnItemForDetail,
  projectDomainEventForWire,
  projectStoredEventForWire,
  projectThreadProjectionForWire,
} from "./WireProjection.ts";
import { threadShellFromProjection } from "./ProjectionStore.ts";
import { MAX_TOOL_OUTPUT_IMAGES, toolOutputImages } from "@t3tools/shared/toolOutput";

const decodeTurnItem = Schema.decodeUnknownSync(OrchestrationV2TurnItem);
const encodeTurnItemJson = Schema.encodeSync(OrchestrationV2TurnItemJson);
const decodeTurnItemJson = Schema.decodeUnknownSync(OrchestrationV2TurnItemJson);

const base = {
  id: TurnItemId.make("tool-1"),
  type: "dynamic_tool" as const,
  threadId: ThreadId.make("thread-1"),
  runId: null,
  nodeId: null,
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 1,
  status: "completed" as const,
  title: "MCP tool",
  toolName: "mcp__github__fetch_pr",
  input: { pr: 42 },
  startedAt: DateTime.makeUnsafe("2026-08-13T00:00:00.000Z"),
  completedAt: DateTime.makeUnsafe("2026-08-13T00:00:01.000Z"),
  updatedAt: DateTime.makeUnsafe("2026-08-13T00:00:01.000Z"),
};

describe("orchestration V2 wire projection", () => {
  it("keeps copied handoff transcripts out of activity items and live events", () => {
    const item = {
      ...base,
      type: "handoff" as const,
      contextHandoffId: ContextHandoffId.make("handoff:wire"),
      fromProviderThreadIds: [],
      toProviderThreadId: ProviderThreadId.make("target"),
      fromProviderInstanceIds: [],
      toProviderInstanceId: ProviderInstanceId.make("codex"),
      strategy: "full_thread_summary" as const,
      summary: "PRIVATE_HANDOFF_TRANSCRIPT",
    };
    const projected = projectTurnItemForWire(item);
    expect(projected).not.toHaveProperty("summary");
    expect(decodeTurnItem(projected)).toMatchObject({ contextHandoffId: item.contextHandoffId });
    const event = {
      id: EventId.make("handoff:wire"),
      type: "turn-item.updated" as const,
      threadId: base.threadId,
      occurredAt: base.updatedAt,
      payload: item,
    };
    expect(JSON.stringify(projectDomainEventForWire(event))).not.toContain(item.summary);
    expect(item.summary).toBe("PRIVATE_HANDOFF_TRANSCRIPT");
  });

  it("preserves image metadata through wire and JSON contracts while redacting output", () => {
    const item = {
      ...base,
      toolName: "Read",
      input: { file_path: "/workspace/reference.png" },
      viewedImagePath: "/workspace/reference.png",
      output: { data: "private-image-data" },
    } satisfies OrchestrationV2TurnItem;
    const projected = projectTurnItemForWire(item);
    const live = decodeTurnItem(projected);
    const encoded = encodeTurnItemJson(live);
    const json = decodeTurnItemJson(encoded);
    const decoded = decodeTurnItem(json);
    expect(decoded).toMatchObject({ viewedImagePath: "/workspace/reference.png" });
    expect(decoded).not.toHaveProperty("output");
    expect(item.output.data).toBe("private-image-data");
  });

  it("keeps transcript bodies out of shell rows", () => {
    const now = DateTime.makeUnsafe("2026-08-13T00:00:00.000Z");
    const threadId = ThreadId.make("thread-shell-budget");
    const projection = {
      thread: {
        createdBy: "user",
        creationSource: "web",
        id: threadId,
        projectId: ProjectId.make("project-shell-budget"),
        title: "Shell payload budget",
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: null,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        lineage: {
          parentThreadId: null,
          relationshipToParent: null,
          rootThreadId: threadId,
        },
        forkedFrom: null,
        activeProviderThreadId: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        deletedAt: null,
      },
      runs: [],
      attempts: [],
      runtimeRequests: [],
      messages: [
        {
          id: MessageId.make("message-shell-budget"),
          role: "assistant",
          text: "x".repeat(1_000_000),
          updatedAt: now,
        },
      ],
      providerSessions: [],
      providerThreads: [],
      turnItems: [],
      visibleTurnItems: [],
      plans: [],
      updatedAt: now,
    } as unknown as OrchestrationV2ThreadProjection;

    const shell = threadShellFromProjection(projection);

    expect(shell.latestVisibleMessage).toBeNull();
    expect(JSON.stringify(shell).length).toBeLessThan(2_000);
  });

  it("omits even small dynamic tool results while retaining input", () => {
    const item = { ...base, output: { ok: true } } satisfies OrchestrationV2TurnItem;
    expect(projectTurnItemForWire(item)).toEqual({ ...base, outputOmitted: true });
    expect(item.output).toEqual({ ok: true });
  });

  it("uses encoded JSON bytes for strings near the dynamic-value limit", () => {
    const small = '"'.repeat(8_191);
    const large = '"'.repeat(8_192);
    const smallItem = { ...base, input: small };
    expect(projectTurnItemForWire(smallItem)).toEqual(smallItem);
    const projected = projectTurnItemForWire({ ...base, input: large });
    expect(projected.type === "dynamic_tool" ? projected.input : null).toEqual({
      summary: '"'.repeat(159) + "…",
      truncated: true,
    });

    const boundary = projectTurnItemForWire({
      ...base,
      input: { text: "a".repeat(149) + "😀" + "b".repeat(17_000) },
    });
    expect(boundary.type === "dynamic_tool" ? boundary.input : null).toEqual({
      summary: '{"text":"' + "a".repeat(149) + "…",
      truncated: true,
    });
  });

  it("truncates detail at a UTF-8 boundary without changing the source", () => {
    const marker = "\n… output truncated for transport";
    const prefixBytes = 32_768 - Buffer.byteLength(marker);
    const progress = "a".repeat(prefixBytes - 1) + "😀" + "x".repeat(100_000);
    const item = {
      ...base,
      type: "subagent" as const,
      subagentId: NodeId.make("child-agent"),
      origin: "app_owned" as const,
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      childThreadId: null,
      prompt: "漢".repeat(12_000),
      result: null,
      progress,
    };
    const projected = projectTurnItemForWire(item);
    expect(projectTurnItemForWire(projected)).toEqual(projected);
    expect(
      Buffer.byteLength(projected.type === "subagent" ? projected.prompt : ""),
    ).toBeLessThanOrEqual(32_768);
    expect(projected.type === "subagent" ? projected.progress : null).toBe(
      "a".repeat(prefixBytes - 1) + marker,
    );
    expect(item.progress).toBe(progress);
  });

  it("leaves a screenshot's bytes out of the fetched tool output", () => {
    const data = "A".repeat(600_000);
    const item = {
      ...base,
      type: "dynamic_tool" as const,
      toolName: "mcp__t3-code__device_screenshot",
      input: { deviceId: "phone" },
      output: {
        content: [{ type: "image", source: { type: "base64", media_type: "image/png", data } }],
      },
    };
    expect(projectTurnItemForDetail(item)).toMatchObject({
      output: { content: [{ type: "image", mimeType: "image/png" }] },
    });
    expect(item.output.content[0]?.source.data).toBe(data);

    const t3McpApp = {
      attachmentId: "app-html",
      server: "repro",
      tool: "chart",
      resourceUri: "ui://repro/chart",
    };
    const text = { type: "text", text: "Rendered chart" };
    const appItem = {
      ...item,
      toolName: "repro.chart",
      output: {
        t3McpApp,
        result: { content: [text, { type: "image", mimeType: "image/png", data }] },
      },
    };
    expect(projectTurnItemForDetail(appItem)).toMatchObject({
      output: { t3McpApp, result: { content: [text, { type: "image", mimeType: "image/png" }] } },
    });
    const largeOutput = projectTurnItemForDetail(appItem);
    expect(
      JSON.stringify(largeOutput.type === "dynamic_tool" ? largeOutput.output : null),
    ).not.toContain(data);

    const smallApp = {
      t3McpApp,
      result: { content: [text, { type: "image", mimeType: "image/png", data: "iVBORw0KGgo=" }] },
    };
    expect(projectTurnItemForDetail({ ...appItem, output: smallApp })).toMatchObject({
      output: smallApp,
    });

    const forged = projectTurnItemForDetail({
      ...appItem,
      output: { t3McpApp: { ...t3McpApp, resourceUri: data }, result: { content: [text] } },
    });
    expect(
      JSON.stringify(forged.type === "dynamic_tool" ? forged.output : null).length,
    ).toBeLessThan(270_000);
  });

  it("keeps image markers when the rest of a tool output is too large to send", () => {
    const image = {
      type: "image",
      source: { type: "base64", media_type: "image/png", data: "AAAA" },
    };
    const item = {
      ...base,
      type: "dynamic_tool" as const,
      output: { content: [{ type: "text", text: "x".repeat(300_000) }, image] },
    };
    const projected = projectTurnItemForDetail(item);
    const output = projected.type === "dynamic_tool" ? projected.output : null;
    expect(Array.isArray(output) ? output.slice(1) : null).toEqual([
      { type: "image", mimeType: "image/png" },
    ]);
    expect(JSON.stringify(output).length).toBeLessThan(270_000);
  });

  it("bounds a tool output made of many images", () => {
    const image = { type: "image", data: "AAAA", mimeType: "image/png" };
    const item = {
      ...base,
      type: "dynamic_tool" as const,
      output: Array.from({ length: 10_000 }, () => image),
    };
    const projected = projectTurnItemForDetail(item);
    const output = projected.type === "dynamic_tool" ? projected.output : null;
    expect(Array.isArray(output) ? output.length : null).toBe(1 + MAX_TOOL_OUTPUT_IMAGES);
    expect(Array.isArray(output) ? String(output[0]?.text).length : null).toBeLessThan(263_000);
    expect(toolOutputImages(output)).toHaveLength(MAX_TOOL_OUTPUT_IMAGES);
  });

  it("keeps failure evidence without retaining command output", () => {
    const item = {
      ...base,
      type: "command_execution" as const,
      input: "cat missing-file",
      output: "cat: missing-file: No such file or directory",
    };
    const projected = projectTurnItemForWire(item);
    expect(projected).not.toHaveProperty("output");
    expect(projected).toMatchObject({ outputIndicatesFailure: true });
    expect(projectTurnItemForWire(projected)).toEqual(projected);
    expect(projectTurnItemForWire({ ...item, output: "", exitCode: 2 })).toMatchObject({
      outputIndicatesFailure: true,
      exitCode: 2,
    });
  });

  it.each([
    "logs/app.log:12: Error: ENOENT: no such file or directory",
    "fixtures/ci.txt:3: Process exited with exit code 2",
  ])("trusts a zero exit code over failure text in the output: %s", (output) => {
    const projected = projectTurnItemForWire({
      ...base,
      type: "command_execution" as const,
      input: "rg -n error logs fixtures",
      output,
      exitCode: 0,
    });
    expect(projected).not.toHaveProperty("outputIndicatesFailure");
    expect(projected).toMatchObject({ exitCode: 0, outputOmitted: true });
  });

  it("omits inline file bodies but preserves file identity and change counts", () => {
    const item = {
      ...base,
      type: "file_change" as const,
      fileName: "src/main.ts",
      additions: 3,
      deletions: 1,
      diffStr: "+new code",
      oldStr: "old code",
      newStr: "new code",
    };
    const projected = projectTurnItemForWire(item);
    expect(projected).not.toHaveProperty("diffStr");
    expect(projected).not.toHaveProperty("oldStr");
    expect(projected).not.toHaveProperty("newStr");
    expect(projected).toMatchObject({ fileName: "src/main.ts", additions: 3, deletions: 1 });
    expect(item.diffStr).toBe("+new code");
    expect(projectTurnItemForDetail(item)).toMatchObject({ diffStr: "+new code" });
    expect(projectTurnItemForDetail(item)).not.toHaveProperty("oldStr");
    expect(projectTurnItemForDetail(item)).not.toHaveProperty("newStr");
    const { diffStr: _diff, ...contentOnly } = item;
    expect(projectTurnItemForDetail(contentOnly)).toMatchObject({
      oldStr: "old code",
      newStr: "new code",
    });
    expect(projectTurnItemForWire(contentOnly)).not.toHaveProperty("newStr");
    const failed = projectTurnItemForWire({
      ...item,
      status: "failed",
      diffStr: "String to replace not found",
    });
    expect(failed).toMatchObject({ diffStr: "String to replace not found" });
    expect(failed).not.toHaveProperty("newStr");
  });

  it("retains only result identities and failure metadata in live tool events", () => {
    const output = {
      isError: true,
      structuredContent: { threadId: "child-thread", messageId: "message", text: "PRIVATE_BODY" },
      content: [{ type: "text", text: "PRIVATE_BODY" }],
    };
    const event = {
      id: EventId.make("result-event"),
      type: "turn-item.updated" as const,
      threadId: base.threadId,
      occurredAt: base.updatedAt,
      payload: { ...base, output },
    };
    const projected = projectDomainEventForWire(event);
    expect(projected.payload).toMatchObject({
      output: { isError: true, threadId: "child-thread", messageId: "message" },
    });
    expect(JSON.stringify(projected)).not.toContain("PRIVATE_BODY");
    expect(event.payload.output).toBe(output);
    expect(projectDomainEventForWire(projected)).toEqual(projected);
  });

  it("shares one wire projection per stored event", () => {
    const tool: OrchestrationV2StoredEvent = {
      sequence: 1,
      commandId: null,
      event: {
        id: EventId.make("shared-tool-event"),
        type: "turn-item.updated",
        threadId: base.threadId,
        occurredAt: base.updatedAt,
        payload: { ...base, output: { content: [{ type: "text", text: "PRIVATE_BODY" }] } },
      },
    };
    const projected = projectStoredEventForWire(tool);
    expect(projected).toEqual({ ...tool, event: projectDomainEventForWire(tool.event) });
    expect(projectStoredEventForWire(tool)).toBe(projected);
    const detached: OrchestrationV2StoredEvent = {
      sequence: 2,
      commandId: null,
      event: {
        id: EventId.make("detached-event"),
        type: "provider-session.detached",
        threadId: base.threadId,
        occurredAt: base.updatedAt,
        payload: {
          providerSessionId: ProviderSessionId.make("session"),
          detachedAt: base.updatedAt,
        },
      },
    };
    expect(projectStoredEventForWire(detached)).toBe(detached);
  });

  it("bounds workflow-member prompts, results, and titles across events, snapshots, and dedup", () => {
    const longPrompt = "a".repeat(498) + "😀" + "b".repeat(1_000);
    const longResult = "r".repeat(501);
    const longTitle = "t".repeat(198) + "😀" + "z".repeat(50);
    const expectedPrompt = "a".repeat(498) + "…";
    const expectedResult = "r".repeat(499) + "…";
    const expectedTitle = "t".repeat(198) + "…";
    const makeSubagent = (
      id: string,
      overrides: Partial<OrchestrationV2Subagent>,
    ): OrchestrationV2Subagent => ({
      id: NodeId.make(id),
      threadId: base.threadId,
      runId: null,
      parentNodeId: NodeId.make("workflow-node"),
      origin: "app_owned",
      createdBy: "agent",
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerThreadId: null,
      childThreadId: ThreadId.make(`child-${id}`),
      nativeTaskRef: null,
      prompt: longPrompt,
      title: longTitle,
      model: "gpt-5",
      status: "completed",
      result: longResult,
      startedAt: base.startedAt,
      completedAt: base.completedAt,
      updatedAt: base.updatedAt,
      ...overrides,
    });
    const makeSubagentItem = (id: string, subagent: OrchestrationV2Subagent) =>
      ({
        ...base,
        id: TurnItemId.make(id),
        type: "subagent" as const,
        subagentId: subagent.id,
        origin: "app_owned" as const,
        driver: subagent.driver,
        providerInstanceId: subagent.providerInstanceId,
        childThreadId: subagent.childThreadId,
        title: subagent.title,
        prompt: subagent.prompt,
        result: subagent.result,
      }) satisfies OrchestrationV2TurnItem;

    const workflowMember = makeSubagent("wf-member", { completionWake: "never" });
    const shortMember = makeSubagent("wf-member-short", {
      completionWake: "never",
      title: "a".repeat(200),
      prompt: "s".repeat(500),
      result: "d".repeat(500),
    });
    const ordinaryTask = makeSubagent("ordinary-task", { completionWake: "always" });

    const memberEvent = {
      id: EventId.make("wf-member-event"),
      type: "subagent.updated" as const,
      threadId: base.threadId,
      occurredAt: base.updatedAt,
      payload: workflowMember,
    };
    const projectedMemberEvent = projectDomainEventForWire(memberEvent);
    expect(
      projectedMemberEvent.type === "subagent.updated" && projectedMemberEvent.payload,
    ).toEqual({
      ...workflowMember,
      title: expectedTitle,
      prompt: expectedPrompt,
      result: expectedResult,
    });
    expect(projectDomainEventForWire(projectedMemberEvent)).toBe(projectedMemberEvent);
    expect(workflowMember.prompt).toBe(longPrompt);
    expect(workflowMember.result).toBe(longResult);

    const shortEvent = { ...memberEvent, id: EventId.make("short-event"), payload: shortMember };
    expect(projectDomainEventForWire(shortEvent)).toBe(shortEvent);

    const ordinaryItem = makeSubagentItem("item-ordinary", ordinaryTask);
    const snapshot = {
      thread: { id: base.threadId },
      checkpoints: [],
      contextHandoffs: [],
      messages: [],
      subagents: [workflowMember, ordinaryTask],
      turnItems: [ordinaryItem],
      visibleTurnItems: [ordinaryItem].map((item, position) => ({
        position,
        visibility: "local" as const,
        sourceThreadId: base.threadId,
        sourceItemId: item.id,
        item,
      })),
    } as unknown as OrchestrationV2ThreadProjection;

    const projected = projectThreadProjectionForWire(snapshot);
    expect(projected.subagents).toEqual([
      { ...workflowMember, title: expectedTitle, prompt: expectedPrompt, result: expectedResult },
      ordinaryTask,
    ]);
    expect(projected.turnItems[0]).toMatchObject({
      title: longTitle,
      prompt: longPrompt,
      result: longResult,
    });
    expect(projected.visibleTurnItems[0]?.item).toBe(projected.turnItems[0]);

    const deduped = dedupeSnapshotBodies(projected);
    expect(deduped.turnItems[0]).toMatchObject({
      prompt: "",
      promptOmitted: true,
      result: null,
      resultOmitted: true,
    });
    expect(restoreSnapshotBodies(deduped)).toEqual(projected);
  });

  describe("workflow wire bounding and deltas", () => {
    const makeWorkflowAgent = (
      index: number,
      overrides: Partial<OrchestrationV2WorkflowAgent> = {},
    ): OrchestrationV2WorkflowAgent => ({
      index,
      label: `agent-${index}-${"L".repeat(250)}`,
      state: "completed",
      phaseIndex: index % 3,
      phaseTitle: `Phase ${(index % 3) + 1}`,
      startedAt: 1_000 + index,
      completedAt: 2_000 + index,
      prompt: `prompt-${index}-${"p".repeat(1_200)}`,
      result: `result-${index}-${"r".repeat(1_200)}`,
      ...overrides,
    });

    const makeWorkflowSubagent = (
      workflow: OrchestrationV2SubagentWorkflow,
    ): OrchestrationV2Subagent => ({
      id: NodeId.make("workflow-coord"),
      threadId: base.threadId,
      runId: null,
      parentNodeId: NodeId.make("root-node"),
      origin: "app_owned",
      createdBy: "agent",
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerThreadId: null,
      childThreadId: null,
      nativeTaskRef: null,
      prompt: "Run 1000 checks",
      title: "Large workflow",
      model: "gpt-5",
      completionWake: "always",
      status: "running",
      workflow,
      result: null,
      startedAt: base.startedAt,
      completedAt: null,
      updatedAt: base.updatedAt,
    });

    const phases = [
      { index: 0, title: "Phase 1" },
      { index: 1, title: "Phase 2" },
      { index: 2, title: "Phase 3" },
    ];

    it("keeps a 1,000-agent workflow progress event with one changed row under a few KB", () => {
      const agents = Array.from({ length: 1_000 }, (_, index) =>
        makeWorkflowAgent(
          index,
          index === 42 ? { state: "running" as const, completedAt: undefined } : undefined,
        ),
      );
      const rawWorkflow: OrchestrationV2SubagentWorkflow = {
        name: "fanout-1000",
        phases,
        agents,
        changedAgentIndices: [42],
        totalTokens: 500_000,
        durationMs: 42_000,
      };
      const rawEvent: OrchestrationV2DomainEvent = {
        id: EventId.make("wf-delta-1"),
        type: "subagent.updated",
        threadId: base.threadId,
        occurredAt: base.updatedAt,
        payload: makeWorkflowSubagent(rawWorkflow),
      };

      const projected = projectDomainEventForWire(rawEvent);
      expect(projected.type).toBe("subagent.updated");
      const projectedWorkflow =
        projected.type === "subagent.updated" ? projected.payload.workflow : undefined;

      expect(projectedWorkflow?.agentsDelta).toBe(true);
      expect(projectedWorkflow?.agentCount).toBe(1_000);
      expect(projectedWorkflow).not.toHaveProperty("changedAgentIndices");
      expect(projectedWorkflow?.phases).toEqual(phases);
      expect(projectedWorkflow?.agents).toHaveLength(1);
      const [changed] = projectedWorkflow!.agents;
      expect(changed?.index).toBe(42);
      expect(changed?.label.length).toBeLessThanOrEqual(200);
      expect(changed?.label.endsWith("…")).toBe(true);
      expect(changed?.prompt?.length).toBeLessThanOrEqual(500);
      expect(changed?.result?.length).toBeLessThanOrEqual(500);

      const wireBytes = Buffer.byteLength(JSON.stringify(projected), "utf8");
      expect(wireBytes).toBeLessThan(3_072);
      expect(projectDomainEventForWire(projected)).toBe(projected);
      expect(rawWorkflow.changedAgentIndices).toEqual([42]);
      expect(rawWorkflow.agents).toHaveLength(1_000);

      // An empty delta still drops changedAgentIndices and preserves total count.
      const emptyDelta = projectDomainEventForWire({
        ...rawEvent,
        id: EventId.make("wf-delta-empty"),
        payload: makeWorkflowSubagent({ ...rawWorkflow, changedAgentIndices: [] }),
      });
      const emptyWorkflow =
        emptyDelta.type === "subagent.updated" ? emptyDelta.payload.workflow : undefined;
      expect(emptyWorkflow?.agents).toEqual([]);
      expect(emptyWorkflow?.agentsDelta).toBe(true);
      expect(emptyWorkflow?.agentCount).toBe(1_000);
      expect(emptyWorkflow).not.toHaveProperty("changedAgentIndices");

      // A delta naming > 200 changed indices becomes a bounded replacement so no row stays stale.
      const hugeDelta = projectDomainEventForWire({
        ...rawEvent,
        id: EventId.make("wf-delta-huge"),
        payload: makeWorkflowSubagent({
          ...rawWorkflow,
          changedAgentIndices: Array.from({ length: 260 }, (_, index) => index),
        }),
      });
      const hugeWorkflow =
        hugeDelta.type === "subagent.updated" ? hugeDelta.payload.workflow : undefined;
      expect(hugeWorkflow?.agents).toHaveLength(200);
      expect(hugeWorkflow).not.toHaveProperty("agentsDelta");
      expect(hugeWorkflow?.agentCount).toBe(1_000);
      expect(hugeWorkflow).not.toHaveProperty("changedAgentIndices");
    });

    it("bounds snapshots and non-delta events to 200 rows with active first, then most recently completed", () => {
      const agents = Array.from({ length: 1_000 }, (_, index) => {
        if (index < 10) {
          return makeWorkflowAgent(index, { state: "running", completedAt: undefined });
        }
        if (index >= 985) {
          return makeWorkflowAgent(index, {
            state: "queued",
            startedAt: undefined,
            completedAt: undefined,
            queuedAt: 3_000 + index,
          });
        }
        // Index 500 finished last so recency must beat index order.
        const completedAt = index === 500 ? 99_999 : 2_000 + index;
        return makeWorkflowAgent(index, {
          state: index % 20 === 0 ? "failed" : "completed",
          completedAt,
        });
      });

      const subagent = makeWorkflowSubagent({
        name: "bounded-snapshot",
        phases,
        agents,
        changedAgentIndices: [0],
      });
      const snapshot = {
        thread: { id: base.threadId },
        checkpoints: [],
        contextHandoffs: [],
        messages: [],
        subagents: [subagent],
        turnItems: [],
        visibleTurnItems: [],
      } as unknown as OrchestrationV2ThreadProjection;

      const projectedSnapshot = projectThreadProjectionForWire(snapshot);
      const snapshotWorkflow = projectedSnapshot.subagents[0]?.workflow;
      expect(snapshotWorkflow?.agents).toHaveLength(200);
      expect(snapshotWorkflow?.agentCount).toBe(1_000);
      expect(snapshotWorkflow?.phases).toEqual(phases);
      expect(snapshotWorkflow).not.toHaveProperty("changedAgentIndices");
      expect(snapshotWorkflow).not.toHaveProperty("agentsDelta");

      const snapshotAgents = snapshotWorkflow!.agents;
      expect(snapshotAgents.slice(0, 10).map((agent) => agent.index)).toEqual([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
      ]);
      expect(snapshotAgents.slice(10, 25).map((agent) => agent.index)).toEqual(
        Array.from({ length: 15 }, (_, offset) => 985 + offset),
      );
      expect(snapshotAgents[25]?.index).toBe(500);
      expect(snapshotAgents[26]?.index).toBe(984);
      expect(snapshotAgents.at(-1)?.index).toBe(811);
      for (const agent of snapshotAgents) {
        expect(agent.label.length).toBeLessThanOrEqual(200);
        expect(agent.prompt?.length ?? 0).toBeLessThanOrEqual(500);
        expect(agent.result?.length ?? 0).toBeLessThanOrEqual(500);
      }

      // Non-delta subagent.updated uses the same 200-row cap.
      const nonDelta = projectDomainEventForWire({
        id: EventId.make("wf-full"),
        type: "subagent.updated",
        threadId: base.threadId,
        occurredAt: base.updatedAt,
        payload: makeWorkflowSubagent({ name: "bounded-snapshot", phases, agents }),
      });
      const nonDeltaWorkflow =
        nonDelta.type === "subagent.updated" ? nonDelta.payload.workflow : undefined;
      expect(nonDeltaWorkflow).toEqual(snapshotWorkflow);
      expect(projectDomainEventForWire(nonDelta)).toBe(nonDelta);

      // When active rows alone exceed 200, running rows win over queued and settled rows.
      const activeHeavy = projectDomainEventForWire({
        id: EventId.make("wf-active-heavy"),
        type: "subagent.updated",
        threadId: base.threadId,
        occurredAt: base.updatedAt,
        payload: makeWorkflowSubagent({
          name: "active-heavy",
          phases,
          agents: [
            ...Array.from({ length: 50 }, (_, index) =>
              makeWorkflowAgent(index, { state: "running", completedAt: undefined }),
            ),
            ...Array.from({ length: 200 }, (_, offset) =>
              makeWorkflowAgent(50 + offset, {
                state: "queued",
                startedAt: undefined,
                completedAt: undefined,
              }),
            ),
            ...Array.from({ length: 50 }, (_, offset) =>
              makeWorkflowAgent(250 + offset, { state: "completed", completedAt: 9_000 + offset }),
            ),
          ],
        }),
      });
      const activeHeavyWorkflow =
        activeHeavy.type === "subagent.updated" ? activeHeavy.payload.workflow : undefined;
      expect(activeHeavyWorkflow?.agents).toHaveLength(200);
      expect(activeHeavyWorkflow?.agentCount).toBe(300);
      expect(activeHeavyWorkflow?.agents.filter((agent) => agent.state === "running")).toHaveLength(
        50,
      );
      expect(activeHeavyWorkflow?.agents.filter((agent) => agent.state === "queued")).toHaveLength(
        150,
      );
      expect(activeHeavyWorkflow?.agents.some((agent) => agent.state === "completed")).toBe(false);
    });

    it.effect(
      "caps workflow previews, snapshots, replay, and live deltas on ws streams without acceptDedupedBodies",
      () =>
        Effect.gen(function* () {
          const longPrompt = "p".repeat(10_000);
          const longResult = "r".repeat(10_000);
          const longLabel = "l".repeat(400);
          const agents = Array.from({ length: 1_000 }, (_, index) =>
            makeWorkflowAgent(index, { label: longLabel, prompt: longPrompt, result: longResult }),
          );
          const memberId = NodeId.make("wf-member-ws");
          const memberSubagent: OrchestrationV2Subagent = {
            ...makeWorkflowSubagent({ name: "ws-wf", phases, agents }),
            id: memberId,
            childThreadId: ThreadId.make("child-ws"),
            completionWake: "never",
            workflow: undefined,
            title: longLabel,
            prompt: longPrompt,
            result: longResult,
            status: "completed",
            completedAt: base.completedAt,
          };
          const deltaStoredEvent: OrchestrationV2StoredEvent = {
            sequence: 11,
            commandId: null,
            event: {
              id: EventId.make("ev-wf-delta"),
              type: "subagent.updated",
              threadId: base.threadId,
              occurredAt: base.updatedAt,
              payload: makeWorkflowSubagent({
                name: "ws-wf",
                phases,
                agents,
                changedAgentIndices: [42],
              }),
            },
          };
          const memberSubagentStoredEvent: OrchestrationV2StoredEvent = {
            sequence: 12,
            commandId: null,
            event: {
              id: EventId.make("ev-member-subagent"),
              type: "subagent.updated",
              threadId: base.threadId,
              occurredAt: base.updatedAt,
              payload: memberSubagent,
            },
          };
          const snapshotProjection = {
            thread: { id: base.threadId },
            runs: [],
            attempts: [],
            nodes: [],
            subagents: [makeWorkflowSubagent({ name: "ws-wf", phases, agents }), memberSubagent],
            providerSessions: [],
            providerThreads: [],
            providerTurns: [],
            runtimeRequests: [],
            messages: [],
            plans: [],
            turnItems: [],
            checkpointScopes: [],
            checkpoints: [],
            contextHandoffs: [],
            contextTransfers: [],
            visibleTurnItems: [],
            updatedAt: base.updatedAt,
          } as unknown as OrchestrationV2ThreadProjection;

          const layer = Layer.mergeAll(
            Layer.mock(ThreadManagementService.ThreadManagementService)({
              ensureLegacyTranscript: () => Effect.void,
              getThreadRecords: () =>
                Effect.die("cursor-only resume must not decode full subagents"),
              getThreadSnapshot: () =>
                Effect.succeed({
                  schemaVersion: 1,
                  snapshotSequence: 10,
                  projection: snapshotProjection,
                }),
              streamStoredEventsFrom: () =>
                Stream.fromIterable([deltaStoredEvent, memberSubagentStoredEvent]),
            }),
            Layer.mock(OrchestrationEventStore.OrchestrationEventStore)({
              latestAgentSequence: () => Effect.succeed(12),
              prunedThroughSequence: Effect.succeed(0),
              getAgentReplayStats: () =>
                Effect.succeed({
                  eventCount: 2,
                  rawPayloadBytes: 1_024,
                  hasCreateEvent: false,
                }),
              readAgentEvents: () =>
                Stream.fromIterable([deltaStoredEvent, memberSubagentStoredEvent]),
            }),
          );

          // Snapshot + live tail with acceptDedupedBodies omitted (false).
          const snapshotAndLive = yield* subscribeOrchestrationV2Thread({
            threadId: base.threadId,
          }).pipe(Effect.flatMap(Stream.runCollect), Effect.provide(layer));
          const [snap, liveDelta, liveMemberSubagent] = Array.from(snapshotAndLive);
          expect(snap?.kind).toBe("snapshot");
          if (snap?.kind === "snapshot") {
            expect(snap.projection.subagents[0]?.workflow?.agents).toHaveLength(200);
            expect(snap.projection.subagents[0]?.workflow?.agentCount).toBe(1_000);
            expect(snap.projection.subagents[1]?.prompt.length).toBe(500);
            expect(snap.projection.subagents[1]?.result?.length).toBe(500);
            expect(snap.projection.subagents[1]?.title?.length).toBe(200);
          }
          expect(liveDelta?.kind).toBe("event");
          if (liveDelta?.kind === "event" && liveDelta.event.type === "subagent.updated") {
            expect(liveDelta.event.payload.workflow?.agentsDelta).toBe(true);
            expect(liveDelta.event.payload.workflow?.agents).toHaveLength(1);
            expect(liveDelta.event.payload.workflow?.agentCount).toBe(1_000);
            expect(liveDelta.event.payload.workflow).not.toHaveProperty("changedAgentIndices");
            expect(Buffer.byteLength(JSON.stringify(liveDelta), "utf8")).toBeLessThan(3_072);
          }
          expect(liveMemberSubagent?.kind).toBe("event");
          if (
            liveMemberSubagent?.kind === "event" &&
            liveMemberSubagent.event.type === "subagent.updated"
          ) {
            expect(liveMemberSubagent.event.payload.prompt.length).toBe(500);
            expect(liveMemberSubagent.event.payload.result?.length).toBe(500);
            expect(liveMemberSubagent.event.payload.title?.length).toBe(200);
          }

          // Reconnect replay (afterSequence) without acceptDedupedBodies also caps workflow deltas and member rows.
          const replayed = yield* subscribeOrchestrationV2Thread({
            threadId: base.threadId,
            afterSequence: 10,
          }).pipe(Effect.flatMap(Stream.runCollect), Effect.provide(layer));
          const [replayDelta, replayMemberSubagent] = Array.from(replayed);
          expect(replayDelta?.kind).toBe("event");
          if (replayDelta?.kind === "event" && replayDelta.event.type === "subagent.updated") {
            expect(replayDelta.event.payload.workflow?.agentsDelta).toBe(true);
            expect(replayDelta.event.payload.workflow?.agents).toHaveLength(1);
            expect(replayDelta.event.payload.workflow).not.toHaveProperty("changedAgentIndices");
          }
          expect(replayMemberSubagent?.kind).toBe("event");
          if (
            replayMemberSubagent?.kind === "event" &&
            replayMemberSubagent.event.type === "subagent.updated"
          ) {
            expect(replayMemberSubagent.event.payload.prompt.length).toBe(500);
            expect(replayMemberSubagent.event.payload.result?.length).toBe(500);
            expect(replayMemberSubagent.event.payload.title?.length).toBe(200);
          }
        }),
    );

    it("bounds workflow.logs to the newest 50 entries of 2000 chars and truncates workflow agent error previews", () => {
      const logs = Array.from({ length: 65 }, (_, index) =>
        index === 64 ? `last:${"x".repeat(2_500)}` : `log-${index}`,
      );
      const longError = "e".repeat(900);
      const workflowSubagent = makeWorkflowSubagent({
        name: "logs-and-error",
        phases: [{ index: 0, title: "Work" }],
        agents: [
          {
            index: 0,
            label: "failing-worker",
            state: "failed",
            error: longError,
            result: longError,
          },
        ],
        logs,
        changedAgentIndices: [0],
      });
      const event = {
        id: EventId.make("ev-logs-and-error"),
        type: "subagent.updated" as const,
        threadId: base.threadId,
        occurredAt: base.updatedAt,
        payload: workflowSubagent,
      };
      const projectedEvent = projectDomainEventForWire(event);
      expect(projectedEvent.type).toBe("subagent.updated");
      if (projectedEvent.type === "subagent.updated") {
        const wf = projectedEvent.payload.workflow;
        expect(wf?.logs).toHaveLength(50);
        expect(wf?.logs?.[0]).toBe("log-15");
        expect(wf?.logs?.[49]?.length).toBe(2_000);
        expect(wf?.logs?.[49]?.endsWith("…")).toBe(true);
        expect(wf?.agents[0]?.error?.length).toBe(500);
        expect(wf?.agents[0]?.error?.endsWith("…")).toBe(true);
        expect(wf?.agents[0]?.result?.length).toBe(500);
      }

      const snapshot = {
        thread: { id: base.threadId },
        checkpoints: [],
        contextHandoffs: [],
        messages: [],
        subagents: [workflowSubagent],
        turnItems: [],
        visibleTurnItems: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const projectedSnapshot = projectThreadProjectionForWire(snapshot);
      const snapWf = projectedSnapshot.subagents[0]?.workflow;
      expect(snapWf?.logs).toHaveLength(50);
      expect(snapWf?.logs?.[0]).toBe("log-15");
      expect(snapWf?.logs?.[49]?.length).toBe(2_000);
      expect(snapWf?.agents[0]?.error?.length).toBe(500);
    });
  });
});

describe("live body deduper", () => {
  const at = base.updatedAt;
  const messageId = MessageId.make("message-live");
  const event = (
    sequence: number,
    rest: Pick<OrchestrationV2DomainEvent, "type" | "payload">,
  ): Extract<OrchestrationV2ThreadStreamItem, { kind: "event" }> => ({
    kind: "event",
    sequence,
    event: {
      id: EventId.make(`event-${sequence}`),
      threadId: base.threadId,
      occurredAt: at,
      ...rest,
    } as OrchestrationV2DomainEvent,
  });
  const message = (sequence: number, text: string) =>
    event(sequence, {
      type: "message.updated",
      payload: {
        id: messageId,
        threadId: base.threadId,
        runId: null,
        nodeId: null,
        role: "assistant",
        text,
        attachments: [],
        streaming: true,
        createdBy: "agent",
        creationSource: "provider",
        createdAt: at,
        updatedAt: at,
      },
    });
  const reply = (sequence: number, text: string) =>
    event(sequence, {
      type: "turn-item.updated",
      payload: {
        ...base,
        status: "running",
        type: "assistant_message",
        messageId,
        text,
        streaming: true,
      },
    });
  const payloadOf = (item: OrchestrationV2ThreadStreamItem) =>
    item.kind === "event" ? item.event.payload : undefined;

  it("blanks a reply that repeats the message delivered before it, after coalescing", () => {
    const delivered = coalesceLiveUpdatedEvents([
      message(1, "a"),
      reply(2, "a"),
      message(3, "ab"),
      reply(4, "ab"),
    ]).map(makeLiveBodyDeduper());

    expect(
      delivered.map((item) => [item.kind === "event" && item.sequence, payloadOf(item)]),
    ).toEqual([
      [3, message(3, "ab").event.payload],
      [4, { ...reply(4, "ab").event.payload, text: "", textOmitted: true }],
    ]);
  });

  it("caps a large checkpoint's files for the wire and still dedupes its item", () => {
    const files = Array.from({ length: 250 }, (_, index) => ({
      path: `vendor/file-${index}.ts`,
      kind: "modified",
      additions: 2,
      deletions: 1,
    }));
    const checkpointId = CheckpointId.make("checkpoint-large");
    const scopeId = CheckpointScopeId.make("scope-large");
    const captured = event(1, {
      type: "checkpoint.captured",
      payload: {
        id: checkpointId,
        threadId: base.threadId,
        scopeId,
        runId: null,
        nodeId: NodeId.make("node-large"),
        parentCheckpointId: null,
        ordinalWithinScope: 1,
        appRunOrdinal: null,
        ref: CheckpointRef.make("refs/t3/checkpoints/large"),
        status: "ready",
        files,
        capturedAt: at,
      },
    });
    const item = event(2, {
      type: "turn-item.updated",
      payload: { ...base, type: "checkpoint", checkpointId, scopeId, files },
    });
    const dedupe = makeLiveBodyDeduper();
    const [record, card] = [captured, item].map((streamed) =>
      dedupe({ ...streamed, event: projectDomainEventForWire(streamed.event) }),
    );

    const filesTotal = { count: 250, additions: 500, deletions: 250 };
    expect(payloadOf(record!)).toMatchObject({ files: files.slice(0, 200), filesTotal });
    expect(payloadOf(card!)).toMatchObject({ files: [], filesOmitted: true, filesTotal });
  });

  it("ships a reply whole when its message arrives after it", () => {
    const first = reply(1, "a");
    expect([first, message(2, "a")].map(makeLiveBodyDeduper())[0]).toBe(first);
  });

  it("bounds workflow-member subagent updates and still dedupes repeated records and turn items", () => {
    const subagentId = NodeId.make("wf-member-live");
    const longPrompt = "p".repeat(40_000);
    const longResult = "r".repeat(40_000);
    const previewPrompt = "p".repeat(499) + "…";
    const previewResult = "r".repeat(499) + "…";
    const subagentRecord = (
      status: OrchestrationV2Subagent["status"],
      result: string | null,
    ): OrchestrationV2Subagent => ({
      id: subagentId,
      threadId: base.threadId,
      runId: null,
      parentNodeId: NodeId.make("workflow-node"),
      origin: "app_owned",
      createdBy: "agent",
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerThreadId: null,
      childThreadId: ThreadId.make("child-wf-member-live"),
      nativeTaskRef: null,
      prompt: longPrompt,
      title: "Worker",
      model: "gpt-5",
      completionWake: "never",
      status,
      result,
      startedAt: at,
      completedAt: status === "completed" ? at : null,
      updatedAt: at,
    });

    const raw = [
      event(1, { type: "subagent.updated", payload: subagentRecord("running", null) }),
      event(2, { type: "subagent.updated", payload: subagentRecord("completed", longResult) }),
    ];
    const dedupe = makeLiveBodyDeduper();
    const [startedRecord, completedRecord] = raw.map((streamed) =>
      dedupe({ ...streamed, event: projectDomainEventForWire(streamed.event) }),
    );

    const startedPayload = payloadOf(startedRecord!) as OrchestrationV2Subagent;
    expect(startedPayload).toMatchObject({ prompt: previewPrompt, result: null });
    expect(startedPayload).not.toHaveProperty("promptOmitted");

    const completedPayload = payloadOf(completedRecord!) as OrchestrationV2Subagent;
    expect(completedPayload).toMatchObject({
      prompt: "",
      promptOmitted: true,
      result: previewResult,
    });
    expect(completedPayload).not.toHaveProperty("resultOmitted");

    const restoredSubagent = restoreSubagentBodies(completedPayload, startedPayload);
    expect(restoredSubagent).toMatchObject({ prompt: previewPrompt, result: previewResult });
  });
});
