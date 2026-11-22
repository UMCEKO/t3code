import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  ClaudeSettings,
  CommandId,
  EnvironmentId,
  isProviderNativeSubagentThread,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  type ServerProvider,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/ai";

import { applyOrchestrationV2ProjectionEvent } from "../../../../packages/client-runtime/src/state/orchestrationV2Projection.ts";
import {
  deriveWorkflowGroups,
  projectedSubagentsToRuntime,
  resolveWorkflowOpening,
  workflowOpeningHeadline,
} from "../../../../packages/client-runtime/src/state/subagentRuntime.ts";
import * as ServerConfig from "../config.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import * as ClaudeAdapterV2 from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import {
  projectDomainEventForWire,
  projectThreadProjectionForWire,
} from "../orchestration-v2/WireProjection.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinkRequests from "../peer/PeerLinkRequests.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as WorkflowService from "./WorkflowService.ts";

const WORKFLOW_NAME = "TwoPhaseAudit";
const WORKFLOW_DESCRIPTION = "Audits and summarizes across two phases.";
const EQUIV_SCRIPT = [
  "export const meta = {",
  `  name: "${WORKFLOW_NAME}",`,
  `  description: "${WORKFLOW_DESCRIPTION}",`,
  '  phases: [{ title: "Alpha" }, { title: "Beta" }],',
  "};",
  'phase("Alpha");',
  'log("Alpha started");',
  'const a1 = await agent("Check module A", { label: "alpha-ok", phase: "Alpha" });',
  'const a2 = await agent("Check module B", { label: "alpha-fail", phase: "Alpha" });',
  'phase("Beta");',
  'log("Beta started");',
  'const b1 = await agent("Summarize results", { label: "beta-sum", phase: "Beta" });',
  "return { a1, a2, b1 };",
].join("\n");

const CLAUDE_SESSION_ID = "equiv-claude-session";
const CLAUDE_WORKFLOW_TASK_ID = "task-equiv-wf";
const CLAUDE_WORKFLOW_TOOL_USE_ID = "toolu-equiv-wf";

const claudeSettings = Schema.decodeSync(ClaudeSettings)({});
const claudeModelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-sonnet-4-6",
};

const serverDriver = ProviderDriverKind.make("codex");
const serverInstanceId = ProviderInstanceId.make("codex");
const serverModelSelection = {
  instanceId: serverInstanceId,
  model: "gpt-5.3-codex",
};
const serverProviderSnapshot: ServerProvider = {
  instanceId: serverInstanceId,
  driver: serverDriver,
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-08-01T00:00:00.000Z",
  models: [
    {
      slug: serverModelSelection.model,
      name: serverModelSelection.model,
      isCustom: false,
      capabilities: null,
    },
  ],
  slashCommands: [],
  skills: [],
};

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "workflow-equivalence-test", version: "1.0.0" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "workflow-equivalence-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

function sdkFrame(value: unknown): SDKMessage {
  return value as SDKMessage;
}

interface CapturedWorkflowGraph {
  readonly parent: OrchestrationV2ThreadProjection;
  readonly coordinator: OrchestrationV2ThreadProjection;
  readonly members: ReadonlyArray<OrchestrationV2ThreadProjection>;
  readonly parentEvents: ReadonlyArray<OrchestrationV2DomainEvent>;
  readonly coordinatorEvents: ReadonlyArray<OrchestrationV2DomainEvent>;
}

function replayWireEventsIntoProjection(
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
): OrchestrationV2ThreadProjection | null {
  let projection: OrchestrationV2ThreadProjection | null = null;
  for (const rawEvent of events) {
    const projected = projectDomainEventForWire(rawEvent);
    if (projection === null && projected.type === "thread.created") {
      projection = {
        thread: projected.payload,
        runs: [],
        attempts: [],
        nodes: [],
        subagents: [],
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
        updatedAt: projected.occurredAt,
      };
      continue;
    }
    projection = applyOrchestrationV2ProjectionEvent(projection, projected);
  }
  return projection;
}

function collectStoredDomainEvents(
  orchestrator: Orchestrator.OrchestratorV2["Service"],
  threadId: ThreadId,
): Effect.Effect<ReadonlyArray<OrchestrationV2DomainEvent>, Orchestrator.OrchestratorV2Error> {
  return Effect.gen(function* () {
    const latestSeq = yield* orchestrator.getThreadEventSequence(threadId);
    if (latestSeq <= 0) return [];
    const chunk = yield* orchestrator.streamStoredEventsFrom({ threadId, afterSequence: 0 }).pipe(
      Stream.takeUntil((stored) => stored.sequence >= latestSeq),
      Stream.map((stored) => stored.event),
      Stream.runCollect,
    );
    return Array.from(chunk);
  });
}

/** Normalise ids, timestamps, origin, creationSource, driver, nativeTaskRef, and provider metrics. */
function normaliseWorkflowGraph(graph: CapturedWorkflowGraph) {
  const wireParent = projectThreadProjectionForWire(graph.parent);
  const wireCoordinator = projectThreadProjectionForWire(graph.coordinator);
  const replayedParent = replayWireEventsIntoProjection(graph.parentEvents);
  const replayedCoordinator = replayWireEventsIntoProjection(graph.coordinatorEvents);

  expect(replayedParent).not.toBeNull();
  expect(replayedCoordinator).not.toBeNull();

  const normaliseSide = (
    parent: OrchestrationV2ThreadProjection,
    coordinator: OrchestrationV2ThreadProjection,
  ) => {
    const workflowSubagents = parent.subagents.map((sub) => ({
      hasChildThreadId: sub.childThreadId === coordinator.thread.id,
      title: sub.title,
      prompt: sub.prompt,
      status: sub.status,
      workflow: sub.workflow
        ? {
            name: sub.workflow.name,
            phases: sub.workflow.phases,
            logs: sub.workflow.logs,
            agents: sub.workflow.agents.map((agent) => ({
              index: agent.index,
              label: agent.label,
              state: agent.state,
              phaseIndex: agent.phaseIndex,
              phaseTitle: agent.phaseTitle,
              attempt: agent.attempt ?? 1,
              error: agent.error ?? null,
              hasChildThreadId: agent.childThreadId !== undefined,
            })),
          }
        : null,
    }));

    const parentSubagentTurnItems = parent.turnItems
      .filter((item) => item.type === "subagent")
      .map((item) => ({
        type: item.type,
        hasChildThreadId: item.childThreadId === coordinator.thread.id,
        title: item.title,
        prompt: item.prompt,
        status: item.status,
      }));

    const rootNode = coordinator.nodes.find(
      (node) => node.kind === "root_turn" && node.runId === null,
    );
    const openingMessage = coordinator.messages[0];
    const coordinatorMemberSubagents = coordinator.subagents
      .toSorted((left, right) => (left.title ?? "").localeCompare(right.title ?? ""))
      .map((member) => ({
        status: member.status,
        hasChildThreadId: member.childThreadId !== null,
        runIdIsNull: member.runId === null,
      }));
    const coordinatorSubagentTurnItems = coordinator.turnItems.filter(
      (item) => item.type === "subagent",
    );

    const runtimeGroups = deriveWorkflowGroups(projectedSubagentsToRuntime(parent.subagents));
    const opening = resolveWorkflowOpening({
      thread: coordinator.thread,
      messages: coordinator.messages,
      parentSubagents: parent.subagents,
    });
    const headline = opening ? workflowOpeningHeadline(opening) : null;

    return {
      parentSubagents: workflowSubagents,
      parentSubagentTurnItems,
      coordinatorShell: {
        isNativeOrServerSubagent: isProviderNativeSubagentThread(coordinator.thread),
        parentMatchesP: coordinator.thread.lineage.parentThreadId === parent.thread.id,
        relationshipToParent: coordinator.thread.lineage.relationshipToParent,
        rootNodeKind: rootNode?.kind ?? null,
        rootNodeRunId: rootNode ? rootNode.runId : "missing",
        rootNodeStatus: rootNode?.status ?? null,
        openingMessageRole: openingMessage?.role ?? null,
        openingMessageWorkflowScript: openingMessage?.workflowScript ?? null,
        openingMessageText: openingMessage?.text ?? null,
        memberCount: coordinatorMemberSubagents.length,
        memberSubagents: coordinatorMemberSubagents,
        subagentTurnItemCount: coordinatorSubagentTurnItems.length,
      },
      memberLineages: graph.members
        .map((member) => ({
          parentMatchesC: member.thread.lineage.parentThreadId === coordinator.thread.id,
          relationshipToParent: member.thread.lineage.relationshipToParent,
          rootMatchesP: member.thread.lineage.rootThreadId === parent.thread.id,
        }))
        .toSorted((a, b) => Number(a.parentMatchesC) - Number(b.parentMatchesC)),
      clientGroups: runtimeGroups.map((group) => ({
        workflowName: group.workflow.workflowName,
        status: group.workflow.status,
        agentCount: group.agentCount,
        logs: group.workflow.logs,
        phases: group.phases.map((phase) => ({
          index: phase.index,
          title: phase.title,
          state: phase.state,
          settledCount: phase.settledCount,
          members: phase.members.map((member) => ({
            title: member.title,
            status: member.status,
            phaseIndex: member.phaseIndex,
            phaseTitle: member.phaseTitle,
            attempt: member.attempt ?? 1,
            error: member.error,
            hasChildThreadId: member.childThreadId !== null,
          })),
        })),
        unphasedMembers: group.unphasedMembers.length,
      })),
      clientOpening: opening
        ? {
            name: opening.name,
            description: opening.description,
            script: opening.script,
            stoppable: opening.stoppable,
            headline,
            groupPhases: opening.group?.phases.map((phase) => ({
              index: phase.index,
              title: phase.title,
              state: phase.state,
              settledCount: phase.settledCount,
              members: phase.members.map((member) => ({
                title: member.title,
                status: member.status,
                error: member.error,
                hasChildThreadId: member.childThreadId !== null,
              })),
            })),
          }
        : null,
    };
  };

  const fromWireSnapshot = normaliseSide(wireParent, wireCoordinator);
  const fromLiveReplay = normaliseSide(replayedParent!, replayedCoordinator!);
  expect(fromLiveReplay).toEqual(fromWireSnapshot);
  return fromWireSnapshot;
}

const runClaudeWorkflowFixture = Effect.gen(function* () {
  const cwd = yield* checkpointWorkspace("workflow-equivalence-claude");
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const configDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-equiv-claude-" });
  const transcriptDir = path.join(configDir, "projects", "equiv");
  yield* fileSystem.makeDirectory(transcriptDir, { recursive: true });

  const sdkMessages = yield* Queue.unbounded<SDKMessage>();
  const adapter = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
    instanceId: claudeModelSelection.instanceId,
    settings: claudeSettings,
    environment: { CLAUDE_CONFIG_DIR: configDir },
    attachmentsDir: cwd,
    fileSystem,
    path,
    random: yield* SecureRandom.SecureRandom,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    queryRunner: {
      allocateSessionId: Effect.succeed(CLAUDE_SESSION_ID),
      open: () =>
        Effect.succeed({
          messages: Stream.fromQueue(sdkMessages),
          offer: () =>
            Effect.forEach(
              [
                sdkFrame({
                  type: "system",
                  subtype: "init",
                  session_id: CLAUDE_SESSION_ID,
                  model: claudeModelSelection.model,
                  tools: ["Workflow"],
                  uuid: "00000000-0000-4000-8000-000000009000",
                }),
                sdkFrame({
                  type: "assistant",
                  message: {
                    id: "msg_equiv_1",
                    role: "assistant",
                    type: "message",
                    model: claudeModelSelection.model,
                    content: [
                      {
                        type: "tool_use",
                        id: CLAUDE_WORKFLOW_TOOL_USE_ID,
                        name: "Workflow",
                        input: { script: EQUIV_SCRIPT, description: WORKFLOW_DESCRIPTION },
                      },
                    ],
                    stop_reason: "tool_use",
                    stop_sequence: null,
                    usage: { input_tokens: 1, output_tokens: 1 },
                  },
                  parent_tool_use_id: null,
                  uuid: "00000000-0000-4000-8000-000000009001",
                  session_id: CLAUDE_SESSION_ID,
                }),
                sdkFrame({
                  type: "system",
                  subtype: "task_started",
                  task_id: CLAUDE_WORKFLOW_TASK_ID,
                  tool_use_id: CLAUDE_WORKFLOW_TOOL_USE_ID,
                  description: WORKFLOW_DESCRIPTION,
                  prompt: EQUIV_SCRIPT,
                  task_type: "local_workflow",
                  workflow_name: WORKFLOW_NAME,
                  uuid: "00000000-0000-4000-8000-000000009002",
                  session_id: CLAUDE_SESSION_ID,
                }),
                sdkFrame({
                  type: "user",
                  message: {
                    role: "user",
                    content: [
                      {
                        type: "tool_result",
                        tool_use_id: CLAUDE_WORKFLOW_TOOL_USE_ID,
                        content: [{ type: "text", text: "Workflow launched." }],
                      },
                    ],
                  },
                  parent_tool_use_id: null,
                  uuid: "00000000-0000-4000-8000-000000009003",
                  session_id: CLAUDE_SESSION_ID,
                  tool_use_result: {
                    taskType: "local_workflow",
                    runId: "wf_claude_equiv",
                    transcriptDir,
                  },
                }),
                sdkFrame({
                  type: "system",
                  subtype: "task_progress",
                  task_id: CLAUDE_WORKFLOW_TASK_ID,
                  tool_use_id: CLAUDE_WORKFLOW_TOOL_USE_ID,
                  description: "Alpha started",
                  workflow_progress: [
                    { type: "workflow_phase", index: 0, title: "Alpha" },
                    { type: "workflow_phase", index: 1, title: "Beta" },
                    {
                      type: "workflow_agent",
                      index: 0,
                      label: "alpha-ok",
                      agentId: "a0",
                      state: "done",
                      phaseIndex: 0,
                      phaseTitle: "Alpha",
                      attempt: 1,
                      startedAt: 1_000,
                      lastProgressAt: 1_500,
                      promptPreview: "Check module A",
                      resultPreview: "A-ok",
                    },
                    {
                      type: "workflow_agent",
                      index: 1,
                      label: "alpha-fail",
                      agentId: "a1",
                      state: "error",
                      phaseIndex: 0,
                      phaseTitle: "Alpha",
                      attempt: 1,
                      startedAt: 1_500,
                      lastProgressAt: 2_000,
                      promptPreview: "Check module B",
                      resultPreview: "Provider error: boom",
                    },
                  ],
                  uuid: "00000000-0000-4000-8000-000000009004",
                  session_id: CLAUDE_SESSION_ID,
                }),
                sdkFrame({
                  type: "system",
                  subtype: "task_progress",
                  task_id: CLAUDE_WORKFLOW_TASK_ID,
                  tool_use_id: CLAUDE_WORKFLOW_TOOL_USE_ID,
                  description: "Beta started",
                  workflow_progress: [
                    { type: "workflow_phase", index: 0, title: "Alpha" },
                    { type: "workflow_phase", index: 1, title: "Beta" },
                    {
                      type: "workflow_agent",
                      index: 0,
                      label: "alpha-ok",
                      agentId: "a0",
                      state: "done",
                      phaseIndex: 0,
                      phaseTitle: "Alpha",
                      attempt: 1,
                      startedAt: 1_000,
                      lastProgressAt: 1_500,
                      promptPreview: "Check module A",
                      resultPreview: "A-ok",
                    },
                    {
                      type: "workflow_agent",
                      index: 1,
                      label: "alpha-fail",
                      agentId: "a1",
                      state: "error",
                      phaseIndex: 0,
                      phaseTitle: "Alpha",
                      attempt: 1,
                      startedAt: 1_500,
                      lastProgressAt: 2_000,
                      promptPreview: "Check module B",
                      resultPreview: "Provider error: boom",
                    },
                    {
                      type: "workflow_agent",
                      index: 2,
                      label: "beta-sum",
                      agentId: "a2",
                      state: "done",
                      phaseIndex: 1,
                      phaseTitle: "Beta",
                      attempt: 1,
                      startedAt: 2_000,
                      lastProgressAt: 2_500,
                      promptPreview: "Summarize results",
                      resultPreview: "Summary: A-ok",
                    },
                  ],
                  uuid: "00000000-0000-4000-8000-000000009005",
                  session_id: CLAUDE_SESSION_ID,
                }),
                sdkFrame({
                  type: "system",
                  subtype: "task_notification",
                  task_id: CLAUDE_WORKFLOW_TASK_ID,
                  tool_use_id: CLAUDE_WORKFLOW_TOOL_USE_ID,
                  status: "completed",
                  summary: "Workflow finished.",
                  output_file: path.join(transcriptDir, "workflow.output"),
                  uuid: "00000000-0000-4000-8000-000000009006",
                  session_id: CLAUDE_SESSION_ID,
                }),
                sdkFrame({
                  type: "result",
                  subtype: "success",
                  is_error: false,
                  result: "Done.",
                  duration_ms: 100,
                  duration_api_ms: 50,
                  num_turns: 1,
                  total_cost_usd: 0,
                  usage: { input_tokens: 10, output_tokens: 5 },
                  uuid: "00000000-0000-4000-8000-000000009007",
                  session_id: CLAUDE_SESSION_ID,
                }),
              ],
              (frame) => Queue.offer(sdkMessages, frame).pipe(Effect.asVoid),
            ),
          setModel: () => Effect.void,
          applyFlagSettings: () => Effect.void,
          setPermissionMode: () => Effect.void,
          renameSession: () => Effect.void,
          backgroundTasks: () => Effect.succeed(false),
          mcpServerStatus: Effect.succeed([]),
          readMcpResource: () => Effect.die("unexpected MCP resource read"),
          callMcpTool: () => Effect.die("unexpected MCP tool call"),
          mcpAuthenticate: () => Effect.die("unexpected MCP sign-in"),
          mcpSubmitOAuthCallbackUrl: () => Effect.die("unexpected MCP sign-in"),
          reconnectMcpServer: () => Effect.die("unexpected MCP reconnect"),
          listPermissionRules: Effect.die("unexpected permission rules read"),
          stopTask: () => Effect.void,
          interrupt: Effect.void,
          close: Effect.void,
        }),
      forkSession: () => Effect.die("unused"),
      subagentLaunchToolUseId: () => Effect.succeed(null),
      subagentModel: () => Effect.succeed(null),
      assertComplete: Effect.void,
    },
  });

  const parentThreadId = ThreadId.make("thread:equiv:claude:parent");
  const projectId = ProjectId.make("project:equiv:claude");

  return yield* Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("cmd:equiv:claude:create"),
      threadId: parentThreadId,
      projectId,
      title: "Claude workflow parent",
      modelSelection: claudeModelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
    });
    const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("cmd:equiv:claude:msg"),
      threadId: parentThreadId,
      messageId: MessageId.make("msg:equiv:claude:start"),
      text: "Run the two-phase audit workflow.",
      attachments: [],
      modelSelection: claudeModelSelection,
      dispatchMode: { type: "start_immediately" },
    });

    // Wait for both the parent run and the workflow task to complete.
    yield* orchestrator
      .streamStoredEventsFrom({ threadId: parentThreadId, afterSequence: beforeSeq })
      .pipe(
        Stream.mapEffect(() => orchestrator.getThreadProjection(parentThreadId)),
        Stream.filter(
          (projection) =>
            projection.runs.some((run) => run.status === "completed") &&
            projection.subagents.some(
              (sub) => sub.workflow !== undefined && sub.status === "completed",
            ),
        ),
        Stream.runHead,
      );

    const parent = yield* orchestrator.getThreadProjection(parentThreadId);
    const coordinatorThreadId = parent.subagents[0]!.childThreadId!;
    const coordinator = yield* orchestrator.getThreadProjection(coordinatorThreadId);
    const memberIds = parent.subagents[0]!.workflow!.agents.map((agent) => agent.childThreadId!);
    const members = yield* Effect.forEach(memberIds, (memberId) =>
      orchestrator.getThreadProjection(memberId),
    );
    const parentEvents = yield* collectStoredDomainEvents(orchestrator, parentThreadId);
    const coordinatorEvents = yield* collectStoredDomainEvents(orchestrator, coordinatorThreadId);
    return { parent, coordinator, members, parentEvents, coordinatorEvents };
  }).pipe(
    Effect.provide(
      ProviderReplayHarness.layerWithRegistry(
        { name: "workflow-equivalence-claude", runtimePolicyOverride: { cwd } },
        ProviderAdapterRegistry.layerSingle(adapter),
      ),
    ),
  );
}).pipe(
  Effect.provide(
    Layer.mergeAll(
      IdAllocator.layer,
      McpProviderSessions.layer,
      NodeServices.layer,
      NodeCrypto.layer,
    ),
  ),
);

const runServerWorkflowFixture = Effect.gen(function* () {
  const cwd = yield* checkpointWorkspace("workflow-equivalence-server");
  const parentThreadId = ThreadId.make("thread:equiv:server:parent");
  const projectId = ProjectId.make("project:equiv:server");
  const parentScope: McpInvocationContext.McpThreadInvocationScope = {
    environmentId: EnvironmentId.make("environment:local"),
    requestNamespace: "provider-session:equiv-server",
    thread: {
      threadId: parentThreadId,
      providerSessionId: "provider-session:equiv-server",
      providerInstanceId: serverInstanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"]),
    issuedAt: 1,
  };

  interface TurnItem {
    readonly turn: ProviderAdapter.ProviderAdapterV2TurnInput;
    readonly complete: (text: string) => Effect.Effect<void>;
    readonly fail: (message: string) => Effect.Effect<void>;
  }
  const turnQueue = yield* Queue.unbounded<TurnItem>();

  const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
    instanceId: serverInstanceId,
    driver: serverDriver,
    getCapabilities: () => Effect.succeed(ClaudeAdapterV2.ClaudeProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: ({ providerSessionId }) =>
      Effect.gen(function* () {
        const openedAt = yield* DateTime.now;
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        return {
          instanceId: serverInstanceId,
          driver: serverDriver,
          providerSessionId,
          providerSession: {
            id: providerSessionId,
            driver: serverDriver,
            providerInstanceId: serverInstanceId,
            status: "ready",
            cwd,
            model: serverModelSelection.model,
            capabilities: ClaudeAdapterV2.ClaudeProviderCapabilitiesV2,
            createdAt: openedAt,
            updatedAt: openedAt,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          ensureThread: ({ threadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${threadId}`),
              driver: serverDriver,
              providerInstanceId: serverInstanceId,
              providerSessionId,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: {
                driver: serverDriver,
                nativeId: `native:${threadId}`,
                strength: "strong",
              },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: openedAt,
              updatedAt: openedAt,
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turn) =>
            Effect.gen(function* () {
              const now = yield* DateTime.now;
              const providerTurnId = ProviderTurnId.make(`provider-turn:${turn.attemptId}`);
              const baseProviderTurn = {
                id: providerTurnId,
                providerThreadId: turn.providerThread.id,
                nodeId: turn.rootNodeId,
                runAttemptId: turn.attemptId,
                nativeTurnRef: null,
                ordinal: turn.providerTurnOrdinal,
                startedAt: now,
              };
              yield* Queue.offer(events, {
                type: "provider_turn.updated",
                driver: serverDriver,
                providerTurn: { ...baseProviderTurn, status: "running", completedAt: null },
              });
              yield* Queue.offer(turnQueue, {
                turn,
                complete: (text) =>
                  Effect.gen(function* () {
                    const doneAt = yield* DateTime.now;
                    const messageId = MessageId.make(`msg:assistant:${turn.attemptId}`);
                    yield* Queue.offer(events, {
                      type: "message.updated",
                      driver: serverDriver,
                      message: {
                        id: messageId,
                        threadId: turn.threadId,
                        runId: turn.runId,
                        nodeId: turn.rootNodeId,
                        role: "assistant",
                        text,
                        attachments: [],
                        streaming: false,
                        createdBy: "agent",
                        creationSource: "provider",
                        createdAt: doneAt,
                        updatedAt: doneAt,
                      },
                    });
                    yield* Queue.offer(events, {
                      type: "turn_item.updated",
                      driver: serverDriver,
                      turnItem: {
                        id: TurnItemId.make(`item:assistant:${turn.attemptId}`),
                        threadId: turn.threadId,
                        runId: turn.runId,
                        nodeId: turn.rootNodeId,
                        providerThreadId: turn.providerThread.id,
                        providerTurnId,
                        nativeItemRef: null,
                        parentItemId: null,
                        ordinal: 1,
                        status: "completed",
                        title: null,
                        startedAt: doneAt,
                        completedAt: doneAt,
                        updatedAt: doneAt,
                        type: "assistant_message",
                        messageId,
                        text,
                        streaming: false,
                      },
                    });
                    yield* Queue.offer(events, {
                      type: "provider_turn.updated",
                      driver: serverDriver,
                      providerTurn: {
                        ...baseProviderTurn,
                        status: "completed",
                        completedAt: doneAt,
                      },
                    });
                    yield* Queue.offer(events, {
                      type: "turn.terminal",
                      driver: serverDriver,
                      providerThreadId: turn.providerThread.id,
                      providerTurnId,
                      runOrdinal: turn.runOrdinal,
                      status: "completed",
                      failure: null,
                      threadDisposition: "reusable",
                    });
                  }),
                fail: (message) =>
                  Effect.gen(function* () {
                    const doneAt = yield* DateTime.now;
                    yield* Queue.offer(events, {
                      type: "provider_turn.updated",
                      driver: serverDriver,
                      providerTurn: { ...baseProviderTurn, status: "failed", completedAt: doneAt },
                    });
                    yield* Queue.offer(events, {
                      type: "turn.terminal",
                      driver: serverDriver,
                      providerThreadId: turn.providerThread.id,
                      providerTurnId,
                      runOrdinal: turn.runOrdinal,
                      failureItemOrdinal: 1,
                      status: "failed",
                      failure: {
                        class: "provider_error",
                        message,
                        code: null,
                        retryable: null,
                      },
                      threadDisposition: "reusable",
                    });
                  }),
              });
            }),
          steerTurn: () => Effect.die("unused"),
          interruptTurn: () => Effect.void,
          respondToRuntimeRequest: () => Effect.die("unused"),
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };

  const fakeSandboxRunner: WorkflowService.WorkflowSandboxRunner = async (input) => {
    input.host.onPhase("Alpha");
    input.host.onLog("Alpha started");
    const a1 = await input.host.runAgent({
      index: 0,
      prompt: "Check module A",
      opts: { label: "alpha-ok", phase: "Alpha" },
      phase: "Alpha",
    });
    const a2 = await input.host.runAgent({
      index: 1,
      prompt: "Check module B",
      opts: { label: "alpha-fail", phase: "Alpha" },
      phase: "Alpha",
    });
    input.host.onPhase("Beta");
    input.host.onLog("Beta started");
    const b1 = await input.host.runAgent({
      index: 2,
      prompt: "Summarize results",
      opts: { label: "beta-sum", phase: "Beta" },
      phase: "Beta",
    });
    return { a1, a2, b1 };
  };

  const layerAdapters = ProviderAdapterRegistry.layerSingle(adapter);
  const layerDatabase = SqlitePersistence.layerMemory;
  const layerServerSettings = ServerSettings.layerTest({});
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "workflow-equivalence-server", runtimePolicyOverride: { cwd } },
    layerAdapters,
    { databaseLayer: layerDatabase },
  ).pipe(
    Layer.provide(
      Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: () => Effect.void,
        take: Effect.never,
      }),
    ),
  );
  const layerThreadManagement = ThreadManagementService.layer.pipe(
    Layer.provide(layerOrchestrator),
    Layer.provide(layerServerSettings),
    Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
  );
  const layerServerConfig = ServerConfig.layerTest(cwd, {
    prefix: "t3-equiv-server-",
  }).pipe(Layer.provide(NodeServices.layer));

  const layerToolkit = Layer.merge(
    McpHttpServer.layerOrchestratorToolkit,
    OrchestratorMcpService.layer,
  ).pipe(
    Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
    Layer.provideMerge(Layer.merge(layerOrchestrator, layerThreadManagement)),
    Layer.provideMerge(layerServerConfig),
    Layer.provide(layerAdapters),
    Layer.provide(ProviderRegistryMock.layer([serverProviderSnapshot])),
    Layer.provide(layerServerSettings),
    Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
    Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
    Layer.provide(
      Layer.mock(ProjectService.ProjectService)({ workspaceScanDirectory: Effect.succeed }),
    ),
    Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
    Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
    Layer.provideMerge(
      Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.succeed(WorkflowService.WorkflowSandboxRunner, fakeSandboxRunner),
      ),
    ),
  );

  return yield* Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const mcpServer = yield* McpServer.McpServer;

    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("cmd:equiv:server:create"),
      threadId: parentThreadId,
      projectId,
      title: "Server workflow parent",
      modelSelection: serverModelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("cmd:equiv:server:msg"),
      threadId: parentThreadId,
      messageId: MessageId.make("msg:equiv:server:start"),
      text: "Run the two-phase audit workflow.",
      attachments: [],
      modelSelection: serverModelSelection,
      dispatchMode: { type: "start_immediately" },
    });
    const parentTurn = yield* Queue.take(turnQueue);
    expect(parentTurn.turn.threadId).toBe(parentThreadId);

    const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
    const runCall = yield* mcpServer
      .callTool({ name: "t3_workflow_run", arguments: { script: EQUIV_SCRIPT } })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, parentScope),
        Effect.provideService(McpSchema.McpServerClient, mcpClient),
      );
    expect(runCall.isError).not.toBe(true);
    const started = runCall.structuredContent as { readonly taskId: string };
    const taskId = NodeId.make(started.taskId);

    const turn0 = yield* Queue.take(turnQueue);
    yield* turn0.complete("A-ok");
    const turn1 = yield* Queue.take(turnQueue);
    yield* turn1.fail("Provider error: boom");
    const turn2 = yield* Queue.take(turnQueue);
    yield* turn2.complete("Summary: A-ok");

    yield* orchestrator
      .streamStoredEventsFrom({
        threadId: parentThreadId,
        afterSequence: beforeSeq,
        eventType: "subagent.updated",
      })
      .pipe(
        Stream.filter(
          (
            stored,
          ): stored is typeof stored & {
            readonly event: Extract<OrchestrationV2DomainEvent, { type: "subagent.updated" }>;
          } =>
            stored.event.type === "subagent.updated" &&
            stored.event.payload.id === taskId &&
            stored.event.payload.status === "completed",
        ),
        Stream.runHead,
      );

    yield* parentTurn.complete("Done.");

    const parent = yield* orchestrator.getThreadProjection(parentThreadId);
    const workflowRow = parent.subagents.find(
      (sub): sub is OrchestrationV2Subagent => sub.id === taskId,
    )!;
    const coordinatorThreadId = workflowRow.childThreadId!;
    const coordinator = yield* orchestrator.getThreadProjection(coordinatorThreadId);
    const memberIds = workflowRow.workflow!.agents.map((agent) => agent.childThreadId!);
    const members = yield* Effect.forEach(memberIds, (memberId) =>
      orchestrator.getThreadProjection(memberId),
    );
    const parentEvents = yield* collectStoredDomainEvents(orchestrator, parentThreadId);
    const coordinatorEvents = yield* collectStoredDomainEvents(orchestrator, coordinatorThreadId);
    return { parent, coordinator, members, parentEvents, coordinatorEvents };
  }).pipe(Effect.provide(layerToolkit));
}).pipe(Effect.provide(NodeServices.layer));

describe("engine-level workflow equivalence (Claude native vs server engine)", () => {
  it.effect(
    "produces the exact same 3-layer graph, wire projection, live event replay, and client views for a 2-phase 3-member (1 failed) workflow",
    () =>
      Effect.gen(function* () {
        const claudeGraph = yield* runClaudeWorkflowFixture;
        const serverGraph = yield* runServerWorkflowFixture;

        const claudeNormalised = normaliseWorkflowGraph(claudeGraph);
        const serverNormalised = normaliseWorkflowGraph(serverGraph);

        expect(serverNormalised).toEqual(claudeNormalised);

        // Guard against vacuously equal shapes on both sides.
        expect(serverNormalised.parentSubagents).toHaveLength(1);
        expect(serverNormalised.parentSubagents[0]?.hasChildThreadId).toBe(true);
        expect(serverNormalised.parentSubagents[0]?.status).toBe("completed");
        expect(serverNormalised.parentSubagents[0]?.workflow?.phases).toEqual([
          { index: 0, title: "Alpha" },
          { index: 1, title: "Beta" },
        ]);
        expect(serverNormalised.parentSubagents[0]?.workflow?.logs).toEqual([
          "Alpha started",
          "Beta started",
        ]);
        expect(
          serverNormalised.parentSubagents[0]?.workflow?.agents.map((agent) => ({
            label: agent.label,
            state: agent.state,
            error: agent.error,
          })),
        ).toEqual([
          { label: "alpha-ok", state: "completed", error: null },
          { label: "alpha-fail", state: "failed", error: "Provider error: boom" },
          { label: "beta-sum", state: "completed", error: null },
        ]);
        expect(serverNormalised.parentSubagentTurnItems).toHaveLength(1);
        expect(serverNormalised.parentSubagentTurnItems[0]?.hasChildThreadId).toBe(true);
        expect(serverNormalised.parentSubagentTurnItems[0]?.status).toBe("completed");
        expect(serverNormalised.coordinatorShell).toMatchObject({
          isNativeOrServerSubagent: true,
          parentMatchesP: true,
          relationshipToParent: "subagent",
          rootNodeKind: "root_turn",
          rootNodeRunId: null,
          rootNodeStatus: "completed",
          openingMessageRole: "user",
          openingMessageWorkflowScript: true,
          memberCount: 3,
          subagentTurnItemCount: 0,
        });
        expect(serverNormalised.memberLineages).toEqual([
          { parentMatchesC: true, relationshipToParent: "subagent", rootMatchesP: true },
          { parentMatchesC: true, relationshipToParent: "subagent", rootMatchesP: true },
          { parentMatchesC: true, relationshipToParent: "subagent", rootMatchesP: true },
        ]);
        expect(serverNormalised.clientGroups).toHaveLength(1);
        expect(serverNormalised.clientOpening?.name).toBe(WORKFLOW_NAME);
        expect(serverNormalised.clientOpening?.groupPhases).toHaveLength(2);
      }),
  );
});
