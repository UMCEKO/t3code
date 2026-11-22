// @effect-diagnostics nodeBuiltinImport:off -- Uses mkfifo on POSIX to verify non-blocking FIFO rejection.
import * as NodeChildProcess from "node:child_process";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ORCHESTRATOR_MCP_RUNNING_TASK_NEXT,
  OrchestratorMcpWorkflowRunResult,
  OrchestratorMcpWorkflowStatusResult,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2PendingBackgroundTask,
  type OrchestrationV2Subagent,
  type ServerProvider,
  type TurnTokenUsage,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as ServerConfig from "../config.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import { ClaudeProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
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
import {
  JournalRecordTooLargeError,
  MAX_JOURNAL_LINE_BYTES,
  agentCallKey,
  decodeJournal,
  makeJournalWriter,
  type JournalEntry,
} from "./journal.ts";
import { getUnconfirmedSandboxChildPids, reapUnconfirmedSandboxChildren } from "./sandbox.ts";
import * as WorkflowService from "./WorkflowService.ts";

const driver = ProviderDriverKind.make("claudeAgent");
const instanceId = ProviderInstanceId.make("claudeAgent");
const model = "claude-test";
const altModel = "claude-test-alt";
const modelSelection: ModelSelection = { instanceId, model };
const projectId = ProjectId.make("project:workflow-test");
const parentThreadId = ThreadId.make("thread:workflow-parent");

const decodeRunResult = Schema.decodeUnknownEffect(OrchestratorMcpWorkflowRunResult);
const decodeStatusResult = Schema.decodeUnknownEffect(OrchestratorMcpWorkflowStatusResult);

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "workflow-service-test", version: "1.0.0" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "workflow-service-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const parentScope: McpInvocationContext.McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("environment:workflow-test"),
  requestNamespace: "provider-session:workflow-parent",
  thread: {
    threadId: parentThreadId,
    providerSessionId: "provider-session:workflow-parent",
    providerInstanceId: instanceId,
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

const providerSnapshot: ServerProvider = {
  instanceId,
  driver,
  enabled: true,
  installed: true,
  version: "test",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-11-21T00:00:00.000Z",
  models: [
    {
      slug: model,
      name: model,
      isCustom: false,
      capabilities: null,
    },
    {
      slug: altModel,
      name: altModel,
      isCustom: false,
      capabilities: null,
    },
  ],
  slashCommands: [],
  skills: [],
};

interface ActiveTurnHandle {
  readonly turn: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurnId: ProviderTurnId;
  readonly interrupted: Deferred.Deferred<void>;
  readonly setPendingBackgroundTasks: (
    tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
  ) => Effect.Effect<void>;
  readonly complete: (
    text: string,
    options?: { readonly turnTokenUsage?: TurnTokenUsage },
  ) => Effect.Effect<void>;
  readonly failUsageLimit: () => Effect.Effect<void>;
}

const declaredFailure = (result: McpSchema.CallToolResult): { code: string; message: string } => {
  expect(result.isError).toBe(true);
  const first = result.content[0];
  if (first?.type !== "text") {
    throw new Error("Expected text failure payload");
  }
  return JSON.parse(first.text) as { code: string; message: string };
};

interface HarnessOptions {
  readonly name: string;
  readonly databaseLayer?: typeof SqlitePersistence.layerMemory;
  readonly baseDir?: string;
  readonly runContinuationWorker?: boolean;
  readonly runEffectWorker?: boolean;
  readonly recoverOnStartup?: boolean;
  readonly continueThreadsAfterServerUpdate?: boolean;
  readonly wrapThreadManagement?: (
    service: ThreadManagementService.ThreadManagementService["Service"],
  ) => ThreadManagementService.ThreadManagementService["Service"];
  readonly wrapFileSystem?: (service: FileSystem.FileSystem) => FileSystem.FileSystem;
  readonly wrapScriptFileOpener?: (
    opener: WorkflowService.WorkflowScriptFileOpener,
  ) => WorkflowService.WorkflowScriptFileOpener;
  readonly wrapSandboxRunner?: (
    runner: WorkflowService.WorkflowSandboxRunner,
  ) => WorkflowService.WorkflowSandboxRunner;
}

const makeWorkflowHarness = (options: HarnessOptions) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace(`workflow-${options.name}`);
    const turnQueue = yield* Queue.unbounded<ActiveTurnHandle>();
    const startedTurns = yield* Ref.make<ReadonlyArray<ActiveTurnHandle>>([]);
    const continuationOffers = yield* Ref.make<
      ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
    >([]);
    const turnsByProviderTurnId = new Map<
      string,
      {
        readonly handle: ActiveTurnHandle;
        readonly baseProviderTurn: {
          readonly id: ProviderTurnId;
          readonly providerThreadId: ProviderThreadId;
          readonly nodeId: NodeId;
          readonly runAttemptId: ProviderAdapter.ProviderAdapterV2TurnInput["attemptId"];
          readonly nativeTurnRef: null;
          readonly ordinal: number;
          readonly startedAt: DateTime.Utc;
        };
        readonly events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>;
        terminal: boolean;
      }
    >();

    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const openedAt = yield* DateTime.now;
          const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model,
              capabilities: ClaudeProviderCapabilitiesV2,
              createdAt: openedAt,
              updatedAt: openedAt,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: `native:${threadId}`, strength: "strong" },
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
                const interrupted = yield* Deferred.make<void>();
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
                  driver,
                  providerTurn: {
                    ...baseProviderTurn,
                    status: "running",
                    completedAt: null,
                  },
                });
                const handle: ActiveTurnHandle = {
                  turn,
                  providerTurnId,
                  interrupted,
                  setPendingBackgroundTasks: (pendingBackgroundTasks) =>
                    Effect.gen(function* () {
                      const updatedAt = yield* DateTime.now;
                      yield* Queue.offer(events, {
                        type: "provider_thread.updated",
                        driver,
                        providerThread: {
                          ...turn.providerThread,
                          status: "active",
                          pendingBackgroundTasks,
                          updatedAt,
                        },
                      });
                    }),
                  complete: (text, opts) =>
                    Effect.gen(function* () {
                      const entry = turnsByProviderTurnId.get(providerTurnId);
                      if (entry !== undefined) entry.terminal = true;
                      const doneAt = yield* DateTime.now;
                      const messageId = MessageId.make(`msg:assistant:${turn.attemptId}`);
                      if (text.length > 0) {
                        yield* Queue.offer(events, {
                          type: "message.updated",
                          driver,
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
                          driver,
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
                      }
                      yield* Queue.offer(events, {
                        type: "provider_turn.updated",
                        driver,
                        providerTurn: {
                          ...baseProviderTurn,
                          status: "completed",
                          completedAt: doneAt,
                          ...(opts?.turnTokenUsage === undefined
                            ? {}
                            : { turnTokenUsage: opts.turnTokenUsage }),
                        },
                      });
                      yield* Queue.offer(events, {
                        type: "turn.terminal",
                        driver,
                        providerThreadId: turn.providerThread.id,
                        providerTurnId,
                        runOrdinal: turn.runOrdinal,
                        status: "completed",
                        failure: null,
                        threadDisposition: "reusable",
                      });
                    }),
                  failUsageLimit: () =>
                    Effect.gen(function* () {
                      const entry = turnsByProviderTurnId.get(providerTurnId);
                      if (entry !== undefined) entry.terminal = true;
                      const doneAt = yield* DateTime.now;
                      yield* Queue.offer(events, {
                        type: "provider_turn.updated",
                        driver,
                        providerTurn: {
                          ...baseProviderTurn,
                          status: "failed",
                          completedAt: doneAt,
                        },
                      });
                      yield* Queue.offer(events, {
                        type: "turn.terminal",
                        driver,
                        providerThreadId: turn.providerThread.id,
                        providerTurnId,
                        runOrdinal: turn.runOrdinal,
                        failureItemOrdinal: 1,
                        status: "failed",
                        failure: {
                          class: "usage_limit",
                          message: "Usage limit reached.",
                          code: null,
                          retryable: null,
                          resetAt: DateTime.formatIso(DateTime.add(doneAt, { minutes: 5 })),
                        },
                        threadDisposition: "reusable",
                      });
                    }),
                };
                turnsByProviderTurnId.set(providerTurnId, {
                  handle,
                  baseProviderTurn,
                  events,
                  terminal: false,
                });
                yield* Ref.update(startedTurns, (list) => [...list, handle]);
                yield* Queue.offer(turnQueue, handle);
              }),
            steerTurn: () => Effect.die("unused"),
            interruptTurn: ({ providerThread, providerTurnId }) =>
              Effect.gen(function* () {
                const entry = turnsByProviderTurnId.get(providerTurnId);
                if (entry === undefined) return;
                yield* Deferred.succeed(entry.handle.interrupted, undefined);
                if (entry.terminal) return;
                const doneAt = yield* DateTime.now;
                yield* Queue.offer(entry.events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    ...entry.baseProviderTurn,
                    status: "interrupted",
                    completedAt: doneAt,
                  },
                });
                yield* Queue.offer(entry.events, {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: providerThread.id,
                  providerTurnId,
                  runOrdinal: entry.handle.turn.runOrdinal,
                  status: "interrupted",
                  failure: null,
                  threadDisposition: "reusable",
                });
              }),
            respondToRuntimeRequest: () => Effect.die("unused"),
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };

    const layerAdapters = ProviderAdapterRegistry.layerSingle(adapter);
    const layerDatabase = options.databaseLayer ?? SqlitePersistence.layerMemory;
    const layerServerSettings = ServerSettings.layerTest(
      options.continueThreadsAfterServerUpdate === undefined
        ? {}
        : { continueThreadsAfterServerUpdate: options.continueThreadsAfterServerUpdate },
    );
    const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
      { name: `workflow-${options.name}`, runtimePolicyOverride: { cwd } },
      layerAdapters,
      {
        databaseLayer: layerDatabase,
        runEffectWorker: options.runEffectWorker ?? true,
        ...(options.recoverOnStartup === true ? { recoverOnStartup: true } : {}),
        ...(options.runContinuationWorker === true ? { runContinuationWorker: true } : {}),
        ...(options.continueThreadsAfterServerUpdate === undefined
          ? {}
          : { continueThreadsAfterServerUpdate: options.continueThreadsAfterServerUpdate }),
      },
    ).pipe(
      options.runContinuationWorker === true
        ? (layer) => layer
        : Layer.provide(
            Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
              offer: (request) => Ref.update(continuationOffers, (seen) => [...seen, request]),
              take: Effect.never,
            }),
          ),
    );
    const baseThreadManagementLayer = ThreadManagementService.layer.pipe(
      Layer.provide(layerOrchestrator),
      Layer.provide(layerServerSettings),
      Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
    );
    const threadManagementLayer =
      options.wrapThreadManagement === undefined
        ? baseThreadManagementLayer
        : Layer.effect(
            ThreadManagementService.ThreadManagementService,
            Effect.map(
              ThreadManagementService.ThreadManagementService,
              options.wrapThreadManagement,
            ),
          ).pipe(Layer.provide(baseThreadManagementLayer));
    const layerOrchestration = Layer.merge(layerOrchestrator, threadManagementLayer);
    const layerServerConfig = ServerConfig.layerTest(
      cwd,
      options.baseDir ?? { prefix: `t3-workflow-${options.name}-` },
    ).pipe(Layer.provide(NodeServices.layer));
    const layerNodeServices =
      options.wrapFileSystem === undefined
        ? NodeServices.layer
        : Layer.effect(
            FileSystem.FileSystem,
            Effect.map(FileSystem.FileSystem, options.wrapFileSystem),
          ).pipe(Layer.provideMerge(NodeServices.layer));
    const layerScriptFileOpener =
      options.wrapScriptFileOpener === undefined
        ? Layer.empty
        : Layer.succeed(
            WorkflowService.WorkflowScriptFileOpener,
            options.wrapScriptFileOpener(WorkflowService.WorkflowScriptFileOpener.defaultValue()),
          );
    const layerSandboxRunner =
      options.wrapSandboxRunner === undefined
        ? Layer.empty
        : Layer.succeed(
            WorkflowService.WorkflowSandboxRunner,
            options.wrapSandboxRunner(WorkflowService.WorkflowSandboxRunner.defaultValue()),
          );
    const layerToolkit = Layer.merge(
      McpHttpServer.layerOrchestratorToolkit,
      OrchestratorMcpService.layer,
    ).pipe(
      Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
      Layer.provideMerge(layerOrchestration),
      Layer.provideMerge(layerServerConfig),
      Layer.provide(layerAdapters),
      Layer.provide(ProviderRegistryMock.layer([providerSnapshot])),
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
          layerNodeServices,
          NodeCrypto.layer,
          layerScriptFileOpener,
          layerSandboxRunner,
        ),
      ),
    );

    return {
      cwd,
      turnQueue,
      startedTurns,
      continuationOffers,
      layerToolkit,
    };
  });

const startParentThreadAndRun = (
  turnQueue: Queue.Queue<ActiveTurnHandle>,
  cwd: string,
  commandPrefix: string,
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:${commandPrefix}:create`),
      threadId: parentThreadId,
      projectId,
      title: "Workflow parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:${commandPrefix}:start`),
      threadId: parentThreadId,
      messageId: MessageId.make(`message:${commandPrefix}:start`),
      text: "Run workflow",
      attachments: [],
      modelSelection,
      dispatchMode: { type: "start_immediately" },
    });
    const parentTurn = yield* Queue.take(turnQueue);
    expect(parentTurn.turn.threadId).toBe(parentThreadId);
    return parentTurn;
  });

const callTool = (
  name: string,
  args: Record<string, unknown>,
  scope: McpInvocationContext.McpInvocationScope = parentScope,
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name, arguments: args })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
        Effect.provideService(McpSchema.McpServerClient, mcpClient),
      );
  });

const awaitWorkflowTerminal = (taskId: NodeId, afterSequence: number) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const isTerminal = (subagent: OrchestrationV2Subagent | undefined) =>
      subagent !== undefined &&
      (subagent.status === "completed" ||
        subagent.status === "failed" ||
        subagent.status === "cancelled" ||
        subagent.status === "interrupted");
    const readCurrent = () =>
      orchestrator
        .getThreadProjection(parentThreadId)
        .pipe(
          Effect.map((projection) =>
            projection.subagents.find((candidate) => candidate.id === taskId),
          ),
        );
    const current = yield* readCurrent();
    if (isTerminal(current)) return current!;
    const matched = yield* orchestrator
      .streamStoredEventsFrom({
        threadId: parentThreadId,
        afterSequence,
        eventType: "subagent.updated",
      })
      .pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "subagent.updated" &&
            event.payload.id === taskId &&
            isTerminal(event.payload),
        ),
        Stream.runHead,
      );
    if (Option.isNone(matched) || matched.value.event.type !== "subagent.updated") {
      throw new Error(`Workflow task ${taskId} did not settle`);
    }
    // Streamed subagent.updated events carry wire deltas, so read the full persisted projection.
    return (yield* readCurrent()) ?? matched.value.event.payload;
  });

const awaitRunStatus = (threadId: ThreadId, runId: string, status: string, afterSequence: number) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const current = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
      (candidate) => candidate.id === runId,
    );
    if (current?.status === status) return current;
    const matched = yield* orchestrator
      .streamStoredEventsFrom({ threadId, afterSequence, eventType: "run.updated" })
      .pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "run.updated" &&
            event.payload.id === runId &&
            event.payload.status === status,
        ),
        Stream.runHead,
      );
    if (Option.isNone(matched) || matched.value.event.type !== "run.updated") {
      throw new Error(`Run ${runId} did not reach ${status}`);
    }
    return matched.value.event.payload;
  });

describe("WorkflowService and workflow MCP tools", () => {
  it.effect(
    "runs a two-stage workflow (parallel + schema), wakes the parent once with the result, and keeps child wakes suppressed",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({
          name: "parallel-schema",
          runContinuationWorker: true,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threads = yield* ThreadManagementService.ThreadManagementService;
          const fs = yield* FileSystem.FileSystem;

          const parentTurn = yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "parallel-schema",
          );
          const beforeWorkflowSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "Review & Summarize",
              description: "Gather parallel inputs then return a typed summary.",
              phases: [{ title: "gather" }, { title: "summarize" }],
            };
            phase("gather");
            log("Starting parallel agents");
            const parts = await parallel([
              () => agent("Gather part A", { label: "Part A" }),
              () => agent("Gather part B", { label: "Part B" }),
            ]);
            phase("summarize");
            log("Summarizing " + parts.join(","));
            const summary = await agent("Combine " + parts.join(" + "), {
              label: "Summary",
              schema: {
                type: "object",
                properties: {
                  answer: { type: "string" },
                  count: { type: "number" },
                },
                required: ["answer", "count"],
                additionalProperties: false,
              },
            });
            return { parts, summary };
          `;

          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);
          expect(started.status).toBe("running");
          expect(started.runId).toMatch(/^wf_[0-9a-f]{12}$/);
          expect(started.next).toBe(ORCHESTRATOR_MCP_RUNNING_TASK_NEXT);
          expect(yield* fs.exists(started.scriptPath)).toBe(true);

          // Complete the parent turn so workflow completion wakes it as an async completion.
          yield* parentTurn.complete("Workflow started.");
          yield* awaitRunStatus(
            parentThreadId,
            parentTurn.turn.runId,
            "completed",
            beforeWorkflowSeq,
          );

          // Two parallel child turns start concurrently.
          const firstParallel = yield* Queue.take(harness.turnQueue);
          const secondParallel = yield* Queue.take(harness.turnQueue);
          for (const turn of [firstParallel, secondParallel]) {
            const reply = turn.turn.message.text.includes("part A") ? "alpha" : "beta";
            yield* turn.complete(reply, {
              turnTokenUsage: {
                usageScope: "main_agent",
                usageStatus: "complete",
                inputTokens: 40,
                outputTokens: 10,
                hasSubagents: false,
              },
            });
          }

          // Third agent has outputSchema; submit structured output via ThreadManagementService.submitRunOutput.
          const summaryTurn = yield* Queue.take(harness.turnQueue);
          expect(summaryTurn.turn.message.text).toContain("t3_submit_output");
          yield* threads.submitRunOutput({
            threadId: summaryTurn.turn.threadId,
            commandId: CommandId.make("command:submit-summary-output"),
            json: JSON.stringify({ answer: "alpha+beta", count: 2 }),
          });
          yield* summaryTurn.complete("Submitted summary.", {
            turnTokenUsage: {
              usageScope: "main_agent",
              usageStatus: "complete",
              inputTokens: 100,
              outputTokens: 50,
              hasSubagents: false,
            },
          });

          const terminalTask = yield* awaitWorkflowTerminal(started.taskId, beforeWorkflowSeq);
          expect(terminalTask.status).toBe("completed");
          expect(terminalTask.result).toContain("Workflow Review & Summarize completed.");
          expect(terminalTask.result).toContain('"answer":"alpha+beta"');
          expect(terminalTask.result).toContain("250 tokens");
          expect(terminalTask.result).toContain(`Journal: ${started.journalPath}`);

          // Only the workflow completion wakes the parent; its wake turn carries the result text.
          const wakeTurn = yield* Queue.take(harness.turnQueue);
          expect(wakeTurn.turn.threadId).toBe(parentThreadId);
          expect(wakeTurn.turn.message.text).toContain("Workflow Review & Summarize completed.");
          expect(wakeTurn.turn.message.text).toContain('"answer":"alpha+beta"');
          expect(wakeTurn.turn.message.text).toContain(`Journal: ${started.journalPath}`);
          yield* wakeTurn.complete("Acknowledged workflow result.");

          const allTurns = yield* Ref.get(harness.startedTurns);
          expect(allTurns.filter((t) => t.turn.threadId === parentThreadId)).toHaveLength(2);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const parentRun = parentProjection.runs.find((run) => run.id === parentTurn.turn.runId);
          expect(parentRun?.delegatedCompletion?.delivery?.taskIds).toEqual([started.taskId]);

          // Status reads persisted run.json + journal.jsonl cleanly after completion.
          const statusCall = yield* callTool("t3_workflow_status", { runId: started.runId });
          expect(statusCall.isError).not.toBe(true);
          const status = yield* decodeStatusResult(statusCall.structuredContent);
          expect(status.status).toBe("completed");
          expect(status.name).toBe("Review & Summarize");
          expect(status.phases.map((p) => p.title)).toEqual(["gather", "summarize"]);
          expect(status.logs).toEqual(["Starting parallel agents", "Summarizing alpha,beta"]);
          expect(status.usage).toMatchObject({
            agents: 3,
            done: 3,
            errors: 0,
            empty: 0,
            cached: 0,
            tokens: 250,
          });
          expect(status.result).toEqual({
            parts: ["alpha", "beta"],
            summary: { answer: "alpha+beta", count: 2 },
          });
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "allows agents spawned after the parent turn completes to run under the workflow node",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "outlives-parent" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentTurn = yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "outlives-parent",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "Sequential",
              description: "Second agent starts only after parent turn ended.",
            };
            const first = await agent("First step");
            const second = await agent("Second step after " + first);
            return { first, second };
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          const started = yield* decodeRunResult(runCall.structuredContent);

          // Wait for first agent to start, then end the parent turn while agent 1 is still running.
          const firstTurn = yield* Queue.take(harness.turnQueue);
          yield* parentTurn.complete("Parent turn finished early.");
          yield* awaitRunStatus(parentThreadId, parentTurn.turn.runId, "completed", beforeSeq);

          // Now finish agent 1; the script spawns agent 2 AFTER the parent run is already terminal.
          yield* firstTurn.complete("one");
          const secondTurn = yield* Queue.take(harness.turnQueue);
          expect(secondTurn.turn.message.text).toContain("Second step after one");
          yield* secondTurn.complete("two");

          const terminalTask = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminalTask.status).toBe("completed");
          expect(terminalTask.result).toContain('"first":"one"');
          expect(terminalTask.result).toContain('"second":"two"');
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "re-prompts on invalid structured output until valid JSON arrives, and returns null after three bad re-prompts",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({
          name: "schema-reprompt",
          runContinuationWorker: true,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentTurn = yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "schema-reprompt",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "Schema validation",
              description: "Tests schema re-prompting and exhaustion.",
            };
            const schema = {
              type: "object",
              properties: { ok: { type: "boolean" }, score: { type: "number" } },
              required: ["ok", "score"],
              additionalProperties: false,
            };
            const recovered = await agent("Produce valid JSON on retry", { label: "Recover", schema });
            const exhausted = await agent("Always produce bad JSON", { label: "Exhaust", schema });
            return { recovered, exhausted };
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          const started = yield* decodeRunResult(runCall.structuredContent);

          yield* parentTurn.complete("Waiting for workflow.");
          yield* awaitRunStatus(parentThreadId, parentTurn.turn.runId, "completed", beforeSeq);

          // Agent 0: invalid JSON on attempt 1, valid fenced JSON on re-prompt 1.
          const recoverAttempt1 = yield* Queue.take(harness.turnQueue);
          yield* recoverAttempt1.complete("not json at all");

          const recoverAttempt2 = yield* Queue.take(harness.turnQueue);
          expect(recoverAttempt2.turn.threadId).toBe(recoverAttempt1.turn.threadId);
          expect(recoverAttempt2.turn.message.text).toContain(
            "Your result did not match the schema:",
          );
          expect(recoverAttempt2.turn.message.text).toContain("Submit it with t3_submit_output.");
          yield* recoverAttempt2.complete('```json\n{"ok": true, "score": 7}\n```');

          // Agent 1: bad JSON on initial attempt + 3 re-prompts (4 turns total) -> null.
          const exhaustAttempt1 = yield* Queue.take(harness.turnQueue);
          yield* exhaustAttempt1.complete('{"ok": "wrong-type"}');
          for (let retry = 1; retry <= 3; retry += 1) {
            const retryTurn = yield* Queue.take(harness.turnQueue);
            expect(retryTurn.turn.threadId).toBe(exhaustAttempt1.turn.threadId);
            expect(retryTurn.turn.message.text).toContain("Your result did not match the schema:");
            yield* retryTurn.complete(`still bad #${retry}`);
          }

          const terminalTask = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminalTask.status).toBe("completed");
          expect(terminalTask.result).toContain('"recovered":{"ok":true,"score":7}');
          expect(terminalTask.result).toContain('"exhausted":null');

          // Re-prompts must not wake the parent; only workflow completion wakes it once.
          const wakeTurn = yield* Queue.take(harness.turnQueue);
          expect(wakeTurn.turn.threadId).toBe(parentThreadId);
          expect(wakeTurn.turn.message.text).toContain('"recovered":{"ok":true,"score":7}');
          expect(wakeTurn.turn.message.text).toContain('"exhausted":null');
          yield* wakeTurn.complete("Done.");

          const allTurns = yield* Ref.get(harness.startedTurns);
          expect(allTurns.filter((t) => t.turn.threadId === parentThreadId)).toHaveLength(2);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "resumes from a stopped run's journal: caches the finished first agent and runs the edited second agent live",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "resume" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;

          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "resume");

          const scriptFile = path.join(harness.cwd, "resumable.workflow.js");
          yield* fs.writeFileString(
            scriptFile,
            `
              export const meta = { name: "Resumable", description: "Two sequential agents." };
              const a = await agent("First task");
              const b = await agent("Second task v1");
              return { a, b };
            `,
          );

          const run1Call = yield* callTool("t3_workflow_run", { scriptPath: scriptFile });
          const run1 = yield* decodeRunResult(run1Call.structuredContent);

          // Finish first agent, wait until second agent has started, then stop run 1.
          const firstTurn = yield* Queue.take(harness.turnQueue);
          yield* firstTurn.complete("first-result");
          const secondTurnV1 = yield* Queue.take(harness.turnQueue);
          expect(secondTurnV1.turn.message.text).toContain("Second task v1");

          const stopCall = yield* callTool("t3_workflow_stop", { runId: run1.runId });
          expect(stopCall.isError).not.toBe(true);
          expect(stopCall.structuredContent).toEqual({ runId: run1.runId, status: "cancelled" });
          yield* Deferred.await(secondTurnV1.interrupted);

          // Edit the second prompt on disk while keeping the first prompt identical.
          yield* fs.writeFileString(
            scriptFile,
            `
              export const meta = { name: "Resumable", description: "Two sequential agents." };
              const a = await agent("First task");
              const b = await agent("Second task v2");
              return { a, b };
            `,
          );

          const turnsBeforeResume = (yield* Ref.get(harness.startedTurns)).length;
          const beforeResumeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const run2Call = yield* callTool("t3_workflow_run", {
            scriptPath: scriptFile,
            resumeFromRunId: run1.runId,
          });
          const run2 = yield* decodeRunResult(run2Call.structuredContent);

          // Only the edited second agent starts a live child thread; agent 0 came from cache.
          const secondTurnV2 = yield* Queue.take(harness.turnQueue);
          expect(secondTurnV2.turn.message.text).toContain("Second task v2");
          yield* secondTurnV2.complete("second-result-v2");

          const terminalRun2 = yield* awaitWorkflowTerminal(run2.taskId, beforeResumeSeq);
          expect(terminalRun2.status).toBe("completed");
          expect(terminalRun2.result).toContain('"a":"first-result"');
          expect(terminalRun2.result).toContain('"b":"second-result-v2"');

          const turnsAfterResume = yield* Ref.get(harness.startedTurns);
          expect(turnsAfterResume.length - turnsBeforeResume).toBe(1);

          const statusCall = yield* callTool("t3_workflow_status", { runId: run2.runId });
          const status = yield* decodeStatusResult(statusCall.structuredContent);
          expect(status.usage).toMatchObject({ agents: 2, done: 2, cached: 1, errors: 0 });

          // Resuming the exact same script with a different run-level target caches nothing.
          const turnsBeforeAltResume = (yield* Ref.get(harness.startedTurns)).length;
          const beforeAltResumeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const run3Call = yield* callTool("t3_workflow_run", {
            scriptPath: scriptFile,
            resumeFromRunId: run2.runId,
            target: { model: altModel },
          });
          expect(run3Call.isError).not.toBe(true);
          const run3 = yield* decodeRunResult(run3Call.structuredContent);

          const altFirstTurn = yield* Queue.take(harness.turnQueue);
          expect(altFirstTurn.turn.modelSelection.model).toBe(altModel);
          expect(altFirstTurn.turn.message.text).toContain("First task");
          yield* altFirstTurn.complete("alt-first");

          const altSecondTurn = yield* Queue.take(harness.turnQueue);
          expect(altSecondTurn.turn.modelSelection.model).toBe(altModel);
          expect(altSecondTurn.turn.message.text).toContain("Second task v2");
          yield* altSecondTurn.complete("alt-second");

          const terminalRun3 = yield* awaitWorkflowTerminal(run3.taskId, beforeAltResumeSeq);
          expect(terminalRun3.status).toBe("completed");
          expect(terminalRun3.result).toContain('"a":"alt-first"');
          expect(terminalRun3.result).toContain('"b":"alt-second"');

          const turnsAfterAltResume = yield* Ref.get(harness.startedTurns);
          expect(turnsAfterAltResume.length - turnsBeforeAltResume).toBe(2);

          const altStatusCall = yield* callTool("t3_workflow_status", { runId: run3.runId });
          const altStatus = yield* decodeStatusResult(altStatusCall.structuredContent);
          expect(altStatus.usage).toMatchObject({ agents: 2, done: 2, cached: 0, errors: 0 });
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "t3_workflow_stop {runId} cancels the running child, marks the workflow cancelled, and runs no further agents",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "stop" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "stop");

          const script = `
            export const meta = { name: "Stoppable", description: "Cancelled mid-flight." };
            await agent("Long running child");
            await agent("Should never start");
            return "unreachable";
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          const started = yield* decodeRunResult(runCall.structuredContent);

          const longChild = yield* Queue.take(harness.turnQueue);
          expect(longChild.turn.message.text).toContain("Long running child");

          const stopCall = yield* callTool("t3_workflow_stop", { runId: started.runId });
          expect(stopCall.isError).not.toBe(true);
          expect(stopCall.structuredContent).toEqual({
            runId: started.runId,
            status: "cancelled",
          });
          yield* Deferred.await(longChild.interrupted);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(parentProjection.subagents.find((s) => s.id === started.taskId)?.status).toBe(
            "cancelled",
          );

          // Only the parent turn and the one cancelled child turn ever started.
          const allTurns = yield* Ref.get(harness.startedTurns);
          expect(allTurns.filter((t) => t.turn.threadId !== parentThreadId)).toHaveLength(1);

          // Stopping a terminal workflow returns its status unchanged; empty params fail invalid_request.
          const stopAgain = yield* callTool("t3_workflow_stop", { runId: started.runId });
          expect(stopAgain.isError).not.toBe(true);
          expect(stopAgain.structuredContent).toEqual({
            runId: started.runId,
            status: "cancelled",
          });

          const badStop = yield* callTool("t3_workflow_stop", {});
          expect(declaredFailure(badStop).code).toBe("invalid_request");
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect("refuses t3_workflow_run with parent_not_active when the caller has no live run", () =>
    Effect.gen(function* () {
      const harness = yield* makeWorkflowHarness({ name: "not-active" });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:not-active:create"),
          threadId: parentThreadId,
          projectId,
          title: "Idle parent",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: harness.cwd,
        });

        const result = yield* callTool("t3_workflow_run", {
          script: `export const meta = { name: "Nope", description: "Should fail." }; return 1;`,
        });
        expect(declaredFailure(result).code).toBe("parent_not_active");
      }).pipe(Effect.provide(harness.layerToolkit));
    }),
  );

  it.effect("coalesces workflow.progress updates to at most one per second via TestClock", () =>
    Effect.gen(function* () {
      const harness = yield* makeWorkflowHarness({ name: "progress-coalesce" });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "progress-coalesce");
        const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

        const progressEvents = yield* Ref.make<ReadonlyArray<OrchestrationV2Subagent>>([]);
        const progressQueue = yield* Queue.unbounded<OrchestrationV2Subagent>();
        const watcher = yield* orchestrator
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
                readonly event: Extract<
                  OrchestrationV2DomainEvent,
                  { readonly type: "subagent.updated" }
                >;
              } =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.workflow !== undefined &&
                stored.event.payload.progress !== undefined,
            ),
            Stream.runForEach(({ event }) =>
              Ref.update(progressEvents, (list) => [...list, event.payload]).pipe(
                Effect.andThen(Queue.offer(progressQueue, event.payload)),
              ),
            ),
            Effect.forkScoped,
          );

        const script = `
          export const meta = { name: "Coalesced", description: "Tests 1s coalescing." };
          log("first");
          phase("p1");
          const a = await agent("First child");
          log("second");
          log("third");
          log("fourth");
          const b = await agent("Second child");
          return [a, b];
        `;
        const runCall = yield* callTool("t3_workflow_run", { script });
        const started = yield* decodeRunResult(runCall.structuredContent);

        const child1 = yield* Queue.take(harness.turnQueue);
        // First progress event fires immediately at T=0; the log comes first so that event carries it.
        const firstProgress = yield* Queue.take(progressQueue);
        expect(firstProgress.progress).toBe("first");

        // Finish child 1; script emits 3 rapid logs and starts child 2 all at T=0.
        yield* child1.complete("done-1");
        const child2 = yield* Queue.take(harness.turnQueue);

        // Live status already shows all 4 logs, while coalesced progress waits for the 1s window.
        const liveStatusCall = yield* callTool("t3_workflow_status", { runId: started.runId });
        const liveStatus = yield* decodeStatusResult(liveStatusCall.structuredContent);
        expect(liveStatus.logs).toEqual(["first", "second", "third", "fourth"]);
        expect(liveStatus.agents.running).toBe(1);
        expect(yield* Ref.get(progressEvents)).toHaveLength(1);

        // Advancing TestClock by 1 second flushes one coalesced update carrying the newest log.
        yield* TestClock.adjust("1 second");
        const secondProgress = yield* Queue.take(progressQueue);
        expect(secondProgress.progress).toBe("fourth");
        expect(yield* Ref.get(progressEvents)).toHaveLength(2);

        yield* child2.complete("done-2");
        const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
        expect(terminal.status).toBe("completed");
        yield* Fiber.interrupt(watcher);
      }).pipe(Effect.provide(harness.layerToolkit));
    }),
  );

  it.effect(
    "journals usage_limited children as usage_limited and reruns them on resume without breaking later cached results",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "usage-limit-resume" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const fs = yield* FileSystem.FileSystem;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "usage-limit-resume");
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = { name: "LimitResume", description: "Reruns usage-limited agent." };
            const a = await agent("May hit usage limit");
            const b = await agent("Stable second agent");
            return [a, b];
          `;
          const run1Call = yield* callTool("t3_workflow_run", { script });
          const run1 = yield* decodeRunResult(run1Call.structuredContent);

          const firstTurn = yield* Queue.take(harness.turnQueue);
          yield* firstTurn.failUsageLimit();

          const secondTurn = yield* Queue.take(harness.turnQueue);
          yield* secondTurn.complete("kept-from-run-1");

          const terminal1 = yield* awaitWorkflowTerminal(run1.taskId, beforeSeq);
          expect(terminal1.status).toBe("completed");
          expect(terminal1.result).toContain('[null,"kept-from-run-1"]');

          const journalEntries = decodeJournal(yield* fs.readFileString(run1.journalPath));
          expect(
            journalEntries.find((entry) => entry.type === "result" && entry.index === 0),
          ).toMatchObject({ outcome: "usage_limited", value: null });

          // Resume: agent 0 reruns live, agent 1 stays cached!
          const turnsBeforeResume = (yield* Ref.get(harness.startedTurns)).length;
          const beforeResumeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const run2Call = yield* callTool("t3_workflow_run", {
            script,
            resumeFromRunId: run1.runId,
          });
          const run2 = yield* decodeRunResult(run2Call.structuredContent);

          const rerunFirst = yield* Queue.take(harness.turnQueue);
          expect(rerunFirst.turn.message.text).toContain("May hit usage limit");
          yield* rerunFirst.complete("recovered");

          const terminal2 = yield* awaitWorkflowTerminal(run2.taskId, beforeResumeSeq);
          expect(terminal2.status).toBe("completed");
          expect(terminal2.result).toContain('["recovered","kept-from-run-1"]');

          const turnsAfterResume = yield* Ref.get(harness.startedTurns);
          expect(turnsAfterResume.length - turnsBeforeResume).toBe(1);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "startup sweep stops unfinished children, reconciles already-completed tasks without overwriting results, and marks unstarted runs terminal",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-sweep-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);
        const harness1 = yield* makeWorkflowHarness({
          name: "sweep-1",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const zeroUsage = {
          agents: 0,
          done: 0,
          errors: 0,
          empty: 0,
          cached: 0,
          tokens: 0,
          durationMs: 0,
        };

        // Seed three running run.json files: stale with an unfinished child, already completed in SQL, and never started.
        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "sweep",
          );
          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const parentRun = parentProjection.runs.find((r) => r.id === parentTurn.turn.runId)!;

          // 1. Stale running workflow with a live child thread + a queued follow-up run on that child.
          const staleRunId = "wf_deadbeef1234";
          const staleDir = path.join(config.workflowRunsDir, staleRunId);
          const staleScriptPath = path.join(staleDir, "script.js");
          const staleJournalPath = path.join(staleDir, "journal.jsonl");
          yield* fs.makeDirectory(staleDir, { recursive: true });
          yield* fs.writeFileString(
            staleScriptPath,
            `export const meta = { name: "Stale", description: "Interrupted by restart." }; return 1;`,
          );

          const staleStarted = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:sweep:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "Stale",
            description: "Interrupted by restart.",
            modelSelection,
            workflow: {
              name: "Stale",
              runHandles: {
                runId: staleRunId,
                scriptPath: staleScriptPath,
                transcriptDir: staleDir,
              },
              phases: [],
              agents: [],
            },
          });
          const staleTaskEvent = staleStarted.storedEvents.find(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.origin === "app_owned",
          );
          if (staleTaskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing stale workflow task");
          }
          const staleTaskId = staleTaskEvent.event.payload.id;

          const child = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: staleTaskId,
            requestKey: `${staleRunId}:0`,
            task: { task: "Stale child task", title: "Orphan child" },
          });
          const staleChildTurn = yield* Queue.take(harness1.turnQueue);
          expect(staleChildTurn.turn.threadId).toBe(child.childThreadId);

          // Queue another run behind the active child turn so recovery would resume it unless sweep cancels it.
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "agent",
            creationSource: "mcp",
            commandId: CommandId.make("command:sweep:child-queued"),
            threadId: child.childThreadId,
            senderThreadId: parentThreadId,
            messageId: MessageId.make("message:sweep:child-queued"),
            text: "Queued work behind active child",
            attachments: [],
            modelSelection,
            dispatchMode: { type: "queue_after_active" },
          });
          const childBeforeRestart = yield* orchestrator.getThreadProjection(child.childThreadId);
          expect(childBeforeRestart.runs.some((run) => run.status === "queued")).toBe(true);

          yield* fs.writeFileString(
            staleJournalPath,
            `${JSON.stringify({
              type: "started",
              index: 0,
              key: "stale-child-key",
              label: "Orphan child",
              phase: null,
              agentId: child.taskId,
              childThreadId: child.childThreadId,
            })}\n`,
          );
          yield* fs.writeFileString(
            path.join(staleDir, "run.json"),
            JSON.stringify({
              runId: staleRunId,
              threadId: parentThreadId,
              taskId: staleTaskId,
              name: "Stale",
              scriptPath: staleScriptPath,
              journalPath: staleJournalPath,
              startedAt: 0,
              status: "running",
              usage: zeroUsage,
            }),
          );

          // 2. Workflow already completed in SQL whose run.json still says "running" (crash between complete and write).
          const completedRunId = "wf_c0ffee123456";
          const completedDir = path.join(config.workflowRunsDir, completedRunId);
          const completedScriptPath = path.join(completedDir, "script.js");
          const completedJournalPath = path.join(completedDir, "journal.jsonl");
          const completedResultText = "Workflow AlreadyDone completed.\nResult: 42";
          yield* fs.makeDirectory(completedDir, { recursive: true });
          yield* fs.writeFileString(
            completedScriptPath,
            `export const meta = { name: "AlreadyDone", description: "Finished before crash." }; return 42;`,
          );
          yield* fs.writeFileString(
            completedJournalPath,
            `${JSON.stringify({ type: "end", status: "completed", result: 42 })}\n`,
          );
          const completedStarted = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:sweep:completed-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "AlreadyDone",
            description: "Finished before crash.",
            modelSelection,
            workflow: {
              name: "AlreadyDone",
              runHandles: {
                runId: completedRunId,
                scriptPath: completedScriptPath,
                transcriptDir: completedDir,
              },
              phases: [],
              agents: [],
            },
          });
          const completedTaskEvent = completedStarted.storedEvents.find(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.origin === "app_owned",
          );
          if (completedTaskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing completed workflow task");
          }
          const completedTaskId = completedTaskEvent.event.payload.id;
          yield* orchestrator.dispatch({
            type: "workflow.complete",
            commandId: CommandId.make("command:sweep:completed-finish"),
            parentThreadId,
            taskId: completedTaskId,
            status: "completed",
            result: completedResultText,
          });
          yield* fs.writeFileString(
            path.join(completedDir, "run.json"),
            JSON.stringify({
              runId: completedRunId,
              threadId: parentThreadId,
              taskId: completedTaskId,
              name: "AlreadyDone",
              scriptPath: completedScriptPath,
              journalPath: completedJournalPath,
              startedAt: 0,
              status: "running",
              usage: zeroUsage,
            }),
          );

          // 3. run.json written before workflow.start crashed, so the task never existed in SQL.
          const unstartedRunId = "wf_0000dead0001";
          const unstartedDir = path.join(config.workflowRunsDir, unstartedRunId);
          const unstartedScriptPath = path.join(unstartedDir, "script.js");
          const unstartedJournalPath = path.join(unstartedDir, "journal.jsonl");
          yield* fs.makeDirectory(unstartedDir, { recursive: true });
          yield* fs.writeFileString(
            unstartedScriptPath,
            `export const meta = { name: "Unstarted", description: "Crashed before start." }; return 0;`,
          );
          yield* fs.writeFileString(unstartedJournalPath, "");
          yield* fs.writeFileString(
            path.join(unstartedDir, "run.json"),
            JSON.stringify({
              runId: unstartedRunId,
              threadId: parentThreadId,
              taskId: NodeId.make("node:delegated-task:never-dispatched"),
              name: "Unstarted",
              scriptPath: unstartedScriptPath,
              journalPath: unstartedJournalPath,
              startedAt: 0,
              status: "running",
              usage: zeroUsage,
            }),
          );

          return {
            staleRunId,
            staleTaskId,
            childTaskId: child.taskId,
            childThreadId: child.childThreadId,
            completedRunId,
            completedTaskId,
            completedResultText,
            unstartedRunId,
            completedMetaPath: path.join(completedDir, "run.json"),
            unstartedMetaPath: path.join(unstartedDir, "run.json"),
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        // Run startup workflow recovery before inspecting recovered runs against the shared DB.
        const harness2 = yield* makeWorkflowHarness({
          name: "sweep-2",
          databaseLayer,
          baseDir: sharedBaseDir,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* WorkflowService.recoverInterruptedWorkflows;

          // 1. Stale workflow is marked failed and its unfinished child is stopped with queued runs cancelled.
          const staleStatusCall = yield* callTool("t3_workflow_status", {
            runId: seeded.staleRunId,
          });
          expect(staleStatusCall.isError).not.toBe(true);
          const staleStatus = yield* decodeStatusResult(staleStatusCall.structuredContent);
          expect(staleStatus.status).toBe("failed");
          expect(staleStatus.error).toContain(`resumeFromRunId=${seeded.staleRunId}`);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const staleTask = parentProjection.subagents.find((s) => s.id === seeded.staleTaskId);
          expect(staleTask?.status).toBe("failed");
          expect(staleTask?.result).toContain(`resumeFromRunId=${seeded.staleRunId}`);

          const childProjection = yield* orchestrator.getThreadProjection(seeded.childThreadId);
          expect(childProjection.runs.filter((run) => run.status === "queued")).toHaveLength(0);
          expect(childProjection.runs.some((run) => run.status === "cancelled")).toBe(true);
          expect(
            childProjection.turnItems.some((item) => item.type === "run_interrupt_request") ||
              childProjection.runs.some((run) => run.status === "interrupted"),
          ).toBe(true);

          // 2. Already-completed task keeps its result untouched and run.json reconciles to completed.
          const completedTask = parentProjection.subagents.find(
            (s) => s.id === seeded.completedTaskId,
          );
          expect(completedTask?.status).toBe("completed");
          expect(completedTask?.result).toBe(seeded.completedResultText);

          const completedStatusCall = yield* callTool("t3_workflow_status", {
            runId: seeded.completedRunId,
          });
          const completedStatus = yield* decodeStatusResult(completedStatusCall.structuredContent);
          expect(completedStatus.status).toBe("completed");
          expect(completedStatus.result).toBe(42);
          expect(completedStatus.error).toBeUndefined();
          const completedMeta = JSON.parse(yield* fs.readFileString(seeded.completedMetaPath)) as {
            status: string;
          };
          expect(completedMeta.status).toBe("completed");

          // 3. Unstarted run.json is marked terminal failed without erroring on missing SQL task.
          const unstartedStatusCall = yield* callTool("t3_workflow_status", {
            runId: seeded.unstartedRunId,
          });
          const unstartedStatus = yield* decodeStatusResult(unstartedStatusCall.structuredContent);
          expect(unstartedStatus.status).toBe("failed");
          const unstartedMeta = JSON.parse(yield* fs.readFileString(seeded.unstartedMetaPath)) as {
            status: string;
          };
          expect(unstartedMeta.status).toBe("failed");
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "cancels unawaited agents when the script returns, journals them cancelled (unfinished), and reruns them on resume",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "unawaited-agent" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const fs = yield* FileSystem.FileSystem;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "unawaited-agent");
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "Unawaited",
              description: "Leaves one agent running when returning.",
            };
            agent("Hold me open", { label: "Orphan" });
            const gate = await agent("Release script", { label: "Gate" });
            return { gate };
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          const turnA = yield* Queue.take(harness.turnQueue);
          const turnB = yield* Queue.take(harness.turnQueue);
          const orphanTurn = turnA.turn.message.text.includes("Hold me open") ? turnA : turnB;
          const gateTurn = orphanTurn === turnA ? turnB : turnA;

          // Keep orphanTurn held open while gateTurn finishes so the script returns with a live child.
          yield* gateTurn.complete("go");

          yield* Deferred.await(orphanTurn.interrupted);
          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("completed");
          expect(terminal.result).toContain('"gate":"go"');
          expect(terminal.workflow?.agents).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ label: "Orphan", state: "failed" }),
              expect.objectContaining({ label: "Gate", state: "completed" }),
            ]),
          );

          const statusCall = yield* callTool("t3_workflow_status", { runId: started.runId });
          const status = yield* decodeStatusResult(statusCall.structuredContent);
          expect(status.logs).toContain("1 unawaited agent was cancelled when the script ended");

          const journal = decodeJournal(yield* fs.readFileString(started.journalPath));
          const orphanResultIndex = journal.findIndex(
            (entry) =>
              entry.type === "result" && entry.index === 0 && entry.outcome === "cancelled",
          );
          const logIndex = journal.findIndex(
            (entry) =>
              entry.type === "log" &&
              entry.message === "1 unawaited agent was cancelled when the script ended",
          );
          const endIndex = journal.findIndex((entry) => entry.type === "end");
          expect(orphanResultIndex).toBeGreaterThan(-1);
          expect(logIndex).toBeGreaterThan(orphanResultIndex);
          expect(endIndex).toBeGreaterThan(logIndex);

          // Resuming treats the cancelled unawaited call as unfinished, so it reruns live.
          const turnsBeforeResume = (yield* Ref.get(harness.startedTurns)).length;
          const beforeResumeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const resumedCall = yield* callTool("t3_workflow_run", {
            script: `
              export const meta = {
                name: "Unawaited",
                description: "Awaits previously unawaited call.",
              };
              const first = await agent("Hold me open", { label: "Orphan" });
              const gate = await agent("Release script", { label: "Gate" });
              return { first, gate };
            `,
            resumeFromRunId: started.runId,
          });
          expect(resumedCall.isError).not.toBe(true);
          const resumed = yield* decodeRunResult(resumedCall.structuredContent);

          const rerunOrphan = yield* Queue.take(harness.turnQueue);
          expect(rerunOrphan.turn.message.text).toContain("Hold me open");
          yield* rerunOrphan.complete("finished-on-resume");

          const rerunGate = yield* Queue.take(harness.turnQueue);
          expect(rerunGate.turn.message.text).toContain("Release script");
          yield* rerunGate.complete("go-2");

          const resumedTerminal = yield* awaitWorkflowTerminal(resumed.taskId, beforeResumeSeq);
          expect(resumedTerminal.status).toBe("completed");
          expect(resumedTerminal.result).toContain('"first":"finished-on-resume"');
          expect(resumedTerminal.result).toContain('"gate":"go-2"');
          expect((yield* Ref.get(harness.startedTurns)).length - turnsBeforeResume).toBe(2);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "cancels a queued schema re-prompt on stop so no queued run remains and nothing wakes the parent",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({
          name: "stop-queued-reprompt",
          runContinuationWorker: true,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentTurn = yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "stop-queued-reprompt",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "QueuedReprompt",
              description: "Stops while schema re-prompt is queued.",
            };
            return await agent("Return JSON", {
              label: "Structured",
              schema: {
                type: "object",
                properties: { ok: { type: "boolean" } },
                required: ["ok"],
                additionalProperties: false,
              },
            });
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          // Complete the parent turn so any unwanted wake would actually start a second parent turn.
          yield* parentTurn.complete("Started workflow.");
          yield* awaitRunStatus(parentThreadId, parentTurn.turn.runId, "completed", beforeSeq);

          const attempt1 = yield* Queue.take(harness.turnQueue);
          const childThreadId = attempt1.turn.threadId;
          yield* attempt1.complete("invalid json 1");

          // Let re-prompt #1 start; queue a blocker behind it so re-prompt #2 lands queued.
          const reprompt1 = yield* Queue.take(harness.turnQueue);
          expect(reprompt1.turn.threadId).toBe(childThreadId);
          expect(reprompt1.turn.message.text).toContain("Your result did not match the schema:");
          const beforeChildSeq = yield* orchestrator.getThreadEventSequence(childThreadId);

          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make("command:stop-queued-reprompt:blocker"),
            threadId: childThreadId,
            messageId: MessageId.make("message:stop-queued-reprompt:blocker"),
            text: "Hold child busy while re-prompt #2 queues",
            attachments: [],
            modelSelection,
            dispatchMode: { type: "queue_after_active" },
          });

          yield* reprompt1.complete("invalid json 2");
          const blockerTurn = yield* Queue.take(harness.turnQueue);
          expect(blockerTurn.turn.threadId).toBe(childThreadId);

          // Wait until re-prompt #2 is durably queued on the child thread behind the blocker turn.
          const queuedReprompt = yield* orchestrator
            .streamStoredEventsFrom({
              threadId: childThreadId,
              afterSequence: beforeChildSeq,
              eventType: "run.updated",
            })
            .pipe(
              Stream.filter(
                ({ event }) =>
                  event.type === "run.updated" &&
                  event.payload.id !== attempt1.turn.runId &&
                  event.payload.id !== reprompt1.turn.runId &&
                  event.payload.outputSchema !== undefined &&
                  event.payload.status === "queued",
              ),
              Stream.runHead,
            );
          if (Option.isNone(queuedReprompt) || queuedReprompt.value.event.type !== "run.updated") {
            throw new Error("Expected queued schema re-prompt run");
          }
          const repromptRunId = queuedReprompt.value.event.payload.id;

          const externalFullAccessScope: McpInvocationContext.McpInvocationScope = {
            environmentId: parentScope.environmentId,
            requestNamespace: "client-session:external-stop",
            thread: undefined,
            client: {
              sessionId: "client-session:external-stop",
              label: "External Stop",
              access: "full-access",
            },
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };
          const stopCall = yield* callTool(
            "t3_workflow_stop",
            { runId: started.runId },
            externalFullAccessScope,
          );
          expect(stopCall.isError).not.toBe(true);
          expect(stopCall.structuredContent).toEqual({
            runId: started.runId,
            status: "cancelled",
          });

          yield* Deferred.await(blockerTurn.interrupted);

          const childProjection = yield* orchestrator.getThreadProjection(childThreadId);
          expect(childProjection.runs.find((run) => run.id === repromptRunId)?.status).toBe(
            "cancelled",
          );
          expect(childProjection.runs.filter((run) => run.status === "queued")).toHaveLength(0);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(parentProjection.runs).toHaveLength(1);
          expect(
            parentProjection.subagents.find((s) => s.id === started.taskId)?.completionDelivery
              ?.state,
          ).toBe("disposed");
          const allTurns = yield* Ref.get(harness.startedTurns);
          expect(allTurns.filter((t) => t.turn.threadId === parentThreadId)).toHaveLength(1);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "bounds large completion text to 12,000 UTF-8 bytes, names the runId in the wake, and keeps full result in status",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({
          name: "large-result",
          runContinuationWorker: true,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentTurn = yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "large-result",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "LargeResult",
              description: "Returns a 40 KB payload.",
            };
            return { payload: "x".repeat(40 * 1024) };
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          yield* parentTurn.complete("Waiting for large workflow.");
          yield* awaitRunStatus(parentThreadId, parentTurn.turn.runId, "completed", beforeSeq);

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("completed");
          expect(terminal.result).not.toBeNull();
          expect(Buffer.byteLength(terminal.result!, "utf8")).toBeLessThanOrEqual(12_000);
          expect(terminal.result).toContain(started.runId);
          expect(terminal.result).toContain("t3_workflow_status");
          expect(terminal.result).toContain("Usage:");
          expect(terminal.result).toContain(`Journal: ${started.journalPath}`);

          // Delegated completion delivery inlines the bounded summary rather than dropping to task_status.
          const wakeTurn = yield* Queue.take(harness.turnQueue);
          expect(wakeTurn.turn.threadId).toBe(parentThreadId);
          expect(wakeTurn.turn.message.text).toContain(started.runId);
          expect(wakeTurn.turn.message.text).toContain("t3_workflow_status");
          expect(wakeTurn.turn.message.text).not.toContain("Use task_status with taskId");
          yield* wakeTurn.complete("Acknowledged.");

          const statusCall = yield* callTool("t3_workflow_status", { runId: started.runId });
          const status = yield* decodeStatusResult(statusCall.structuredContent);
          expect(status.result).toEqual({ payload: "x".repeat(40 * 1024) });
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "publishes the final workflow snapshot on stop before the next TestClock progress tick",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "stop-final-snapshot" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "stop-final-snapshot");
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const progressQueue = yield* Queue.unbounded<OrchestrationV2Subagent>();
          const watcher = yield* orchestrator
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
                  readonly event: Extract<
                    OrchestrationV2DomainEvent,
                    { readonly type: "subagent.updated" }
                  >;
                } =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.workflow !== undefined &&
                  stored.event.payload.progress !== undefined,
              ),
              Stream.runForEach(({ event }) => Queue.offer(progressQueue, event.payload)),
              Effect.forkScoped,
            );

          const script = `
            export const meta = {
              name: "StopSnapshot",
              description: "Stops before the next progress tick.",
            };
            log("boot");
            await agent("First step", { label: "First" });
            await agent("Second step", { label: "Second" });
            return "done";
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          // Consume the initial T=0 progress tick so subsequent updates wait for the 1s coalescing window.
          const firstTick = yield* Queue.take(progressQueue);
          expect(firstTick.progress).toBe("boot");

          const firstTurn = yield* Queue.take(harness.turnQueue);
          yield* firstTurn.complete("done-1");

          const secondTurn = yield* Queue.take(harness.turnQueue);
          expect(secondTurn.turn.message.text).toContain("Second step");

          // Before stopping (still at T=0), the coalesced row has not yet recorded the second agent.
          const beforeStopTask = (yield* orchestrator.getThreadProjection(
            parentThreadId,
          )).subagents.find((candidate) => candidate.id === started.taskId);
          expect(beforeStopTask?.workflow?.agents.some((agent) => agent.label === "Second")).toBe(
            false,
          );

          const stopCall = yield* callTool("t3_workflow_stop", { runId: started.runId });
          expect(stopCall.isError).not.toBe(true);
          expect(stopCall.structuredContent).toEqual({
            runId: started.runId,
            status: "cancelled",
          });
          yield* Deferred.await(secondTurn.interrupted);

          const terminalTask = (yield* orchestrator.getThreadProjection(
            parentThreadId,
          )).subagents.find((candidate) => candidate.id === started.taskId);
          expect(terminalTask?.status).toBe("cancelled");
          expect(terminalTask?.workflow?.agents).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ label: "First", state: "completed" }),
              expect.objectContaining({
                label: "Second",
                state: "failed",
                childThreadId: secondTurn.turn.threadId,
              }),
            ]),
          );
          yield* Fiber.interrupt(watcher);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect("rejects target.environmentId in OrchestratorMcpService.delegateWorkflowAgent", () =>
    Effect.gen(function* () {
      const harness = yield* makeWorkflowHarness({ name: "reject-remote-env" });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
        const parentTurn = yield* startParentThreadAndRun(
          harness.turnQueue,
          harness.cwd,
          "reject-remote-env",
        );
        const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
          (r) => r.id === parentTurn.turn.runId,
        )!;
        const err = yield* mcpService
          .delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: parentRun.rootNodeId!,
            requestKey: "wf_test:0",
            task: {
              task: "Do work",
              target: { environmentId: EnvironmentId.make("environment:remote") },
            },
          })
          .pipe(Effect.flip);
        expect(err.code).toBe("invalid_request");
        expect(err.message).toMatch(/this environment/i);
      }).pipe(Effect.provide(harness.layerToolkit));
    }),
  );

  it.effect(
    "startup recovery stops a SQL-owned child with no journal started record before provider recovery so it is not resumed",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-sql-recovery-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);
        const harness1 = yield* makeWorkflowHarness({
          name: "sql-recovery-1",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "sql-recovery",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = "wf_sqlonly00001";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "CrashGap", description: "Crashed before journal append." }; return 1;`,
          );
          // Empty journal simulates crashing after delegated_task.request committed and before journal append.
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:sql-recovery:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "CrashGap",
            description: "Crashed before journal append.",
            modelSelection,
            workflow: {
              name: "CrashGap",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing workflow task");
          }
          const taskId = taskEvent.event.payload.id;
          yield* fs.writeFileString(
            path.join(dir, "run.json"),
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "CrashGap",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 0,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );

          const child = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:0`,
            task: { task: "Unjournaled child work", title: "Unjournaled" },
          });
          const childTurn = yield* Queue.take(harness1.turnQueue);
          expect(childTurn.turn.threadId).toBe(child.childThreadId);

          return {
            runId,
            taskId,
            childThreadId: child.childThreadId,
            metaPath: path.join(dir, "run.json"),
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        // Phase 1: run workflow recovery before provider recovery and before the effect worker starts.
        const preRecoveryHarness = yield* makeWorkflowHarness({
          name: "sql-recovery-pre",
          databaseLayer,
          baseDir: sharedBaseDir,
          runEffectWorker: false,
        });
        yield* WorkflowService.recoverInterruptedWorkflows.pipe(
          Effect.provide(preRecoveryHarness.layerToolkit),
        );

        // Phase 2: run provider recovery and start the effect worker; the stopped child must not resume.
        const postRecoveryHarness = yield* makeWorkflowHarness({
          name: "sql-recovery-post",
          databaseLayer,
          baseDir: sharedBaseDir,
          recoverOnStartup: true,
          runEffectWorker: true,
          runContinuationWorker: true,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const task = parentProjection.subagents.find((s) => s.id === seeded.taskId);
          expect(task?.status).toBe("failed");
          expect(task?.result).toContain(`resumeFromRunId=${seeded.runId}`);

          const childProjection = yield* orchestrator.getThreadProjection(seeded.childThreadId);
          expect(
            childProjection.turnItems.some((item) => item.type === "run_interrupt_request"),
          ).toBe(true);
          expect(
            childProjection.runs.some((run) => run.restartContinuationOfRunId !== undefined),
          ).toBe(false);
          expect(
            childProjection.runs.every(
              (run) =>
                run.status === "interrupted" ||
                run.status === "cancelled" ||
                run.status === "failed",
            ),
          ).toBe(true);

          const restartedChildTurns = (yield* Ref.get(postRecoveryHarness.startedTurns)).filter(
            (t) => t.turn.threadId === seeded.childThreadId,
          );
          expect(restartedChildTurns).toHaveLength(0);

          const meta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as { status: string };
          expect(meta.status).toBe("failed");
        }).pipe(Effect.provide(postRecoveryHarness.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "stops children server-side even after parent runtime mode is lowered, completing only after child stop commits",
    () =>
      Effect.gen(function* () {
        const order: Array<string> = [];
        const harness = yield* makeWorkflowHarness({
          name: "lowered-parent-mode",
          wrapThreadManagement: (tm) => ({
            ...tm,
            cancelDelegatedTask: (input) =>
              tm
                .cancelDelegatedTask(input)
                .pipe(Effect.tap(() => Effect.sync(() => order.push("child-stop")))),
            dispatch: (command) =>
              tm
                .dispatch(command)
                .pipe(
                  Effect.tap(() =>
                    command.type === "workflow.complete"
                      ? Effect.sync(() => order.push("workflow-complete"))
                      : Effect.void,
                  ),
                ),
          }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "lowered-parent-mode");

          const script = `
            export const meta = { name: "ModeDrop", description: "Stops after parent mode drops." };
            await agent("Full-access child");
            return "unreachable";
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          const started = yield* decodeRunResult(runCall.structuredContent);

          const childTurn = yield* Queue.take(harness.turnQueue);

          // Lower the parent thread's runtime mode below the child's full-access mode.
          yield* orchestrator.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("command:lowered-parent-mode:drop"),
            threadId: parentThreadId,
            runtimeMode: "approval-required",
          });
          const parentShell = yield* orchestrator.getThreadShell(parentThreadId);
          const childShell = yield* orchestrator.getThreadShell(childTurn.turn.threadId);
          expect(parentShell?.runtimeMode).toBe("approval-required");
          expect(childShell?.runtimeMode).toBe("full-access");

          const stopCall = yield* callTool("t3_workflow_stop", { runId: started.runId });
          expect(stopCall.isError).not.toBe(true);
          expect(stopCall.structuredContent).toEqual({ runId: started.runId, status: "cancelled" });
          yield* Deferred.await(childTurn.interrupted);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          expect(parentProjection.subagents.find((s) => s.id === started.taskId)?.status).toBe(
            "cancelled",
          );
          expect(order).toContain("child-stop");
          expect(order).toContain("workflow-complete");
          expect(order.indexOf("child-stop")).toBeLessThan(order.indexOf("workflow-complete"));
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect("retries a child stop that fails once before completing the workflow", () =>
    Effect.gen(function* () {
      let cancelAttempts = 0;
      const firstFailed = yield* Deferred.make<void>();
      const harness = yield* makeWorkflowHarness({
        name: "retry-child-stop",
        wrapThreadManagement: (tm) => ({
          ...tm,
          cancelDelegatedTask: (input) =>
            Effect.gen(function* () {
              cancelAttempts += 1;
              if (cancelAttempts === 1) {
                yield* Deferred.succeed(firstFailed, undefined);
                return yield* new Orchestrator.OrchestratorDispatchError({
                  commandId: input.command.commandId,
                  commandType: input.command.type,
                  cause: "transient cancel failure",
                });
              }
              return yield* tm.cancelDelegatedTask(input);
            }),
        }),
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const config = yield* ServerConfig.ServerConfig;
        yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "retry-child-stop");

        const script = `
          export const meta = { name: "RetryStop", description: "Retries transient child cancel failure." };
          await agent("Stubborn child");
          return "unreachable";
        `;
        const runCall = yield* callTool("t3_workflow_run", { script });
        const started = yield* decodeRunResult(runCall.structuredContent);
        const childTurn = yield* Queue.take(harness.turnQueue);

        const stopFiber = yield* callTool("t3_workflow_stop", { runId: started.runId }).pipe(
          Effect.forkScoped,
        );
        yield* Deferred.await(firstFailed);

        // While the child stop is still owed, both SQL and run.json stay running.
        const midTask = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (s) => s.id === started.taskId,
        );
        expect(midTask?.status).toBe("running");
        const midMeta = JSON.parse(
          yield* fs.readFileString(path.join(config.workflowRunsDir, started.runId, "run.json")),
        ) as { status: string };
        expect(midMeta.status).toBe("running");

        yield* TestClock.adjust("50 millis");
        const stopCall = yield* Fiber.join(stopFiber);
        expect(stopCall.isError).not.toBe(true);
        expect(stopCall.structuredContent).toEqual({ runId: started.runId, status: "cancelled" });
        yield* Deferred.await(childTurn.interrupted);
        expect(cancelAttempts).toBeGreaterThanOrEqual(2);
      }).pipe(Effect.provide(harness.layerToolkit));
    }),
  );

  it.effect.each(["client-stop-request", "mcp-stop"] as const)(
    "one stop path (%s): final snapshot keeps a child launched after the last tick, uses disposeDelivery:true, and does not wake parent",
    (mode) =>
      Effect.gen(function* () {
        const completeCommands: Array<{
          readonly status: string;
          readonly disposeDelivery?: boolean;
        }> = [];
        let separateDisposeDispatches = 0;
        const harness = yield* makeWorkflowHarness({
          name: `one-stop-${mode}`,
          runContinuationWorker: true,
          wrapThreadManagement: (tm) => ({
            ...tm,
            dispatch: (command) => {
              if (command.type === "delegated_task.completion-delivery.dispose") {
                separateDisposeDispatches += 1;
              }
              if (command.type === "workflow.complete") {
                completeCommands.push({
                  status: command.status,
                  ...(command.disposeDelivery === undefined
                    ? {}
                    : { disposeDelivery: command.disposeDelivery }),
                });
              }
              return tm.dispatch(command);
            },
          }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentTurn = yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            `one-stop-${mode}`,
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const progressQueue = yield* Queue.unbounded<OrchestrationV2Subagent>();
          const watcher = yield* orchestrator
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
                  readonly event: Extract<
                    OrchestrationV2DomainEvent,
                    { readonly type: "subagent.updated" }
                  >;
                } =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.workflow !== undefined &&
                  stored.event.payload.progress !== undefined,
              ),
              Stream.runForEach(({ event }) => Queue.offer(progressQueue, event.payload)),
              Effect.forkScoped,
            );

          const script = `
            export const meta = { name: "OneStopPath", description: "Keeps late child on stop." };
            log("boot");
            await agent("Step 1", { label: "First" });
            log("running step 2");
            await agent("Step 2", { label: "Second" });
            return "done";
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          const started = yield* decodeRunResult(runCall.structuredContent);

          // End parent turn so an undisposed completion would wake it.
          yield* parentTurn.complete("Workflow launched.");
          yield* awaitRunStatus(parentThreadId, parentTurn.turn.runId, "completed", beforeSeq);

          const firstTick = yield* Queue.take(progressQueue);
          expect(firstTick.progress).toBe("boot");

          const firstTurn = yield* Queue.take(harness.turnQueue);
          yield* firstTurn.complete("done-1");

          const secondTurn = yield* Queue.take(harness.turnQueue);
          expect(secondTurn.turn.message.text).toContain("Step 2");

          // Confirm the coalesced row has not yet published the second child or newest log.
          const beforeStopTask = (yield* orchestrator.getThreadProjection(
            parentThreadId,
          )).subagents.find((s) => s.id === started.taskId);
          expect(beforeStopTask?.workflow?.agents.some((a) => a.label === "Second")).toBe(false);
          expect(beforeStopTask?.progress).toBe("boot");

          if (mode === "client-stop-request") {
            yield* orchestrator.dispatch({
              type: "workflow.stop-request",
              commandId: CommandId.make("command:one-stop:client-stop-request"),
              parentThreadId,
              taskId: started.taskId,
            });
            yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          } else {
            const externalScope: McpInvocationContext.McpInvocationScope = {
              environmentId: parentScope.environmentId,
              requestNamespace: "client-session:one-stop-mcp",
              thread: undefined,
              client: {
                sessionId: "client-session:one-stop-mcp",
                label: "Stop Client",
                access: "full-access",
              },
              capabilities: new Set(["orchestration"]),
              issuedAt: 1,
            };
            const stopCall = yield* callTool(
              "t3_workflow_stop",
              { runId: started.runId },
              externalScope,
            );
            expect(stopCall.isError).not.toBe(true);
            expect(stopCall.structuredContent).toEqual({
              runId: started.runId,
              status: "cancelled",
            });
          }

          yield* Deferred.await(secondTurn.interrupted);
          expect(separateDisposeDispatches).toBe(0);
          expect(completeCommands).toEqual([{ status: "cancelled", disposeDelivery: true }]);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const terminalTask = parentProjection.subagents.find((s) => s.id === started.taskId);
          expect(terminalTask?.status).toBe("cancelled");
          expect(terminalTask?.progress).toBe("running step 2");
          expect(terminalTask?.workflow?.agents).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ label: "First", state: "completed" }),
              expect.objectContaining({
                label: "Second",
                state: "failed",
                childThreadId: secondTurn.turn.threadId,
              }),
            ]),
          );
          expect(terminalTask?.completionDelivery?.state).toBe("disposed");
          expect(parentProjection.runs).toHaveLength(1);
          expect(
            (yield* Ref.get(harness.startedTurns)).filter(
              (t) => t.turn.threadId === parentThreadId,
            ),
          ).toHaveLength(1);
          yield* Fiber.interrupt(watcher);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "aborts schema reprompt cleanly with no new run when Stop arrives while fetching sequence",
    () =>
      Effect.gen(function* () {
        const fetchingChildSeq = yield* Deferred.make<ThreadId>();
        const releaseChildSeq = yield* Deferred.make<void>();
        let interceptedChildSeq = false;
        const harness = yield* makeWorkflowHarness({
          name: "stop-during-reprompt-seq",
          wrapThreadManagement: (tm) => ({
            ...tm,
            getThreadEventSequence: (threadId) =>
              threadId !== parentThreadId && !interceptedChildSeq
                ? Effect.gen(function* () {
                    interceptedChildSeq = true;
                    yield* Deferred.succeed(fetchingChildSeq, threadId);
                    yield* Deferred.await(releaseChildSeq);
                    return yield* tm.getThreadEventSequence(threadId);
                  })
                : tm.getThreadEventSequence(threadId),
          }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "stop-during-reprompt-seq",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = { name: "RepromptRace", description: "Stop during sequence fetch." };
            return await agent("Return schema", {
              schema: {
                type: "object",
                properties: { ok: { type: "boolean" } },
                required: ["ok"],
                additionalProperties: false,
              },
            });
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          const started = yield* decodeRunResult(runCall.structuredContent);

          const attempt1 = yield* Queue.take(harness.turnQueue);
          yield* attempt1.complete("invalid json");

          const childThreadId = yield* Deferred.await(fetchingChildSeq);
          yield* orchestrator.dispatch({
            type: "workflow.stop-request",
            commandId: CommandId.make("command:reprompt-race:stop"),
            parentThreadId,
            taskId: started.taskId,
          });
          yield* Deferred.succeed(releaseChildSeq, undefined);

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("cancelled");

          const childProjection = yield* orchestrator.getThreadProjection(childThreadId);
          expect(childProjection.runs).toHaveLength(1);
          expect(
            (yield* Ref.get(harness.startedTurns)).filter((t) => t.turn.threadId === childThreadId),
          ).toHaveLength(1);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "retries failed workflow.complete while keeping run.json and live status running, and reconciles both directions at startup",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        let completeAttempts = 0;
        const completeFailedOnce = yield* Deferred.make<void>();
        const terminalMetaWritten = yield* Deferred.make<void>();
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-medium3-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        // Part 1: live workflow.complete fails once -> run.json and live status remain running until retry commits.
        const harness1 = yield* makeWorkflowHarness({
          name: "medium3-live",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              fsService
                .rename(oldPath, newPath)
                .pipe(
                  Effect.tap(() =>
                    completeAttempts >= 2 && path.basename(newPath) === "run.json"
                      ? Deferred.succeed(terminalMetaWritten, undefined)
                      : Effect.void,
                  ),
                ),
          }),
          wrapThreadManagement: (tm) => ({
            ...tm,
            dispatch: (command) =>
              command.type === "workflow.complete"
                ? Effect.gen(function* () {
                    completeAttempts += 1;
                    if (completeAttempts === 1) {
                      yield* Deferred.succeed(completeFailedOnce, undefined);
                      return yield* new Orchestrator.OrchestratorDispatchError({
                        commandId: command.commandId,
                        commandType: command.type,
                        cause: "transient complete failure",
                      });
                    }
                    return yield* tm.dispatch(command);
                  })
                : tm.dispatch(command),
          }),
        });

        const seededForReconcile = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "medium3",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const runCall = yield* callTool("t3_workflow_run", {
            script: `export const meta = { name: "CompleteRetry", description: "Retries terminal SQL write." }; return { ok: true };`,
          });
          const started = yield* decodeRunResult(runCall.structuredContent);

          yield* Deferred.await(completeFailedOnce);
          const metaPath = path.join(config.workflowRunsDir, started.runId, "run.json");
          const midMeta = JSON.parse(yield* fs.readFileString(metaPath)) as { status: string };
          expect(midMeta.status).toBe("running");
          const midStatusCall = yield* callTool("t3_workflow_status", { runId: started.runId });
          expect((yield* decodeStatusResult(midStatusCall.structuredContent)).status).toBe(
            "running",
          );

          yield* TestClock.adjust("50 millis");
          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("completed");
          // Terminal run.json writes asynchronously after SQL commit; await its atomic rename.
          yield* Deferred.await(terminalMetaWritten);
          const finalStatus = yield* decodeStatusResult(
            (yield* callTool("t3_workflow_status", { runId: started.runId })).structuredContent,
          );
          expect(finalStatus.status).toBe("completed");
          const finalMeta = JSON.parse(yield* fs.readFileString(metaPath)) as { status: string };
          expect(finalMeta.status).toBe("completed");

          // Part 2: seed both startup reconcile directions:
          // (A) run.json terminal while SQL is still running with an active child.
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;
          const termMetaRunId = "wf_termmeta0001";
          const termMetaDir = path.join(config.workflowRunsDir, termMetaRunId);
          const termMetaScript = path.join(termMetaDir, "script.js");
          const termMetaJournal = path.join(termMetaDir, "journal.jsonl");
          yield* fs.makeDirectory(termMetaDir, { recursive: true });
          yield* fs.writeFileString(
            termMetaScript,
            `export const meta = { name: "TermMeta", description: "run.json terminal, SQL running." }; return 7;`,
          );
          yield* fs.writeFileString(
            termMetaJournal,
            `${JSON.stringify({ type: "end", status: "completed", result: 7 })}\n`,
          );
          const termMetaStarted = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:medium3:term-meta-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "TermMeta",
            description: "run.json terminal, SQL running.",
            modelSelection,
            workflow: {
              name: "TermMeta",
              runHandles: {
                runId: termMetaRunId,
                scriptPath: termMetaScript,
                transcriptDir: termMetaDir,
              },
              phases: [],
              agents: [],
            },
          });
          const termMetaTaskEvent = termMetaStarted.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (termMetaTaskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing termMeta task");
          }
          const termMetaTaskId = termMetaTaskEvent.event.payload.id;
          const termMetaChild = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: termMetaTaskId,
            requestKey: `${termMetaRunId}:0`,
            task: { task: "Lingering child", title: "Lingering" },
          });
          yield* Queue.take(harness1.turnQueue);
          yield* fs.writeFileString(
            path.join(termMetaDir, "run.json"),
            JSON.stringify({
              runId: termMetaRunId,
              threadId: parentThreadId,
              taskId: termMetaTaskId,
              name: "TermMeta",
              scriptPath: termMetaScript,
              journalPath: termMetaJournal,
              startedAt: 0,
              status: "completed",
              result: 7,
              usage: {
                agents: 1,
                done: 1,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 1,
              },
            }),
          );

          // (B) SQL terminal while run.json is still running.
          const termSqlRunId = "wf_termsql00001";
          const termSqlDir = path.join(config.workflowRunsDir, termSqlRunId);
          const termSqlScript = path.join(termSqlDir, "script.js");
          const termSqlJournal = path.join(termSqlDir, "journal.jsonl");
          yield* fs.makeDirectory(termSqlDir, { recursive: true });
          yield* fs.writeFileString(
            termSqlScript,
            `export const meta = { name: "TermSql", description: "SQL terminal, run.json running." }; return 9;`,
          );
          yield* fs.writeFileString(
            termSqlJournal,
            `${JSON.stringify({ type: "end", status: "completed", result: 9 })}\n`,
          );
          const termSqlStarted = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:medium3:term-sql-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "TermSql",
            description: "SQL terminal, run.json running.",
            modelSelection,
            workflow: {
              name: "TermSql",
              runHandles: {
                runId: termSqlRunId,
                scriptPath: termSqlScript,
                transcriptDir: termSqlDir,
              },
              phases: [],
              agents: [],
            },
          });
          const termSqlTaskEvent = termSqlStarted.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (termSqlTaskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing termSql task");
          }
          const termSqlTaskId = termSqlTaskEvent.event.payload.id;
          yield* orchestrator.dispatch({
            type: "workflow.complete",
            commandId: CommandId.make("command:medium3:term-sql-complete"),
            parentThreadId,
            taskId: termSqlTaskId,
            status: "completed",
            result: "Workflow TermSql completed.\nResult: 9",
          });
          yield* fs.writeFileString(
            path.join(termSqlDir, "run.json"),
            JSON.stringify({
              runId: termSqlRunId,
              threadId: parentThreadId,
              taskId: termSqlTaskId,
              name: "TermSql",
              scriptPath: termSqlScript,
              journalPath: termSqlJournal,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 0,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );

          return {
            termMetaTaskId,
            termMetaChildThreadId: termMetaChild.childThreadId,
            termSqlRunId,
            termSqlMetaPath: path.join(termSqlDir, "run.json"),
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        const harness2 = yield* makeWorkflowHarness({
          name: "medium3-reconcile",
          databaseLayer,
          baseDir: sharedBaseDir,
          runEffectWorker: false,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* WorkflowService.recoverInterruptedWorkflows;

          // Direction A: run.json terminal + SQL running -> child stopped and SQL completed.
          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const termMetaTask = parentProjection.subagents.find(
            (s) => s.id === seededForReconcile.termMetaTaskId,
          );
          expect(termMetaTask?.status).toBe("completed");
          const childProjection = yield* orchestrator.getThreadProjection(
            seededForReconcile.termMetaChildThreadId,
          );
          expect(
            childProjection.turnItems.some((item) => item.type === "run_interrupt_request"),
          ).toBe(true);

          // Direction B: SQL terminal + run.json running -> run.json updated to completed with result 9.
          const termSqlMeta = JSON.parse(
            yield* fs.readFileString(seededForReconcile.termSqlMetaPath),
          ) as { status: string; result?: unknown };
          expect(termSqlMeta.status).toBe("completed");
          expect(termSqlMeta.result).toBe(9);
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "resume cache key includes inherited model selection and treats stopped agents as unfinished",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "resume-model-and-cancel" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "resume-model-and-cancel");

          const script = `
            export const meta = { name: "ModelResume", description: "Checks inherited model key and stop." };
            const first = await agent("First inherited call");
            const second = await agent("Second inherited call");
            return { first, second };
          `;

          // Run 1 on default model: agent 0 finishes, agent 1 is stopped mid-flight.
          const run1 = yield* decodeRunResult(
            (yield* callTool("t3_workflow_run", { script })).structuredContent,
          );
          const r1First = yield* Queue.take(harness.turnQueue);
          yield* r1First.complete("r1-first");
          const r1Second = yield* Queue.take(harness.turnQueue);
          const stop1 = yield* callTool("t3_workflow_stop", { runId: run1.runId });
          expect(stop1.structuredContent).toEqual({ runId: run1.runId, status: "cancelled" });
          yield* Deferred.await(r1Second.interrupted);

          // Run 2 on the SAME model and UNEDITED script: agent 0 hits cache, stopped agent 1 reruns live.
          const turnsBeforeRun2 = (yield* Ref.get(harness.startedTurns)).length;
          const beforeRun2Seq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const run2 = yield* decodeRunResult(
            (yield* callTool("t3_workflow_run", { script, resumeFromRunId: run1.runId }))
              .structuredContent,
          );
          const r2Second = yield* Queue.take(harness.turnQueue);
          expect(r2Second.turn.message.text).toContain("Second inherited call");
          yield* r2Second.complete("r2-second");
          const terminal2 = yield* awaitWorkflowTerminal(run2.taskId, beforeRun2Seq);
          expect(terminal2.status).toBe("completed");
          expect(terminal2.result).toContain('"first":"r1-first"');
          expect(terminal2.result).toContain('"second":"r2-second"');
          expect((yield* Ref.get(harness.startedTurns)).length - turnsBeforeRun2).toBe(1);

          // Switch the calling thread's model selection, then resume the SAME script: cache must miss!
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("command:resume-model-and-cancel:switch-model"),
            threadId: parentThreadId,
            modelSelection: { instanceId, model: altModel },
          });
          const turnsBeforeRun3 = (yield* Ref.get(harness.startedTurns)).length;
          const beforeRun3Seq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const run3 = yield* decodeRunResult(
            (yield* callTool("t3_workflow_run", { script, resumeFromRunId: run2.runId }))
              .structuredContent,
          );
          const r3First = yield* Queue.take(harness.turnQueue);
          expect(r3First.turn.modelSelection.model).toBe(altModel);
          yield* r3First.complete("r3-first");
          const r3Second = yield* Queue.take(harness.turnQueue);
          expect(r3Second.turn.modelSelection.model).toBe(altModel);
          yield* r3Second.complete("r3-second");

          const terminal3 = yield* awaitWorkflowTerminal(run3.taskId, beforeRun3Seq);
          expect(terminal3.status).toBe("completed");
          expect((yield* Ref.get(harness.startedTurns)).length - turnsBeforeRun3).toBe(2);
          const status3 = yield* decodeStatusResult(
            (yield* callTool("t3_workflow_status", { runId: run3.runId })).structuredContent,
          );
          expect(status3.usage).toMatchObject({ agents: 2, done: 2, cached: 0 });
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "bounds completion text and wake for a very long workflow name and points task_status to t3_workflow_status",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({
          name: "very-long-name",
          runContinuationWorker: true,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentTurn = yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "very-long-name",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const hugeName = "VeryLongWorkflowName_".repeat(1_000);
          const script = `
            export const meta = {
              name: ${JSON.stringify(hugeName)},
              description: "Workflow with a 21 KB name.",
            };
            return { payload: "y".repeat(20 * 1024) };
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          yield* parentTurn.complete("Started huge-name workflow.");
          yield* awaitRunStatus(parentThreadId, parentTurn.turn.runId, "completed", beforeSeq);

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("completed");
          expect(terminal.result).not.toBeNull();
          expect(Buffer.byteLength(terminal.result!, "utf8")).toBeLessThanOrEqual(12_000);
          expect(terminal.result).toContain("t3_workflow_status");
          expect(terminal.result).toContain(started.runId);
          expect(terminal.result).not.toContain("task_status");

          const wakeTurn = yield* Queue.take(harness.turnQueue);
          expect(wakeTurn.turn.threadId).toBe(parentThreadId);
          expect(wakeTurn.turn.message.text).toContain("t3_workflow_status");
          expect(wakeTurn.turn.message.text).toContain(started.runId);
          expect(wakeTurn.turn.message.text).not.toContain("task_status");
          yield* wakeTurn.complete("Done.");

          // Calling task_status on a workflow coordinator tells the parent to call t3_workflow_status with runId.
          const taskStatusCall = yield* callTool("task_status", { taskId: started.taskId });
          const taskStatusFailure = declaredFailure(taskStatusCall);
          expect(taskStatusFailure.code).toBe("invalid_request");
          expect(taskStatusFailure.message).toContain("t3_workflow_status");
          expect(taskStatusFailure.message).toContain(started.runId);
          expect(taskStatusFailure.message).not.toContain("task_status");
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "stops pending provider background work on a completed child before completing the workflow",
    () =>
      Effect.gen(function* () {
        let childThreadId: ThreadId | undefined;
        let childInterruptedDeferred: Deferred.Deferred<void> | undefined;
        let childInterruptedBeforeComplete = false;
        let childPendingBgBeforeComplete: number | undefined;
        const harness = yield* makeWorkflowHarness({
          name: "completed-child-bg-work",
          wrapThreadManagement: (tm) => ({
            ...tm,
            dispatch: (command) =>
              command.type === "workflow.complete"
                ? Effect.gen(function* () {
                    if (childInterruptedDeferred !== undefined) {
                      childInterruptedBeforeComplete =
                        yield* Deferred.isDone(childInterruptedDeferred);
                    }
                    if (childThreadId !== undefined) {
                      const shell = yield* tm.getThreadShell(childThreadId);
                      childPendingBgBeforeComplete = shell?.pendingBackgroundTasks?.length ?? 0;
                    }
                    return yield* tm.dispatch(command);
                  })
                : tm.dispatch(command),
          }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "completed-child-bg-work");
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "BackgroundChild",
              description: "Child finishes its turn with pending background command.",
            };
            const out = await agent("Start a background process");
            return { out };
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          const childTurn = yield* Queue.take(harness.turnQueue);
          childThreadId = childTurn.turn.threadId;
          childInterruptedDeferred = childTurn.interrupted;
          yield* childTurn.setPendingBackgroundTasks([
            { taskId: "bg-shell-1", kind: "command", description: "sleep 60" },
          ]);
          yield* childTurn.complete("child-done");

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("completed");
          expect(terminal.result).toContain('"out":"child-done"');

          // The child's pending background work must have been interrupted and settled before workflow.complete.
          expect(childInterruptedBeforeComplete).toBe(true);
          expect(childPendingBgBeforeComplete).toBe(0);

          const childShell = yield* orchestrator.getThreadShell(childTurn.turn.threadId);
          expect(childShell?.pendingBackgroundTasks ?? []).toHaveLength(0);
          const childProjection = yield* orchestrator.getThreadProjection(childTurn.turn.threadId);
          expect(
            childProjection.providerThreads.every(
              (pt) => (pt.pendingBackgroundTasks?.length ?? 0) === 0,
            ),
          ).toBe(true);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "deferred startup sweep marks workflow stop-requested and cleanupPending, refuses resume until background retry confirms child stop, then completes and allows resume",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-cleanup-pending-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "cleanup-pending-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "cleanup-pending",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = "wf_cleanpend001";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          yield* fs.makeDirectory(dir, { recursive: true });
          const scriptText = `
            export const meta = { name: "CleanupPending", description: "Fails initial sweep stops." };
            const r = await agent("Work to resume");
            return { r };
          `;
          yield* fs.writeFileString(scriptPath, scriptText);
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:cleanup-pending:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "CleanupPending",
            description: "Fails initial sweep stops.",
            modelSelection,
            workflow: {
              name: "CleanupPending",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing workflow task");
          }
          const taskId = taskEvent.event.payload.id;
          const child = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:0`,
            task: { task: "Work to resume", title: "Worker" },
          });
          yield* Queue.take(harness1.turnQueue);

          const metaPath = path.join(dir, "run.json");
          yield* fs.writeFileString(
            metaPath,
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "CleanupPending",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 1,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );

          return {
            runId,
            taskId,
            childThreadId: child.childThreadId,
            scriptPath,
            metaPath,
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        let failChildStops = true;
        const stopAttemptFailed = yield* Queue.unbounded<void>();
        const finalMetaWritten = yield* Deferred.make<void>();
        const deferOrder: Array<string> = [];
        const harness2 = yield* makeWorkflowHarness({
          name: "cleanup-pending-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              fsService.rename(oldPath, newPath).pipe(
                Effect.tap(() => {
                  if (newPath !== seeded.metaPath) return Effect.void;
                  if (failChildStops) {
                    deferOrder.push("run.json");
                    return Effect.void;
                  }
                  return Deferred.succeed(finalMetaWritten, undefined);
                }),
              ),
          }),
          wrapThreadManagement: (tm) => ({
            ...tm,
            dispatch: (command) => {
              if (command.type === "workflow.stop-request" && failChildStops) {
                deferOrder.push("workflow.stop-request");
              }
              return tm.dispatch(command);
            },
            cancelDelegatedTask: (input) =>
              failChildStops
                ? Queue.offer(stopAttemptFailed, undefined).pipe(
                    Effect.andThen(
                      Effect.fail(
                        new Orchestrator.OrchestratorDispatchError({
                          commandId: input.command.commandId,
                          commandType: input.command.type,
                          cause: "child stop temporarily unavailable",
                        }),
                      ),
                    ),
                  )
                : tm.cancelDelegatedTask(input),
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const beforeRecoverSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          // Startup runs one bounded-concurrency child-stop pass before persisting cleanupPending + stop-request.
          const sweepFiber = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
            Effect.forkScoped,
          );
          yield* Queue.take(stopAttemptFailed);
          yield* Fiber.join(sweepFiber);

          // cleanupPending is persisted to run.json before workflow.stop-request is dispatched.
          expect(deferOrder).toEqual(["run.json", "workflow.stop-request"]);

          // Workflow stays running in SQL, is stop-requested, and run.json is marked cleanupPending.
          const midTask = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
            (s) => s.id === seeded.taskId,
          );
          expect(midTask?.status).toBe("running");
          expect(midTask?.workflow?.stopRequestedAt).toBeDefined();

          const midMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            status: string;
            cleanupPending?: boolean;
          };
          expect(midMeta.status).toBe("running");
          expect(midMeta.cleanupPending).toBe(true);

          // Resume must be refused while cleanupPending.
          const refusedResume = yield* callTool("t3_workflow_run", {
            scriptPath: seeded.scriptPath,
            resumeFromRunId: seeded.runId,
          });
          const refusedFailure = declaredFailure(refusedResume);
          expect(refusedFailure.code).toBe("invalid_request");
          expect(refusedFailure.message).toMatch(/cleanup pending/i);

          // Lift the stop failure and advance TestClock so the background retry fiber completes cleanup.
          failChildStops = false;
          yield* TestClock.adjust("50 millis");

          const terminalTask = yield* awaitWorkflowTerminal(seeded.taskId, beforeRecoverSeq);
          expect(terminalTask.status).toBe("failed");
          expect(terminalTask.result).toContain(`resumeFromRunId=${seeded.runId}`);

          yield* Deferred.await(finalMetaWritten);
          const finalMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            status: string;
            cleanupPending?: boolean;
          };
          expect(finalMeta.status).toBe("failed");
          expect(finalMeta.cleanupPending).toBeUndefined();

          const childProjection = yield* orchestrator.getThreadProjection(seeded.childThreadId);
          expect(
            childProjection.runs.every(
              (run) =>
                run.status === "interrupted" ||
                run.status === "cancelled" ||
                run.status === "failed",
            ),
          ).toBe(true);

          // Resume now succeeds once cleanup is confirmed, while resuming a live running workflow is refused.
          const beforeResumeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const resumedCall = yield* callTool("t3_workflow_run", {
            scriptPath: seeded.scriptPath,
            resumeFromRunId: seeded.runId,
          });
          expect(resumedCall.isError).not.toBe(true);
          const resumed = yield* decodeRunResult(resumedCall.structuredContent);

          const resumedTurn = yield* Queue.take(harness2.turnQueue);
          const refusedLiveResume = yield* callTool("t3_workflow_run", {
            scriptPath: seeded.scriptPath,
            resumeFromRunId: resumed.runId,
          });
          expect(declaredFailure(refusedLiveResume).code).toBe("invalid_request");
          expect(declaredFailure(refusedLiveResume).message).toMatch(/still running/i);

          yield* resumedTurn.complete("resumed-ok");
          const resumedTerminal = yield* awaitWorkflowTerminal(resumed.taskId, beforeResumeSeq);
          expect(resumedTerminal.status).toBe("completed");
          expect(resumedTerminal.result).toContain('"r":"resumed-ok"');
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "caps distinct phases at 256 and fails the run when exceeded (including via agent opts inside try/catch)",
    () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        let runMetaWrites = 0;
        const terminalMetaWritten = yield* Deferred.make<void>();
        const harness = yield* makeWorkflowHarness({
          name: "phase-cap",
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              fsService.rename(oldPath, newPath).pipe(
                Effect.tap(() => {
                  if (path.basename(newPath) === "run.json") {
                    runMetaWrites += 1;
                    if (runMetaWrites >= 2) {
                      return Deferred.succeed(terminalMetaWritten, undefined);
                    }
                  }
                  return Effect.void;
                }),
              ),
          }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "phase-cap");
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          // Declaring > 256 distinct phases in meta.phases is rejected at launch.
          const declaredPhases = Array.from({ length: 257 }, (_, i) => ({
            title: `declared-${i}`,
          }));
          const tooManyDeclared = yield* callTool("t3_workflow_run", {
            script: `export const meta = { name: "TooManyDeclared", description: "Over phase cap.", phases: ${JSON.stringify(declaredPhases)} }; return 1;`,
          });
          expect(declaredFailure(tooManyDeclared).code).toBe("invalid_request");
          expect(declaredFailure(tooManyDeclared).message).toContain("256");

          // Recording 256 phases then passing a 257th phase on agent() inside try/catch fails the run.
          const script = `
            export const meta = { name: "RuntimePhaseCap", description: "Exceeds 256 distinct phases at runtime." };
            for (let i = 0; i < 256; i += 1) {
              phase("p-" + i);
            }
            try {
              await agent("Past cap", { phase: "p-256" });
            } catch {
              // Ignored by script; host cap must still fail the run.
            }
            return "should-not-succeed";
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("failed");
          expect(terminal.result).toContain("256");
          expect(terminal.workflow?.phases.length ?? 0).toBeLessThanOrEqual(256);

          yield* Deferred.await(terminalMetaWritten);
          const status = yield* decodeStatusResult(
            (yield* callTool("t3_workflow_status", { runId: started.runId })).structuredContent,
          );
          expect(status.status).toBe("failed");
          expect(status.phases.length).toBeLessThanOrEqual(256);
        }).pipe(Effect.provide(harness.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "fails the workflow and stops accepting worker entries when a log burst overflows the 1,024-entry journal backlog",
    () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const journalWriteBlocked = yield* Deferred.make<void>();
        const releaseJournalWrites = yield* Deferred.make<void>();
        const terminalMetaWritten = yield* Deferred.make<void>();
        let blockedFirstJournalAppend = false;
        let runMetaWrites = 0;

        const harness = yield* makeWorkflowHarness({
          name: "journal-backlog-cap",
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              fsService.rename(oldPath, newPath).pipe(
                Effect.tap(() => {
                  if (path.basename(newPath) === "run.json") {
                    runMetaWrites += 1;
                    if (runMetaWrites >= 2) {
                      return Deferred.succeed(terminalMetaWritten, undefined);
                    }
                  }
                  return Effect.void;
                }),
              ),
            writeFileString: (filePath, data, options) =>
              options?.flag === "a" &&
              filePath.endsWith("journal.jsonl") &&
              !blockedFirstJournalAppend
                ? Effect.gen(function* () {
                    blockedFirstJournalAppend = true;
                    yield* Deferred.succeed(journalWriteBlocked, undefined);
                    yield* Deferred.await(releaseJournalWrites);
                    return yield* fsService.writeFileString(filePath, data, options);
                  })
                : fsService.writeFileString(filePath, data, options),
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const fs = yield* FileSystem.FileSystem;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "journal-backlog-cap");
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = { name: "LogBurst", description: "Bursts logs past journal backlog limit." };
            for (let i = 0; i < 2000; i += 1) {
              log("burst-" + i);
            }
            return "should-not-succeed";
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          // Wait until the first journal append is blocked AND the 1,024-entry backlog has filled and failed the worker.
          yield* Deferred.await(journalWriteBlocked);
          const overflowFlushed = yield* orchestrator
            .streamStoredEventsFrom({
              threadId: parentThreadId,
              afterSequence: beforeSeq,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                ({ event }) =>
                  event.type === "subagent.updated" &&
                  event.payload.id === started.taskId &&
                  (event.payload.progress === "burst-1023" ||
                    event.payload.progress === "burst-1024"),
              ),
              Stream.runHead,
            );
          expect(Option.isSome(overflowFlushed)).toBe(true);
          yield* Deferred.succeed(releaseJournalWrites, undefined);

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("failed");
          expect(terminal.result).toContain("1024-entry or 16 MiB limit");

          const journalEntries = decodeJournal(yield* fs.readFileString(started.journalPath));
          const logEntries = journalEntries.filter((entry) => entry.type === "log");
          expect(logEntries.length).toBeGreaterThan(0);
          expect(logEntries.length).toBeLessThanOrEqual(1_025);
          for (let i = 0; i < logEntries.length; i += 1) {
            expect(logEntries[i]).toEqual({ type: "log", message: `burst-${i}` });
          }
          expect(journalEntries.at(-1)).toMatchObject({ type: "end", status: "failed" });

          yield* Deferred.await(terminalMetaWritten);
          const status = yield* decodeStatusResult(
            (yield* callTool("t3_workflow_status", { runId: started.runId })).structuredContent,
          );
          expect(status.status).toBe("failed");
          expect(status.logs).toHaveLength(50);
        }).pipe(Effect.provide(harness.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each(["run.json", "stop-request"] as const)(
    "fails the startup sweep phase when the %s cleanup gate write fails",
    (failing) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-gate-fail-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "cleanup-gate-fail-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "cleanup-gate-fail",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = "wf_gatefail0001";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "GateFail", description: "Fails run.json gate write." }; return 1;`,
          );
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:gate-fail:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "GateFail",
            description: "Fails run.json gate write.",
            modelSelection,
            workflow: {
              name: "GateFail",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing workflow task");
          }
          const taskId = taskEvent.event.payload.id;
          yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:0`,
            task: { task: "Stuck child", title: "Worker" },
          });
          yield* Queue.take(harness1.turnQueue);

          const metaPath = path.join(dir, "run.json");
          yield* fs.writeFileString(
            metaPath,
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "GateFail",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 1,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
          return { runId, taskId, metaPath };
        }).pipe(Effect.provide(harness1.layerToolkit));

        const stopAttemptFailed = yield* Queue.unbounded<void>();
        let stopRequestDispatched = false;
        const harness2 = yield* makeWorkflowHarness({
          name: "cleanup-gate-fail-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              failing === "run.json" && newPath === seeded.metaPath
                ? Effect.fail(
                    PlatformError.systemError({
                      _tag: "PermissionDenied",
                      module: "FileSystem",
                      method: "rename",
                      description: "read-only workflows dir",
                      pathOrDescriptor: newPath,
                    }),
                  )
                : fsService.rename(oldPath, newPath),
          }),
          wrapThreadManagement: (tm) => ({
            ...tm,
            dispatch: (command) => {
              if (command.type === "workflow.stop-request") {
                stopRequestDispatched = true;
                if (failing === "stop-request") {
                  return Queue.offer(stopAttemptFailed, undefined).pipe(
                    Effect.andThen(
                      Effect.fail(
                        new Orchestrator.OrchestratorDispatchError({
                          commandId: command.commandId,
                          commandType: command.type,
                          cause: "database unavailable",
                        }),
                      ),
                    ),
                  );
                }
              }
              return tm.dispatch(command);
            },
            cancelDelegatedTask: (input) =>
              Queue.offer(stopAttemptFailed, undefined).pipe(
                Effect.andThen(
                  Effect.fail(
                    new Orchestrator.OrchestratorDispatchError({
                      commandId: input.command.commandId,
                      commandType: input.command.type,
                      cause: "child stop temporarily unavailable",
                    }),
                  ),
                ),
              ),
          }),
        });

        yield* Effect.gen(function* () {
          const sweepFiber = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
            Effect.exit,
            Effect.forkScoped,
          );
          // Startup runs one child-stop pass, then retries the mandatory workflow.stop-request gate write.
          yield* Queue.take(stopAttemptFailed);
          if (failing === "stop-request") {
            for (const delay of [50, 100, 200, 400, 800]) {
              yield* Queue.take(stopAttemptFailed);
              yield* TestClock.adjust(`${delay} millis`);
            }
            yield* Queue.take(stopAttemptFailed);
          }
          const exit = yield* Fiber.join(sweepFiber);
          expect(Exit.isFailure(exit)).toBe(true);
          expect(stopRequestDispatched).toBe(failing === "stop-request");
          if (failing === "stop-request") {
            const record = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
              cleanupPending?: boolean;
            };
            expect(record.cleanupPending).toBe(true);
          }
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "stop on a terminal or taskless row with cleanupPending keeps the gate until child stops are confirmed, then allows resume",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-stop-terminal-gate-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "stop-terminal-gate-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "stop-terminal-gate",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          // 1. Terminal SQL workflow row with cleanupPending and a live child thread.
          const runId = "wf_termgate0001";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "TermGate", description: "Terminal with pending child." };
const out = await agent("Resumed work");
return { out };`,
          );

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:term-gate:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "TermGate",
            description: "Terminal with pending child.",
            modelSelection,
            workflow: {
              name: "TermGate",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing workflow task");
          }
          const taskId = taskEvent.event.payload.id;
          const child = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:0`,
            task: { task: "Still running child", title: "Worker" },
          });
          yield* Queue.take(harness1.turnQueue);

          yield* orchestrator.dispatch({
            type: "workflow.complete",
            commandId: CommandId.make("command:term-gate:workflow-complete"),
            parentThreadId,
            taskId,
            status: "failed",
            result: "Failed earlier.",
          });

          yield* fs.writeFileString(
            journalPath,
            `${JSON.stringify({
              type: "started",
              index: 0,
              key: "term-gate-key",
              label: "Worker",
              phase: null,
              agentId: child.taskId,
              childThreadId: child.childThreadId,
            })}\n`,
          );
          const metaPath = path.join(dir, "run.json");
          yield* fs.writeFileString(
            metaPath,
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "TermGate",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "failed",
              cleanupPending: true,
              error: "Failed earlier.",
              usage: {
                agents: 1,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );

          // 2. Taskless (taskId: null) run.json with cleanupPending and a journal-only child thread.
          const orphanRunId = "wf_taskless0001";
          const orphanDir = path.join(config.workflowRunsDir, orphanRunId);
          const orphanScriptPath = path.join(orphanDir, "script.js");
          const orphanJournalPath = path.join(orphanDir, "journal.jsonl");
          yield* fs.makeDirectory(orphanDir, { recursive: true });
          yield* fs.writeFileString(
            orphanScriptPath,
            `export const meta = { name: "TasklessGate", description: "Missing task row." }; return 1;`,
          );
          const orphanChild = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: parentRun.rootNodeId!,
            requestKey: `${orphanRunId}:0`,
            task: { task: "Journal-only child", title: "JournalWorker" },
          });
          yield* Queue.take(harness1.turnQueue);
          yield* fs.writeFileString(
            orphanJournalPath,
            `${JSON.stringify({
              type: "started",
              index: 0,
              key: "orphan-key",
              label: "JournalWorker",
              phase: null,
              agentId: null,
              childThreadId: orphanChild.childThreadId,
            })}\n`,
          );
          const orphanMetaPath = path.join(orphanDir, "run.json");
          yield* fs.writeFileString(
            orphanMetaPath,
            JSON.stringify({
              runId: orphanRunId,
              threadId: parentThreadId,
              taskId: null,
              name: "TasklessGate",
              scriptPath: orphanScriptPath,
              journalPath: orphanJournalPath,
              startedAt: 0,
              status: "cancelled",
              cleanupPending: true,
              usage: {
                agents: 1,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );

          return {
            runId,
            scriptPath,
            metaPath,
            childThreadId: child.childThreadId,
            orphanRunId,
            orphanMetaPath,
            orphanChildThreadId: orphanChild.childThreadId,
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        let failChildStops = true;
        const stopAttemptFailed = yield* Queue.unbounded<void>();
        const harness2 = yield* makeWorkflowHarness({
          name: "stop-terminal-gate-verify",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapThreadManagement: (tm) => ({
            ...tm,
            cancelDelegatedTask: (input) =>
              failChildStops
                ? Queue.offer(stopAttemptFailed, undefined).pipe(
                    Effect.andThen(
                      Effect.fail(
                        new Orchestrator.OrchestratorDispatchError({
                          commandId: input.command.commandId,
                          commandType: input.command.type,
                          cause: "child stop temporarily unavailable",
                        }),
                      ),
                    ),
                  )
                : tm.cancelDelegatedTask(input),
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;

          // Terminal row: unconfirmed child stop returns orchestration_error and preserves cleanupPending.
          const firstStopFiber = yield* callTool("t3_workflow_stop", { runId: seeded.runId }).pipe(
            Effect.forkScoped,
          );
          for (const delay of [50, 100, 200, 400, 800]) {
            yield* Queue.take(stopAttemptFailed);
            yield* TestClock.adjust(`${delay} millis`);
          }
          yield* Queue.take(stopAttemptFailed);
          const firstStop = yield* Fiber.join(firstStopFiber);
          expect(declaredFailure(firstStop).code).toBe("orchestration_error");

          const stillPendingMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            cleanupPending?: boolean;
          };
          expect(stillPendingMeta.cleanupPending).toBe(true);

          const refusedResume = yield* callTool("t3_workflow_run", {
            scriptPath: seeded.scriptPath,
            resumeFromRunId: seeded.runId,
          });
          expect(declaredFailure(refusedResume).code).toBe("invalid_request");
          expect(declaredFailure(refusedResume).message).toMatch(/cleanup pending/i);

          // Taskless row with journal child also fails closed while its journal child cannot be confirmed stopped.
          const orphanStopFiber = yield* callTool("t3_workflow_stop", {
            runId: seeded.orphanRunId,
          }).pipe(Effect.forkScoped);
          for (const delay of [50, 100, 200, 400, 800]) {
            yield* Queue.take(stopAttemptFailed);
            yield* TestClock.adjust(`${delay} millis`);
          }
          yield* Queue.take(stopAttemptFailed);
          const orphanFirstStop = yield* Fiber.join(orphanStopFiber);
          expect(declaredFailure(orphanFirstStop).code).toBe("orchestration_error");
          const orphanStillPending = JSON.parse(
            yield* fs.readFileString(seeded.orphanMetaPath),
          ) as { cleanupPending?: boolean };
          expect(orphanStillPending.cleanupPending).toBe(true);

          // Lift the failure: second stop confirms child stops, clears cleanupPending, and allows resume.
          failChildStops = false;
          const secondStop = yield* callTool("t3_workflow_stop", { runId: seeded.runId });
          expect(secondStop.isError).not.toBe(true);
          expect(secondStop.structuredContent).toEqual({
            runId: seeded.runId,
            status: "failed",
          });
          const clearedMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            status: string;
            cleanupPending?: boolean;
          };
          expect(clearedMeta.status).toBe("failed");
          expect(clearedMeta.cleanupPending).toBeUndefined();

          const orphanSecondStop = yield* callTool("t3_workflow_stop", {
            runId: seeded.orphanRunId,
          });
          expect(orphanSecondStop.isError).not.toBe(true);
          expect(orphanSecondStop.structuredContent).toEqual({
            runId: seeded.orphanRunId,
            status: "cancelled",
          });
          const orphanClearedMeta = JSON.parse(yield* fs.readFileString(seeded.orphanMetaPath)) as {
            status: string;
            cleanupPending?: boolean;
          };
          expect(orphanClearedMeta.status).toBe("cancelled");
          expect(orphanClearedMeta.cleanupPending).toBeUndefined();

          const beforeResumeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const resumedCall = yield* callTool("t3_workflow_run", {
            scriptPath: seeded.scriptPath,
            resumeFromRunId: seeded.runId,
          });
          expect(resumedCall.isError).not.toBe(true);
          const resumed = yield* decodeRunResult(resumedCall.structuredContent);
          const resumedTurn = yield* Queue.take(harness2.turnQueue);
          yield* resumedTurn.complete("after-gate-cleared");
          const resumedTerminal = yield* awaitWorkflowTerminal(resumed.taskId, beforeResumeSeq);
          expect(resumedTerminal.status).toBe("completed");
          expect(resumedTerminal.result).toContain('"out":"after-gate-cleared"');
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "rejects oversized scriptPath files in run and nested workflow() without reading them whole",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-oversized-script-" }),
        );
        const oversizedPath = path.join(tempDir, "oversized.js");
        const growingPath = path.join(tempDir, "growing.js");
        const fiveMiB = 5 * 1024 * 1024;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const sparse = yield* fs.open(oversizedPath, { flag: "w" });
            yield* sparse.truncate(fiveMiB);
            const growing = yield* fs.open(growingPath, { flag: "w" });
            yield* growing.truncate(fiveMiB);
          }),
        );

        let oversizedReadCalls = 0;
        let growingBytesRead = 0;
        const harness = yield* makeWorkflowHarness({
          name: "oversized-script-path",
          wrapScriptFileOpener: (openScriptFile) => (filePath) =>
            openScriptFile(filePath).pipe(
              Effect.map((file) => {
                if (filePath === oversizedPath) {
                  return {
                    ...file,
                    readChunk: (maxBytes) => {
                      oversizedReadCalls += 1;
                      return file.readChunk(maxBytes);
                    },
                  };
                }
                if (filePath === growingPath) {
                  return {
                    ...file,
                    stat: file.stat.pipe(Effect.map((info) => ({ ...info, size: 16 }))),
                    readChunk: (maxBytes) =>
                      file.readChunk(maxBytes).pipe(
                        Effect.tap((chunk) =>
                          Effect.sync(() => {
                            if (Option.isSome(chunk)) {
                              growingBytesRead += chunk.value.byteLength;
                            }
                          }),
                        ),
                      ),
                  };
                }
                return file;
              }),
            ),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(harness.turnQueue, harness.cwd, "oversized-script-path");
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          // 1. Top-level t3_workflow_run rejects oversized scriptPath via stat without reading any bytes.
          const topLevel = yield* callTool("t3_workflow_run", { scriptPath: oversizedPath });
          const topLevelErr = declaredFailure(topLevel);
          expect(topLevelErr.code).toBe("invalid_request");
          expect(topLevelErr.message).toBe(
            `Workflow script at ${oversizedPath} exceeds the 4 MiB limit.`,
          );
          expect(oversizedReadCalls).toBe(0);

          // 2. A file that grows past 4 MiB after stat is rejected after reading at most 4 MiB + 1 byte.
          const growing = yield* callTool("t3_workflow_run", { scriptPath: growingPath });
          const growingErr = declaredFailure(growing);
          expect(growingErr.code).toBe("invalid_request");
          expect(growingErr.message).toBe(
            `Workflow script at ${growingPath} exceeds the 4 MiB limit.`,
          );
          expect(growingBytesRead).toBe(4 * 1024 * 1024 + 1);

          // 3. Nested workflow({ scriptPath }) rejects with WorkflowScriptError and the same text without reading.
          const nestedCaller = `
            export const meta = {
              name: "NestedOversized",
              description: "Loads an oversized nested script.",
            };
            try {
              await workflow({ scriptPath: ${JSON.stringify(oversizedPath)} });
            } catch (err) {
              if (err && err.name === "WorkflowScriptError") {
                throw err;
              }
              throw new Error("wrong-error-kind:" + String(err && err.name));
            }
            return "unexpected";
          `;
          const nestedCall = yield* callTool("t3_workflow_run", { script: nestedCaller });
          expect(nestedCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(nestedCall.structuredContent);

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("failed");
          expect(terminal.result).toContain(
            `Workflow script at ${oversizedPath} exceeds the 4 MiB limit.`,
          );
          expect(oversizedReadCalls).toBe(0);
        }).pipe(Effect.provide(harness.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    "list-shells",
    "coordinator-records-missing-meta",
    "workflows-dir-read",
    "meta-task-records",
  ] as const)("startup sweep fails closed when %s discovery fails", (scenario) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const sharedBaseDir = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: `t3-workflow-discovery-fail-${scenario}-` }),
      );
      const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
      const databaseLayer = Layer.succeedContext(sharedDbContext);

      const harness1 = yield* makeWorkflowHarness({
        name: `discovery-fail-seed-${scenario}`,
        databaseLayer,
        baseDir: sharedBaseDir,
      });

      const seeded = yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
        const config = yield* ServerConfig.ServerConfig;
        const parentTurn = yield* startParentThreadAndRun(
          harness1.turnQueue,
          harness1.cwd,
          `discovery-fail-${scenario}`,
        );
        const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
          (r) => r.id === parentTurn.turn.runId,
        )!;
        const runId = `wf_disc_${scenario.replaceAll("-", "_")}`;
        const dir = path.join(config.workflowRunsDir, runId);
        yield* fs.makeDirectory(dir, { recursive: true });
        const scriptPath = path.join(dir, "script.js");
        const journalPath = path.join(dir, "journal.jsonl");
        yield* fs.writeFileString(
          scriptPath,
          `export const meta = { name: "DiscoveryFail", description: "Fails discovery." }; return 1;`,
        );
        yield* fs.writeFileString(journalPath, "");
        const started = yield* orchestrator.dispatch({
          type: "workflow.start",
          commandId: CommandId.make(`seed-discovery-start:${runId}`),
          parentThreadId,
          parentRunId: parentRun.id,
          parentNodeId: parentRun.rootNodeId!,
          title: "DiscoveryFail",
          description: "Fails discovery.",
          modelSelection,
          workflow: {
            name: "DiscoveryFail",
            runHandles: { runId, scriptPath, transcriptDir: dir },
            phases: [],
            agents: [],
          },
        });
        const taskEvent = started.storedEvents.find(
          (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
        );
        if (taskEvent?.event.type !== "subagent.updated") {
          throw new Error("missing workflow task");
        }
        const taskId = taskEvent.event.payload.id;
        yield* mcpService.delegateWorkflowAgent(parentScope, {
          parentRunId: parentRun.id,
          workflowNodeId: taskId,
          requestKey: `${runId}:0`,
          task: { task: "Unaccepted child", title: "Worker" },
        });
        yield* Queue.take(harness1.turnQueue);

        if (scenario !== "coordinator-records-missing-meta") {
          yield* fs.writeFileString(
            path.join(dir, "run.json"),
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "DiscoveryFail",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 1,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
        }
        return { runId, taskId, workflowsDir: config.workflowRunsDir };
      }).pipe(Effect.provide(harness1.layerToolkit));

      let coordinatorRecordReads = 0;
      const harness2 = yield* makeWorkflowHarness({
        name: `discovery-fail-recover-${scenario}`,
        databaseLayer,
        baseDir: sharedBaseDir,
        wrapFileSystem: (fsService) => ({
          ...fsService,
          readDirectory: (dirPath, opts) =>
            scenario === "workflows-dir-read" && dirPath === seeded.workflowsDir
              ? Effect.fail(
                  PlatformError.systemError({
                    _tag: "PermissionDenied",
                    module: "FileSystem",
                    method: "readDirectory",
                    description: "workflows dir unreadable",
                    pathOrDescriptor: dirPath,
                  }),
                )
              : fsService.readDirectory(dirPath, opts),
        }),
        wrapThreadManagement: (tm) => ({
          ...tm,
          listActiveShellIndex: () =>
            scenario === "list-shells"
              ? Effect.fail(
                  new Orchestrator.OrchestratorProjectionError({
                    threadId: parentThreadId,
                    cause: "transient shell index read failure",
                  }),
                )
              : scenario === "meta-task-records"
                ? Effect.succeed([])
                : tm.listActiveShellIndex(),
          getThreadRecords: ((threadId, fields, filter) => {
            if (
              threadId === parentThreadId &&
              (scenario === "coordinator-records-missing-meta" ||
                scenario === "meta-task-records") &&
              coordinatorRecordReads === 0
            ) {
              coordinatorRecordReads += 1;
              return Effect.fail(
                new Orchestrator.OrchestratorProjectionError({
                  threadId,
                  cause: "transient projection read failure",
                }),
              );
            }
            return tm.getThreadRecords(threadId, fields, filter);
          }) as typeof tm.getThreadRecords,
        }),
      });

      const exit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
        Effect.exit,
        Effect.provide(harness2.layerToolkit),
      );
      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "stops pending background work on an older inactive provider thread before completing the workflow",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWorkflowHarness({ name: "inactive-provider-roster-bg-work" });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* EventSink.EventSinkV2;
          yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "inactive-provider-roster-bg-work",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const script = `
            export const meta = {
              name: "InactiveRoster",
              description: "Stops background work left on an older inactive provider thread.",
            };
            const first = await agent("Start background work on old provider thread");
            const second = await agent("Gate completion");
            return { first, second };
          `;
          const runCall = yield* callTool("t3_workflow_run", { script });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);

          const childTurn = yield* Queue.take(harness.turnQueue);
          const childThreadId = childTurn.turn.threadId;
          yield* childTurn.setPendingBackgroundTasks([
            { taskId: "bg-old-roster", kind: "command", description: "stale watcher" },
          ]);
          yield* childTurn.complete("child-done");

          // Wait for the second agent so the child's first turn is settled before switching its active provider thread.
          const gateTurn = yield* Queue.take(harness.turnQueue);
          const childBeforeSwitch = yield* orchestrator.getThreadProjection(childThreadId);
          const oldProviderThread = childBeforeSwitch.providerThreads[0]!;
          const oldRun = childBeforeSwitch.runs[0]!;
          expect(oldProviderThread.pendingBackgroundTasks?.length ?? 0).toBe(1);

          const now = yield* DateTime.now;
          const newerProviderThreadId = ProviderThreadId.make(
            `provider-thread:${childThreadId}:newer`,
          );
          const newerRunId = RunId.make(`run:${childThreadId}:newer`);
          yield* sink.write({
            events: [
              {
                id: EventId.make(`event:newer-pt:${childThreadId}`),
                type: "provider-thread.updated",
                threadId: childThreadId,
                occurredAt: now,
                payload: {
                  ...oldProviderThread,
                  id: newerProviderThreadId,
                  status: "idle",
                  firstRunOrdinal: oldRun.ordinal + 1,
                  lastRunOrdinal: oldRun.ordinal + 1,
                  pendingBackgroundTasks: [],
                  updatedAt: now,
                },
              },
              {
                id: EventId.make(`event:newer-run:${childThreadId}`),
                type: "run.created",
                threadId: childThreadId,
                runId: newerRunId,
                providerInstanceId: instanceId,
                occurredAt: now,
                payload: {
                  ...oldRun,
                  id: newerRunId,
                  ordinal: oldRun.ordinal + 1,
                  providerThreadId: newerProviderThreadId,
                  status: "completed",
                  startedAt: now,
                  completedAt: now,
                },
              },
              {
                id: EventId.make(`event:switch-active-pt:${childThreadId}`),
                type: "thread.provider-switched",
                threadId: childThreadId,
                occurredAt: now,
                payload: {
                  ...childBeforeSwitch.thread,
                  activeProviderThreadId: newerProviderThreadId,
                  updatedAt: now,
                },
              },
            ],
          });

          // Release the gate agent so the workflow completes and must stop the older inactive provider roster.
          yield* gateTurn.complete("gate-done");

          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("completed");
          expect(yield* Deferred.isDone(childTurn.interrupted)).toBe(true);

          const childAfter = yield* orchestrator.getThreadProjection(childThreadId);
          expect(
            childAfter.providerThreads.every(
              (pt) => (pt.pendingBackgroundTasks?.length ?? 0) === 0,
            ),
          ).toBe(true);
        }).pipe(Effect.provide(harness.layerToolkit));
      }),
  );

  it.effect(
    "startup sweep ignores stray files and mismatched orphan directories while staying strict for SQL-active runs",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-stray-files-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "stray-files-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const workflowsDir = yield* Effect.gen(function* () {
          const config = yield* ServerConfig.ServerConfig;
          yield* fs.makeDirectory(config.workflowRunsDir, { recursive: true });
          // Stray regular files (like Finder's .DS_Store) must not cause ENOTDIR during startup discovery.
          yield* fs.writeFileString(path.join(config.workflowRunsDir, ".DS_Store"), "finder-meta");
          yield* fs.writeFileString(path.join(config.workflowRunsDir, "README.txt"), "notes");
          // Unrelated subdirectory without run.json.
          yield* fs.makeDirectory(path.join(config.workflowRunsDir, "unrelated-dir"), {
            recursive: true,
          });
          // Directory whose run.json has a mismatched runId.
          const mismatchedDir = path.join(config.workflowRunsDir, "wf_wrongdir0001");
          yield* fs.makeDirectory(mismatchedDir, { recursive: true });
          yield* fs.writeFileString(
            path.join(mismatchedDir, "run.json"),
            JSON.stringify({
              runId: "wf_otherid00001",
              threadId: parentThreadId,
              taskId: null,
              name: "Mismatched",
              scriptPath: path.join(mismatchedDir, "script.js"),
              journalPath: path.join(mismatchedDir, "journal.jsonl"),
              startedAt: 0,
              status: "running",
              usage: {
                agents: 0,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
          // Orphan directory whose run.json is itself a directory (unreadable orphan history).
          yield* fs.makeDirectory(
            path.join(config.workflowRunsDir, "wf_orphanbad001", "run.json"),
            {
              recursive: true,
            },
          );
          return config.workflowRunsDir;
        }).pipe(Effect.provide(harness1.layerToolkit));

        const exit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
          Effect.exit,
          Effect.provide(harness1.layerToolkit),
        );
        expect(Exit.isSuccess(exit)).toBe(true);
        // Mismatched orphan was ignored (not rewritten to failed).
        const mismatchedRecord = JSON.parse(
          yield* fs.readFileString(path.join(workflowsDir, "wf_wrongdir0001", "run.json")),
        ) as { status: string };
        expect(mismatchedRecord.status).toBe("running");

        // SQL-active workflows stay strict: an unreadable run.json for an active SQL run must fail closed.
        const activeRunId = "wf_sqlactive001";
        const activeMetaPath = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "stray-active-strict",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;
          const activeDir = path.join(workflowsDir, activeRunId);
          const scriptPath = path.join(activeDir, "script.js");
          const metaPath = path.join(activeDir, "run.json");
          yield* fs.makeDirectory(activeDir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "StrictActive", description: "Active SQL run." }; return 1;`,
          );
          yield* fs.writeFileString(metaPath, "{}");
          yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:stray-active-strict:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "StrictActive",
            description: "Active SQL run.",
            modelSelection,
            workflow: {
              name: "StrictActive",
              runHandles: { runId: activeRunId, scriptPath, transcriptDir: activeDir },
              phases: [],
              agents: [],
            },
          });
          return metaPath;
        }).pipe(Effect.provide(harness1.layerToolkit));

        const strictHarness = yield* makeWorkflowHarness({
          name: "stray-active-strict-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapScriptFileOpener: (openFile) => (filePath) =>
            filePath === activeMetaPath ? openFile(sharedBaseDir) : openFile(filePath),
        });
        const strictExit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
          Effect.exit,
          Effect.provide(strictHarness.layerToolkit),
        );
        expect(Exit.isFailure(strictExit)).toBe(true);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "rejects non-regular scriptPath files (directories and FIFOs) without blocking and cancels in-flight nested script reads on stop",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-special-script-" }),
        );
        const dirScriptPath = path.join(tempDir, "not-a-file");
        yield* fs.makeDirectory(dirScriptPath, { recursive: true });

        const isPosix = HostProcess.Platform.defaultValue() !== "win32";
        const fifoPath = path.join(tempDir, "script.fifo");
        if (isPosix) {
          NodeChildProcess.execFileSync("mkfifo", [fifoPath]);
        }

        const slowNestedPath = path.join(tempDir, "slow-nested.js");
        const readOpened = yield* Deferred.make<void>();
        const readClosingStarted = yield* Deferred.make<void>();
        const allowReadClose = yield* Deferred.make<void>();
        const readClosed = yield* Deferred.make<void>();
        const slowSandboxSettled = yield* Deferred.make<void>();
        const events: Array<string> = [];
        let trackSlowRun = false;
        let stopIssued = false;

        const harness = yield* makeWorkflowHarness({
          name: "special-and-cancel-script-reads",
          wrapThreadManagement: (tm) => ({
            ...tm,
            getThreadRecords: ((threadId, fields, filter) => {
              if (
                trackSlowRun &&
                stopIssued &&
                threadId === parentThreadId &&
                fields.length === 1 &&
                fields[0] === "subagents" &&
                filter === undefined
              ) {
                events.push("stop-children");
              }
              return tm.getThreadRecords(threadId, fields, filter);
            }) as typeof tm.getThreadRecords,
            dispatch: (command) => {
              if (trackSlowRun && command.type === "workflow.complete") {
                events.push("workflow-complete");
              }
              return tm.dispatch(command);
            },
          }),
          wrapSandboxRunner: (runner) => (input) => {
            const p = runner(input);
            if (trackSlowRun) {
              void p.then(
                () => Deferred.doneUnsafe(slowSandboxSettled, Exit.void),
                () => Deferred.doneUnsafe(slowSandboxSettled, Exit.void),
              );
            }
            return p;
          },
          wrapScriptFileOpener: (openScriptFile) => (filePath) =>
            filePath === slowNestedPath
              ? Effect.acquireRelease(
                  Deferred.succeed(readOpened, undefined).pipe(Effect.as("slow-handle")),
                  () =>
                    Effect.gen(function* () {
                      yield* Deferred.succeed(readClosingStarted, undefined);
                      yield* Deferred.await(allowReadClose);
                      events.push("read-closed");
                      yield* Deferred.succeed(readClosed, undefined);
                    }),
                ).pipe(
                  Effect.as({
                    stat: Effect.succeed({ isRegularFile: true, size: 128 }),
                    readChunk: () => Effect.never,
                  }),
                )
              : openScriptFile(filePath),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* startParentThreadAndRun(
            harness.turnQueue,
            harness.cwd,
            "special-and-cancel-script-reads",
          );

          // 1. Directory rejected immediately as non-regular in both top-level and nested workflow().
          const dirTop = yield* callTool("t3_workflow_run", { scriptPath: dirScriptPath });
          expect(declaredFailure(dirTop).code).toBe("invalid_request");
          expect(declaredFailure(dirTop).message).toBe(
            `Workflow script at ${dirScriptPath} must be a regular file.`,
          );

          const beforeDirNestedSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const dirNestedCall = yield* callTool("t3_workflow_run", {
            script: `
              export const meta = { name: "NestedDir", description: "Loads a directory." };
              await workflow({ scriptPath: ${JSON.stringify(dirScriptPath)} });
              return "unexpected";
            `,
          });
          expect(dirNestedCall.isError).not.toBe(true);
          const dirNestedStarted = yield* decodeRunResult(dirNestedCall.structuredContent);
          const dirNestedTerminal = yield* awaitWorkflowTerminal(
            dirNestedStarted.taskId,
            beforeDirNestedSeq,
          );
          expect(dirNestedTerminal.status).toBe("failed");
          expect(dirNestedTerminal.result).toContain(
            `Workflow script at ${dirScriptPath} must be a regular file.`,
          );

          // 2. FIFO (named pipe) with no writer rejects immediately without blocking on POSIX.
          if (isPosix) {
            const fifoTop = yield* callTool("t3_workflow_run", { scriptPath: fifoPath });
            expect(declaredFailure(fifoTop).code).toBe("invalid_request");
            expect(declaredFailure(fifoTop).message).toBe(
              `Workflow script at ${fifoPath} must be a regular file.`,
            );

            const beforeFifoNestedSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
            const fifoNestedCall = yield* callTool("t3_workflow_run", {
              script: `
                export const meta = { name: "NestedFifo", description: "Loads a FIFO." };
                await workflow({ scriptPath: ${JSON.stringify(fifoPath)} });
                return "unexpected";
              `,
            });
            expect(fifoNestedCall.isError).not.toBe(true);
            const fifoNestedStarted = yield* decodeRunResult(fifoNestedCall.structuredContent);
            const fifoNestedTerminal = yield* awaitWorkflowTerminal(
              fifoNestedStarted.taskId,
              beforeFifoNestedSeq,
            );
            expect(fifoNestedTerminal.status).toBe("failed");
            expect(fifoNestedTerminal.result).toContain(
              `Workflow script at ${fifoPath} must be a regular file.`,
            );
          }

          // 3. Stopping a workflow awaits the nested script read scope's close finalizer before committing terminal state or returning.
          trackSlowRun = true;
          const beforeCancelSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const slowCall = yield* callTool("t3_workflow_run", {
            script: `
              export const meta = { name: "SlowNestedRead", description: "Cancels in-flight read." };
              return await workflow({ scriptPath: ${JSON.stringify(slowNestedPath)} });
            `,
          });
          expect(slowCall.isError).not.toBe(true);
          const slowStarted = yield* decodeRunResult(slowCall.structuredContent);

          yield* Deferred.await(readOpened);
          expect(yield* Deferred.isDone(readClosed)).toBe(false);

          stopIssued = true;
          const stopFiber = yield* callTool("t3_workflow_stop", { runId: slowStarted.runId }).pipe(
            Effect.forkScoped,
          );
          yield* Deferred.await(readClosingStarted);
          yield* Deferred.await(slowSandboxSettled);
          // Let any runnable Effect fibers after runWorkflowScript settle before asserting Stop is still waiting.
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;

          // While the read scope finalizer is still draining, Stop must not return and post-close cleanup must not start.
          const midStatus = yield* decodeStatusResult(
            (yield* callTool("t3_workflow_status", { runId: slowStarted.runId })).structuredContent,
          );
          expect(midStatus.status).toBe("running");
          expect(yield* Deferred.isDone(readClosed)).toBe(false);
          expect(events).toEqual([]);
          expect(stopFiber.pollUnsafe()).toBeUndefined();

          yield* Deferred.succeed(allowReadClose, undefined);
          const stopped = yield* Fiber.join(stopFiber);
          expect(stopped.isError).not.toBe(true);
          expect(stopped.structuredContent).toEqual({
            runId: slowStarted.runId,
            status: "cancelled",
          });
          // The read scope must already be fully closed before Stop returns.
          expect(yield* Deferred.isDone(readClosed)).toBe(true);
          expect(events).toEqual(["read-closed", "stop-children", "workflow-complete"]);

          const cancelTerminal = yield* awaitWorkflowTerminal(slowStarted.taskId, beforeCancelSeq);
          expect(cancelTerminal.status).toBe("cancelled");
        }).pipe(Effect.provide(harness.layerToolkit));

        // 4. Layer scope shutdown also awaits the same stored read-scope close promise before returning.
        const shutdownReadOpened = yield* Deferred.make<void>();
        const shutdownReadClosing = yield* Deferred.make<void>();
        const allowShutdownReadClose = yield* Deferred.make<void>();
        const shutdownReadClosed = yield* Deferred.make<void>();
        const shutdownSandboxExited = yield* Deferred.make<void>();
        const shutdownHarness = yield* makeWorkflowHarness({
          name: "special-and-cancel-script-reads-shutdown",
          wrapSandboxRunner: (runner) => (input) =>
            runner({
              ...input,
              spawnChild: (command, args, opts) => {
                const child = NodeChildProcess.spawn(command, args, opts);
                child.once("exit", () => {
                  Deferred.doneUnsafe(shutdownSandboxExited, Exit.void);
                });
                return child;
              },
            }),
          wrapScriptFileOpener: (openScriptFile) => (filePath) =>
            filePath === slowNestedPath
              ? Effect.acquireRelease(
                  Deferred.succeed(shutdownReadOpened, undefined).pipe(Effect.as("slow-handle")),
                  () =>
                    Effect.gen(function* () {
                      yield* Deferred.succeed(shutdownReadClosing, undefined);
                      yield* Deferred.await(allowShutdownReadClose);
                      yield* Deferred.succeed(shutdownReadClosed, undefined);
                    }),
                ).pipe(
                  Effect.as({
                    stat: Effect.succeed({ isRegularFile: true, size: 128 }),
                    readChunk: () => Effect.never,
                  }),
                )
              : openScriptFile(filePath),
        });
        const serviceScope = yield* Scope.make();
        const serviceContext = yield* Layer.buildWithScope(
          shutdownHarness.layerToolkit,
          serviceScope,
        );
        yield* Effect.gen(function* () {
          yield* startParentThreadAndRun(
            shutdownHarness.turnQueue,
            shutdownHarness.cwd,
            "special-and-cancel-script-reads-shutdown",
          );
          const runCall = yield* callTool("t3_workflow_run", {
            script: `
              export const meta = { name: "ShutdownNestedRead", description: "Awaits read close on shutdown." };
              return await workflow({ scriptPath: ${JSON.stringify(slowNestedPath)} });
            `,
          });
          expect(runCall.isError).not.toBe(true);
          yield* Deferred.await(shutdownReadOpened);
        }).pipe(Effect.provide(serviceContext));

        const closeFiber = yield* Scope.close(serviceScope, Exit.void).pipe(Effect.forkScoped);
        yield* Deferred.await(shutdownReadClosing);
        yield* Deferred.await(shutdownSandboxExited);
        expect(yield* Deferred.isDone(shutdownReadClosed)).toBe(false);
        expect(closeFiber.pollUnsafe()).toBeUndefined();
        yield* Deferred.succeed(allowShutdownReadClose, undefined);
        yield* Fiber.join(closeFiber);
        expect(yield* Deferred.isDone(shutdownReadClosed)).toBe(true);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each(["first-child-fails", "both-children-fail"] as const)(
    "deferred startup cleanup never lets a second child's unaccepted grandchild run resend (%s)",
    (failureMode) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: `t3-workflow-grandchild-${failureMode}-` }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: `grandchild-seed-${failureMode}`,
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seededChildren = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            `grandchild-${failureMode}`,
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = `wf_gc_${failureMode.replaceAll("-", "_")}`;
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "GrandchildGate", description: "Two children with grandchild." }; return 1;`,
          );
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make(`command:gc-start:${runId}`),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "GrandchildGate",
            description: "Two children with grandchild.",
            modelSelection,
            workflow: {
              name: "GrandchildGate",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing workflow task");
          }
          const taskId = taskEvent.event.payload.id;

          const child1 = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:0`,
            task: { task: "First child (cleanup fails)", title: "Child 1" },
          });
          yield* Queue.take(harness1.turnQueue);

          const child2 = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:1`,
            task: { task: "Second child (delegates grandchild)", title: "Child 2" },
          });
          yield* Queue.take(harness1.turnQueue);

          yield* fs.writeFileString(
            path.join(dir, "run.json"),
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "GrandchildGate",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 2,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
          return {
            runId,
            taskId,
            child1ThreadId: child1.childThreadId,
            child1TaskId: child1.taskId,
            child2ThreadId: child2.childThreadId,
            child2TaskId: child2.taskId,
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        // Seed child2's grandchild with runEffectWorker: false so its delegated run stays unaccepted (starting, startedAt: null).
        const unacceptedGrandchildHarness = yield* makeWorkflowHarness({
          name: `grandchild-unaccepted-${failureMode}`,
          databaseLayer,
          baseDir: sharedBaseDir,
          runEffectWorker: false,
        });
        const grandchildThreadId = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const child2Scope: McpInvocationContext.McpThreadInvocationScope = {
            environmentId: EnvironmentId.make("environment:workflow-test"),
            requestNamespace: `provider-session:${seededChildren.child2ThreadId}`,
            thread: {
              threadId: seededChildren.child2ThreadId,
              providerSessionId: `provider-session:${seededChildren.child2ThreadId}`,
              providerInstanceId: instanceId,
            },
            client: undefined,
            capabilities: new Set(["orchestration"]),
            issuedAt: 1,
          };
          const delegated = yield* callTool(
            "delegate_task",
            { task: "Unaccepted grandchild task", title: "Grandchild" },
            child2Scope,
          );
          expect(delegated.isError).not.toBe(true);
          const child2Projection = yield* orchestrator.getThreadProjection(
            seededChildren.child2ThreadId,
          );
          const gcTask = child2Projection.subagents.find(
            (s) => s.origin === "app_owned" && s.childThreadId !== null,
          );
          if (gcTask?.childThreadId == null) {
            throw new Error("missing grandchild task");
          }
          const gcProjection = yield* orchestrator.getThreadProjection(gcTask.childThreadId);
          expect(gcProjection.runs).toHaveLength(1);
          expect(gcProjection.runs[0]?.status).toBe("starting");
          expect(gcProjection.runs[0]?.startedAt).toBeNull();
          return gcTask.childThreadId;
        }).pipe(Effect.provide(unacceptedGrandchildHarness.layerToolkit));

        // Restart phase 1: workflow sweep runs; child1 fails cleanup (and optionally child2 too), forcing deferred cleanup.
        const preRecoveryHarness = yield* makeWorkflowHarness({
          name: `grandchild-pre-${failureMode}`,
          databaseLayer,
          baseDir: sharedBaseDir,
          runEffectWorker: false,
          continueThreadsAfterServerUpdate: true,
          wrapThreadManagement: (tm) => ({
            ...tm,
            cancelDelegatedTask: (input) => {
              const shouldFail =
                input.taskId === seededChildren.child1TaskId ||
                (failureMode === "both-children-fail" &&
                  input.taskId === seededChildren.child2TaskId);
              if (shouldFail) {
                return Effect.fail(
                  new Orchestrator.OrchestratorDispatchError({
                    commandId: input.command.commandId,
                    commandType: input.command.type,
                    cause: "child stop temporarily unavailable",
                  }),
                );
              }
              return tm.cancelDelegatedTask(input);
            },
          }),
        });

        yield* Effect.gen(function* () {
          yield* WorkflowService.recoverInterruptedWorkflows;

          // When only child1 failed, child2 and grandchild must already have been attempted/stopped.
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          if (failureMode === "first-child-fails") {
            const gcAfterSweep = yield* orchestrator.getThreadProjection(grandchildThreadId);
            expect(
              gcAfterSweep.runs.every(
                (run) => run.status === "cancelled" || run.status === "interrupted",
              ),
            ).toBe(true);
          }
        }).pipe(Effect.provide(preRecoveryHarness.layerToolkit));

        // Restart phase 2: provider recovery runs with continueThreadsAfterServerUpdate enabled.
        // The unaccepted grandchild run must NEVER be resent.
        const postRecoveryHarness = yield* makeWorkflowHarness({
          name: `grandchild-post-${failureMode}`,
          databaseLayer,
          baseDir: sharedBaseDir,
          recoverOnStartup: true,
          continueThreadsAfterServerUpdate: true,
          runEffectWorker: true,
          runContinuationWorker: true,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const gcAfterRecovery = yield* orchestrator.getThreadProjection(grandchildThreadId);
          expect(
            gcAfterRecovery.runs.every(
              (run) => run.status === "cancelled" || run.status === "interrupted",
            ),
          ).toBe(true);
          expect(
            gcAfterRecovery.runs.some((run) => run.restartContinuationOfRunId !== undefined),
          ).toBe(false);
          const gcTurns = (yield* Ref.get(postRecoveryHarness.startedTurns)).filter(
            (t) => t.turn.threadId === grandchildThreadId,
          );
          expect(gcTurns).toHaveLength(0);
        }).pipe(Effect.provide(postRecoveryHarness.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "keeps workflow cleanup pending with live retries on persistent sandbox kill failure and joins spawn-tracked cleanup on layer shutdown",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;

        // Part A: Service-level Stop with persistent EPERM on child.kill keeps workflow running + cleanupPending
        // until a live retry confirms child exit.
        const workerModuleUrl = new URL("./sandboxWorker.ts", import.meta.url).href;
        const pinnedWorkerCommand = {
          command: process.execPath,
          args: [
            "-e",
            `setInterval(() => {}, 60_000); import(${JSON.stringify(workerModuleUrl)}).then((m) => m.runWorkflowSandboxWorker());`,
          ],
          env: { ELECTRON_RUN_AS_NODE: "1" },
        };
        let allowStopKill = false;
        let holdChildUnconfirmed = true;
        let stopKillAttempts = 0;
        let stopChildThreadId: ThreadId | undefined;
        let stopChild: NodeChildProcess.ChildProcessWithoutNullStreams | undefined;
        let stopRealKill: NodeChildProcess.ChildProcessWithoutNullStreams["kill"] | undefined;
        let activeExitListeners = 0;
        let peakExitListeners = 0;
        const firstStopKillPassFailed = yield* Deferred.make<void>();
        const liveStopRetryAttempted = yield* Deferred.make<void>();
        const childConfirmWaitStarted = yield* Deferred.make<void>();
        const cleanupPendingMetaWritten = yield* Deferred.make<void>();
        const cancelledMetaWritten = yield* Deferred.make<void>();

        const stopHarness = yield* makeWorkflowHarness({
          name: "persistent-kill-stop",
          wrapThreadManagement: (tm) => ({
            ...tm,
            streamStoredEventsFrom: (input) => {
              if (
                holdChildUnconfirmed &&
                stopChildThreadId !== undefined &&
                input?.threadId === stopChildThreadId
              ) {
                Deferred.doneUnsafe(childConfirmWaitStarted, Exit.void);
              }
              return tm.streamStoredEventsFrom(input);
            },
            getThreadRecords: ((threadId, fields, filter) =>
              tm.getThreadRecords(threadId, fields, filter).pipe(
                Effect.map((records) => {
                  if (
                    !holdChildUnconfirmed ||
                    stopChildThreadId === undefined ||
                    threadId !== stopChildThreadId ||
                    !("providerThreads" in records) ||
                    !Array.isArray(records.providerThreads)
                  ) {
                    return records;
                  }
                  return {
                    ...records,
                    providerThreads: records.providerThreads.map((pt) => ({
                      ...pt,
                      pendingBackgroundTasks: [
                        {
                          taskId: `held-${threadId}`,
                          kind: "command",
                          description: "still stopping",
                        },
                      ],
                    })),
                  };
                }),
              )) as typeof tm.getThreadRecords,
          }),
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              Effect.gen(function* () {
                const raw =
                  path.basename(newPath) === "run.json"
                    ? yield* fsService.readFileString(oldPath).pipe(Effect.orElseSucceed(() => ""))
                    : "";
                yield* fsService.rename(oldPath, newPath);
                if (raw.length > 0) {
                  const parsed = JSON.parse(raw) as {
                    readonly status?: string;
                    readonly cleanupPending?: boolean;
                    readonly sandboxCleanupPending?: { readonly pid?: number };
                  };
                  if (parsed.status === "running" && parsed.sandboxCleanupPending !== undefined) {
                    yield* Deferred.succeed(cleanupPendingMetaWritten, undefined);
                  } else if (
                    parsed.status === "cancelled" &&
                    parsed.cleanupPending === undefined &&
                    parsed.sandboxCleanupPending === undefined
                  ) {
                    yield* Deferred.succeed(cancelledMetaWritten, undefined);
                  }
                }
              }),
          }),
          wrapSandboxRunner: (runner) => (input) =>
            runner({
              ...input,
              maxKillAttempts: 2,
              killRetryDelayMs: 0,
              spawnCommand: pinnedWorkerCommand,
              onWorkerSpawned: (worker) => {
                const origOnExit = worker.onExit.bind(worker);
                input.onWorkerSpawned?.({
                  ...worker,
                  onExit: (listener) => {
                    activeExitListeners += 1;
                    if (activeExitListeners > peakExitListeners) {
                      peakExitListeners = activeExitListeners;
                    }
                    let removed = false;
                    const unsub = origOnExit(() => {
                      if (!removed) {
                        removed = true;
                        activeExitListeners -= 1;
                      }
                      listener();
                    });
                    return () => {
                      if (!removed) {
                        removed = true;
                        activeExitListeners -= 1;
                      }
                      unsub();
                    };
                  },
                });
              },
              spawnChild: (command, args, opts) => {
                const child = NodeChildProcess.spawn(command, args, opts);
                stopChild = child;
                const origKill = child.kill.bind(child);
                stopRealKill = origKill;
                child.kill = ((signal?: NodeJS.Signals | number) => {
                  stopKillAttempts += 1;
                  if (!allowStopKill) {
                    if (stopKillAttempts >= 2) {
                      Deferred.doneUnsafe(firstStopKillPassFailed, Exit.void);
                    }
                    if (stopKillAttempts >= 3) {
                      Deferred.doneUnsafe(liveStopRetryAttempted, Exit.void);
                    }
                    const err = new Error("Operation not permitted") as Error & { code: string };
                    err.code = "EPERM";
                    throw err;
                  }
                  return origKill(signal);
                }) as typeof child.kill;
                return child;
              },
            }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const config = yield* ServerConfig.ServerConfig;
          yield* startParentThreadAndRun(
            stopHarness.turnQueue,
            stopHarness.cwd,
            "persistent-kill-stop",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const runCall = yield* callTool("t3_workflow_run", {
            script: `
              export const meta = { name: "PersistentKillStop", description: "Tests persistent sandbox kill failure on stop." };
              await agent("Wait forever");
              return "unreachable";
            `,
          });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);
          const childTurn = yield* Queue.take(stopHarness.turnQueue);
          stopChildThreadId = childTurn.turn.threadId;

          const stopFiber = yield* callTool("t3_workflow_stop", { runId: started.runId }).pipe(
            Effect.forkScoped,
          );
          yield* Deferred.await(firstStopKillPassFailed);
          yield* Deferred.await(cleanupPendingMetaWritten);
          yield* Deferred.await(childTurn.interrupted);
          yield* Deferred.await(childConfirmWaitStarted);

          const childPid = stopChild?.pid;
          expect(childPid).toBeDefined();
          expect(getUnconfirmedSandboxChildPids()).toContain(childPid);

          // Live sandbox retries must start and run even while the workflow child stays unconfirmed.
          yield* TestClock.adjust("50 millis");
          expect(stopKillAttempts).toBeGreaterThanOrEqual(3);
          yield* Deferred.await(liveStopRetryAttempted);

          // Once the child confirms stopped, persistent sandbox failure still blocks settlement and resume.
          holdChildUnconfirmed = false;
          yield* TestClock.adjust("2 seconds");
          yield* TestClock.adjust("50 millis");

          const midTask = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
            (s) => s.id === started.taskId,
          );
          expect(midTask?.status).toBe("running");
          expect(midTask?.workflow?.stopRequestedAt).toBeDefined();
          expect(midTask?.result).not.toBe("Stopped.");

          const metaPath = path.join(config.workflowRunsDir, started.runId, "run.json");
          const midMeta = JSON.parse(yield* fs.readFileString(metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
            readonly sandboxCleanupPending?: { readonly pid?: number; readonly startTime?: string };
          };
          expect(midMeta.status).toBe("running");
          expect(midMeta.cleanupPending).toBeUndefined();
          expect(midMeta.sandboxCleanupPending?.pid).toBe(childPid);
          expect(midMeta.sandboxCleanupPending?.startTime).toBeDefined();

          const refusedResume = yield* callTool("t3_workflow_run", {
            scriptPath: started.scriptPath,
            resumeFromRunId: started.runId,
          });
          expect(declaredFailure(refusedResume).code).toBe("invalid_request");

          // Advancing past STOP_WAIT_BOUND_MS returns status:"running" (never "cancelled").
          yield* TestClock.adjust("30 seconds");
          const boundedStop = yield* Fiber.join(stopFiber);
          expect(boundedStop.isError).not.toBe(true);
          expect(boundedStop.structuredContent).toEqual({
            runId: started.runId,
            status: "running",
          });

          // Repeated timed-out retries must not accumulate exit-wait callbacks on the worker.
          expect(peakExitListeners).toBeLessThanOrEqual(1);

          // Once kill succeeds on a live retry, exit is confirmed and the workflow completes as cancelled.
          allowStopKill = true;
          yield* TestClock.adjust("2 seconds");
          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("cancelled");
          expect(terminal.result).toBe("Stopped.");
          expect(activeExitListeners).toBe(0);

          yield* Deferred.await(cancelledMetaWritten);
          const finalMeta = JSON.parse(yield* fs.readFileString(metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
            readonly sandboxCleanupPending?: { readonly pid?: number };
          };
          expect(finalMeta.status).toBe("cancelled");
          expect(finalMeta.cleanupPending).toBeUndefined();
          expect(finalMeta.sandboxCleanupPending).toBeUndefined();
          expect(getUnconfirmedSandboxChildPids()).not.toContain(childPid);
        }).pipe(
          Effect.provide(stopHarness.layerToolkit),
          Effect.ensuring(
            Effect.sync(() => {
              if (
                stopChild !== undefined &&
                stopRealKill !== undefined &&
                stopChild.exitCode === null &&
                stopChild.signalCode === null
              ) {
                stopChild.kill = stopRealKill;
                stopRealKill("SIGKILL");
              }
            }),
          ),
        );

        // Part B: Layer shutdown interrupting a live workflow joins the spawn-tracked cleanup operation
        // even before the runner finishes its own termination retries, and retains unconfirmed ownership on persistent EPERM.
        const shutdownBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-persistent-kill-shutdown-" }),
        );
        let shutdownKillAttempts = 0;
        let shutdownChild: NodeChildProcess.ChildProcessWithoutNullStreams | undefined;
        let shutdownRealKill: NodeChildProcess.ChildProcessWithoutNullStreams["kill"] | undefined;
        const shutdownHarness = yield* makeWorkflowHarness({
          name: "persistent-kill-shutdown",
          baseDir: shutdownBaseDir,
          wrapSandboxRunner: (runner) => (input) =>
            runner({
              ...input,
              maxKillAttempts: 2,
              killRetryDelayMs: 0,
              spawnCommand: pinnedWorkerCommand,
              spawnChild: (command, args, opts) => {
                const child = NodeChildProcess.spawn(command, args, opts);
                shutdownChild = child;
                const origKill = child.kill.bind(child);
                shutdownRealKill = origKill;
                child.kill = (() => {
                  shutdownKillAttempts += 1;
                  const err = new Error("Operation not permitted") as Error & { code: string };
                  err.code = "EPERM";
                  throw err;
                }) as typeof child.kill;
                return child;
              },
            }),
        });

        const serviceScope = yield* Scope.make();
        const serviceContext = yield* Layer.buildWithScope(
          shutdownHarness.layerToolkit,
          serviceScope,
        );
        yield* Effect.gen(function* () {
          const { metaPath } = yield* Effect.gen(function* () {
            const config = yield* ServerConfig.ServerConfig;
            yield* startParentThreadAndRun(
              shutdownHarness.turnQueue,
              shutdownHarness.cwd,
              "persistent-kill-shutdown",
            );
            const runCall = yield* callTool("t3_workflow_run", {
              script: `
                export const meta = { name: "PersistentKillShutdown", description: "Tests shutdown cleanup on persistent EPERM." };
                await agent("Wait during shutdown");
                return "unreachable";
              `,
            });
            expect(runCall.isError).not.toBe(true);
            const started = yield* decodeRunResult(runCall.structuredContent);
            yield* Queue.take(shutdownHarness.turnQueue);
            return { metaPath: path.join(config.workflowRunsDir, started.runId, "run.json") };
          }).pipe(Effect.provide(serviceContext));

          const shutdownPid = shutdownChild?.pid;
          expect(shutdownPid).toBeDefined();
          // Prior to shutdown, the live running child is not yet in unconfirmedSandboxChildren.
          expect(getUnconfirmedSandboxChildPids()).not.toContain(shutdownPid);

          yield* Scope.close(serviceScope, Exit.void);

          // Shutdown must have waited for the spawn-tracked worker's bounded kill attempts and kept ownership.
          expect(shutdownKillAttempts).toBeGreaterThanOrEqual(2);
          expect(getUnconfirmedSandboxChildPids()).toContain(shutdownPid);

          const metaAfterShutdown = JSON.parse(yield* fs.readFileString(metaPath)) as {
            readonly status: string;
          };
          expect(metaAfterShutdown.status).toBe("running");

          // Restoring kill lets reapUnconfirmedSandboxChildren confirm exit cleanly.
          shutdownChild!.kill = shutdownRealKill!;
          yield* Effect.promise(() => reapUnconfirmedSandboxChildren());
          expect(getUnconfirmedSandboxChildPids()).not.toContain(shutdownPid);
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (
                shutdownChild !== undefined &&
                shutdownRealKill !== undefined &&
                shutdownChild.exitCode === null &&
                shutdownChild.signalCode === null
              ) {
                shutdownChild.kill = shutdownRealKill;
                shutdownRealKill("SIGKILL");
              }
            }),
          ),
        );
        // Part C: Sandbox exits on a live retry while the child stays unconfirmed; terminal settlement waits for both.
        const dualBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-dual-cleanup-" }),
        );
        let dualAllowSandboxKill = false;
        let dualHoldChildUnconfirmed = true;
        let dualKillAttempts = 0;
        let dualChildThreadId: ThreadId | undefined;
        let dualSandboxChild: NodeChildProcess.ChildProcessWithoutNullStreams | undefined;
        let dualRealKill: NodeChildProcess.ChildProcessWithoutNullStreams["kill"] | undefined;
        const dualFirstKillFailed = yield* Deferred.make<void>();
        const dualCleanupPendingWritten = yield* Deferred.make<void>();
        const dualChildOnlyPendingWritten = yield* Deferred.make<void>();
        const dualChildConfirmStarted = yield* Deferred.make<void>();
        const dualSandboxExited = yield* Deferred.make<void>();
        const dualCancelledWritten = yield* Deferred.make<void>();

        const dualHarness = yield* makeWorkflowHarness({
          name: "dual-cleanup-wait",
          baseDir: dualBaseDir,
          wrapThreadManagement: (tm) => ({
            ...tm,
            streamStoredEventsFrom: (input) => {
              if (
                dualHoldChildUnconfirmed &&
                dualChildThreadId !== undefined &&
                input?.threadId === dualChildThreadId
              ) {
                Deferred.doneUnsafe(dualChildConfirmStarted, Exit.void);
              }
              return tm.streamStoredEventsFrom(input);
            },
            getThreadRecords: ((threadId, fields, filter) =>
              tm.getThreadRecords(threadId, fields, filter).pipe(
                Effect.map((records) => {
                  if (
                    !dualHoldChildUnconfirmed ||
                    dualChildThreadId === undefined ||
                    threadId !== dualChildThreadId ||
                    !("providerThreads" in records) ||
                    !Array.isArray(records.providerThreads)
                  ) {
                    return records;
                  }
                  return {
                    ...records,
                    providerThreads: records.providerThreads.map((pt) => ({
                      ...pt,
                      pendingBackgroundTasks: [
                        {
                          taskId: `dual-held-${threadId}`,
                          kind: "command",
                          description: "still stopping",
                        },
                      ],
                    })),
                  };
                }),
              )) as typeof tm.getThreadRecords,
          }),
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              Effect.gen(function* () {
                const raw =
                  path.basename(newPath) === "run.json"
                    ? yield* fsService.readFileString(oldPath).pipe(Effect.orElseSucceed(() => ""))
                    : "";
                yield* fsService.rename(oldPath, newPath);
                if (raw.length > 0) {
                  const parsed = JSON.parse(raw) as {
                    readonly status?: string;
                    readonly cleanupPending?: boolean;
                    readonly sandboxCleanupPending?: { readonly pid?: number };
                  };
                  if (parsed.status === "running" && parsed.sandboxCleanupPending !== undefined) {
                    yield* Deferred.succeed(dualCleanupPendingWritten, undefined);
                  } else if (
                    parsed.status === "running" &&
                    parsed.cleanupPending === true &&
                    parsed.sandboxCleanupPending === undefined
                  ) {
                    yield* Deferred.succeed(dualChildOnlyPendingWritten, undefined);
                  } else if (
                    parsed.status === "cancelled" &&
                    parsed.cleanupPending === undefined &&
                    parsed.sandboxCleanupPending === undefined
                  ) {
                    yield* Deferred.succeed(dualCancelledWritten, undefined);
                  }
                }
              }),
          }),
          wrapSandboxRunner: (runner) => (input) =>
            runner({
              ...input,
              maxKillAttempts: 2,
              killRetryDelayMs: 0,
              spawnCommand: pinnedWorkerCommand,
              onWorkerDone: () => {
                input.onWorkerDone?.();
                Deferred.doneUnsafe(dualSandboxExited, Exit.void);
              },
              spawnChild: (command, args, opts) => {
                const child = NodeChildProcess.spawn(command, args, opts);
                dualSandboxChild = child;
                const origKill = child.kill.bind(child);
                dualRealKill = origKill;
                child.kill = ((signal?: NodeJS.Signals | number) => {
                  dualKillAttempts += 1;
                  if (!dualAllowSandboxKill) {
                    if (dualKillAttempts >= 2) {
                      Deferred.doneUnsafe(dualFirstKillFailed, Exit.void);
                    }
                    const err = new Error("Operation not permitted") as Error & { code: string };
                    err.code = "EPERM";
                    throw err;
                  }
                  return origKill(signal);
                }) as typeof child.kill;
                return child;
              },
            }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const config = yield* ServerConfig.ServerConfig;
          yield* startParentThreadAndRun(
            dualHarness.turnQueue,
            dualHarness.cwd,
            "dual-cleanup-wait",
          );
          const beforeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const runCall = yield* callTool("t3_workflow_run", {
            script: `
              export const meta = { name: "DualCleanupWait", description: "Sandbox exits while child stays unconfirmed." };
              await agent("Wait forever");
              return "unreachable";
            `,
          });
          expect(runCall.isError).not.toBe(true);
          const started = yield* decodeRunResult(runCall.structuredContent);
          const childTurn = yield* Queue.take(dualHarness.turnQueue);
          dualChildThreadId = childTurn.turn.threadId;

          const stopFiber = yield* callTool("t3_workflow_stop", { runId: started.runId }).pipe(
            Effect.forkScoped,
          );
          yield* Deferred.await(dualFirstKillFailed);
          yield* Deferred.await(dualCleanupPendingWritten);
          yield* Deferred.await(childTurn.interrupted);
          yield* Deferred.await(dualChildConfirmStarted);

          const dualPid = dualSandboxChild?.pid;
          expect(dualPid).toBeDefined();
          expect(getUnconfirmedSandboxChildPids()).toContain(dualPid);

          // Unblocking sandbox kill while the child stays unconfirmed lets the sandbox exit,
          // while terminal settlement still waits for the child.
          dualAllowSandboxKill = true;
          yield* TestClock.adjust("50 millis");
          yield* Deferred.await(dualSandboxExited);
          yield* Deferred.await(dualChildOnlyPendingWritten);
          expect(dualKillAttempts).toBeGreaterThanOrEqual(3);
          expect(getUnconfirmedSandboxChildPids()).not.toContain(dualPid);

          const midTask = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
            (s) => s.id === started.taskId,
          );
          expect(midTask?.status).toBe("running");
          const metaPath = path.join(config.workflowRunsDir, started.runId, "run.json");
          const midMeta = JSON.parse(yield* fs.readFileString(metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
            readonly sandboxCleanupPending?: { readonly pid?: number; readonly startTime?: string };
          };
          expect(midMeta.status).toBe("running");
          expect(midMeta.cleanupPending).toBe(true);
          expect(midMeta.sandboxCleanupPending).toBeUndefined();

          // Once the child also confirms stopped, terminal settlement completes.
          dualHoldChildUnconfirmed = false;
          yield* TestClock.adjust("2 seconds");
          yield* TestClock.adjust("50 millis");
          const stopped = yield* Fiber.join(stopFiber);
          expect(stopped.isError).not.toBe(true);
          expect(stopped.structuredContent).toEqual({
            runId: started.runId,
            status: "cancelled",
          });
          const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeSeq);
          expect(terminal.status).toBe("cancelled");
          yield* Deferred.await(dualCancelledWritten);
        }).pipe(
          Effect.provide(dualHarness.layerToolkit),
          Effect.ensuring(
            Effect.sync(() => {
              if (
                dualSandboxChild !== undefined &&
                dualRealKill !== undefined &&
                dualSandboxChild.exitCode === null &&
                dualSandboxChild.signalCode === null
              ) {
                dualSandboxChild.kill = dualRealKill;
                dualRealKill("SIGKILL");
              }
            }),
          ),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "startup sweep rejects non-regular or oversized run.json and journals without blocking, reads journals only when needed, and stays fail-closed for SQL-owned runs",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const isPosix = HostProcess.Platform.defaultValue() !== "win32";
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-bounded-sweep-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "bounded-sweep-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const zeroUsage = {
          agents: 0,
          done: 0,
          errors: 0,
          empty: 0,
          cached: 0,
          tokens: 0,
          durationMs: 0,
        };

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "bounded-sweep-seed",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;
          yield* fs.makeDirectory(config.workflowRunsDir, { recursive: true });

          // 1. Disk-only directory with a non-regular (FIFO on POSIX, directory otherwise) run.json.
          const fifoMetaDir = path.join(config.workflowRunsDir, "wf_fifometa0001");
          yield* fs.makeDirectory(fifoMetaDir, { recursive: true });
          const fifoMetaPath = path.join(fifoMetaDir, "run.json");
          if (isPosix) {
            NodeChildProcess.execFileSync("mkfifo", [fifoMetaPath]);
          } else {
            yield* fs.makeDirectory(fifoMetaPath, { recursive: true });
          }

          // 2. Disk-only directory with an oversized (> 8 MiB) regular run.json.
          const hugeMetaDir = path.join(config.workflowRunsDir, "wf_hugemeta0001");
          yield* fs.makeDirectory(hugeMetaDir, { recursive: true });
          const hugeMetaPath = path.join(hugeMetaDir, "run.json");
          yield* fs.writeFileString(hugeMetaPath, "");
          yield* fs.truncate(hugeMetaPath, 9 * 1024 * 1024);

          // 3. Disk-only valid run.json whose stored journalPath AND canonical journal.jsonl are FIFOs (or directories).
          const badJournalDir = path.join(config.workflowRunsDir, "wf_badjrnl0001");
          yield* fs.makeDirectory(badJournalDir, { recursive: true });
          const badExternalFifo = path.join(sharedBaseDir, "external-journal.fifo");
          const canonicalBadJournal = path.join(badJournalDir, "journal.jsonl");
          if (isPosix) {
            NodeChildProcess.execFileSync("mkfifo", [badExternalFifo]);
            NodeChildProcess.execFileSync("mkfifo", [canonicalBadJournal]);
          } else {
            yield* fs.makeDirectory(badExternalFifo, { recursive: true });
            yield* fs.makeDirectory(canonicalBadJournal, { recursive: true });
          }
          yield* fs.writeFileString(
            path.join(badJournalDir, "run.json"),
            JSON.stringify({
              runId: "wf_badjrnl0001",
              threadId: parentThreadId,
              taskId: null,
              name: "BadJournal",
              scriptPath: path.join(badJournalDir, "script.js"),
              journalPath: badExternalFifo,
              startedAt: 0,
              status: "running",
              usage: zeroUsage,
            }),
          );

          // 4. Disk-only valid run.json whose canonical journal.jsonl has an oversized single line (> 8 MiB).
          const hugeJournalDir = path.join(config.workflowRunsDir, "wf_hugejrnl0001");
          yield* fs.makeDirectory(hugeJournalDir, { recursive: true });
          const hugeJournalPath = path.join(hugeJournalDir, "journal.jsonl");
          yield* fs.writeFileString(hugeJournalPath, "");
          yield* fs.truncate(hugeJournalPath, 65 * 1024 * 1024);
          yield* fs.writeFileString(
            path.join(hugeJournalDir, "run.json"),
            JSON.stringify({
              runId: "wf_hugejrnl0001",
              threadId: parentThreadId,
              taskId: null,
              name: "HugeJournal",
              scriptPath: path.join(hugeJournalDir, "script.js"),
              journalPath: hugeJournalPath,
              startedAt: 0,
              status: "running",
              usage: zeroUsage,
            }),
          );

          // 5. SQL-completed workflow with result already in run.json and no cleanupPending:
          //    startup sweep must reconcile run.json without ever opening its journal.
          const noJournalRunId = "wf_done_nojrnl1";
          const noJournalDir = path.join(config.workflowRunsDir, noJournalRunId);
          const noJournalScript = path.join(noJournalDir, "script.js");
          const noJournalPath = path.join(noJournalDir, "journal.jsonl");
          yield* fs.makeDirectory(noJournalDir, { recursive: true });
          yield* fs.writeFileString(
            noJournalScript,
            `export const meta = { name: "DoneNoJournal", description: "Already has result." }; return 99;`,
          );
          if (isPosix) {
            NodeChildProcess.execFileSync("mkfifo", [noJournalPath]);
          } else {
            yield* fs.makeDirectory(noJournalPath, { recursive: true });
          }
          const doneStarted = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:bounded-sweep:done-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "DoneNoJournal",
            description: "Already has result.",
            modelSelection,
            workflow: {
              name: "DoneNoJournal",
              runHandles: {
                runId: noJournalRunId,
                scriptPath: noJournalScript,
                transcriptDir: noJournalDir,
              },
              phases: [],
              agents: [],
            },
          });
          const doneTaskEvent = doneStarted.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (doneTaskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing done workflow task");
          }
          yield* orchestrator.dispatch({
            type: "workflow.complete",
            commandId: CommandId.make("command:bounded-sweep:done-complete"),
            parentThreadId,
            taskId: doneTaskEvent.event.payload.id,
            status: "completed",
            result: "Workflow DoneNoJournal completed.",
          });
          const noJournalMetaPath = path.join(noJournalDir, "run.json");
          yield* fs.writeFileString(
            noJournalMetaPath,
            JSON.stringify({
              runId: noJournalRunId,
              threadId: parentThreadId,
              taskId: doneTaskEvent.event.payload.id,
              name: "DoneNoJournal",
              scriptPath: noJournalScript,
              journalPath: noJournalPath,
              startedAt: 0,
              status: "running",
              result: 99,
              usage: zeroUsage,
            }),
          );

          return {
            parentRun,
            workflowsDir: config.workflowRunsDir,
            noJournalPath,
            noJournalMetaPath,
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        const openedPaths: Array<string> = [];
        const sweepHarness = yield* makeWorkflowHarness({
          name: "bounded-sweep-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapScriptFileOpener: (openFile) => (filePath) => {
            openedPaths.push(filePath);
            return openFile(filePath);
          },
        });
        const diskOnlyExit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
          Effect.exit,
          Effect.provide(sweepHarness.layerToolkit),
        );
        expect(Exit.isSuccess(diskOnlyExit)).toBe(true);
        // The completed run's journal was never opened because recovery did not need it.
        expect(openedPaths).not.toContain(seeded.noJournalPath);
        const reconciledDoneMeta = JSON.parse(
          yield* fs.readFileString(seeded.noJournalMetaPath),
        ) as {
          readonly status: string;
          readonly result?: number;
        };
        expect(reconciledDoneMeta.status).toBe("completed");
        expect(reconciledDoneMeta.result).toBe(99);

        // 6. SQL-owned active workflow with a non-regular (FIFO/dir) run.json MUST fail closed.
        const sqlBadMetaRunId = "wf_sqlbadmeta01";
        const sqlBadMetaDir = path.join(seeded.workflowsDir, sqlBadMetaRunId);
        yield* fs.makeDirectory(sqlBadMetaDir, { recursive: true });
        const sqlBadMetaScript = path.join(sqlBadMetaDir, "script.js");
        const sqlBadMetaPath = path.join(sqlBadMetaDir, "run.json");
        yield* fs.writeFileString(
          sqlBadMetaScript,
          `export const meta = { name: "SqlBadMeta", description: "Non-regular run.json." }; return 1;`,
        );
        if (isPosix) {
          NodeChildProcess.execFileSync("mkfifo", [sqlBadMetaPath]);
        } else {
          yield* fs.makeDirectory(sqlBadMetaPath, { recursive: true });
        }
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:bounded-sweep:sql-bad-meta"),
            parentThreadId,
            parentRunId: seeded.parentRun.id,
            parentNodeId: seeded.parentRun.rootNodeId!,
            title: "SqlBadMeta",
            description: "Non-regular run.json.",
            modelSelection,
            workflow: {
              name: "SqlBadMeta",
              runHandles: {
                runId: sqlBadMetaRunId,
                scriptPath: sqlBadMetaScript,
                transcriptDir: sqlBadMetaDir,
              },
              phases: [],
              agents: [],
            },
          });
        }).pipe(Effect.provide(harness1.layerToolkit));

        const sqlBadMetaExit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
          Effect.exit,
          Effect.provide(sweepHarness.layerToolkit),
        );
        expect(Exit.isFailure(sqlBadMetaExit)).toBe(true);

        // 7. Replace run.json with a valid file, but put a non-regular (FIFO/dir) canonical journal.jsonl:
        //    SQL-owned recovery MUST still fail closed without blocking.
        yield* fs.remove(sqlBadMetaPath, { recursive: true });
        const sqlBadJournalPath = path.join(sqlBadMetaDir, "journal.jsonl");
        if (isPosix) {
          NodeChildProcess.execFileSync("mkfifo", [sqlBadJournalPath]);
        } else {
          yield* fs.makeDirectory(sqlBadJournalPath, { recursive: true });
        }
        yield* fs.writeFileString(
          sqlBadMetaPath,
          JSON.stringify({
            runId: sqlBadMetaRunId,
            threadId: parentThreadId,
            taskId: null,
            name: "SqlBadMeta",
            scriptPath: sqlBadMetaScript,
            journalPath: sqlBadJournalPath,
            startedAt: 0,
            status: "running",
            usage: zeroUsage,
          }),
        );
        const sqlBadJournalExit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
          Effect.exit,
          Effect.provide(sweepHarness.layerToolkit),
        );
        expect(Exit.isFailure(sqlBadJournalExit)).toBe(true);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "startup sweep dispatches stops with bounded concurrency under one startup confirmation budget and finishes unconfirmed children in the background worker",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-fanout-sweep-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const childCount = 12;
        const harness1 = yield* makeWorkflowHarness({
          name: "fanout-sweep-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "fanout-sweep-seed",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = "wf_fanout000001";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "FanoutSweep", description: "Many unconfirmed children." }; return 1;`,
          );
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:fanout-sweep:start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "FanoutSweep",
            description: "Many unconfirmed children.",
            modelSelection,
            workflow: {
              name: "FanoutSweep",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing fanout workflow task");
          }
          const taskId = taskEvent.event.payload.id;

          const childThreadIds: Array<ThreadId> = [];
          for (let i = 0; i < childCount; i += 1) {
            const child = yield* mcpService.delegateWorkflowAgent(parentScope, {
              parentRunId: parentRun.id,
              workflowNodeId: taskId,
              requestKey: `${runId}:${i}`,
              task: { task: `Fanout child ${i}`, title: `Child ${i}` },
            });
            yield* Queue.take(harness1.turnQueue);
            childThreadIds.push(child.childThreadId);
          }

          const metaPath = path.join(dir, "run.json");
          yield* fs.writeFileString(
            metaPath,
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "FanoutSweep",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: childCount,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
          return { runId, taskId, metaPath, childThreadIds };
        }).pipe(Effect.provide(harness1.layerToolkit));

        let holdUnconfirmed = true;
        let blockPostDeadlineReads = false;
        let sweepDeadlineMs = Number.POSITIVE_INFINITY;
        let inFlightStops = 0;
        let maxInFlightStops = 0;
        const dispatchedChildIds = new Set<ThreadId>();
        const confirmStreamChildIds = new Set<ThreadId>();
        const batchConcurrencyGate = yield* Deferred.make<void>();
        const allDispatched = yield* Deferred.make<void>();
        const confirmWaitStarted = yield* Deferred.make<void>();
        const postDeadlineReadAttempted = yield* Deferred.make<void>();
        const finalMetaWritten = yield* Deferred.make<void>();
        const childThreadIdSet = new Set<ThreadId>(seeded.childThreadIds);

        const harness2 = yield* makeWorkflowHarness({
          name: "fanout-sweep-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              fsService
                .rename(oldPath, newPath)
                .pipe(
                  Effect.tap(() =>
                    !holdUnconfirmed && newPath === seeded.metaPath
                      ? Deferred.succeed(finalMetaWritten, undefined)
                      : Effect.void,
                  ),
                ),
          }),
          wrapThreadManagement: (tm) => ({
            ...tm,
            streamStoredEventsFrom: (input) => {
              if (
                holdUnconfirmed &&
                input?.threadId !== undefined &&
                childThreadIdSet.has(input.threadId)
              ) {
                confirmStreamChildIds.add(input.threadId);
                if (confirmStreamChildIds.size >= Math.min(childCount, 8)) {
                  Deferred.doneUnsafe(confirmWaitStarted, Exit.void);
                }
              }
              return tm.streamStoredEventsFrom(input);
            },
            getThreadRecords: ((threadId, fields, filter) =>
              Effect.gen(function* () {
                const now = yield* Clock.currentTimeMillis;
                if (
                  blockPostDeadlineReads &&
                  now >= sweepDeadlineMs &&
                  childThreadIdSet.has(threadId)
                ) {
                  yield* Deferred.succeed(postDeadlineReadAttempted, undefined);
                  return yield* Effect.never;
                }
                const records = yield* tm.getThreadRecords(threadId, fields, filter);
                if (
                  !holdUnconfirmed ||
                  !childThreadIdSet.has(threadId) ||
                  !("providerThreads" in records) ||
                  !Array.isArray(records.providerThreads)
                ) {
                  return records;
                }
                // Keep pending background work visible while holdUnconfirmed is true.
                return {
                  ...records,
                  providerThreads: records.providerThreads.map((pt) => ({
                    ...pt,
                    pendingBackgroundTasks: [
                      {
                        taskId: `held-${threadId}`,
                        kind: "command",
                        description: "still stopping",
                      },
                    ],
                  })),
                };
              })) as typeof tm.getThreadRecords,
            cancelDelegatedTask: (input) =>
              Effect.gen(function* () {
                if (childThreadIdSet.has(input.command.threadId) && holdUnconfirmed) {
                  inFlightStops += 1;
                  if (inFlightStops > maxInFlightStops) {
                    maxInFlightStops = inFlightStops;
                  }
                  if (inFlightStops >= 2) {
                    yield* Deferred.succeed(batchConcurrencyGate, undefined);
                  }
                  if (dispatchedChildIds.size < 2) {
                    // Let a sibling overlap briefly to observe bounded concurrency without relying on wall-clock time.
                    yield* Deferred.await(batchConcurrencyGate);
                  }
                }
                return yield* tm.cancelDelegatedTask(input);
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    if (childThreadIdSet.has(input.command.threadId) && holdUnconfirmed) {
                      inFlightStops = Math.max(0, inFlightStops - 1);
                      dispatchedChildIds.add(input.command.threadId);
                      if (dispatchedChildIds.size === childCount) {
                        Deferred.doneUnsafe(allDispatched, Exit.void);
                      }
                    }
                  }),
                ),
              ),
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const beforeRecoverSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const sweepFiber = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
            Effect.forkScoped,
          );
          yield* Deferred.await(allDispatched);
          yield* Deferred.await(confirmWaitStarted);
          expect(dispatchedChildIds.size).toBe(childCount);
          expect(maxInFlightStops).toBeGreaterThan(1);
          expect(maxInFlightStops).toBeLessThanOrEqual(8);

          // A projection read after the 2s deadline that never resolves must not hold startup.
          sweepDeadlineMs = (yield* Clock.currentTimeMillis) + 2_000;
          blockPostDeadlineReads = true;
          yield* TestClock.adjust("2 seconds");
          const sweepOutcome = yield* Effect.raceFirst(
            Fiber.join(sweepFiber).pipe(Effect.as("settled" as const)),
            Deferred.await(postDeadlineReadAttempted).pipe(
              Effect.as("post-deadline-read-hung" as const),
            ),
          );
          expect(sweepOutcome).toBe("settled");
          expect(yield* Deferred.isDone(postDeadlineReadAttempted)).toBe(false);

          const midTask = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
            (s) => s.id === seeded.taskId,
          );
          expect(midTask?.status).toBe("running");
          expect(midTask?.workflow?.stopRequestedAt).toBeDefined();
          const midMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
          };
          expect(midMeta.status).toBe("running");
          expect(midMeta.cleanupPending).toBe(true);

          // Once children settle, the background worker completes recovery and clears cleanupPending.
          blockPostDeadlineReads = false;
          holdUnconfirmed = false;
          yield* TestClock.adjust("50 millis");
          const terminal = yield* awaitWorkflowTerminal(seeded.taskId, beforeRecoverSeq);
          expect(terminal.status).toBe("failed");
          yield* Deferred.await(finalMetaWritten);
          const finalMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
          };
          expect(finalMeta.status).toBe("failed");
          expect(finalMeta.cleanupPending).toBeUndefined();
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "recovers, reports status, and resumes a valid workflow journal larger than 64 MiB without failing boot",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-large-valid-journal-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "large-valid-journal-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "large-valid-journal",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = "wf_largevalid01";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          const metaPath = path.join(dir, "run.json");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "LargeValidJournal", description: "Valid journal exceeding 64 MiB." };
            const first = await agent("First cached step");
            const second = await agent("Second large cached step");
            return { first, secondLength: second.length, secondHead: second.slice(0, 8) };`,
          );

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:large-valid-journal:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "LargeValidJournal",
            description: "Valid journal exceeding 64 MiB.",
            modelSelection,
            workflow: {
              name: "LargeValidJournal",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing large-journal workflow task");
          }
          const taskId = taskEvent.event.payload.id;

          // Launch an unfinished child whose started entry appears at the tail of the >64 MiB journal.
          const tailChild = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:20`,
            task: { task: "Tail active child", title: "Tail child" },
          });
          yield* Queue.take(harness1.turnQueue);

          // Build a valid journal > 64 MiB: index 0 (small cached result), 20 x 3.5 MiB valid results, plus unfinished index 21.
          const firstKey = agentCallKey("First cached step", {
            modelSelection: { instanceId, model },
          });
          const secondKey = agentCallKey("Second large cached step", {
            modelSelection: { instanceId, model },
          });
          const largePayload = "a".repeat(3_500_000);
          yield* fs.writeFileString(
            journalPath,
            `${JSON.stringify({
              type: "started",
              index: 0,
              key: firstKey,
              label: "First cached step",
              phase: null,
              agentId: null,
              childThreadId: null,
            })}\n${JSON.stringify({
              type: "result",
              index: 0,
              key: firstKey,
              outcome: "ok",
              value: "cached-from-large-journal",
            })}\n`,
          );
          for (let i = 1; i <= 20; i += 1) {
            const key = i === 1 ? secondKey : `k_${i}`;
            const value = i === 1 ? `bulk-01-${"a".repeat(3_500_000 - 8)}` : largePayload;
            yield* fs.writeFileString(
              journalPath,
              `${JSON.stringify({
                type: "started",
                index: i,
                key,
                label: `Bulk ${i}`,
                phase: null,
                agentId: null,
                childThreadId: null,
              })}\n${JSON.stringify({
                type: "result",
                index: i,
                key,
                outcome: "ok",
                value,
              })}\n`,
              { flag: "a" },
            );
          }
          yield* fs.writeFileString(
            journalPath,
            `${JSON.stringify({
              type: "started",
              index: 21,
              key: "k_21",
              label: "Tail child",
              phase: null,
              agentId: tailChild.taskId,
              childThreadId: tailChild.childThreadId,
            })}\n`,
            { flag: "a" },
          );
          const journalInfo = yield* fs.stat(journalPath);
          expect(Number(journalInfo.size)).toBeGreaterThan(64 * 1024 * 1024);

          yield* fs.writeFileString(
            metaPath,
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "LargeValidJournal",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 22,
                done: 21,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
          return {
            runId,
            taskId,
            scriptPath,
            metaPath,
            tailChildThreadId: tailChild.childThreadId,
          };
        }).pipe(Effect.provide(harness1.layerToolkit));

        const resumeReads: Array<{ readonly offset: number; readonly length: number }> = [];
        let resumeJournalPath = "";
        const harness2 = yield* makeWorkflowHarness({
          name: "large-valid-journal-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapScriptFileOpener: (openFile) => (filePath) =>
            openFile(filePath).pipe(
              Effect.map((handle) => ({
                ...handle,
                ...(handle.readAt === undefined
                  ? {}
                  : {
                      readAt: (offset: number, length: number) => {
                        if (filePath === resumeJournalPath) {
                          resumeReads.push({ offset, length });
                        }
                        return handle.readAt!(offset, length);
                      },
                    }),
              })),
            ),
        });
        yield* Effect.gen(function* () {
          const config = yield* ServerConfig.ServerConfig;
          resumeJournalPath = path.join(config.workflowRunsDir, seeded.runId, "journal.jsonl");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const recoverExit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(Effect.exit);
          expect(Exit.isSuccess(recoverExit)).toBe(true);

          const recoveredTask = (yield* orchestrator.getThreadProjection(
            parentThreadId,
          )).subagents.find((s) => s.id === seeded.taskId);
          expect(recoveredTask?.status).toBe("failed");

          const tailChildProj = yield* orchestrator.getThreadProjection(seeded.tailChildThreadId);
          expect(
            tailChildProj.runs.every(
              (run) => run.status === "cancelled" || run.status === "interrupted",
            ),
          ).toBe(true);

          const statusCall = yield* callTool("t3_workflow_status", { runId: seeded.runId });
          expect(statusCall.isError).not.toBe(true);
          const statusRes = yield* decodeStatusResult(statusCall.structuredContent);
          expect(statusRes.status).toBe("failed");
          expect(statusRes.agents.completed).toBe(21);
          expect(statusRes.agents.failed).toBe(1);

          // Resume from the >64 MiB journal replays index 0 on demand without retaining the 20 x 3.5 MiB bodies.
          const beforeResumeSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);
          const resumeCall = yield* callTool("t3_workflow_run", {
            scriptPath: seeded.scriptPath,
            resumeFromRunId: seeded.runId,
          });
          expect(resumeCall.isError).not.toBe(true);
          const resumed = yield* decodeRunResult(resumeCall.structuredContent);
          const resumeTerminal = yield* awaitWorkflowTerminal(resumed.taskId, beforeResumeSeq);
          expect(resumeTerminal.status).toBe("completed");
          expect(resumeTerminal.result).toContain("cached-from-large-journal");
          expect(resumeTerminal.result).toContain("3500000");
          expect(resumeTerminal.result).toContain("bulk-01-");
          expect(resumeReads).toHaveLength(2);
          expect(resumeReads[0]!.length).toBeLessThan(1_024);
          expect(resumeReads[1]!.length).toBeGreaterThan(3_500_000);
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "startup sweep does not stall when a child's first confirmation read hangs right after stop dispatch",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-stalled-first-confirm-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "stalled-first-confirm-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "stalled-first-confirm",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = "wf_stallfirst01";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          const metaPath = path.join(dir, "run.json");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "StallFirstConfirm", description: "Stalled first confirmation read." }; return 1;`,
          );
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:stalled-first-confirm:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "StallFirstConfirm",
            description: "Stalled first confirmation read.",
            modelSelection,
            workflow: {
              name: "StallFirstConfirm",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing workflow task");
          }
          const taskId = taskEvent.event.payload.id;

          const child = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:0`,
            task: { task: "Stalling child", title: "Stalling child" },
          });
          yield* Queue.take(harness1.turnQueue);

          yield* fs.writeFileString(
            metaPath,
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "StallFirstConfirm",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 1,
                done: 0,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
          return { runId, taskId, metaPath, childThreadId: child.childThreadId };
        }).pipe(Effect.provide(harness1.layerToolkit));

        let stopDispatched = false;
        let stallFirstRead = true;
        const firstReadStalled = yield* Deferred.make<void>();
        const finalMetaWritten = yield* Deferred.make<void>();

        const harness2 = yield* makeWorkflowHarness({
          name: "stalled-first-confirm-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapFileSystem: (fsService) => ({
            ...fsService,
            rename: (oldPath, newPath) =>
              fsService
                .rename(oldPath, newPath)
                .pipe(
                  Effect.tap(() =>
                    !stallFirstRead && newPath === seeded.metaPath
                      ? Deferred.succeed(finalMetaWritten, undefined)
                      : Effect.void,
                  ),
                ),
          }),
          wrapThreadManagement: (tm) => ({
            ...tm,
            cancelDelegatedTask: (input) =>
              tm.cancelDelegatedTask(input).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    if (input.command.threadId === seeded.childThreadId) {
                      stopDispatched = true;
                    }
                  }),
                ),
              ),
            getThreadRecords: ((threadId, fields, filter) =>
              Effect.gen(function* () {
                if (threadId === seeded.childThreadId && stopDispatched && stallFirstRead) {
                  yield* Deferred.succeed(firstReadStalled, undefined);
                  return yield* Effect.never;
                }
                return yield* tm.getThreadRecords(threadId, fields, filter);
              })) as typeof tm.getThreadRecords,
          }),
        });

        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const beforeRecoverSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

          const sweepFiber = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
            Effect.forkScoped,
          );
          yield* Deferred.await(firstReadStalled);
          yield* TestClock.adjust("2 seconds");
          yield* Fiber.join(sweepFiber);

          const midTask = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
            (s) => s.id === seeded.taskId,
          );
          expect(midTask?.status).toBe("running");
          expect(midTask?.workflow?.stopRequestedAt).toBeDefined();
          const midMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
          };
          expect(midMeta.status).toBe("running");
          expect(midMeta.cleanupPending).toBe(true);

          // Once the stalled read clears, background cleanup finishes recovery.
          stallFirstRead = false;
          yield* TestClock.adjust("50 millis");
          const terminal = yield* awaitWorkflowTerminal(seeded.taskId, beforeRecoverSeq);
          expect(terminal.status).toBe("failed");
          yield* Deferred.await(finalMetaWritten);
          const finalMeta = JSON.parse(yield* fs.readFileString(seeded.metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
          };
          expect(finalMeta.status).toBe("failed");
          expect(finalMeta.cleanupPending).toBeUndefined();
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "restart preserves sandbox cleanup gate after unconfirmed shutdown termination until verified process exit",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-sandbox-restart-gate-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const workerModuleUrl = new URL("./sandboxWorker.ts", import.meta.url).href;
        const pinnedWorkerCommand = {
          command: process.execPath,
          args: [
            "-e",
            `setInterval(() => {}, 60_000); import(${JSON.stringify(workerModuleUrl)}).then((m) => m.runWorkflowSandboxWorker());`,
          ],
          env: { ELECTRON_RUN_AS_NODE: "1" },
        };

        let shutdownChild: NodeChildProcess.ChildProcessWithoutNullStreams | undefined;
        let shutdownRealKill: NodeChildProcess.ChildProcessWithoutNullStreams["kill"] | undefined;
        const harness1 = yield* makeWorkflowHarness({
          name: "sandbox-restart-gate-1",
          databaseLayer,
          baseDir: sharedBaseDir,
          wrapSandboxRunner: (runner) => (input) =>
            runner({
              ...input,
              maxKillAttempts: 2,
              killRetryDelayMs: 0,
              spawnCommand: pinnedWorkerCommand,
              spawnChild: (command, args, opts) => {
                const child = NodeChildProcess.spawn(command, args, opts);
                shutdownChild = child;
                shutdownRealKill = child.kill.bind(child);
                child.kill = (() => {
                  const err = new Error("Operation not permitted") as Error & { code: string };
                  err.code = "EPERM";
                  throw err;
                }) as typeof child.kill;
                return child;
              },
            }),
        });

        const serviceScope = yield* Scope.make();
        const serviceContext = yield* Layer.buildWithScope(harness1.layerToolkit, serviceScope);
        yield* Effect.gen(function* () {
          const started = yield* Effect.gen(function* () {
            const config = yield* ServerConfig.ServerConfig;
            yield* startParentThreadAndRun(
              harness1.turnQueue,
              harness1.cwd,
              "sandbox-restart-gate",
            );
            const runCall = yield* callTool("t3_workflow_run", {
              script: `
                export const meta = { name: "SandboxRestartGate", description: "Survives shutdown kill." };
                await agent("Child stopped on restart");
                return "unreachable";
              `,
            });
            expect(runCall.isError).not.toBe(true);
            const result = yield* decodeRunResult(runCall.structuredContent);
            yield* Queue.take(harness1.turnQueue);
            return {
              ...result,
              metaPath: path.join(config.workflowRunsDir, result.runId, "run.json"),
            };
          }).pipe(Effect.provide(serviceContext));

          const workerPid = shutdownChild?.pid;
          expect(workerPid).toBeDefined();

          // Shut down server #1 while sandbox kill fails with EPERM.
          yield* Scope.close(serviceScope, Exit.void);
          expect(getUnconfirmedSandboxChildPids()).toContain(workerPid);

          const metaAfterShutdown = JSON.parse(yield* fs.readFileString(started.metaPath)) as {
            readonly status: string;
            readonly cleanupPending?: boolean;
            readonly sandboxCleanupPending?: {
              readonly pid?: number;
              readonly startTime?: string;
            };
          };
          expect(metaAfterShutdown.status).toBe("running");
          expect(metaAfterShutdown.cleanupPending).toBeUndefined();
          expect(metaAfterShutdown.sandboxCleanupPending?.pid).toBe(workerPid);
          expect(metaAfterShutdown.sandboxCleanupPending?.startTime).toBeDefined();

          // Server #2 restarts while the old sandbox worker is still alive.
          const finalMetaWritten = yield* Deferred.make<void>();
          let sandboxKilled = false;
          const harness2 = yield* makeWorkflowHarness({
            name: "sandbox-restart-gate-2",
            databaseLayer,
            baseDir: sharedBaseDir,
            wrapFileSystem: (fsService) => ({
              ...fsService,
              rename: (oldPath, newPath) =>
                fsService
                  .rename(oldPath, newPath)
                  .pipe(
                    Effect.tap(() =>
                      sandboxKilled && newPath === started.metaPath
                        ? Deferred.succeed(finalMetaWritten, undefined)
                        : Effect.void,
                    ),
                  ),
            }),
          });

          yield* Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            const config = yield* ServerConfig.ServerConfig;
            const beforeRecoverSeq = yield* orchestrator.getThreadEventSequence(parentThreadId);

            // Seed two extra terminal runs: one with unverifiable sandbox identity (missing startTime)
            // and one whose recorded PID matches this live process with a stale startTime (PID reuse).
            const unverifiableRunId = "wf_unverifiable01";
            const unverifiableDir = path.join(config.workflowRunsDir, unverifiableRunId);
            const unverifiableScript = path.join(unverifiableDir, "script.js");
            const unverifiableJournal = path.join(unverifiableDir, "journal.jsonl");
            const unverifiableMetaPath = path.join(unverifiableDir, "run.json");
            yield* fs.makeDirectory(unverifiableDir, { recursive: true });
            yield* fs.writeFileString(
              unverifiableScript,
              `export const meta = { name: "Unverifiable", description: "Missing startTime." }; return 1;`,
            );
            yield* fs.writeFileString(unverifiableJournal, "");
            yield* fs.writeFileString(
              unverifiableMetaPath,
              JSON.stringify({
                runId: unverifiableRunId,
                threadId: parentThreadId,
                taskId: null,
                name: "Unverifiable",
                scriptPath: unverifiableScript,
                journalPath: unverifiableJournal,
                startedAt: 0,
                status: "failed",
                sandboxCleanupPending: { pid: process.pid },
                usage: {
                  agents: 0,
                  done: 0,
                  errors: 0,
                  empty: 0,
                  cached: 0,
                  tokens: 0,
                  durationMs: 0,
                },
              }),
            );

            const recycledRunId = "wf_pidreused01";
            const recycledDir = path.join(config.workflowRunsDir, recycledRunId);
            const recycledScript = path.join(recycledDir, "script.js");
            const recycledJournal = path.join(recycledDir, "journal.jsonl");
            const recycledMetaPath = path.join(recycledDir, "run.json");
            yield* fs.makeDirectory(recycledDir, { recursive: true });
            yield* fs.writeFileString(
              recycledScript,
              `export const meta = { name: "RecycledPid", description: "Stale startTime on live PID." }; return 2;`,
            );
            yield* fs.writeFileString(recycledJournal, "");
            yield* fs.writeFileString(
              recycledMetaPath,
              JSON.stringify({
                runId: recycledRunId,
                threadId: parentThreadId,
                taskId: null,
                name: "RecycledPid",
                scriptPath: recycledScript,
                journalPath: recycledJournal,
                startedAt: 0,
                status: "failed",
                sandboxCleanupPending: { pid: process.pid, startTime: "stale-boot:0" },
                usage: {
                  agents: 0,
                  done: 0,
                  errors: 0,
                  empty: 0,
                  cached: 0,
                  tokens: 0,
                  durationMs: 0,
                },
              }),
            );

            yield* WorkflowService.recoverInterruptedWorkflows;

            // Provider child stopped cleanly, but sandbox exit is still unconfirmed: run MUST stay gated.
            const midTask = (yield* orchestrator.getThreadProjection(
              parentThreadId,
            )).subagents.find((s) => s.id === started.taskId);
            expect(midTask?.status).toBe("running");
            expect(midTask?.workflow?.stopRequestedAt).toBeDefined();

            const midMeta = JSON.parse(yield* fs.readFileString(started.metaPath)) as {
              readonly status: string;
              readonly cleanupPending?: boolean;
              readonly sandboxCleanupPending?: {
                readonly pid?: number;
                readonly startTime?: string;
              };
            };
            expect(midMeta.cleanupPending).toBeUndefined();
            expect(midMeta.sandboxCleanupPending?.pid).toBe(workerPid);

            const refusedResume = yield* callTool("t3_workflow_run", {
              scriptPath: started.scriptPath,
              resumeFromRunId: started.runId,
            });
            expect(declaredFailure(refusedResume).code).toBe("invalid_request");

            const refusedStop = yield* callTool("t3_workflow_stop", { runId: started.runId });
            expect(declaredFailure(refusedStop).code).toBe("orchestration_error");

            // Unverifiable sandbox identity stays fail-closed across recovery and refuses resume.
            const unverifiableAfter = JSON.parse(
              yield* fs.readFileString(unverifiableMetaPath),
            ) as {
              readonly sandboxCleanupPending?: { readonly pid?: number };
            };
            expect(unverifiableAfter.sandboxCleanupPending?.pid).toBe(process.pid);
            const refusedUnverifiable = yield* callTool("t3_workflow_run", {
              scriptPath: unverifiableScript,
              resumeFromRunId: unverifiableRunId,
            });
            expect(declaredFailure(refusedUnverifiable).code).toBe("invalid_request");

            // Stale startTime on a live PID proves the old worker exited (PID reuse) and clears the gate.
            const recycledAfter = JSON.parse(yield* fs.readFileString(recycledMetaPath)) as {
              readonly sandboxCleanupPending?: unknown;
            };
            expect(recycledAfter.sandboxCleanupPending).toBeUndefined();

            // An inspector that reports unverifiable (e.g. unreadable /proc) also stays fail-closed.
            const inspectorUnverifiableExit =
              yield* WorkflowService.recoverInterruptedWorkflows.pipe(
                Effect.provideService(WorkflowService.WorkflowSandboxProcessInspector, {
                  captureIdentity: () => Effect.succeed({ ok: false, reason: "unavailable" }),
                  checkExited: () =>
                    Effect.succeed({ status: "unverifiable", reason: "/proc unreadable" }),
                }),
                Effect.exit,
              );
            expect(Exit.isSuccess(inspectorUnverifiableExit)).toBe(true);
            const stillGatedMeta = JSON.parse(yield* fs.readFileString(started.metaPath)) as {
              readonly sandboxCleanupPending?: { readonly pid?: number };
            };
            expect(stillGatedMeta.sandboxCleanupPending?.pid).toBe(workerPid);

            // Terminate the real sandbox worker so verified identity confirms exit.
            sandboxKilled = true;
            shutdownChild!.kill = shutdownRealKill!;
            yield* Effect.promise(() => reapUnconfirmedSandboxChildren());
            expect(getUnconfirmedSandboxChildPids()).not.toContain(workerPid);

            yield* TestClock.adjust("50 millis");
            const terminal = yield* awaitWorkflowTerminal(started.taskId, beforeRecoverSeq);
            expect(terminal.status).toBe("failed");
            yield* Deferred.await(finalMetaWritten);
            const clearedMeta = JSON.parse(yield* fs.readFileString(started.metaPath)) as {
              readonly status: string;
              readonly sandboxCleanupPending?: unknown;
            };
            expect(clearedMeta.status).toBe("failed");
            expect(clearedMeta.sandboxCleanupPending).toBeUndefined();

            // Resume now succeeds once sandbox exit is verified.
            const resumeCall = yield* callTool("t3_workflow_run", {
              scriptPath: started.scriptPath,
              resumeFromRunId: started.runId,
            });
            expect(resumeCall.isError).not.toBe(true);
          }).pipe(Effect.provide(harness2.layerToolkit));
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (
                shutdownChild !== undefined &&
                shutdownRealKill !== undefined &&
                shutdownChild.exitCode === null &&
                shutdownChild.signalCode === null
              ) {
                shutdownChild.kill = shutdownRealKill;
                shutdownRealKill("SIGKILL");
              }
            }),
          ),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "recovers a journal containing records at the exact 8 MiB shared limit and refuses oversized writes",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sharedBaseDir = yield* fs.realPath(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-workflow-at-limit-journal-" }),
        );
        const sharedDbContext = yield* Layer.build(SqlitePersistence.layerMemory);
        const databaseLayer = Layer.succeedContext(sharedDbContext);

        const harness1 = yield* makeWorkflowHarness({
          name: "at-limit-journal-seed",
          databaseLayer,
          baseDir: sharedBaseDir,
        });

        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const mcpService = yield* OrchestratorMcpService.OrchestratorMcpService;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "at-limit-journal",
          );
          const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (r) => r.id === parentTurn.turn.runId,
          )!;

          const runId = "wf_atlimit0001";
          const dir = path.join(config.workflowRunsDir, runId);
          const scriptPath = path.join(dir, "script.js");
          const journalPath = path.join(dir, "journal.jsonl");
          const metaPath = path.join(dir, "run.json");
          yield* fs.makeDirectory(dir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "AtLimitJournal", description: "Records at exact 8 MiB limit." }; return 1;`,
          );
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
            type: "workflow.start",
            commandId: CommandId.make("command:at-limit-journal:workflow-start"),
            parentThreadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId!,
            title: "AtLimitJournal",
            description: "Records at exact 8 MiB limit.",
            modelSelection,
            workflow: {
              name: "AtLimitJournal",
              runHandles: { runId, scriptPath, transcriptDir: dir },
              phases: [],
              agents: [],
            },
          });
          const taskEvent = started.storedEvents.find(
            (s) => s.event.type === "subagent.updated" && s.event.payload.origin === "app_owned",
          );
          if (taskEvent?.event.type !== "subagent.updated") {
            throw new Error("missing at-limit workflow task");
          }
          const taskId = taskEvent.event.payload.id;

          const tailChild = yield* mcpService.delegateWorkflowAgent(parentScope, {
            parentRunId: parentRun.id,
            workflowNodeId: taskId,
            requestKey: `${runId}:1`,
            task: { task: "Unfinished tail child", title: "Tail child" },
          });
          yield* Queue.take(harness1.turnQueue);

          const buildResult = (filler: string): JournalEntry => ({
            type: "result",
            index: 0,
            key: "k_at_limit",
            outcome: "ok",
            value: filler,
          });
          const overhead = Buffer.byteLength(JSON.stringify(buildResult("")), "utf8");
          const exactLimitEntry = buildResult("a".repeat(MAX_JOURNAL_LINE_BYTES - overhead));
          const oversizedEntry = buildResult("b".repeat(MAX_JOURNAL_LINE_BYTES - overhead + 1));
          expect(Buffer.byteLength(JSON.stringify(exactLimitEntry), "utf8")).toBe(
            MAX_JOURNAL_LINE_BYTES,
          );

          const writer = yield* makeJournalWriter({
            writeLine: (line) => fs.writeFileString(journalPath, line, { flag: "a" }),
          });
          const oversizedExit = yield* writer.append(oversizedEntry).pipe(Effect.exit);
          expect(Exit.isFailure(oversizedExit)).toBe(true);
          if (Exit.isFailure(oversizedExit)) {
            expect(oversizedExit.cause).toBeDefined();
            const failed = yield* writer.append(oversizedEntry).pipe(Effect.flip);
            expect(failed).toBeInstanceOf(JournalRecordTooLargeError);
          }

          expect(yield* writer.append(exactLimitEntry)).toBe(true);
          expect(
            yield* writer.append({
              type: "started",
              index: 1,
              key: "k_tail",
              label: "Tail child",
              phase: null,
              agentId: tailChild.taskId,
              childThreadId: tailChild.childThreadId,
            }),
          ).toBe(true);
          yield* writer.flush;
          yield* writer.close;

          yield* fs.writeFileString(
            metaPath,
            JSON.stringify({
              runId,
              threadId: parentThreadId,
              taskId,
              name: "AtLimitJournal",
              scriptPath,
              journalPath,
              startedAt: 0,
              status: "running",
              usage: {
                agents: 2,
                done: 1,
                errors: 0,
                empty: 0,
                cached: 0,
                tokens: 0,
                durationMs: 0,
              },
            }),
          );
          return { runId, taskId, tailChildThreadId: tailChild.childThreadId };
        }).pipe(Effect.provide(harness1.layerToolkit));

        const harness2 = yield* makeWorkflowHarness({
          name: "at-limit-journal-recover",
          databaseLayer,
          baseDir: sharedBaseDir,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const recoverExit = yield* WorkflowService.recoverInterruptedWorkflows.pipe(Effect.exit);
          expect(Exit.isSuccess(recoverExit)).toBe(true);

          const recoveredTask = (yield* orchestrator.getThreadProjection(
            parentThreadId,
          )).subagents.find((s) => s.id === seeded.taskId);
          expect(recoveredTask?.status).toBe("failed");

          const tailChildProj = yield* orchestrator.getThreadProjection(seeded.tailChildThreadId);
          expect(
            tailChildProj.runs.every(
              (run) => run.status === "cancelled" || run.status === "interrupted",
            ),
          ).toBe(true);
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});
