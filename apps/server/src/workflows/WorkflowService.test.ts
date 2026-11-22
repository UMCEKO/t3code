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
import * as ByteSize from "effect/ByteSize";
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
import { decodeJournal } from "./journal.ts";
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
  readonly wrapThreadManagement?: (
    service: ThreadManagementService.ThreadManagementService["Service"],
  ) => ThreadManagementService.ThreadManagementService["Service"];
  readonly wrapFileSystem?: (service: FileSystem.FileSystem) => FileSystem.FileSystem;
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
    const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
      { name: `workflow-${options.name}`, runtimePolicyOverride: { cwd } },
      layerAdapters,
      {
        databaseLayer: layerDatabase,
        runEffectWorker: options.runEffectWorker ?? true,
        ...(options.recoverOnStartup === true ? { recoverOnStartup: true } : {}),
        ...(options.runContinuationWorker === true ? { runContinuationWorker: true } : {}),
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
      Layer.provide(ServerSettings.layerTest()),
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
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
      Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
      Layer.provide(
        Layer.mock(ProjectService.ProjectService)({ workspaceScanDirectory: Effect.succeed }),
      ),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
      Layer.provideMerge(Layer.mergeAll(layerNodeServices, NodeCrypto.layer)),
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
          const completedDir = path.join(config.workflowsDir, completedRunId);
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
          const unstartedDir = path.join(config.workflowsDir, unstartedRunId);
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
          const dir = path.join(config.workflowsDir, runId);
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
          yield* fs.readFileString(path.join(config.workflowsDir, started.runId, "run.json")),
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
          const metaPath = path.join(config.workflowsDir, started.runId, "run.json");
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
          const termMetaDir = path.join(config.workflowsDir, termMetaRunId);
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
          const termSqlDir = path.join(config.workflowsDir, termSqlRunId);
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
          const dir = path.join(config.workflowsDir, runId);
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

          // Step deterministically through each bounded retry failure before advancing TestClock.
          const sweepFiber = yield* WorkflowService.recoverInterruptedWorkflows.pipe(
            Effect.forkScoped,
          );
          for (const delay of [50, 100, 200, 400, 800]) {
            yield* Queue.take(stopAttemptFailed);
            yield* TestClock.adjust(`${delay} millis`);
          }
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
          const dir = path.join(config.workflowsDir, runId);
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
          for (const delay of [50, 100, 200, 400, 800]) {
            yield* Queue.take(stopAttemptFailed);
            yield* TestClock.adjust(`${delay} millis`);
          }
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
          const dir = path.join(config.workflowsDir, runId);
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
          const orphanDir = path.join(config.workflowsDir, orphanRunId);
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
          wrapFileSystem: (fsService) => ({
            ...fsService,
            open: (filePath, options) =>
              fsService.open(filePath, options).pipe(
                Effect.map((file) => {
                  if (filePath === oversizedPath) {
                    return {
                      ...file,
                      stat: file.stat,
                      read: (buffer, readOptions) => {
                        oversizedReadCalls += 1;
                        return file.read(buffer, readOptions);
                      },
                      readAlloc: (size, readOptions) => {
                        oversizedReadCalls += 1;
                        return file.readAlloc(size, readOptions);
                      },
                    };
                  }
                  if (filePath === growingPath) {
                    return {
                      ...file,
                      stat: file.stat.pipe(
                        Effect.map((info) => ({ ...info, size: ByteSize.bytes(16) })),
                      ),
                      readAlloc: (size, readOptions) =>
                        file.readAlloc(size, readOptions).pipe(
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
          }),
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
        const dir = path.join(config.workflowsDir, runId);
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
        return { runId, taskId, workflowsDir: config.workflowsDir };
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
});
