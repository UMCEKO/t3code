import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
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
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Subagent,
  type ServerProvider,
  type TurnTokenUsage,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
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

const driver = ProviderDriverKind.make("claudeAgent");
const instanceId = ProviderInstanceId.make("claudeAgent");
const model = "claude-test";
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
  ],
  slashCommands: [],
  skills: [],
};

interface ActiveTurnHandle {
  readonly turn: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurnId: ProviderTurnId;
  readonly interrupted: Deferred.Deferred<void>;
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
                  complete: (text, opts) =>
                    Effect.gen(function* () {
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
                turnsByProviderTurnId.set(providerTurnId, { handle, baseProviderTurn, events });
                yield* Ref.update(startedTurns, (list) => [...list, handle]);
                yield* Queue.offer(turnQueue, handle);
              }),
            steerTurn: () => Effect.die("unused"),
            interruptTurn: ({ providerThread, providerTurnId }) =>
              Effect.gen(function* () {
                const entry = turnsByProviderTurnId.get(providerTurnId);
                if (entry === undefined) return;
                const doneAt = yield* DateTime.now;
                yield* Deferred.succeed(entry.handle.interrupted, undefined);
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
        runEffectWorker: true,
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
    const layerOrchestration = Layer.merge(
      layerOrchestrator,
      ThreadManagementService.layer.pipe(
        Layer.provide(layerOrchestrator),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
      ),
    );
    const layerServerConfig = ServerConfig.layerTest(
      cwd,
      options.baseDir ?? { prefix: `t3-workflow-${options.name}-` },
    ).pipe(Layer.provide(NodeServices.layer));
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
      Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
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
    const current = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
      (candidate) => candidate.id === taskId,
    );
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
    return matched.value.event.payload;
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
          phase("p1");
          log("first");
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
        // First progress event fires immediately at T=0.
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
    "startup sweep marks running run.json files from a previous process as failed with a resume hint",
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

        // Seed a running workflow in SQLite and write its run.json with status "running".
        const seeded = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const config = yield* ServerConfig.ServerConfig;
          const parentTurn = yield* startParentThreadAndRun(
            harness1.turnQueue,
            harness1.cwd,
            "sweep",
          );
          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const parentRun = parentProjection.runs.find((r) => r.id === parentTurn.turn.runId)!;
          const staleRunId = "wf_deadbeef1234";
          const runDir = path.join(config.workflowRunsDir, staleRunId);
          const scriptPath = path.join(runDir, "script.js");
          const journalPath = path.join(runDir, "journal.jsonl");
          yield* fs.makeDirectory(runDir, { recursive: true });
          yield* fs.writeFileString(
            scriptPath,
            `export const meta = { name: "Stale", description: "Interrupted by restart." }; return 1;`,
          );
          yield* fs.writeFileString(journalPath, "");

          const started = yield* orchestrator.dispatch({
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
              runHandles: { runId: staleRunId, scriptPath, transcriptDir: runDir },
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
            path.join(runDir, "run.json"),
            JSON.stringify({
              runId: staleRunId,
              threadId: parentThreadId,
              taskId,
              name: "Stale",
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
          return { staleRunId, taskId };
        }).pipe(Effect.provide(harness1.layerToolkit));

        // Build a fresh WorkflowService layer against the same workflowsDir and database.
        const harness2 = yield* makeWorkflowHarness({
          name: "sweep-2",
          databaseLayer,
          baseDir: sharedBaseDir,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const statusCall = yield* callTool("t3_workflow_status", { runId: seeded.staleRunId });
          expect(statusCall.isError).not.toBe(true);
          const status = yield* decodeStatusResult(statusCall.structuredContent);
          expect(status.status).toBe("failed");
          expect(status.error).toContain(`resumeFromRunId=${seeded.staleRunId}`);

          const parentProjection = yield* orchestrator.getThreadProjection(parentThreadId);
          const task = parentProjection.subagents.find((s) => s.id === seeded.taskId);
          expect(task?.status).toBe("failed");
          expect(task?.result).toContain(`resumeFromRunId=${seeded.staleRunId}`);
        }).pipe(Effect.provide(harness2.layerToolkit));
      }).pipe(Effect.provide(NodeServices.layer)),
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
});
