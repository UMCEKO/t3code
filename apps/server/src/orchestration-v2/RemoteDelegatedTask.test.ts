import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import { DispatchModeLimit } from "./DispatchModeLimit.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

// A delegated task whose child runs in a linked environment is recorded on
// its parent without a child thread here, and completing it wakes the parent
// the way a local child's result does.

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for delegated task records"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

/** Every parent wake the orchestrator offers, for the tests to read. */
class Offers extends Context.Service<
  Offers,
  Ref.Ref<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>
>()("t3/orchestration-v2/RemoteDelegatedTask.test/Offers") {}
const layerOffers = Layer.effect(
  Offers,
  Ref.make<ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>>([]),
);
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "remote-delegated-task" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
).pipe(
  Layer.provide(
    Layer.effect(
      ProviderContinuationRequests.ProviderContinuationRequests,
      Offers.pipe(
        Effect.map((offers) => ({
          offer: (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
            Ref.update(offers, (seen) => [...seen, request]),
          take: Effect.never,
        })),
      ),
    ),
  ),
  Layer.provideMerge(layerOffers),
);

/** A parent thread whose run is live, as when its agent calls delegate_task. */
const seedLiveParent = (name: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make(`thread:${name}`);
    const runId = RunId.make(`run:${name}`);
    const rootNodeId = NodeId.make(`node:${name}:root`);
    const providerThreadId = ProviderThreadId.make(`provider-thread:${name}`);
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:create:${name}`),
      threadId,
      projectId: ProjectId.make("project:remote-delegated-task"),
      title: "Parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    yield* sink.write({
      commandId: CommandId.make(`command:seed:${name}`),
      events: [
        {
          id: EventId.make(`event:seed-provider-thread:${name}`),
          type: "provider-thread.updated",
          threadId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: threadId,
            ownerNodeId: rootNodeId,
            nativeThreadRef: { driver, nativeId: `native:${name}`, strength: "strong" },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:seed-node:${name}`),
          type: "node.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: rootNodeId,
            threadId,
            runId,
            parentNodeId: null,
            rootNodeId,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        },
        {
          id: EventId.make(`event:seed-run:${name}`),
          type: "run.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: runId,
            threadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make(`message:seed-user:${name}`),
            rootNodeId,
            activeAttemptId: null,
            status: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
      ],
    });
    return { threadId, runId, rootNodeId };
  });

const remoteChild = {
  environmentId: EnvironmentId.make("environment:box"),
  threadId: ThreadId.make("thread:on-the-box"),
  label: "Box",
};

it.layer(layerTest)("a delegated task in a linked environment", (it) => {
  it.effect("is recorded on its parent without a child thread here, once", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedLiveParent("records");
      const request = {
        type: "delegated_task.remote.request" as const,
        commandId: CommandId.make("command:remote-delegate:records"),
        parentThreadId: parent.threadId,
        parentRunId: parent.runId,
        parentNodeId: parent.rootNodeId,
        task: "Run the suite there.",
        driver: ProviderDriverKind.make("remote"),
        modelSelection,
        remoteChild,
        completionWake: "always" as const,
      };
      yield* orchestrator.dispatch(request);
      yield* orchestrator.dispatch(request);
      const projection = yield* orchestrator.getThreadProjection(parent.threadId);
      assert.equal(projection.subagents.length, 1);
      const [task] = projection.subagents;
      assert.equal(task?.childThreadId, null);
      assert.deepEqual(task?.remoteChild, remoteChild);
      assert.deepEqual(task?.modelSelection, modelSelection);
      assert.equal(task?.status, "running");
      const item = projection.turnItems.find((candidate) => candidate.type === "subagent");
      assert.deepEqual(item?.type === "subagent" ? item.remoteChild : undefined, remoteChild);
      // No thread was created here for the child.
      assert.equal(yield* orchestrator.getThreadShell(remoteChild.threadId), null);
      const other = yield* seedLiveParent("records-no-task");
      const index = yield* (yield* ProjectionStore.ProjectionStoreV2).listActiveShellIndex();
      assert.isTrue(index.find((entry) => entry.id === parent.threadId)?.hasRemoteTasks);
      assert.isFalse(index.find((entry) => entry.id === other.threadId)?.hasRemoteTasks);
      // Nothing here can cancel the task there, so it must not block archiving.
      yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("command:archive:records"),
        threadId: parent.threadId,
      });
      assert.isNotNull((yield* orchestrator.getThreadShell(parent.threadId))?.archivedAt);
    }),
  );

  it.effect("keeps a newer stop recoverable after an older acknowledgement", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const parent = yield* seedLiveParent("stop-generation");
      yield* orchestrator.dispatch({
        type: "delegated_task.remote.request",
        commandId: CommandId.make("command:remote-delegate:stop-generation"),
        parentThreadId: parent.threadId,
        parentRunId: parent.runId,
        parentNodeId: parent.rootNodeId,
        task: "Run a check.",
        driver: ProviderDriverKind.make("remote"),
        modelSelection,
        remoteChild,
      });
      const task = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents[0]!;
      const openFor = projections.getOpenRemoteDelegatedTasks.pipe(
        Effect.map((open) => open.filter((entry) => entry.parentThreadId === parent.threadId)),
      );
      const complete = {
        type: "delegated_task.remote.complete" as const,
        parentThreadId: parent.threadId,
        taskId: task.id,
        status: "completed" as const,
        result: "Original result.",
      };
      yield* orchestrator.dispatch({
        ...complete,
        commandId: CommandId.make("command:result:stop-generation"),
        stoppedThere: "ran-out",
      });
      for (const count of [1, 2]) {
        yield* orchestrator.dispatch({
          type: "delegated_task.remote.stop",
          parentThreadId: parent.threadId,
          taskId: task.id,
          commandId: CommandId.make(`command:stop-generation:${count}`),
        });
      }
      // Reports from before generation tracking cannot clear a later numbered Stop either.
      for (const stopCount of [undefined, 1]) {
        const rejected = yield* orchestrator
          .dispatch({
            ...complete,
            commandId: CommandId.make(`command:stale-stop:${stopCount}`),
            stoppedThere: "stopped",
            ...(stopCount === undefined ? {} : { stopCount }),
          })
          .pipe(Effect.flip);
        assert.equal(rejected._tag, "OrchestratorDispatchError");
        assert.deepEqual(yield* openFor, [{ parentThreadId: parent.threadId, taskId: task.id }]);
      }
      yield* orchestrator.dispatch({
        ...complete,
        commandId: CommandId.make("command:current-stop:2"),
        stoppedThere: "stopped",
        stopCount: 2,
      });
      const settled = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents[0]!;
      assert.equal(settled.result, "Original result.");
      assert.equal(settled.status, "completed");
      assert.equal(settled.remoteChild?.stoppedThere, true);
      assert.deepEqual(yield* openFor, []);
    }),
  );

  it.effect("owes a stop there for a Stop here that loses the race to its result", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const parent = yield* seedLiveParent("stop-after-result");
      yield* orchestrator.dispatch({
        type: "delegated_task.remote.request",
        commandId: CommandId.make("command:remote-delegate:stop-after-result"),
        parentThreadId: parent.threadId,
        parentRunId: parent.runId,
        parentNodeId: parent.rootNodeId,
        task: "Run a check.",
        driver: ProviderDriverKind.make("remote"),
        modelSelection,
        remoteChild,
      });
      const task = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents[0]!;
      const complete = {
        type: "delegated_task.remote.complete" as const,
        parentThreadId: parent.threadId,
        taskId: task.id,
      };
      yield* orchestrator.dispatch({
        ...complete,
        commandId: CommandId.make("command:result:stop-after-result"),
        status: "completed",
        result: "Done there.",
        stoppedThere: "ran-out",
      });
      // The parent's Stop read the task before its result landed.
      yield* orchestrator.dispatch({
        ...complete,
        commandId: CommandId.make("command:parent-stop:stop-after-result"),
        status: "interrupted",
        result: "Stopped with its parent.",
      });

      const stopped = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents[0]!;
      assert.equal(stopped.status, "completed");
      assert.equal(stopped.result, "Done there.");
      assert.equal(stopped.remoteChild?.stoppedThere, false);
      assert.equal(stopped.remoteChild?.stopCount, 1);
      const open = yield* projections.getOpenRemoteDelegatedTasks;
      assert.deepEqual(
        open.filter((entry) => entry.parentThreadId === parent.threadId),
        [{ parentThreadId: parent.threadId, taskId: task.id }],
      );
    }),
  );

  it.effect("wakes its parent with the result when it completes there", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedLiveParent("completes");
      const recorded = yield* orchestrator.dispatch({
        type: "delegated_task.remote.request",
        commandId: CommandId.make("command:remote-delegate:completes"),
        parentThreadId: parent.threadId,
        parentRunId: parent.runId,
        parentNodeId: parent.rootNodeId,
        task: "Run the suite there.",
        driver: ProviderDriverKind.make("remote"),
        modelSelection,
        remoteChild,
        completionWake: "always",
      });
      const taskEvent = recorded.storedEvents.find(
        (stored) => stored.event.type === "subagent.updated",
      );
      const taskId =
        taskEvent?.event.type === "subagent.updated" ? taskEvent.event.payload.id : undefined;
      assert.isDefined(taskId);
      // Until it completes, it is open, and a restarted follower picks it up.
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const openFor = projections.getOpenRemoteDelegatedTasks.pipe(
        Effect.map((open) => open.filter((task) => task.parentThreadId === parent.threadId)),
      );
      assert.deepEqual(yield* openFor, [{ parentThreadId: parent.threadId, taskId: taskId! }]);
      const offers = yield* Offers;
      yield* Ref.set(offers, []);
      const complete = (result: string) =>
        orchestrator.dispatch({
          type: "delegated_task.remote.complete",
          commandId: CommandId.make(`command:remote-complete:${result}`),
          parentThreadId: parent.threadId,
          taskId: taskId!,
          status: "completed",
          result,
          stoppedThere: "ran-out",
        });
      yield* complete("All 412 tests pass on the box.");
      // A second report is refused, so the first result stands.
      const late = yield* complete("A late, different answer.").pipe(Effect.flip);
      assert.equal(late._tag, "OrchestratorDispatchError");

      const projection = yield* orchestrator.getThreadProjection(parent.threadId);
      const task = projection.subagents.find((candidate) => candidate.id === taskId);
      assert.equal(task?.status, "completed");
      assert.equal(task?.result, "All 412 tests pass on the box.");
      // The live parent run claimed it, so task_status acknowledges it as for a local child.
      assert.equal(task?.completionDelivery?.state, "claimed");
      // Nothing is left for the follower to pick up after a restart.
      assert.deepEqual(yield* openFor, []);
      const item = projection.turnItems.find(
        (candidate) => candidate.type === "subagent" && candidate.subagentId === taskId,
      );
      assert.equal(item?.status, "completed");
      // The parent's live run is offered the delivery, as for a local child.
      const delivery = projection.runs.find((run) => run.id === parent.runId)?.delegatedCompletion
        ?.delivery;
      assert.deepEqual(delivery?.taskIds, [taskId]);
      const offered = yield* Ref.get(offers);
      assert.equal(offered.length, 1);
      assert.equal(offered[0]?.threadId, parent.threadId);
    }),
  );
});

const layerWorkflowTest = ThreadManagementService.layer.pipe(
  Layer.provideMerge(layerTest),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
);

const sampleWorkflow = {
  name: "Review workflow",
  runHandles: {
    runId: "wf_0123456789ab",
    scriptPath: "/tmp/wf/script.js",
    transcriptDir: "/tmp/wf",
  },
  phases: [{ index: 0, title: "Inspect" }],
  agents: [],
};

const completeParentRun = (threadId: ThreadId, runId: RunId, key: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const run = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
      (candidate) => candidate.id === runId,
    )!;
    yield* sink.write({
      commandId: CommandId.make(`command:complete-parent:${key}`),
      events: [
        {
          id: EventId.make(`event:complete-parent:${key}`),
          type: "run.updated",
          threadId,
          runId,
          ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        },
      ],
    });
  });

const completeChildRun = (
  childThreadId: ThreadId,
  runId: RunId,
  assistantText: string,
  key: string,
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const projection = yield* orchestrator.getThreadProjection(childThreadId);
    const run = projection.runs.find((candidate) => candidate.id === runId)!;
    const messageId = MessageId.make(`message:child-assistant:${key}`);
    yield* sink.write({
      commandId: CommandId.make(`command:complete-child:${key}`),
      events: [
        {
          id: EventId.make(`event:child-assistant-msg:${key}`),
          type: "message.updated",
          threadId: childThreadId,
          runId: run.id,
          ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: {
            createdBy: "agent",
            creationSource: "provider",
            id: messageId,
            threadId: childThreadId,
            runId: run.id,
            nodeId: run.rootNodeId,
            role: "assistant",
            text: assistantText,
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:child-assistant-item:${key}`),
          type: "turn-item.updated",
          threadId: childThreadId,
          runId: run.id,
          ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: {
            id: TurnItemId.make(`turn-item:child-assistant:${key}`),
            threadId: childThreadId,
            runId: run.id,
            nodeId: run.rootNodeId,
            providerThreadId: run.providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: projection.turnItems.length + 1,
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "assistant_message",
            messageId,
            text: assistantText,
            streaming: false,
          },
        },
        {
          id: EventId.make(`event:child-run-complete:${key}`),
          type: "run.updated",
          threadId: childThreadId,
          runId: run.id,
          ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...run, status: "completed", startedAt: now, completedAt: now },
        },
      ],
    });
  });

const awaitSubagentStatus = (taskId: NodeId, status: string, afterSequence: number) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    return yield* sink.stream({ afterSequence, eventType: "subagent.updated" }).pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          stored.event.payload.status === status,
      ),
      Stream.take(1),
      Stream.runDrain,
    );
  });

const startWorkflow = (
  parent: { readonly threadId: ThreadId; readonly runId: RunId; readonly rootNodeId: NodeId },
  key: string,
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const command = {
      type: "workflow.start" as const,
      commandId: CommandId.make(`command:workflow-start:${key}`),
      parentThreadId: parent.threadId,
      parentRunId: parent.runId,
      parentNodeId: parent.rootNodeId,
      title: "Review workflow",
      description: "Inspect changes across modules.",
      modelSelection,
      workflow: sampleWorkflow,
    };
    const recorded = yield* orchestrator.dispatch(command);
    const taskEvent = recorded.storedEvents.find(
      (stored) => stored.event.type === "subagent.updated",
    );
    const taskId =
      taskEvent?.event.type === "subagent.updated" ? taskEvent.event.payload.id : undefined;
    assert.isDefined(taskId);
    return { command, recorded, taskId: taskId! };
  });

it.layer(layerWorkflowTest)("a server workflow task", (it) => {
  it.effect(
    "records workflow.start once, updates workflow.progress, and treats terminal updates as no-ops",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent("wf-lifecycle");
        const { command, taskId } = yield* startWorkflow(parent, "wf-lifecycle");

        // Retry with the same commandId is idempotent and records nothing new.
        const replay = yield* orchestrator.dispatch(command);
        assert.isTrue(replay.replayed);

        const started = yield* orchestrator.getThreadProjection(parent.threadId);
        assert.equal(started.subagents.length, 1);
        const task = started.subagents[0]!;
        assert.equal(task.id, taskId);
        assert.equal(task.origin, "app_owned");
        assert.equal(task.childThreadId, null);
        assert.equal(task.nativeTaskRef, null);
        assert.equal(task.driver, driver);
        assert.equal(task.providerInstanceId, modelSelection.instanceId);
        assert.equal(task.prompt, "Inspect changes across modules.");
        assert.equal(task.title, "Review workflow");
        assert.equal(task.model, modelSelection.model);
        assert.deepEqual(task.modelSelection, modelSelection);
        assert.equal(task.completionWake, "always");
        assert.equal(task.status, "running");
        assert.deepEqual(task.workflow, sampleWorkflow);
        assert.equal(task.result, null);

        const node = started.nodes.find((candidate) => candidate.id === taskId);
        assert.equal(node?.kind, "subagent");
        assert.equal(node?.countsForRun, false);
        assert.equal(node?.parentNodeId, parent.rootNodeId);
        assert.equal(node?.status, "running");

        const item = started.turnItems.find(
          (candidate) => candidate.type === "subagent" && candidate.subagentId === taskId,
        );
        assert.equal(item?.status, "running");
        assert.equal(item?.type === "subagent" ? item.childThreadId : undefined, null);

        const progressedWorkflow = {
          ...sampleWorkflow,
          agents: [{ index: 0, label: "Check A", state: "running" as const }],
        };
        const progressResult = yield* orchestrator.dispatch({
          type: "workflow.progress",
          commandId: CommandId.make("command:wf-progress:1"),
          parentThreadId: parent.threadId,
          taskId,
          workflow: progressedWorkflow,
          progress: "Checking module A",
        });
        assert.deepEqual(
          progressResult.storedEvents.map((stored) => stored.event.type),
          ["subagent.updated"],
        );
        const afterProgress = (yield* orchestrator.getThreadProjection(parent.threadId))
          .subagents[0]!;
        assert.deepEqual(afterProgress.workflow, progressedWorkflow);
        assert.equal(afterProgress.progress, "Checking module A");

        const missingProgress = yield* orchestrator
          .dispatch({
            type: "workflow.progress",
            commandId: CommandId.make("command:wf-progress:missing"),
            parentThreadId: parent.threadId,
            taskId: NodeId.make("node:missing-workflow"),
            workflow: progressedWorkflow,
          })
          .pipe(Effect.flip);
        assert.equal(missingProgress._tag, "OrchestratorDispatchError");

        const finalWorkflow = {
          ...sampleWorkflow,
          agents: [{ index: 0, label: "Check A", state: "completed" as const }],
        };
        yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-complete:1"),
          parentThreadId: parent.threadId,
          taskId,
          status: "completed",
          result: "Workflow done.",
          workflow: finalWorkflow,
        });

        // Terminal progress and duplicate completions are no-ops, preserving the first result.
        const lateProgress = yield* orchestrator.dispatch({
          type: "workflow.progress",
          commandId: CommandId.make("command:wf-progress:late"),
          parentThreadId: parent.threadId,
          taskId,
          workflow: sampleWorkflow,
          progress: "Should be ignored",
        });
        assert.lengthOf(lateProgress.storedEvents, 0);

        const lateComplete = yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-complete:late"),
          parentThreadId: parent.threadId,
          taskId,
          status: "failed",
          result: "Late overwrite",
        });
        assert.lengthOf(lateComplete.storedEvents, 0);

        const finished = yield* orchestrator.getThreadProjection(parent.threadId);
        const finishedTask = finished.subagents.find((candidate) => candidate.id === taskId);
        assert.equal(finishedTask?.status, "completed");
        assert.equal(finishedTask?.result, "Workflow done.");
        assert.deepEqual(finishedTask?.workflow, finalWorkflow);
        assert.equal(finishedTask?.progress, "Checking module A");
        assert.equal(
          finished.nodes.find((candidate) => candidate.id === taskId)?.status,
          "completed",
        );
        const finishedItem = finished.turnItems.find(
          (candidate) => candidate.type === "subagent" && candidate.subagentId === taskId,
        );
        assert.equal(finishedItem?.status, "completed");
        assert.equal(
          finishedItem?.type === "subagent" ? finishedItem.result : undefined,
          "Workflow done.",
        );
      }),
  );

  it.effect(
    "accepts delegated_task.request under a running workflow node after parent run ends, and rejects a finished workflow",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent("wf-parenting");
        const { taskId: workflowId } = yield* startWorkflow(parent, "wf-parenting");

        yield* completeParentRun(parent.threadId, parent.runId, "wf-parenting");

        yield* orchestrator.dispatch({
          type: "delegated_task.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-parenting:under-workflow"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: workflowId,
          task: "Run after parent turn ended.",
          title: "Worker 1",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          completionWake: "never",
        });

        const projection = yield* orchestrator.getThreadProjection(parent.threadId);
        const childTask = projection.subagents.find(
          (candidate) => candidate.parentNodeId === workflowId,
        );
        assert.isDefined(childTask);
        assert.equal(childTask?.completionWake, "never");
        assert.isNotNull(childTask?.childThreadId);

        // Once the workflow ends, new delegations under its node are rejected too.
        yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-parenting:complete"),
          parentThreadId: parent.threadId,
          taskId: workflowId,
          status: "completed",
          result: "Done.",
        });
        const afterEnd = yield* orchestrator
          .dispatch({
            type: "delegated_task.request",
            createdBy: "agent",
            creationSource: "mcp",
            commandId: CommandId.make("command:wf-parenting:after-end"),
            parentThreadId: parent.threadId,
            parentRunId: parent.runId,
            parentNodeId: workflowId,
            task: "Should fail after workflow ends.",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            completionWake: "never",
          })
          .pipe(Effect.flip);
        assert.equal(afterEnd._tag, "OrchestratorDispatchError");
      }),
  );

  it.effect(
    "suppresses parent wakes for completionWake never children and follow-ups, while workflow.complete wakes the parent",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const offers = yield* Offers;
        const parent = yield* seedLiveParent("wf-wake-never");
        const { taskId: workflowId } = yield* startWorkflow(parent, "wf-wake-never");

        yield* completeParentRun(parent.threadId, parent.runId, "wf-wake-never");
        yield* Ref.set(offers, []);

        yield* orchestrator.dispatch({
          type: "delegated_task.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-wake-never:child"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: workflowId,
          task: "Produce structured output.",
          title: "Agent 1",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          completionWake: "never",
        });

        const childTask = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
          (candidate) => candidate.parentNodeId === workflowId,
        )!;
        const childThreadId = childTask.childThreadId!;
        const firstChildRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]!;

        const beforeFirstComplete = yield* sink.latestSequence();
        yield* completeChildRun(childThreadId, firstChildRun.id, "Not valid JSON", "wf-never-1");
        yield* awaitSubagentStatus(childTask.id, "completed", beforeFirstComplete);
        yield* orchestrator.recoverDelegatedTask(childThreadId);

        const afterChild = yield* orchestrator.getThreadProjection(parent.threadId);
        const settledChild = afterChild.subagents.find(
          (candidate) => candidate.id === childTask.id,
        );
        assert.equal(settledChild?.status, "completed");
        assert.deepEqual(settledChild?.completionDelivery, {
          state: "disposed",
          observedByRunId: null,
        });
        assert.isUndefined(
          afterChild.runs.find((run) => run.id === parent.runId)?.delegatedCompletion,
        );
        assert.lengthOf(yield* Ref.get(offers), 0);

        // A follow-up turn into the same child thread (e.g. schema re-prompt) inherits never.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-wake-never:reprompt"),
          threadId: childThreadId,
          senderThreadId: parent.threadId,
          messageId: MessageId.make("message:wf-wake-never:reprompt"),
          text: "Your result did not match the schema.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
        });
        const followupTask = (yield* orchestrator.getThreadProjection(
          parent.threadId,
        )).subagents.find(
          (candidate) => candidate.childThreadId === childThreadId && candidate.id !== childTask.id,
        )!;
        assert.isDefined(followupTask);
        assert.equal(followupTask.completionWake, "never");
        assert.equal(followupTask.parentNodeId, workflowId);
        const followupRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs.find(
          (run) => run.delegatedTaskId === followupTask.id,
        )!;
        const beforeFollowupComplete = yield* sink.latestSequence();
        yield* completeChildRun(childThreadId, followupRun.id, '{"ok":true}', "wf-never-2");
        yield* awaitSubagentStatus(followupTask.id, "completed", beforeFollowupComplete);
        yield* orchestrator.recoverDelegatedTask(childThreadId);

        const afterFollowup = yield* orchestrator.getThreadProjection(parent.threadId);
        assert.deepEqual(
          afterFollowup.subagents.find((candidate) => candidate.id === followupTask.id)
            ?.completionDelivery,
          { state: "disposed", observedByRunId: null },
        );
        assert.lengthOf(yield* Ref.get(offers), 0);

        // Completing the workflow itself wakes the parent as an async delegated task does.
        yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-wake-never:complete"),
          parentThreadId: parent.threadId,
          taskId: workflowId,
          status: "completed",
          result: "Workflow result ready.",
        });

        const afterWorkflow = yield* orchestrator.getThreadProjection(parent.threadId);
        const completedWorkflow = afterWorkflow.subagents.find(
          (candidate) => candidate.id === workflowId,
        );
        assert.equal(completedWorkflow?.status, "completed");
        assert.equal(completedWorkflow?.completionDelivery?.state, "claimed");
        const delivery = afterWorkflow.runs.find((run) => run.id === parent.runId)
          ?.delegatedCompletion?.delivery;
        assert.deepEqual(delivery?.taskIds, [workflowId]);
        const offered = yield* Ref.get(offers);
        assert.equal(offered.length, 1);
        assert.equal(offered[0]?.threadId, parent.threadId);
      }),
  );

  it.effect("cancels running app-owned workflows via stopWorkflow and thread.stop", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const offers = yield* Offers;

      // stopWorkflow enforces mode limits, then cancels an app-owned workflow without deadlocking.
      const parentA = yield* seedLiveParent("wf-stop-service");
      const { taskId: workflowA } = yield* startWorkflow(parentA, "wf-stop-service");
      const refused = yield* threads
        .stopWorkflow({ threadId: parentA.threadId, subagentId: workflowA })
        .pipe(
          Effect.provideService(DispatchModeLimit, {
            runtimeMode: "approval-required",
            interactionMode: "plan",
          }),
          Effect.flip,
        );
      assert.equal(refused._tag, "OrchestrationV2StopWorkflowError");
      assert.equal(refused.reason, "unavailable");

      yield* threads.stopWorkflow({ threadId: parentA.threadId, subagentId: workflowA });
      const stoppedA = yield* orchestrator.getThreadProjection(parentA.threadId);
      const taskA = stoppedA.subagents.find((candidate) => candidate.id === workflowA);
      assert.equal(taskA?.status, "cancelled");
      assert.equal(taskA?.result, "Stopped.");
      assert.equal(
        stoppedA.nodes.find((candidate) => candidate.id === workflowA)?.status,
        "cancelled",
      );
      assert.equal(
        stoppedA.turnItems.find(
          (candidate) => candidate.type === "subagent" && candidate.subagentId === workflowA,
        )?.status,
        "cancelled",
      );

      const secondStop = yield* threads
        .stopWorkflow({ threadId: parentA.threadId, subagentId: workflowA })
        .pipe(Effect.flip);
      assert.equal(secondStop._tag, "OrchestrationV2StopWorkflowError");
      assert.equal(secondStop.reason, "not-running");

      // thread.stop cancels a running app-owned workflow after the parent run ended, without waking.
      const parentB = yield* seedLiveParent("wf-stop-thread-settled");
      const { taskId: workflowB } = yield* startWorkflow(parentB, "wf-stop-thread-settled");
      yield* completeParentRun(parentB.threadId, parentB.runId, "wf-stop-thread-settled");
      yield* Ref.set(offers, []);

      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("command:wf-stop-thread-settled:stop"),
        threadId: parentB.threadId,
      });
      const stoppedB = yield* orchestrator.getThreadProjection(parentB.threadId);
      const taskB = stoppedB.subagents.find((candidate) => candidate.id === workflowB);
      assert.equal(taskB?.status, "cancelled");
      assert.equal(taskB?.result, "Stopped.");
      assert.deepEqual(taskB?.completionDelivery, { state: "disposed", observedByRunId: null });
      assert.equal(
        stoppedB.nodes.find((candidate) => candidate.id === workflowB)?.status,
        "cancelled",
      );
      const itemB = stoppedB.turnItems.find(
        (candidate) => candidate.type === "subagent" && candidate.subagentId === workflowB,
      );
      assert.equal(itemB?.status, "cancelled");
      assert.equal(itemB?.type === "subagent" ? itemB.result : undefined, "Stopped.");
      assert.lengthOf(yield* Ref.get(offers), 0);

      // thread.stop while the parent run is live also cancels the running workflow in the same command.
      const parentC = yield* seedLiveParent("wf-stop-thread-live");
      const { taskId: workflowC } = yield* startWorkflow(parentC, "wf-stop-thread-live");
      yield* Ref.set(offers, []);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("command:wf-stop-thread-live:stop"),
        threadId: parentC.threadId,
      });
      const stoppedC = yield* orchestrator.getThreadProjection(parentC.threadId);
      const taskC = stoppedC.subagents.find((candidate) => candidate.id === workflowC);
      assert.equal(taskC?.status, "cancelled");
      assert.equal(taskC?.result, "Stopped.");
      assert.deepEqual(taskC?.completionDelivery, { state: "disposed", observedByRunId: null });
      assert.lengthOf(yield* Ref.get(offers), 0);
    }),
  );

  it.effect("forwards outputSchema from delegated_task.request to the child's first run", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedLiveParent("wf-output-schema");
      const { taskId: workflowId } = yield* startWorkflow(parent, "wf-output-schema");
      const outputSchema = {
        type: "object",
        properties: {
          passed: { type: "boolean" },
          summary: { type: "string" },
        },
        required: ["passed", "summary"],
        additionalProperties: false,
      };

      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        createdBy: "agent",
        creationSource: "mcp",
        commandId: CommandId.make("command:wf-output-schema:child"),
        parentThreadId: parent.threadId,
        parentRunId: parent.runId,
        parentNodeId: workflowId,
        task: "Check the module and submit structured output.",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        completionWake: "never",
        outputSchema,
      });

      const childTask = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
        (candidate) => candidate.parentNodeId === workflowId,
      )!;
      assert.isNotNull(childTask.childThreadId);
      const childProjection = yield* orchestrator.getThreadProjection(childTask.childThreadId!);
      assert.deepEqual(childProjection.runs[0]?.outputSchema, outputSchema);
    }),
  );
});
