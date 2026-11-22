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

const sampleWorkflowScript =
  'export const meta = { name: "Review workflow", description: "Inspect changes across modules." };\nreturn "ok";';

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
      scriptText: sampleWorkflowScript,
      modelSelection,
      workflow: sampleWorkflow,
    };
    const recorded = yield* orchestrator.dispatch(command);
    const taskEvent = recorded.storedEvents.find(
      (stored) => stored.event.type === "subagent.updated",
    );
    const taskId =
      taskEvent?.event.type === "subagent.updated" ? taskEvent.event.payload.id : undefined;
    const coordinatorThreadId =
      taskEvent?.event.type === "subagent.updated"
        ? taskEvent.event.payload.childThreadId
        : undefined;
    assert.isDefined(taskId);
    assert.isNotNull(coordinatorThreadId);
    return { command, recorded, taskId: taskId!, coordinatorThreadId: coordinatorThreadId! };
  });

it.layer(layerWorkflowTest)("a server workflow task", (it) => {
  it.effect(
    "records workflow.start once with coordinator thread C, mirrors root node on progress/complete, refuses messages to C, and treats terminal updates as no-ops",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent("wf-lifecycle");
        const { command, taskId, coordinatorThreadId } = yield* startWorkflow(
          parent,
          "wf-lifecycle",
        );

        // Retry with the same commandId is idempotent and records nothing new.
        const replay = yield* orchestrator.dispatch(command);
        assert.isTrue(replay.replayed);

        const started = yield* orchestrator.getThreadProjection(parent.threadId);
        assert.equal(started.subagents.length, 1);
        const task = started.subagents[0]!;
        assert.equal(task.id, taskId);
        assert.equal(task.origin, "app_owned");
        assert.equal(task.childThreadId, coordinatorThreadId);
        assert.equal(task.nativeTaskRef, null);
        assert.equal(task.driver, driver);
        assert.equal(task.providerInstanceId, modelSelection.instanceId);
        assert.equal(task.prompt, sampleWorkflowScript);
        assert.equal(task.title, "Inspect changes across modules.");
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
        assert.equal(
          item?.type === "subagent" ? item.childThreadId : undefined,
          coordinatorThreadId,
        );
        assert.equal(item?.title, "Inspect changes across modules.");
        assert.equal(item?.type === "subagent" ? item.prompt : undefined, sampleWorkflowScript);

        // Coordinator thread C holds a runless root_turn and the workflowScript opening message.
        const coordinator = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        assert.equal(coordinator.thread.creationSource, "server");
        assert.equal(coordinator.thread.title, "Workflow · Review workflow");
        assert.deepEqual(coordinator.thread.lineage, {
          parentThreadId: parent.threadId,
          relationshipToParent: "subagent",
          rootThreadId: parent.threadId,
        });
        assert.deepEqual(coordinator.thread.forkedFrom, { type: "node", nodeId: taskId });
        assert.lengthOf(coordinator.runs, 0);
        const coordinatorRoot = coordinator.nodes.find(
          (candidate) => candidate.kind === "root_turn" && candidate.runId === null,
        );
        assert.isDefined(coordinatorRoot);
        assert.equal(coordinatorRoot?.status, "running");
        assert.lengthOf(coordinator.messages, 1);
        assert.equal(coordinator.messages[0]?.role, "user");
        assert.equal(coordinator.messages[0]?.workflowScript, true);
        assert.equal(coordinator.messages[0]?.text, sampleWorkflowScript);
        assert.lengthOf(coordinator.turnItems, 1);
        assert.equal(coordinator.turnItems[0]?.type, "user_message");

        // Coordinator thread C is read-only: user messages are refused.
        const refusedMessage = yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make("command:wf-lifecycle:msg-c"),
            threadId: coordinatorThreadId,
            messageId: MessageId.make("message:wf-lifecycle:msg-c"),
            text: "Hello C",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          })
          .pipe(Effect.flip);
        assert.equal(refusedMessage._tag, "OrchestratorSubagentThreadReadOnlyError");

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
        assert.include(
          progressResult.storedEvents.map((stored) => stored.event.type),
          "subagent.updated",
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
        const finishedCoordinator = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        const finishedRoot = finishedCoordinator.nodes.find(
          (candidate) => candidate.id === coordinatorRoot?.id,
        );
        assert.equal(finishedRoot?.status, "completed");
        assert.isNotNull(finishedRoot?.completedAt);
      }),
  );

  it.effect(
    "creates member threads under C via workflow.agent.request with no turn item on C, supports deferStart + workflow.agent.start, and rejects after stop, completion, or archived P",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent("wf-parenting");
        const { taskId: workflowId, coordinatorThreadId } = yield* startWorkflow(
          parent,
          "wf-parenting",
        );

        yield* completeParentRun(parent.threadId, parent.runId, "wf-parenting");

        yield* orchestrator.dispatch({
          type: "workflow.agent.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-parenting:under-workflow"),
          parentThreadId: coordinatorThreadId,
          task: "Run after parent turn ended.",
          title: "Worker 1",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });

        // Parent P has no member subagents or member turn items.
        const parentProjection = yield* orchestrator.getThreadProjection(parent.threadId);
        assert.lengthOf(parentProjection.subagents, 1);
        assert.equal(
          parentProjection.turnItems.filter((item) => item.type === "subagent").length,
          1,
        );

        // Coordinator C holds the member subagent row and no extra turn item beyond its script.
        const coordinatorProjection = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        const childTask = coordinatorProjection.subagents[0];
        assert.isDefined(childTask);
        assert.equal(childTask?.runId, null);
        assert.equal(childTask?.completionWake, "never");
        assert.deepEqual(childTask?.completionDelivery, {
          state: "disposed",
          observedByRunId: null,
        });
        assert.isNotNull(childTask?.childThreadId);
        assert.equal(
          coordinatorProjection.turnItems.filter((item) => item.type === "subagent").length,
          0,
        );
        const memberProjection = yield* orchestrator.getThreadProjection(childTask!.childThreadId!);
        assert.equal(memberProjection.thread.lineage.parentThreadId, coordinatorThreadId);
        assert.equal(memberProjection.thread.creationSource, "mcp");
        assert.lengthOf(memberProjection.runs, 1);

        // deferStart creates a queued member thread under C without starting a run, then workflow.agent.start starts it.
        yield* orchestrator.dispatch({
          type: "workflow.agent.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-parenting:deferred"),
          parentThreadId: coordinatorThreadId,
          task: "Queued worker.",
          title: "Worker 2",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          deferStart: true,
        });
        const afterDefer = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        const deferredTask = afterDefer.subagents.find((s) => s.title === "Worker 2");
        assert.isDefined(deferredTask);
        assert.equal(deferredTask?.status, "pending");
        assert.equal(deferredTask?.startedAt, null);
        const deferredMemberBefore = yield* orchestrator.getThreadProjection(
          deferredTask!.childThreadId!,
        );
        assert.lengthOf(deferredMemberBefore.runs, 0);

        yield* orchestrator.dispatch({
          type: "workflow.agent.start",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-parenting:deferred:start"),
          parentThreadId: coordinatorThreadId,
          taskId: deferredTask!.id,
          childThreadId: deferredTask!.childThreadId!,
        });
        const afterStart = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        assert.equal(
          afterStart.subagents.find((s) => s.id === deferredTask!.id)?.status,
          "running",
        );
        const deferredMemberAfter = yield* orchestrator.getThreadProjection(
          deferredTask!.childThreadId!,
        );
        assert.lengthOf(deferredMemberAfter.runs, 1);

        // Cancelling a pending (deferStart) member settles its subagent row and node on C.
        yield* orchestrator.dispatch({
          type: "workflow.agent.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-parenting:deferred-cancel"),
          parentThreadId: coordinatorThreadId,
          task: "Queued worker to cancel.",
          title: "Worker 3",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          deferStart: true,
        });
        const pendingCancelTask = (yield* orchestrator.getThreadProjection(
          coordinatorThreadId,
        )).subagents.find((s) => s.title === "Worker 3")!;
        assert.equal(pendingCancelTask.status, "pending");
        yield* orchestrator.cancelDelegatedTask({
          parentThreadId: coordinatorThreadId,
          taskId: pendingCancelTask.id,
          command: {
            type: "thread.stop",
            commandId: CommandId.make("command:wf-parenting:deferred-cancel:stop"),
            threadId: pendingCancelTask.childThreadId!,
            reason: "Workflow stopped.",
          },
          stopDescendants: () => Effect.void,
        });
        const afterCancelPending = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        const cancelledPendingTask = afterCancelPending.subagents.find(
          (s) => s.id === pendingCancelTask.id,
        );
        assert.equal(cancelledPendingTask?.status, "cancelled");
        assert.equal(cancelledPendingTask?.result, "Workflow stopped.");
        assert.equal(
          afterCancelPending.nodes.find((n) => n.id === pendingCancelTask.id)?.status,
          "cancelled",
        );

        // Once the workflow ends, new workflow.agent.request calls are rejected.
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
            type: "workflow.agent.request",
            createdBy: "agent",
            creationSource: "mcp",
            commandId: CommandId.make("command:wf-parenting:after-end"),
            parentThreadId: coordinatorThreadId,
            task: "Should fail after workflow ends.",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
          })
          .pipe(Effect.flip);
        assert.equal(afterEnd._tag, "OrchestratorDispatchError");

        // An archived parent P also refuses workflow.agent.request on a running workflow.
        const sink = yield* EventSink.EventSinkV2;
        const parentArchived = yield* seedLiveParent("wf-parenting-archived");
        const { coordinatorThreadId: archivedCoord } = yield* startWorkflow(
          parentArchived,
          "wf-parenting-archived",
        );
        const archivedThread = (yield* orchestrator.getThreadProjection(parentArchived.threadId))
          .thread;
        const archivedAt = DateTime.makeUnsafe("2026-04-20T00:05:00.000Z");
        yield* sink.write({
          commandId: CommandId.make("command:wf-parenting-archived:archive"),
          events: [
            {
              id: EventId.make("event:wf-parenting-archived:archive"),
              type: "thread.archived",
              threadId: parentArchived.threadId,
              providerInstanceId: archivedThread.providerInstanceId,
              occurredAt: archivedAt,
              payload: { ...archivedThread, archivedAt, updatedAt: archivedAt },
            },
          ],
        });
        const afterArchive = yield* orchestrator
          .dispatch({
            type: "workflow.agent.request",
            createdBy: "agent",
            creationSource: "mcp",
            commandId: CommandId.make("command:wf-parenting-archived:child"),
            parentThreadId: archivedCoord,
            task: "Should fail on archived P.",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
          })
          .pipe(Effect.flip);
        assert.equal(afterArchive._tag, "OrchestratorDispatchError");
      }),
  );

  it.effect(
    "suppresses parent wakes for workflow members and creates no follow-up task on runless C, while workflow.complete wakes the parent",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const offers = yield* Offers;
        const parent = yield* seedLiveParent("wf-wake-never");
        const { taskId: workflowId, coordinatorThreadId } = yield* startWorkflow(
          parent,
          "wf-wake-never",
        );

        yield* completeParentRun(parent.threadId, parent.runId, "wf-wake-never");
        yield* Ref.set(offers, []);

        yield* orchestrator.dispatch({
          type: "workflow.agent.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-wake-never:child"),
          parentThreadId: coordinatorThreadId,
          task: "Produce structured output.",
          title: "Agent 1",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });

        const childTask = (yield* orchestrator.getThreadProjection(coordinatorThreadId))
          .subagents[0]!;
        const childThreadId = childTask.childThreadId!;
        const firstChildRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]!;

        const beforeFirstComplete = yield* sink.latestSequence();
        yield* completeChildRun(childThreadId, firstChildRun.id, "Not valid JSON", "wf-never-1");
        yield* awaitSubagentStatus(childTask.id, "completed", beforeFirstComplete);
        yield* orchestrator.recoverDelegatedTask(childThreadId);

        const afterChild = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        const settledChild = afterChild.subagents.find(
          (candidate) => candidate.id === childTask.id,
        );
        assert.equal(settledChild?.status, "completed");
        assert.deepEqual(settledChild?.completionDelivery, {
          state: "disposed",
          observedByRunId: null,
        });
        assert.isUndefined(
          (yield* orchestrator.getThreadProjection(parent.threadId)).runs.find(
            (run) => run.id === parent.runId,
          )?.delegatedCompletion,
        );
        assert.lengthOf(yield* Ref.get(offers), 0);

        // A follow-up turn into the member creates no task on runless C and wakes nobody.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-wake-never:reprompt"),
          threadId: childThreadId,
          senderThreadId: coordinatorThreadId,
          messageId: MessageId.make("message:wf-wake-never:reprompt"),
          text: "Your result did not match the schema.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
        });
        const coordAfterFollowup = yield* orchestrator.getThreadProjection(coordinatorThreadId);
        assert.lengthOf(coordAfterFollowup.subagents, 1);
        const followupRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs.find(
          (run) => run.id !== firstChildRun.id,
        )!;
        assert.isDefined(followupRun);
        assert.isUndefined(followupRun.delegatedTaskId);
        yield* completeChildRun(childThreadId, followupRun.id, '{"ok":true}', "wf-never-2");
        yield* orchestrator.recoverDelegatedTask(childThreadId);
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

        // A user follow-up to M_i after workflow completion creates no task on C.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:wf-wake-never:user-followup"),
          threadId: childThreadId,
          messageId: MessageId.make("message:wf-wake-never:user-followup"),
          text: "Tell me more about that check.",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
        });
        assert.lengthOf(
          (yield* orchestrator.getThreadProjection(coordinatorThreadId)).subagents,
          1,
        );
      }),
  );

  it.effect(
    "requests stop on running app-owned workflows via stopWorkflow, stopDelegatedTasks, and thread.stop without completing early",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const offers = yield* Offers;

        // stopWorkflow enforces mode limits, then records stopRequestedAt without completing early.
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
        const stoppingA = yield* orchestrator.getThreadProjection(parentA.threadId);
        const taskA = stoppingA.subagents.find((candidate) => candidate.id === workflowA);
        assert.equal(taskA?.status, "running");
        assert.equal(taskA?.result, null);
        assert.isNumber(taskA?.workflow?.stopRequestedAt);
        const firstStopRequestedAt = taskA?.workflow?.stopRequestedAt;
        assert.equal(
          stoppingA.nodes.find((candidate) => candidate.id === workflowA)?.status,
          "running",
        );
        assert.equal(
          stoppingA.turnItems.find(
            (candidate) => candidate.type === "subagent" && candidate.subagentId === workflowA,
          )?.status,
          "running",
        );

        // Repeat stop-requests are idempotent and preserve the first timestamp.
        yield* threads.stopWorkflow({ threadId: parentA.threadId, subagentId: workflowA });
        const duplicateStop = yield* orchestrator.dispatch({
          type: "workflow.stop-request",
          commandId: CommandId.make("command:wf-stop-service:duplicate-stop"),
          parentThreadId: parentA.threadId,
          taskId: workflowA,
        });
        assert.lengthOf(duplicateStop.storedEvents, 0);

        // Progress after stop-request preserves stored stopRequestedAt even when omitted in the snapshot.
        const stoppingProgressWorkflow = {
          ...sampleWorkflow,
          agents: [{ index: 0, label: "Cancelling A", state: "failed" as const }],
        };
        yield* orchestrator.dispatch({
          type: "workflow.progress",
          commandId: CommandId.make("command:wf-stop-service:progress-while-stopping"),
          parentThreadId: parentA.threadId,
          taskId: workflowA,
          workflow: stoppingProgressWorkflow,
        });
        const afterProgressA = (yield* orchestrator.getThreadProjection(
          parentA.threadId,
        )).subagents.find((candidate) => candidate.id === workflowA);
        assert.equal(afterProgressA?.status, "running");
        assert.equal(afterProgressA?.workflow?.stopRequestedAt, firstStopRequestedAt);

        // Engine completion with disposeDelivery stores the final snapshot and plans no parent wake.
        yield* completeParentRun(parentA.threadId, parentA.runId, "wf-stop-service");
        yield* Ref.set(offers, []);
        const finalStoppedWorkflow = {
          ...sampleWorkflow,
          agents: [{ index: 0, label: "Cancelling A", state: "failed" as const, durationMs: 42 }],
          durationMs: 42,
        };
        yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-stop-service:complete"),
          parentThreadId: parentA.threadId,
          taskId: workflowA,
          status: "cancelled",
          result: "Stopped.",
          workflow: finalStoppedWorkflow,
          disposeDelivery: true,
        });
        const stoppedA = yield* orchestrator.getThreadProjection(parentA.threadId);
        const doneA = stoppedA.subagents.find((candidate) => candidate.id === workflowA);
        assert.equal(doneA?.status, "cancelled");
        assert.equal(doneA?.result, "Stopped.");
        assert.deepEqual(doneA?.completionDelivery, { state: "disposed", observedByRunId: null });
        assert.deepEqual(doneA?.workflow, {
          ...finalStoppedWorkflow,
          stopRequestedAt: firstStopRequestedAt,
        });
        assert.isUndefined(
          stoppedA.runs.find((run) => run.id === parentA.runId)?.delegatedCompletion,
        );
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
        assert.lengthOf(yield* Ref.get(offers), 0);

        const stopAfterComplete = yield* threads
          .stopWorkflow({ threadId: parentA.threadId, subagentId: workflowA })
          .pipe(Effect.flip);
        assert.equal(stopAfterComplete._tag, "OrchestrationV2StopWorkflowError");
        assert.equal(stopAfterComplete.reason, "not-running");

        // thread.stop requests stop after parent run ended, leaves workflow running until complete, and wakes nobody.
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
        assert.equal(taskB?.status, "running");
        assert.equal(taskB?.result, null);
        assert.isNumber(taskB?.workflow?.stopRequestedAt);
        assert.deepEqual(taskB?.completionDelivery, { state: "disposed", observedByRunId: null });
        assert.equal(
          stoppedB.nodes.find((candidate) => candidate.id === workflowB)?.status,
          "running",
        );
        const itemB = stoppedB.turnItems.find(
          (candidate) => candidate.type === "subagent" && candidate.subagentId === workflowB,
        );
        assert.equal(itemB?.status, "running");
        assert.lengthOf(yield* Ref.get(offers), 0);

        // thread.stop while the parent run is live also requests stop without completing early.
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
        assert.equal(taskC?.status, "running");
        assert.equal(taskC?.result, null);
        assert.isNumber(taskC?.workflow?.stopRequestedAt);
        assert.deepEqual(taskC?.completionDelivery, { state: "disposed", observedByRunId: null });
        assert.lengthOf(yield* Ref.get(offers), 0);

        // stopDelegatedTasks dispatches workflow.stop-request and leaves the workflow running.
        const parentD = yield* seedLiveParent("wf-stop-delegated-tasks");
        const { taskId: workflowD } = yield* startWorkflow(parentD, "wf-stop-delegated-tasks");
        yield* threads.stopDelegatedTasks({
          threadId: parentD.threadId,
          commandId: CommandId.make("command:wf-stop-delegated-tasks:stop"),
        });
        const taskD = (yield* orchestrator.getThreadProjection(parentD.threadId)).subagents.find(
          (candidate) => candidate.id === workflowD,
        );
        assert.equal(taskD?.status, "running");
        assert.isNumber(taskD?.workflow?.stopRequestedAt);
      }),
  );

  it.effect("refuses workflow.agent.request once workflow.stop-request is recorded", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedLiveParent("wf-stop-admission");
      const { taskId: workflowId, coordinatorThreadId } = yield* startWorkflow(
        parent,
        "wf-stop-admission",
      );
      yield* completeParentRun(parent.threadId, parent.runId, "wf-stop-admission");

      yield* orchestrator.dispatch({
        type: "workflow.stop-request",
        commandId: CommandId.make("command:wf-stop-admission:stop"),
        parentThreadId: parent.threadId,
        taskId: workflowId,
      });

      const rejected = yield* orchestrator
        .dispatch({
          type: "workflow.agent.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-stop-admission:child"),
          parentThreadId: coordinatorThreadId,
          task: "Must not start after stop-request.",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
        })
        .pipe(Effect.flip);
      assert.equal(rejected._tag, "OrchestratorDispatchError");
    }),
  );

  it.effect(
    "refuses queued retry admission and queued retry promotion once the owning workflow is stopping or ended, leaving other threads unaffected",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const sink = yield* EventSink.EventSinkV2;
        const projectId = ProjectId.make("project:remote-delegated-task");

        const parent = yield* seedLiveParent("wf-retry-gate");
        const { taskId: workflowId, coordinatorThreadId } = yield* startWorkflow(
          parent,
          "wf-retry-gate",
        );

        // Launch a workflow child under C and a non-workflow delegated child under P.
        yield* orchestrator.dispatch({
          type: "workflow.agent.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-retry-gate:wf-child"),
          parentThreadId: coordinatorThreadId,
          task: "Workflow worker",
          title: "Workflow worker",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });
        yield* orchestrator.dispatch({
          type: "delegated_task.request",
          createdBy: "agent",
          creationSource: "mcp",
          commandId: CommandId.make("command:wf-retry-gate:plain-child"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: parent.rootNodeId,
          task: "Regular child",
          title: "Regular child",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });

        const wfChildTask = (yield* orchestrator.getThreadProjection(coordinatorThreadId))
          .subagents[0]!;
        const wfChildThreadId = wfChildTask.childThreadId!;
        const wfFirstRun = (yield* orchestrator.getThreadProjection(wfChildThreadId)).runs[0]!;
        const parentProjection = yield* orchestrator.getThreadProjection(parent.threadId);
        const plainChildTask = parentProjection.subagents.find(
          (candidate) =>
            candidate.id !== workflowId &&
            candidate.parentNodeId === parent.rootNodeId &&
            candidate.childThreadId !== null,
        )!;
        const plainChildThreadId = plainChildTask.childThreadId!;
        const plainFirstRun = (yield* orchestrator.getThreadProjection(plainChildThreadId))
          .runs[0]!;

        // Queue a schema re-prompt behind the active run while the workflow is still running.
        const queuedSend = yield* threads.sendToThread({
          projectId,
          commandId: CommandId.make("command:wf-retry-gate:queued-before-stop"),
          threadId: wfChildThreadId,
          senderThreadId: coordinatorThreadId,
          messageId: MessageId.make("message:wf-retry-gate:queued-before-stop"),
          text: "Your result did not match the schema.",
          attachments: [],
          mode: "queue",
          createdBy: "agent",
          creationSource: "mcp",
        });
        assert.equal(queuedSend.run.status, "queued");
        const queuedRunId = queuedSend.run.id;

        // Request workflow stop: new queued retries must be refused.
        yield* orchestrator.dispatch({
          type: "workflow.stop-request",
          commandId: CommandId.make("command:wf-retry-gate:stop-request"),
          parentThreadId: parent.threadId,
          taskId: workflowId,
        });

        const refusedAfterStopRequest = yield* threads
          .sendToThread({
            projectId,
            commandId: CommandId.make("command:wf-retry-gate:send-after-stop-request"),
            threadId: wfChildThreadId,
            senderThreadId: coordinatorThreadId,
            messageId: MessageId.make("message:wf-retry-gate:send-after-stop-request"),
            text: "Must be refused after stop-request.",
            attachments: [],
            mode: "queue",
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.flip);
        assert.equal(refusedAfterStopRequest._tag, "OrchestratorDispatchError");

        // Completing the active run must NOT promote the already-queued retry.
        yield* completeChildRun(wfChildThreadId, wfFirstRun.id, "bad json", "wf-retry-gate-first");

        yield* orchestrator.resumeQueuedRuns;

        const wfAfterFinish = yield* orchestrator.getThreadProjection(wfChildThreadId);
        assert.equal(wfAfterFinish.runs.find((run) => run.id === queuedRunId)?.status, "queued");

        const refusedQueueResume = yield* orchestrator
          .dispatch({
            type: "queue.resume",
            commandId: CommandId.make("command:wf-retry-gate:queue-resume"),
            threadId: wfChildThreadId,
          })
          .pipe(Effect.flip);
        assert.equal(refusedQueueResume._tag, "OrchestratorDispatchError");

        const refusedQueuedRunStart = yield* orchestrator
          .dispatch({
            type: "queued-run.start",
            commandId: CommandId.make("command:wf-retry-gate:queued-run-start"),
            threadId: wfChildThreadId,
            runId: queuedRunId,
          })
          .pipe(Effect.flip);
        assert.equal(refusedQueuedRunStart._tag, "OrchestratorDispatchError");

        // Refusal continues after workflow.complete.
        yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-retry-gate:complete"),
          parentThreadId: parent.threadId,
          taskId: workflowId,
          status: "cancelled",
          result: "Stopped.",
          disposeDelivery: true,
        });
        const refusedAfterComplete = yield* threads
          .sendToThread({
            projectId,
            commandId: CommandId.make("command:wf-retry-gate:send-after-complete"),
            threadId: wfChildThreadId,
            senderThreadId: coordinatorThreadId,
            messageId: MessageId.make("message:wf-retry-gate:send-after-complete"),
            text: "Must be refused after workflow complete.",
            attachments: [],
            mode: "queue",
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.flip);
        assert.equal(refusedAfterComplete._tag, "OrchestratorDispatchError");

        // Non-workflow delegated children are unaffected by workflow stop/completion.
        const plainQueued = yield* threads.sendToThread({
          projectId,
          commandId: CommandId.make("command:wf-retry-gate:plain-queued"),
          threadId: plainChildThreadId,
          senderThreadId: parent.threadId,
          messageId: MessageId.make("message:wf-retry-gate:plain-queued"),
          text: "Follow up on regular child.",
          attachments: [],
          mode: "queue",
          createdBy: "agent",
          creationSource: "mcp",
        });
        assert.equal(plainQueued.run.status, "queued");
        const beforePlainComplete = yield* sink.latestSequence();
        yield* completeChildRun(
          plainChildThreadId,
          plainFirstRun.id,
          "done",
          "wf-retry-gate-plain",
        );
        yield* sink.stream({ afterSequence: beforePlainComplete, eventType: "run.updated" }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "run.updated" &&
              stored.event.payload.id === plainQueued.run.id &&
              stored.event.payload.status === "starting",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const plainAfter = yield* orchestrator.getThreadProjection(plainChildThreadId);
        assert.equal(
          plainAfter.runs.find((run) => run.id === plainQueued.run.id)?.status,
          "starting",
        );
      }),
  );

  it.effect(
    "uses t3_workflow_status with runId in large-wake fallback and bounds long workflow titles inside the inline allowance",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const parent = yield* seedLiveParent("wf-wake-fallback");
        const longTitle = `Workflow ${"界".repeat(8_000)}`;
        const runId = "wf_fallback_run_99";
        const started = yield* orchestrator.dispatch({
          type: "workflow.start",
          commandId: CommandId.make("command:wf-wake-fallback:start"),
          parentThreadId: parent.threadId,
          parentRunId: parent.runId,
          parentNodeId: parent.rootNodeId,
          title: longTitle,
          description: longTitle,
          scriptText: sampleWorkflowScript,
          modelSelection,
          workflow: {
            ...sampleWorkflow,
            name: longTitle,
            runHandles: {
              ...sampleWorkflow.runHandles,
              runId,
            },
          },
        });
        const taskEvent = started.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        const workflowId =
          taskEvent?.event.type === "subagent.updated" ? taskEvent.event.payload.id : undefined;
        assert.isDefined(workflowId);

        yield* completeParentRun(parent.threadId, parent.runId, "wf-wake-fallback");
        yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-wake-fallback:complete"),
          parentThreadId: parent.threadId,
          taskId: workflowId!,
          status: "completed",
          result: "x".repeat(20_000),
        });

        const afterComplete = yield* orchestrator.getThreadProjection(parent.threadId);
        const delivery = afterComplete.runs.find((run) => run.id === parent.runId)
          ?.delegatedCompletion?.delivery;
        assert.isDefined(delivery);

        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("command:wf-wake-fallback:wake"),
          threadId: parent.threadId,
          messageId: delivery!.messageId,
          text: "Workflow completed.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
          delegatedCompletion: {
            parentRunId: parent.runId,
            generation: delivery!.generation,
            taskIds: delivery!.taskIds,
          },
        });

        const wakeMessage = (yield* orchestrator.getThreadProjection(
          parent.threadId,
        )).messages.find((message) => message.id === delivery!.messageId);
        assert.isDefined(wakeMessage);
        assert.isAtMost(Buffer.byteLength(wakeMessage!.text, "utf8"), 16_000);
        assert.include(
          wakeMessage!.text,
          `Use t3_workflow_status with runId ${runId} to read the result.`,
        );
        assert.notInclude(wakeMessage!.text, "task_status");
        assert.include(wakeMessage!.text, "...");

        // Even with a short result, a very long workflow title is truncated inside the allowance.
        const parentShort = yield* seedLiveParent("wf-wake-long-title-short-result");
        const startedShort = yield* orchestrator.dispatch({
          type: "workflow.start",
          commandId: CommandId.make("command:wf-wake-short:start"),
          parentThreadId: parentShort.threadId,
          parentRunId: parentShort.runId,
          parentNodeId: parentShort.rootNodeId,
          title: longTitle,
          description: "Short result with huge title.",
          modelSelection,
          workflow: {
            ...sampleWorkflow,
            name: longTitle,
            runHandles: { ...sampleWorkflow.runHandles, runId: "wf_short_1" },
          },
        });
        const shortTaskEvent = startedShort.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        const shortWorkflowId =
          shortTaskEvent?.event.type === "subagent.updated"
            ? shortTaskEvent.event.payload.id
            : undefined;
        yield* completeParentRun(parentShort.threadId, parentShort.runId, "wf-wake-short");
        yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-wake-short:complete"),
          parentThreadId: parentShort.threadId,
          taskId: shortWorkflowId!,
          status: "completed",
          result: "Compact workflow summary.",
        });
        const shortDelivery = (yield* orchestrator.getThreadProjection(
          parentShort.threadId,
        )).runs.find((run) => run.id === parentShort.runId)?.delegatedCompletion?.delivery;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("command:wf-wake-short:wake"),
          threadId: parentShort.threadId,
          messageId: shortDelivery!.messageId,
          text: "Workflow completed.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
          delegatedCompletion: {
            parentRunId: parentShort.runId,
            generation: shortDelivery!.generation,
            taskIds: shortDelivery!.taskIds,
          },
        });
        const shortWakeMessage = (yield* orchestrator.getThreadProjection(
          parentShort.threadId,
        )).messages.find((message) => message.id === shortDelivery!.messageId);
        assert.isDefined(shortWakeMessage);
        assert.isAtMost(Buffer.byteLength(shortWakeMessage!.text, "utf8"), 16_000);
        assert.include(shortWakeMessage!.text, "Compact workflow summary.");
      }),
  );

  it.effect(
    "emits changedAgentIndices on workflow progress and completion events without persisting it on projections",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const parent = yield* seedLiveParent("wf-changed-indices");
        const { taskId: workflowId } = yield* startWorkflow(parent, "wf-changed-indices");

        const initialAgents = [
          { index: 0, label: "Check A", state: "running" as const },
          { index: 1, label: "Check B", state: "running" as const },
          { index: 2, label: "Check C", state: "queued" as const },
        ];
        const firstProgress = yield* orchestrator.dispatch({
          type: "workflow.progress",
          commandId: CommandId.make("command:wf-changed:1"),
          parentThreadId: parent.threadId,
          taskId: workflowId,
          workflow: { ...sampleWorkflow, agents: initialAgents },
        });
        const firstEvent = firstProgress.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        assert.equal(firstEvent?.event.type, "subagent.updated");
        if (firstEvent?.event.type === "subagent.updated") {
          assert.deepEqual(firstEvent.event.payload.workflow?.changedAgentIndices, [0, 1, 2]);
        }

        // Changing only agent 1 emits changedAgentIndices: [1].
        const updatedAgents = [
          { index: 0, label: "Check A", state: "running" as const },
          { index: 1, label: "Check B", state: "completed" as const, result: "ok" },
          { index: 2, label: "Check C", state: "queued" as const },
        ];
        const secondProgress = yield* orchestrator.dispatch({
          type: "workflow.progress",
          commandId: CommandId.make("command:wf-changed:2"),
          parentThreadId: parent.threadId,
          taskId: workflowId,
          workflow: { ...sampleWorkflow, agents: updatedAgents },
        });
        const secondEvent = secondProgress.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        assert.equal(secondEvent?.event.type, "subagent.updated");
        if (secondEvent?.event.type === "subagent.updated") {
          assert.deepEqual(secondEvent.event.payload.workflow?.changedAgentIndices, [1]);
          const inMemory = ProjectionStore.applyToProjection(
            yield* orchestrator.getThreadProjection(parent.threadId),
            secondEvent.event,
          );
          const inMemoryWorkflow = inMemory.subagents.find(
            (candidate) => candidate.id === workflowId,
          )?.workflow;
          assert.isDefined(inMemoryWorkflow);
          assert.isFalse(Object.hasOwn(inMemoryWorkflow!, "changedAgentIndices"));
        }

        const storedAfterSecond = (yield* orchestrator.getThreadProjection(
          parent.threadId,
        )).subagents.find((candidate) => candidate.id === workflowId);
        assert.isDefined(storedAfterSecond?.workflow);
        assert.isFalse(Object.hasOwn(storedAfterSecond!.workflow!, "changedAgentIndices"));
        assert.deepEqual(storedAfterSecond?.workflow?.agents, updatedAgents);

        const recordsAfterSecond = (yield* projectionStore.getThreadRecords(
          parent.threadId,
          ["subagents"],
          {
            subagentIds: [workflowId],
          },
        )).subagents[0];
        assert.isFalse(Object.hasOwn(recordsAfterSecond!.workflow!, "changedAgentIndices"));

        // Completing while updating only agent 2 emits changedAgentIndices: [2], never stored.
        const finalAgents = [
          { index: 0, label: "Check A", state: "running" as const },
          { index: 1, label: "Check B", state: "completed" as const, result: "ok" },
          { index: 2, label: "Check C", state: "completed" as const },
        ];
        const completed = yield* orchestrator.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make("command:wf-changed:complete"),
          parentThreadId: parent.threadId,
          taskId: workflowId,
          status: "completed",
          result: "Done.",
          workflow: { ...sampleWorkflow, agents: finalAgents },
        });
        const completeEvent = completed.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        assert.equal(completeEvent?.event.type, "subagent.updated");
        if (completeEvent?.event.type === "subagent.updated") {
          assert.deepEqual(completeEvent.event.payload.workflow?.changedAgentIndices, [2]);
        }
        const storedAfterComplete = (yield* orchestrator.getThreadProjection(
          parent.threadId,
        )).subagents.find((candidate) => candidate.id === workflowId);
        assert.isDefined(storedAfterComplete?.workflow);
        assert.isFalse(Object.hasOwn(storedAfterComplete!.workflow!, "changedAgentIndices"));
        assert.deepEqual(storedAfterComplete?.workflow?.agents, finalAgents);
      }),
  );

  it.effect("forwards outputSchema from workflow.agent.request to the child's first run", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const parent = yield* seedLiveParent("wf-output-schema");
      const { coordinatorThreadId } = yield* startWorkflow(parent, "wf-output-schema");
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
        type: "workflow.agent.request",
        createdBy: "agent",
        creationSource: "mcp",
        commandId: CommandId.make("command:wf-output-schema:child"),
        parentThreadId: coordinatorThreadId,
        task: "Check the module and submit structured output.",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        outputSchema,
      });

      const childTask = (yield* orchestrator.getThreadProjection(coordinatorThreadId))
        .subagents[0]!;
      assert.isNotNull(childTask.childThreadId);
      const childProjection = yield* orchestrator.getThreadProjection(childTask.childThreadId!);
      assert.deepEqual(childProjection.runs[0]?.outputSchema, outputSchema);
    }),
  );
});
