import * as NodeOS from "node:os";
import {
  CommandId,
  MessageId,
  NodeId,
  NonNegativeInt,
  ORCHESTRATOR_MCP_RUNNING_TASK_NEXT,
  OrchestrationV2WorkflowPhase,
  OrchestratorMcpFailure,
  OrchestratorMcpTargetOptions,
  OrchestratorMcpWorkflowRunStatus,
  OrchestratorMcpWorkflowUsage,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TrimmedNonEmptyString,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  type OrchestrationV2SubagentWorkflow,
  type OrchestrationV2WorkflowAgent,
  type OrchestratorMcpDelegateTaskInput,
  type OrchestratorMcpTarget,
  type OrchestratorMcpTaskRole,
  type OrchestratorMcpWorkflowRunInput,
  type OrchestratorMcpWorkflowRunResult,
  type OrchestratorMcpWorkflowStatusInput,
  type OrchestratorMcpWorkflowStatusResult,
  type ProjectId,
  type ProviderOptionSelection,
  type RunId,
} from "@t3tools/contracts";
import {
  SUBAGENT_RESULT_TURN_ITEM_TYPES,
  subagentResultForRun,
} from "@t3tools/provider-core/server/subagentProjection";
import { latestRootProviderFailure } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import { assertSameLink } from "../mcp/linkOrigin.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as OrchestratorMcpService from "../mcp/OrchestratorMcpService.ts";
import {
  assertLiveCaller,
  assertTargetWithinLimits,
  isLiveCallerShell,
  type Caller,
} from "../mcp/threadAccess.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import { checkOutput, type OutputRejection } from "../orchestration-v2/runOutput.ts";
import { userFacingDispatchErrorMessage } from "../orchestration-v2/UserFacingErrors.ts";
import {
  agentCallKey,
  decodeJournal,
  encodeJournalEntry,
  replayCursor,
  type JournalEntry,
  type ReplayCursor,
} from "./journal.ts";
import {
  parseWorkflowMeta,
  runWorkflowScript,
  workflowConcurrency,
  WorkflowScriptError,
  type WorkflowAgentCall,
  type WorkflowHost,
  type WorkflowMeta,
} from "./sandbox.ts";

const MAX_PROGRESS_PREVIEW_CHARS = 160;
const MAX_RESULT_SUMMARY_CHARS = 20_000;
const MAX_IN_MEMORY_LOGS = 50;
const MAX_SCHEMA_REPROMPTS = 3;
const PROGRESS_COALESCE_INTERVAL_MS = 1_000;
const MAX_DEFAULT_TITLE_CHARS = 80;
const MAX_EXPLICIT_TITLE_CHARS = 512;

const TASK_ROLE_LITERALS: ReadonlySet<string> = new Set<OrchestratorMcpTaskRole>([
  "implementation",
  "research",
  "review",
  "design",
  "test",
  "general",
]);

const PersistedWorkflowRun = Schema.Struct({
  runId: TrimmedNonEmptyString,
  threadId: ThreadId,
  taskId: NodeId,
  name: TrimmedNonEmptyString,
  scriptPath: TrimmedNonEmptyString,
  journalPath: TrimmedNonEmptyString,
  startedAt: NonNegativeInt,
  status: OrchestratorMcpWorkflowRunStatus,
  phases: Schema.optional(Schema.Array(OrchestrationV2WorkflowPhase)),
  currentPhase: Schema.optional(TrimmedNonEmptyString),
  result: Schema.optional(Schema.Json),
  error: Schema.optional(Schema.String),
  usage: OrchestratorMcpWorkflowUsage,
});
type PersistedWorkflowRun = typeof PersistedWorkflowRun.Type;

const decodePersistedWorkflowRun = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PersistedWorkflowRun),
);
const encodePersistedWorkflowRun = Schema.encodeUnknownEffect(
  Schema.fromJsonString(PersistedWorkflowRun),
);
const decodeTargetOptions = Schema.decodeUnknownEffect(OrchestratorMcpTargetOptions);

class WorkflowAbortedError extends Schema.TaggedError<WorkflowAbortedError>()(
  "WorkflowAbortedError",
  {},
) {}

class WorkflowExecutionError extends Schema.TaggedError<WorkflowExecutionError>()(
  "WorkflowExecutionError",
  {
    messageText: Schema.String,
  },
) {}

class WorkflowInternalError extends Schema.TaggedError<WorkflowInternalError>()(
  "WorkflowInternalError",
  {
    messageText: Schema.String,
  },
) {}

function createHostAbortController(): AbortController {
  return new AbortController();
}

type JournalQueueItem =
  | { readonly _tag: "entry"; readonly entry: JournalEntry }
  | { readonly _tag: "flush"; readonly done: Deferred.Deferred<void> };

interface MutableUsage {
  agents: number;
  done: number;
  errors: number;
  empty: number;
  cached: number;
  tokens: number;
}

interface LiveWorkflowRun {
  readonly runId: string;
  readonly threadId: ThreadId;
  readonly taskId: NodeId;
  readonly name: string;
  readonly scriptPath: string;
  readonly journalPath: string;
  readonly startedAt: number;
  readonly done: Deferred.Deferred<void>;
  readonly triggerExternalStop: () => void;
  readonly getStatus: () => Effect.Effect<OrchestratorMcpWorkflowStatusResult>;
  readonly currentStatus: () => OrchestratorMcpWorkflowRunStatus;
}

export class WorkflowService extends Context.Service<
  WorkflowService,
  {
    readonly run: (
      scope: McpInvocationContext.McpInvocationScope,
      input: OrchestratorMcpWorkflowRunInput,
    ) => Effect.Effect<OrchestratorMcpWorkflowRunResult, OrchestratorMcpFailure>;
    readonly status: (
      scope: McpInvocationContext.McpInvocationScope,
      input: OrchestratorMcpWorkflowStatusInput,
    ) => Effect.Effect<OrchestratorMcpWorkflowStatusResult, OrchestratorMcpFailure>;
    readonly stop: (
      scope: McpInvocationContext.McpInvocationScope,
      runId: string,
    ) => Effect.Effect<
      { readonly runId: string; readonly status: OrchestratorMcpWorkflowRunStatus },
      OrchestratorMcpFailure
    >;
  }
>()("t3/workflows/WorkflowService") {}

function failure(code: OrchestratorMcpFailure["code"], message: string): OrchestratorMcpFailure {
  return new OrchestratorMcpFailure({ code, message });
}

function errorMessage(error: unknown): string {
  if (typeof error === "object" && error !== null && "message" in error) {
    const message = (error as { readonly message?: unknown }).message;
    if (typeof message === "string" && message.length > 0) return message;
  }
  return String(error);
}

function isSafeRunId(runId: string): boolean {
  return (
    runId.length > 0 &&
    !runId.includes("/") &&
    !runId.includes("\\") &&
    !runId.includes("..") &&
    !runId.includes("\0")
  );
}

function isTerminalSubagentStatus(
  status: OrchestrationV2Subagent["status"],
): status is "completed" | "failed" | "cancelled" | "interrupted" {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted"
  );
}

function isTerminalRunStatus(status: OrchestrationV2Run["status"]): boolean {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted" ||
    status === "rolled_back"
  );
}

function subagentStatusToWorkflowStatus(
  status: OrchestrationV2Subagent["status"],
): OrchestratorMcpWorkflowRunStatus {
  switch (status) {
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
    case "interrupted":
      return "cancelled";
    default:
      return "running";
  }
}

function truncateChars(text: string, maxChars: number): string {
  const chars = Array.from(text);
  return chars.length <= maxChars ? text : chars.slice(0, maxChars).join("");
}

function previewText(value: string | null | undefined): string | undefined {
  if (value == null) return undefined;
  const trimmed = value.trim();
  if (trimmed.length === 0) return undefined;
  return truncateChars(trimmed, MAX_PROGRESS_PREVIEW_CHARS);
}

function previewValue(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value === "string") return previewText(value);
  try {
    return previewText(JSON.stringify(value));
  } catch {
    return undefined;
  }
}

function agentLabel(call: WorkflowAgentCall): string {
  const explicit = call.opts.label?.trim();
  if (explicit !== undefined && explicit.length > 0) {
    return truncateChars(explicit, MAX_EXPLICIT_TITLE_CHARS);
  }
  const firstNonEmptyLine =
    call.prompt
      .split(/\r?\n/u)
      .map((line) => line.trim())
      .find((line) => line.length > 0) ?? call.prompt.trim();
  const truncated = truncateChars(firstNonEmptyLine, MAX_DEFAULT_TITLE_CHARS).trim();
  return truncated.length > 0 ? truncated : `Agent ${call.index + 1}`;
}

function stripSingleJsonFence(text: string): string {
  const trimmed = text.trim();
  const match = /^```(?:json)?[ \t]*\r?\n([\s\S]*?)\r?\n```$/iu.exec(trimmed);
  return match !== null ? match[1]!.trim() : trimmed;
}

function formatOutputRejection(rejection: OutputRejection): string {
  const detail =
    rejection.cause instanceof Error && rejection.cause.message.length > 0
      ? `${rejection.problem} ${rejection.cause.message.slice(0, 1000)}`
      : rejection.problem;
  return detail.replace(/\.+$/u, "");
}

function formatDuration(durationMs: number): string {
  const ms = Math.max(0, Math.trunc(durationMs));
  if (ms < 1_000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1_000);
  return `${minutes}m ${seconds}s`;
}

function formatCompletionResult(input: {
  readonly name: string;
  readonly result: unknown;
  readonly usage: OrchestratorMcpWorkflowUsage;
  readonly journalPath: string;
}): string {
  const rawJson = JSON.stringify(input.result ?? null) ?? "null";
  const resultJson =
    rawJson.length > MAX_RESULT_SUMMARY_CHARS
      ? `${rawJson.slice(0, MAX_RESULT_SUMMARY_CHARS - 3)}...`
      : rawJson;
  return [
    `Workflow ${input.name} completed.`,
    `Result: ${resultJson}`,
    `Usage: ${input.usage.agents} agents, ${input.usage.done} done, ${input.usage.errors} errors, ${input.usage.empty} empty, ${input.usage.tokens} tokens, ${formatDuration(input.usage.durationMs)}`,
    `Journal: ${input.journalPath}`,
  ].join("\n");
}

function stableCommandId(input: {
  readonly scope: McpInvocationContext.McpThreadInvocationScope;
  readonly operation: string;
  readonly key: string;
}): CommandId {
  return CommandId.make(
    [
      "command",
      "mcp",
      encodeURIComponent(input.scope.requestNamespace),
      encodeURIComponent(input.operation),
      encodeURIComponent(input.key),
    ].join(":"),
  );
}

function stableMessageId(input: {
  readonly scope: McpInvocationContext.McpThreadInvocationScope;
  readonly operation: string;
  readonly key: string;
}): MessageId {
  return MessageId.make(
    [
      "message",
      "mcp",
      encodeURIComponent(input.scope.requestNamespace),
      encodeURIComponent(input.operation),
      encodeURIComponent(input.key),
    ].join(":"),
  );
}

function countAgentStates(agents: Iterable<OrchestrationV2WorkflowAgent>): {
  readonly queued: number;
  readonly running: number;
  readonly completed: number;
  readonly failed: number;
} {
  let queued = 0;
  let running = 0;
  let completed = 0;
  let failed = 0;
  for (const agent of agents) {
    switch (agent.state) {
      case "queued":
        queued += 1;
        break;
      case "running":
        running += 1;
        break;
      case "completed":
        completed += 1;
        break;
      case "failed":
        failed += 1;
        break;
    }
  }
  return { queued, running, completed, failed };
}

function reconstructFromJournal(
  persisted: PersistedWorkflowRun,
  entries: ReadonlyArray<JournalEntry>,
): OrchestratorMcpWorkflowStatusResult {
  const phases: Array<OrchestrationV2WorkflowPhase> = [...(persisted.phases ?? [])];
  const phaseIndexByTitle = new Map<string, number>(phases.map((p) => [p.title, p.index] as const));
  const ensurePhase = (title: string): void => {
    const trimmed = title.trim();
    if (trimmed.length === 0 || phaseIndexByTitle.has(trimmed)) return;
    const index = phases.length;
    phaseIndexByTitle.set(trimmed, index);
    phases.push({ index, title: trimmed });
  };

  let currentPhase = persisted.currentPhase;
  const logs: Array<string> = [];
  const startedIndices = new Set<number>();
  const resultOutcomeByIndex = new Map<number, "ok" | "null" | "usage_limited">();

  for (const entry of entries) {
    switch (entry.type) {
      case "phase":
        ensurePhase(entry.title);
        if (entry.title.trim().length > 0) currentPhase = entry.title.trim();
        break;
      case "log":
        logs.push(entry.message);
        if (logs.length > MAX_IN_MEMORY_LOGS) logs.shift();
        break;
      case "started":
        startedIndices.add(entry.index);
        if (entry.phase !== null) ensurePhase(entry.phase);
        break;
      case "result":
        resultOutcomeByIndex.set(entry.index, entry.outcome);
        break;
      case "end":
        break;
    }
  }

  let running = 0;
  let completed = 0;
  let failed = 0;
  const allIndices = new Set<number>([...startedIndices, ...resultOutcomeByIndex.keys()]);
  for (const index of allIndices) {
    const outcome = resultOutcomeByIndex.get(index);
    if (outcome === undefined) {
      if (persisted.status === "running") running += 1;
      else failed += 1;
    } else if (outcome === "ok") {
      completed += 1;
    } else {
      failed += 1;
    }
  }

  return {
    runId: persisted.runId,
    taskId: persisted.taskId,
    threadId: persisted.threadId,
    name: persisted.name,
    status: persisted.status,
    phases,
    ...(currentPhase === undefined ? {} : { currentPhase }),
    agents: { queued: 0, running, completed, failed },
    logs,
    usage: persisted.usage,
    ...(persisted.result === undefined ? {} : { result: persisted.result }),
    ...(persisted.error === undefined ? {} : { error: persisted.error }),
    scriptPath: persisted.scriptPath,
    journalPath: persisted.journalPath,
  };
}

const make = Effect.gen(function* () {
  const layerScope = yield* Effect.scope;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const orchestratorMcp = yield* OrchestratorMcpService.OrchestratorMcpService;

  const liveRuns = new Map<string, LiveWorkflowRun>();
  const concurrency = workflowConcurrency(NodeOS.cpus().length);
  const startupSweepDone = yield* Deferred.make<void>();

  const requireCapability = (scope: McpInvocationContext.McpInvocationScope) =>
    scope.capabilities.has("orchestration")
      ? Effect.void
      : Effect.fail(
          failure("capability_denied", "This MCP session cannot control orchestration tasks."),
        );

  const threadManagementFailure = (error: unknown) =>
    failure("orchestration_error", `Orchestration error: ${errorMessage(error)}`);

  const runDirFor = (runId: string): string => path.join(serverConfig.workflowRunsDir, runId);
  const runMetaPathFor = (runId: string): string => path.join(runDirFor(runId), "run.json");
  const journalPathFor = (runId: string): string => path.join(runDirFor(runId), "journal.jsonl");

  const readPersistedRun = Effect.fn("WorkflowService.readPersistedRun")(function* (runId: string) {
    if (!isSafeRunId(runId)) return Option.none<PersistedWorkflowRun>();
    const metaPath = runMetaPathFor(runId);
    const exists = yield* fileSystem.exists(metaPath).pipe(Effect.orElseSucceed(() => false));
    if (!exists) return Option.none<PersistedWorkflowRun>();
    const content = yield* fileSystem
      .readFileString(metaPath)
      .pipe(Effect.mapError(() => failure("orchestration_error", `Unable to read ${metaPath}.`)));
    return yield* decodePersistedWorkflowRun(content).pipe(Effect.option);
  });

  const writePersistedRun = Effect.fn("WorkflowService.writePersistedRun")(function* (
    record: PersistedWorkflowRun,
  ) {
    yield* encodePersistedWorkflowRun(record).pipe(
      Effect.flatMap((encoded) =>
        fileSystem.writeFileString(runMetaPathFor(record.runId), encoded),
      ),
      Effect.catch((error) =>
        Effect.logWarning("workflow.run-meta.write-failed", { runId: record.runId, error }),
      ),
    );
  });

  const generateRunId = Effect.gen(function* () {
    const hi = yield* Random.nextIntBetween(0, 0x1000000);
    const lo = yield* Random.nextIntBetween(0, 0x1000000);
    return `wf_${hi.toString(16).padStart(6, "0")}${lo.toString(16).padStart(6, "0")}`;
  });

  // Startup sweep: mark any workflow left running by a previous server process as failed.
  const runStartupSweep = Effect.gen(function* () {
    const dirExists = yield* fileSystem
      .exists(serverConfig.workflowsDir)
      .pipe(Effect.orElseSucceed(() => false));
    if (!dirExists) return;
    const entries = yield* fileSystem
      .readDirectory(serverConfig.workflowsDir)
      .pipe(Effect.orElseSucceed(() => [] as ReadonlyArray<string>));
    for (const entry of entries) {
      if (!isSafeRunId(entry)) continue;
      yield* Effect.gen(function* () {
        const persisted = yield* readPersistedRun(entry);
        if (Option.isNone(persisted)) return;
        const record = persisted.value;
        if (record.status !== "running" || liveRuns.has(record.runId)) return;
        const restartMessage = `The server restarted while this workflow ran. Resume it with t3_workflow_run resumeFromRunId=${record.runId}.`;
        yield* threadManagement.dispatch({
          type: "workflow.complete",
          commandId: CommandId.make(`server:workflow-restart:${record.runId}`),
          parentThreadId: record.threadId,
          taskId: record.taskId,
          status: "failed",
          result: restartMessage,
        });
        yield* writePersistedRun({
          ...record,
          status: "failed",
          error: restartMessage,
        });
      }).pipe(
        Effect.catch((error) =>
          Effect.logWarning("workflow.startup-sweep.entry-failed", { entry, error }),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("workflow.startup-sweep.entry-defect", { entry, cause }),
        ),
      );
    }
  });
  // Fork startup sweep so layer construction stays synchronous and preserves MCP tool registration order.
  yield* runStartupSweep.pipe(
    Effect.ensuring(Deferred.succeed(startupSweepDone, undefined)),
    Effect.catchCause((cause) => Effect.logWarning("workflow.startup-sweep.defect", { cause })),
    Effect.forkIn(layerScope),
  );

  const resolveAgentTarget = Effect.fn("WorkflowService.resolveAgentTarget")(function* (
    call: WorkflowAgentCall,
    runTarget: OrchestratorMcpTarget | undefined,
  ): Effect.fn.Return<OrchestratorMcpTarget | undefined, OrchestratorMcpFailure> {
    const baseTarget = call.opts.target ?? runTarget;
    if (
      baseTarget === undefined &&
      call.opts.model === undefined &&
      call.opts.effort === undefined
    ) {
      return undefined;
    }

    let options: Array<ProviderOptionSelection> | undefined;
    if (call.opts.target !== undefined) {
      if (call.opts.target.options !== undefined) {
        const decoded = yield* decodeTargetOptions(call.opts.target.options).pipe(
          Effect.mapError(() =>
            failure("invalid_request", "agent() opts.target.options is invalid."),
          ),
        );
        options = [...decoded];
      }
    } else if (runTarget?.options !== undefined) {
      options = [...runTarget.options];
    }

    if (call.opts.effort !== undefined) {
      const next = (options ?? []).filter((selection) => selection.id !== "effort");
      next.push({ id: "effort", value: call.opts.effort });
      options = next;
    }

    const providerInstanceId =
      call.opts.target !== undefined
        ? call.opts.target.providerInstanceId === undefined
          ? undefined
          : ProviderInstanceId.make(call.opts.target.providerInstanceId)
        : runTarget?.providerInstanceId;
    const driverKind =
      call.opts.target !== undefined
        ? call.opts.target.driverKind === undefined
          ? undefined
          : ProviderDriverKind.make(call.opts.target.driverKind)
        : runTarget?.driverKind;
    const model = call.opts.model ?? baseTarget?.model;
    const projectId = call.opts.target !== undefined ? undefined : runTarget?.projectId;
    const environmentId = call.opts.target !== undefined ? undefined : runTarget?.environmentId;

    return {
      ...(providerInstanceId === undefined ? {} : { providerInstanceId }),
      ...(driverKind === undefined ? {} : { driverKind }),
      ...(model === undefined ? {} : { model }),
      ...(options === undefined ? {} : { options }),
      ...(projectId === undefined ? {} : { projectId }),
      ...(environmentId === undefined ? {} : { environmentId }),
    };
  });

  const runEngine = Effect.fn("WorkflowService.runEngine")(function* (params: {
    readonly scope: McpInvocationContext.McpThreadInvocationScope;
    readonly parentProjectId: ProjectId;
    readonly parentThreadId: ThreadId;
    readonly parentRunId: RunId;
    readonly taskId: NodeId;
    readonly runId: string;
    readonly meta: WorkflowMeta;
    readonly scriptSource: string;
    readonly scriptPath: string;
    readonly journalPath: string;
    readonly startedAt: number;
    readonly afterSequence: number;
    readonly args: unknown;
    readonly runTarget: OrchestratorMcpTarget | undefined;
    readonly tokenBudget: number | undefined;
    readonly cursor: ReplayCursor | undefined;
    readonly done: Deferred.Deferred<void>;
    readonly onRegisterLive: (live: LiveWorkflowRun) => void;
  }) {
    const {
      scope,
      parentProjectId,
      parentThreadId,
      parentRunId,
      taskId,
      runId,
      meta,
      scriptSource,
      scriptPath,
      journalPath,
      startedAt,
      afterSequence,
      args,
      runTarget,
      tokenBudget,
      cursor,
      done,
    } = params;

    const abortController = createHostAbortController();
    const abortedDeferred = yield* Deferred.make<void>();
    const journalQueue = yield* Queue.unbounded<JournalQueueItem>();
    const progressSignal = yield* Queue.dropping<void>(1);
    const runtimeContext = yield* Effect.context<never>();
    const runSync = Effect.runSyncWith(runtimeContext);
    const runPromise = Effect.runPromiseWith(runtimeContext);

    let status: OrchestratorMcpWorkflowRunStatus = "running";
    let finalResult: Schema.Json | undefined;
    let finalError: string | undefined;
    let endedByEngine = false;
    let stopRequested = false;
    let progressDirty = false;
    let progressSeq = 0;

    const phases: Array<OrchestrationV2WorkflowPhase> = meta.phases.map((p, index) => ({
      index,
      title: p.title,
    }));
    const phaseIndexByTitle = new Map<string, number>(
      phases.map((phase) => [phase.title, phase.index] as const),
    );
    let currentPhase: string | undefined;
    const logs: Array<string> = [];
    const agentsByIndex = new Map<number, OrchestrationV2WorkflowAgent>();
    const liveChildren = new Map<NodeId, ThreadId>();
    const usage: MutableUsage = {
      agents: 0,
      done: 0,
      errors: 0,
      empty: 0,
      cached: 0,
      tokens: 0,
    };

    const ensurePhase = (title: string): { readonly index: number; readonly title: string } => {
      const trimmed = title.trim();
      const existing = phaseIndexByTitle.get(trimmed);
      if (existing !== undefined) return { index: existing, title: trimmed };
      const index = phases.length;
      phaseIndexByTitle.set(trimmed, index);
      phases.push({ index, title: trimmed });
      return { index, title: trimmed };
    };

    const signalProgressSync = (): void => {
      progressDirty = true;
      runSync(Queue.offer(progressSignal, undefined));
    };

    const appendJournalSync = (entry: JournalEntry): void => {
      runSync(Queue.offer(journalQueue, { _tag: "entry", entry }));
    };

    const flushJournal = Effect.gen(function* () {
      const flushed = yield* Deferred.make<void>();
      yield* Queue.offer(journalQueue, { _tag: "flush", done: flushed });
      yield* Deferred.await(flushed);
    });

    const buildUsage = (nowMs: number): OrchestratorMcpWorkflowUsage => ({
      agents: usage.agents,
      done: usage.done,
      errors: usage.errors,
      empty: usage.empty,
      cached: usage.cached,
      tokens: usage.tokens,
      durationMs: Math.max(0, Math.trunc(nowMs - startedAt)),
    });

    const buildWorkflowSnapshot = (nowMs: number): OrchestrationV2SubagentWorkflow => ({
      name: meta.name,
      runHandles: {
        runId,
        scriptPath,
        transcriptDir: runDirFor(runId),
      },
      phases: [...phases],
      agents: [...agentsByIndex.values()].toSorted((a, b) => a.index - b.index),
      totalTokens: usage.tokens,
      durationMs: Math.max(0, Math.trunc(nowMs - startedAt)),
    });

    const buildPersistedRecord = (nowMs: number): PersistedWorkflowRun => ({
      runId,
      threadId: parentThreadId,
      taskId,
      name: meta.name,
      scriptPath,
      journalPath,
      startedAt,
      status,
      phases: [...phases],
      ...(currentPhase === undefined ? {} : { currentPhase }),
      ...(finalResult === undefined ? {} : { result: finalResult }),
      ...(finalError === undefined ? {} : { error: finalError }),
      usage: buildUsage(nowMs),
    });

    const triggerExternalStop = (): void => {
      if (endedByEngine || stopRequested) return;
      stopRequested = true;
      Deferred.doneUnsafe(abortedDeferred, Exit.void);
      if (!abortController.signal.aborted) {
        abortController.abort(new WorkflowAbortedError());
      }
    };

    const liveHandle: LiveWorkflowRun = {
      runId,
      threadId: parentThreadId,
      taskId,
      name: meta.name,
      scriptPath,
      journalPath,
      startedAt,
      done,
      triggerExternalStop,
      currentStatus: () => status,
      getStatus: () =>
        Clock.currentTimeMillis.pipe(
          Effect.map((nowMs) => ({
            runId,
            taskId,
            threadId: parentThreadId,
            name: meta.name,
            status,
            phases: [...phases],
            ...(currentPhase === undefined ? {} : { currentPhase }),
            agents: countAgentStates(agentsByIndex.values()),
            logs: [...logs],
            usage: buildUsage(nowMs),
            ...(finalResult === undefined ? {} : { result: finalResult }),
            ...(finalError === undefined ? {} : { error: finalError }),
            scriptPath,
            journalPath,
          })),
        ),
    };
    params.onRegisterLive(liveHandle);

    const cancelChildTask = Effect.fn("WorkflowService.cancelChildTask")(function* (
      childTaskId: NodeId,
    ) {
      yield* orchestratorMcp
        .cancelTask(scope, {
          taskId: childTaskId,
          reason: "Workflow stopped.",
        })
        .pipe(
          Effect.asVoid,
          Effect.catch((error) =>
            Effect.logWarning("workflow.cancel-child.failed", {
              runId,
              taskId,
              childTaskId,
              error,
            }),
          ),
        );
    });

    const cancelAllLiveChildren = Effect.gen(function* () {
      const ids = [...liveChildren.keys()];
      liveChildren.clear();
      if (ids.length === 0) return;
      yield* Effect.forEach(ids, cancelChildTask, { concurrency: "unbounded", discard: true });
    });

    const waitForChildTaskTerminal = Effect.fn("WorkflowService.waitForChildTaskTerminal")(
      function* (childTaskId: NodeId, childThreadId: ThreadId, fromSequence: number) {
        const readCurrent = Effect.gen(function* () {
          const { subagents } = yield* threadManagement.getThreadRecords(
            parentThreadId,
            ["subagents"],
            { subagentIds: [childTaskId] },
          );
          const subagent = subagents.find((candidate) => candidate.id === childTaskId);
          return subagent;
        });

        const initial = yield* readCurrent;
        if (initial === undefined || isTerminalSubagentStatus(initial.status)) {
          return initial;
        }

        const waitStream = Stream.mergeAll(
          [
            threadManagement
              .streamStoredEventsFrom({
                threadId: parentThreadId,
                afterSequence: fromSequence,
                eventType: "subagent.updated",
              })
              .pipe(
                Stream.filter(
                  ({ event }) =>
                    event.type === "subagent.updated" && event.payload.id === childTaskId,
                ),
              ),
            threadManagement.streamStoredEventsFrom({
              threadId: childThreadId,
              afterSequence: fromSequence,
              eventType: "run.updated",
            }),
            threadManagement.streamStoredEventsFrom({
              threadId: childThreadId,
              afterSequence: fromSequence,
              eventType: "thread.deleted",
            }),
          ],
          { concurrency: "unbounded" },
        ).pipe(
          Stream.mapEffect(() => readCurrent),
          Stream.filter(
            (subagent) => subagent === undefined || isTerminalSubagentStatus(subagent.status),
          ),
          Stream.runHead,
          Effect.map(Option.getOrUndefined),
        );

        return yield* Effect.raceFirst(
          waitStream,
          Deferred.await(abortedDeferred).pipe(
            Effect.andThen(Effect.fail(new WorkflowAbortedError())),
          ),
        );
      },
    );

    const waitForChildRunTerminal = Effect.fn("WorkflowService.waitForChildRunTerminal")(function* (
      childThreadId: ThreadId,
      runIdToWait: RunId,
      fromSequence: number,
    ) {
      const readRun = threadManagement
        .getThreadRecords(childThreadId, ["runs"], { runIds: [runIdToWait] })
        .pipe(Effect.map(({ runs }) => runs.find((candidate) => candidate.id === runIdToWait)));

      const initial = yield* readRun;
      if (initial === undefined || isTerminalRunStatus(initial.status)) {
        return initial;
      }

      const waitStream = Stream.mergeAll(
        [
          threadManagement
            .streamStoredEventsFrom({
              threadId: childThreadId,
              afterSequence: fromSequence,
              eventType: "run.updated",
            })
            .pipe(
              Stream.filter(
                ({ event }) => event.type === "run.updated" && event.payload.id === runIdToWait,
              ),
            ),
          threadManagement.streamStoredEventsFrom({
            threadId: childThreadId,
            afterSequence: fromSequence,
            eventType: "thread.deleted",
          }),
        ],
        { concurrency: "unbounded" },
      ).pipe(
        Stream.mapEffect(() => readRun),
        Stream.filter((run) => run === undefined || isTerminalRunStatus(run.status)),
        Stream.runHead,
        Effect.map(Option.getOrUndefined),
      );

      return yield* Effect.raceFirst(
        waitStream,
        Deferred.await(abortedDeferred).pipe(
          Effect.andThen(Effect.fail(new WorkflowAbortedError())),
        ),
      );
    });

    const loadChildRunSnapshot = Effect.fn("WorkflowService.loadChildRunSnapshot")(function* (
      childThreadId: ThreadId,
      targetRunId: RunId | undefined,
    ) {
      const base = yield* threadManagement.getThreadRecords(childThreadId, [
        "runs",
        "providerTurns",
      ]);
      const run =
        targetRunId === undefined
          ? base.runs.toSorted((a, b) => b.ordinal - a.ordinal)[0]
          : base.runs.find((candidate) => candidate.id === targetRunId);
      const runIds = run === undefined ? [] : [run.id];
      const details = yield* threadManagement.getThreadRecords(
        childThreadId,
        ["messages", "turnItems"],
        {
          messageRoles: ["assistant"],
          messageRunIds: runIds,
          turnItemRunIds: runIds,
          turnItemTypes: SUBAGENT_RESULT_TURN_ITEM_TYPES,
        },
      );
      return {
        run,
        runs: base.runs,
        providerTurns: base.providerTurns,
        messages: details.messages,
        turnItems: details.turnItems,
        model: run?.modelSelection.model ?? base.thread.modelSelection.model,
      };
    });

    const sumThreadTokens = (
      providerTurns: ReadonlyArray<Pick<OrchestrationV2ProviderTurn, "turnTokenUsage">>,
    ): number => {
      let total = 0;
      for (const turn of providerTurns) {
        const tokenUsage = turn.turnTokenUsage;
        if (tokenUsage === undefined) continue;
        total += (tokenUsage.inputTokens ?? 0) + (tokenUsage.outputTokens ?? 0);
      }
      return total;
    };

    const runAgentEffect = Effect.fn("WorkflowService.runAgent")(function* (
      call: WorkflowAgentCall,
      key: string,
      replayed: { readonly cached: true; readonly value: unknown } | { readonly cached: false },
    ): Effect.fn.Return<unknown, WorkflowAbortedError | WorkflowInternalError> {
      const label = agentLabel(call);
      const phaseInfo =
        call.phase !== undefined && call.phase.trim().length > 0
          ? ensurePhase(call.phase)
          : undefined;
      const promptPreview = previewText(call.prompt);
      const effort = call.opts.effort?.trim();

      if (replayed.cached) {
        const now = yield* Clock.currentTimeMillis;
        usage.agents += 1;
        usage.cached += 1;
        const outcome = replayed.value === null ? ("null" as const) : ("ok" as const);
        if (outcome === "ok") usage.done += 1;
        else usage.empty += 1;

        const resultPreview = previewValue(replayed.value);
        agentsByIndex.set(call.index, {
          index: call.index,
          label,
          state: outcome === "ok" ? "completed" : "failed",
          ...(phaseInfo === undefined
            ? {}
            : { phaseIndex: phaseInfo.index, phaseTitle: phaseInfo.title }),
          attempt: 1,
          durationMs: 0,
          queuedAt: now,
          startedAt: now,
          completedAt: now,
          ...(promptPreview === undefined ? {} : { prompt: promptPreview }),
          ...(resultPreview === undefined ? {} : { result: resultPreview }),
        });
        appendJournalSync({
          type: "result",
          index: call.index,
          key,
          outcome,
          value: replayed.value,
          cached: true,
        });
        signalProgressSync();
        yield* flushJournal;
        return replayed.value;
      }

      const queuedAt = yield* Clock.currentTimeMillis;
      usage.agents += 1;
      agentsByIndex.set(call.index, {
        index: call.index,
        label,
        state: "queued",
        ...(phaseInfo === undefined
          ? {}
          : { phaseIndex: phaseInfo.index, phaseTitle: phaseInfo.title }),
        ...(effort !== undefined && effort.length > 0 ? { effort } : {}),
        attempt: 1,
        queuedAt,
        ...(promptPreview === undefined ? {} : { prompt: promptPreview }),
      });
      signalProgressSync();

      const resolvedTarget = yield* resolveAgentTarget(call, runTarget).pipe(Effect.result);
      if (Result.isFailure(resolvedTarget)) {
        const failedAt = yield* Clock.currentTimeMillis;
        usage.errors += 1;
        agentsByIndex.set(call.index, {
          ...agentsByIndex.get(call.index)!,
          state: "failed",
          completedAt: failedAt,
          durationMs: Math.max(0, failedAt - queuedAt),
        });
        appendJournalSync({
          type: "started",
          index: call.index,
          key,
          label,
          phase: phaseInfo?.title ?? null,
          agentId: null,
          childThreadId: null,
        });
        appendJournalSync({
          type: "result",
          index: call.index,
          key,
          outcome: "null",
          value: null,
        });
        signalProgressSync();
        yield* flushJournal;
        return null;
      }
      const target = resolvedTarget.success;

      const role: OrchestratorMcpTaskRole | undefined =
        call.opts.agentType !== undefined && TASK_ROLE_LITERALS.has(call.opts.agentType)
          ? (call.opts.agentType as OrchestratorMcpTaskRole)
          : undefined;
      const delegateTaskInput: OrchestratorMcpDelegateTaskInput = {
        task: call.prompt,
        title: label,
        ...(role === undefined ? {} : { role }),
        ...(call.opts.isolation === undefined ? {} : { isolation: call.opts.isolation }),
        ...(target === undefined ? {} : { target }),
      };
      const outputSchema = call.opts.schema as Readonly<Record<string, Schema.Json>> | undefined;

      const beforeDelegateSeq = yield* threadManagement
        .getThreadEventSequence(parentThreadId)
        .pipe(Effect.orElseSucceed(() => afterSequence));

      const delegated = yield* orchestratorMcp
        .delegateWorkflowAgent(scope, {
          parentRunId,
          workflowNodeId: taskId,
          requestKey: `${runId}:${call.index}`,
          task: delegateTaskInput,
          ...(outputSchema === undefined ? {} : { outputSchema }),
        })
        .pipe(Effect.result);

      if (Result.isFailure(delegated)) {
        if (stopRequested || abortController.signal.aborted) {
          return yield* new WorkflowAbortedError();
        }
        const failedAt = yield* Clock.currentTimeMillis;
        usage.errors += 1;
        agentsByIndex.set(call.index, {
          ...agentsByIndex.get(call.index)!,
          state: "failed",
          completedAt: failedAt,
          durationMs: Math.max(0, failedAt - queuedAt),
        });
        appendJournalSync({
          type: "started",
          index: call.index,
          key,
          label,
          phase: phaseInfo?.title ?? null,
          agentId: null,
          childThreadId: null,
        });
        appendJournalSync({
          type: "result",
          index: call.index,
          key,
          outcome: "null",
          value: null,
        });
        signalProgressSync();
        yield* flushJournal;
        return null;
      }

      const { taskId: childTaskId, childThreadId } = delegated.success;
      liveChildren.set(childTaskId, childThreadId);

      if (stopRequested || abortController.signal.aborted) {
        liveChildren.delete(childTaskId);
        yield* cancelChildTask(childTaskId);
        return yield* new WorkflowAbortedError();
      }

      const startedAtAgent = yield* Clock.currentTimeMillis;
      const requestedModel = target?.model?.trim();
      agentsByIndex.set(call.index, {
        ...agentsByIndex.get(call.index)!,
        agentId: childTaskId,
        childThreadId,
        state: "running",
        startedAt: startedAtAgent,
        ...(requestedModel !== undefined && requestedModel.length > 0
          ? { model: requestedModel }
          : {}),
      });
      appendJournalSync({
        type: "started",
        index: call.index,
        key,
        label,
        phase: phaseInfo?.title ?? null,
        agentId: childTaskId,
        childThreadId,
      });
      signalProgressSync();

      const finishAgent = Effect.fnUntraced(function* (finish: {
        readonly state: "completed" | "failed";
        readonly outcome: "ok" | "null" | "usage_limited";
        readonly value: unknown;
        readonly countAs: "done" | "error" | "empty";
        readonly model?: string | undefined;
        readonly tokens: number;
      }) {
        liveChildren.delete(childTaskId);
        const completedAt = yield* Clock.currentTimeMillis;
        usage.tokens += finish.tokens;
        if (finish.countAs === "done") usage.done += 1;
        else if (finish.countAs === "error") usage.errors += 1;
        else usage.empty += 1;

        const resultPreview = previewValue(finish.value);
        const currentAgent = agentsByIndex.get(call.index)!;
        const resolvedModel = finish.model?.trim() || currentAgent.model;
        agentsByIndex.set(call.index, {
          ...currentAgent,
          state: finish.state,
          ...(resolvedModel !== undefined && resolvedModel.length > 0
            ? { model: resolvedModel }
            : {}),
          totalTokens: finish.tokens,
          completedAt,
          durationMs: Math.max(0, completedAt - startedAtAgent),
          ...(resultPreview === undefined ? {} : { result: resultPreview }),
        });
        appendJournalSync({
          type: "result",
          index: call.index,
          key,
          outcome: finish.outcome,
          value: finish.value,
        });
        signalProgressSync();
        yield* flushJournal;
        return finish.value;
      });

      const settledTask = yield* waitForChildTaskTerminal(
        childTaskId,
        childThreadId,
        beforeDelegateSeq,
      ).pipe(
        Effect.mapError((error) =>
          error._tag === "WorkflowAbortedError"
            ? error
            : new WorkflowInternalError({ messageText: errorMessage(error) }),
        ),
      );
      if (stopRequested || abortController.signal.aborted) {
        return yield* new WorkflowAbortedError();
      }

      const initialSnapshot = yield* loadChildRunSnapshot(childThreadId, undefined).pipe(
        Effect.mapError((error) => new WorkflowInternalError({ messageText: errorMessage(error) })),
      );
      const initialTokens = sumThreadTokens(initialSnapshot.providerTurns);
      const initialRun =
        initialSnapshot.runs.find((candidate) => candidate.delegatedTaskId === childTaskId) ??
        initialSnapshot.run;

      if (settledTask === undefined || settledTask.status !== "completed") {
        const rootFailure =
          initialRun === undefined
            ? undefined
            : latestRootProviderFailure(initialRun, initialSnapshot.turnItems);
        const isUsageLimited =
          initialRun?.status === "failed" && rootFailure?.class === "usage_limit";
        return yield* finishAgent({
          state: "failed",
          outcome: isUsageLimited ? "usage_limited" : "null",
          value: null,
          countAs: "error",
          model: initialSnapshot.model,
          tokens: initialTokens,
        });
      }

      if (outputSchema === undefined) {
        const finalText =
          settledTask.result ??
          (initialRun === undefined ? "" : subagentResultForRun(initialSnapshot, initialRun).text);
        if (finalText.trim().length === 0) {
          return yield* finishAgent({
            state: "completed",
            outcome: "null",
            value: null,
            countAs: "empty",
            model: initialSnapshot.model,
            tokens: initialTokens,
          });
        }
        return yield* finishAgent({
          state: "completed",
          outcome: "ok",
          value: finalText,
          countAs: "done",
          model: initialSnapshot.model,
          tokens: initialTokens,
        });
      }

      // Structured output: prefer run.output.value, fall back to JSON text + checkOutput, re-prompt up to 3 times.
      let currentSnapshot = initialSnapshot;
      let currentRun = initialRun;
      let latestTaskResult = settledTask.result;

      for (let reprompt = 0; ; reprompt += 1) {
        if (currentRun?.output !== undefined) {
          return yield* finishAgent({
            state: "completed",
            outcome: "ok",
            value: currentRun.output.value,
            countAs: "done",
            model: currentSnapshot.model,
            tokens: sumThreadTokens(currentSnapshot.providerTurns),
          });
        }

        const candidateText =
          (reprompt === 0 ? latestTaskResult : undefined) ??
          (currentRun === undefined ? "" : subagentResultForRun(currentSnapshot, currentRun).text);
        const stripped = stripSingleJsonFence(candidateText);
        const checked = yield* checkOutput(outputSchema, stripped);
        if (Result.isSuccess(checked)) {
          return yield* finishAgent({
            state: "completed",
            outcome: "ok",
            value: checked.success,
            countAs: "done",
            model: currentSnapshot.model,
            tokens: sumThreadTokens(currentSnapshot.providerTurns),
          });
        }

        if (reprompt >= MAX_SCHEMA_REPROMPTS) {
          return yield* finishAgent({
            state: "failed",
            outcome: "null",
            value: null,
            countAs: "error",
            model: currentSnapshot.model,
            tokens: sumThreadTokens(currentSnapshot.providerTurns),
          });
        }

        if (stopRequested || abortController.signal.aborted) {
          return yield* new WorkflowAbortedError();
        }

        const problem = formatOutputRejection(checked.failure);
        const nextAttempt = reprompt + 2;
        agentsByIndex.set(call.index, {
          ...agentsByIndex.get(call.index)!,
          attempt: nextAttempt,
        });
        signalProgressSync();

        const beforeRepromptSeq = yield* threadManagement
          .getThreadEventSequence(childThreadId)
          .pipe(Effect.orElseSucceed(() => afterSequence));
        const repromptKey = `${runId}:${call.index}:reprompt:${reprompt + 1}`;
        const sent = yield* threadManagement
          .sendToThread({
            projectId: parentProjectId,
            commandId: stableCommandId({
              scope,
              operation: "workflow-reprompt",
              key: repromptKey,
            }),
            threadId: childThreadId,
            senderThreadId: parentThreadId,
            messageId: stableMessageId({
              scope,
              operation: "workflow-reprompt",
              key: repromptKey,
            }),
            text: `Your result did not match the schema: ${problem}. Submit it with t3_submit_output.`,
            attachments: [],
            mode: "queue",
            outputSchema,
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(Effect.result);

        if (Result.isFailure(sent)) {
          if (stopRequested || abortController.signal.aborted) {
            return yield* new WorkflowAbortedError();
          }
          return yield* finishAgent({
            state: "failed",
            outcome: "null",
            value: null,
            countAs: "error",
            model: currentSnapshot.model,
            tokens: sumThreadTokens(currentSnapshot.providerTurns),
          });
        }

        const settledReprompt = yield* waitForChildRunTerminal(
          childThreadId,
          sent.success.run.id,
          beforeRepromptSeq,
        ).pipe(
          Effect.mapError((error) =>
            error._tag === "WorkflowAbortedError"
              ? error
              : new WorkflowInternalError({ messageText: errorMessage(error) }),
          ),
        );
        if (stopRequested || abortController.signal.aborted) {
          return yield* new WorkflowAbortedError();
        }

        currentSnapshot = yield* loadChildRunSnapshot(childThreadId, sent.success.run.id).pipe(
          Effect.mapError(
            (error) => new WorkflowInternalError({ messageText: errorMessage(error) }),
          ),
        );
        currentRun = currentSnapshot.run ?? settledReprompt;
        latestTaskResult = null;

        if (currentRun === undefined || currentRun.status !== "completed") {
          const rootFailure =
            currentRun === undefined
              ? undefined
              : latestRootProviderFailure(currentRun, currentSnapshot.turnItems);
          const isUsageLimited =
            currentRun?.status === "failed" && rootFailure?.class === "usage_limit";
          return yield* finishAgent({
            state: "failed",
            outcome: isUsageLimited ? "usage_limited" : "null",
            value: null,
            countAs: "error",
            model: currentSnapshot.model,
            tokens: sumThreadTokens(currentSnapshot.providerTurns),
          });
        }
      }
    });

    const host: WorkflowHost = {
      runAgent: (call) => {
        const key = agentCallKey(call.prompt, call.opts);
        // Take from the positional replay cursor synchronously before entering Effect so parallel calls stay ordered.
        const replayed =
          cursor === undefined ? ({ cached: false } as const) : cursor.take(call.index, key);
        return runPromise(
          runAgentEffect(call, key, replayed).pipe(
            Effect.catchTags({
              WorkflowInternalError: () => Effect.succeed(null),
            }),
          ),
        );
      },
      onPhase: (title) => {
        const phaseInfo = ensurePhase(title);
        currentPhase = phaseInfo.title;
        appendJournalSync({ type: "phase", title: phaseInfo.title });
        signalProgressSync();
      },
      onLog: (message) => {
        logs.push(message);
        if (logs.length > MAX_IN_MEMORY_LOGS) logs.shift();
        appendJournalSync({ type: "log", message });
        signalProgressSync();
      },
      loadScript: ({ scriptPath: nestedPath }) =>
        runPromise(fileSystem.readFileString(nestedPath)).catch(() => {
          throw new WorkflowScriptError(`Unable to read workflow script at ${nestedPath}`);
        }),
      budget: {
        total: tokenBudget ?? null,
        spent: () => usage.tokens,
      },
      signal: abortController.signal,
    };

    yield* Effect.scoped(
      Effect.gen(function* () {
        // Single writer fiber keeps journal.jsonl appends strictly serialized.
        yield* Effect.forever(
          Queue.take(journalQueue).pipe(
            Effect.flatMap((item) =>
              item._tag === "flush"
                ? Deferred.succeed(item.done, undefined)
                : fileSystem
                    .writeFileString(journalPath, encodeJournalEntry(item.entry), { flag: "a" })
                    .pipe(
                      Effect.catch((error) =>
                        Effect.logWarning("workflow.journal.append-failed", { runId, error }),
                      ),
                    ),
            ),
          ),
        ).pipe(Effect.forkScoped);

        // Coalesce workflow.progress updates to at most once per second via Effect Clock.
        yield* Effect.gen(function* () {
          let lastSentAt: number | undefined;
          while (true) {
            yield* Queue.take(progressSignal);
            if (!progressDirty || endedByEngine || stopRequested) continue;
            const now = yield* Clock.currentTimeMillis;
            if (lastSentAt !== undefined) {
              const remaining = PROGRESS_COALESCE_INTERVAL_MS - (now - lastSentAt);
              if (remaining > 0) {
                yield* Effect.sleep(Duration.millis(remaining));
              }
            }
            if (!progressDirty || endedByEngine || stopRequested) continue;
            progressDirty = false;
            lastSentAt = yield* Clock.currentTimeMillis;
            progressSeq += 1;
            const latestLog = logs.at(-1);
            yield* threadManagement
              .dispatch({
                type: "workflow.progress",
                commandId: stableCommandId({
                  scope,
                  operation: "workflow-progress",
                  key: `${runId}:${progressSeq}`,
                }),
                parentThreadId,
                taskId,
                workflow: buildWorkflowSnapshot(lastSentAt),
                ...(latestLog === undefined ? {} : { progress: latestLog }),
              })
              .pipe(
                Effect.catch((error) =>
                  Effect.logWarning("workflow.progress.dispatch-failed", {
                    runId,
                    taskId,
                    error,
                  }),
                ),
              );
          }
        }).pipe(Effect.forkScoped);

        // Watch our own task row so stopWorkflow, thread Stop, or t3_workflow_stop aborts the script.
        yield* threadManagement
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
                isTerminalSubagentStatus(event.payload.status),
            ),
            Stream.runHead,
            Effect.tap((matched) =>
              Option.isSome(matched) && !endedByEngine
                ? Effect.sync(triggerExternalStop)
                : Effect.void,
            ),
            Effect.catch((error) =>
              Effect.logWarning("workflow.stop-watcher.failed", { runId, taskId, error }),
            ),
            Effect.forkScoped,
          );

        const scriptOutcome = yield* Effect.tryPromise({
          try: (signal) => {
            signal.addEventListener(
              "abort",
              () => {
                // Layer shutdown interrupts this fiber; leave the task and run.json running for startup sweep.
                Deferred.doneUnsafe(abortedDeferred, Exit.void);
                if (!abortController.signal.aborted) {
                  abortController.abort(new WorkflowAbortedError());
                }
              },
              { once: true },
            );
            return runWorkflowScript({
              script: scriptSource,
              args,
              host,
              concurrency,
            });
          },
          catch: (err) => new WorkflowExecutionError({ messageText: errorMessage(err) }),
        }).pipe(Effect.result);

        if (stopRequested) {
          status = "cancelled";
          yield* cancelAllLiveChildren;
          appendJournalSync({ type: "end", status: "cancelled" });
          yield* flushJournal;
          const now = yield* Clock.currentTimeMillis;
          yield* writePersistedRun(buildPersistedRecord(now));
          return;
        }

        endedByEngine = true;
        if (Result.isSuccess(scriptOutcome)) {
          status = "completed";
          finalResult = scriptOutcome.success as Schema.Json;
          const completedAt = yield* Clock.currentTimeMillis;
          const finalUsage = buildUsage(completedAt);
          appendJournalSync({
            type: "end",
            status: "completed",
            result: scriptOutcome.success,
          });
          yield* flushJournal;
          yield* writePersistedRun(buildPersistedRecord(completedAt));
          yield* threadManagement
            .dispatch({
              type: "workflow.complete",
              commandId: stableCommandId({
                scope,
                operation: "workflow-complete",
                key: runId,
              }),
              parentThreadId,
              taskId,
              status: "completed",
              result: formatCompletionResult({
                name: meta.name,
                result: scriptOutcome.success,
                usage: finalUsage,
                journalPath,
              }),
              workflow: buildWorkflowSnapshot(completedAt),
            })
            .pipe(
              Effect.catch((error) =>
                Effect.logWarning("workflow.complete.dispatch-failed", { runId, taskId, error }),
              ),
            );
          return;
        }

        const errText = scriptOutcome.failure.messageText;
        status = "failed";
        finalError = errText;
        yield* cancelAllLiveChildren;
        const failedAt = yield* Clock.currentTimeMillis;
        appendJournalSync({
          type: "end",
          status: "failed",
          error: errText,
        });
        yield* flushJournal;
        yield* writePersistedRun(buildPersistedRecord(failedAt));
        yield* threadManagement
          .dispatch({
            type: "workflow.complete",
            commandId: stableCommandId({
              scope,
              operation: "workflow-complete",
              key: runId,
            }),
            parentThreadId,
            taskId,
            status: "failed",
            result: errText,
            workflow: buildWorkflowSnapshot(failedAt),
          })
          .pipe(
            Effect.catch((error) =>
              Effect.logWarning("workflow.complete.dispatch-failed", { runId, taskId, error }),
            ),
          );
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          liveRuns.delete(runId);
          Deferred.doneUnsafe(done, Exit.void);
        }),
      ),
    );
  });

  const run = Effect.fn("WorkflowService.run")(function* (
    callerScope: McpInvocationContext.McpInvocationScope,
    input: OrchestratorMcpWorkflowRunInput,
  ): Effect.fn.Return<OrchestratorMcpWorkflowRunResult, OrchestratorMcpFailure> {
    yield* requireCapability(callerScope);
    const scope = yield* McpInvocationContext.requireThreadScope(callerScope, "t3_workflow_run");
    if (input.resumeFromRunId !== undefined) {
      yield* Deferred.await(startupSweepDone);
    }

    const parent = yield* threadManagement
      .getThreadRecords(scope.thread.threadId, ["runs"], {
        runStatuses: ThreadManagementService.ACTIVE_RUN_STATUSES,
      })
      .pipe(Effect.mapError(threadManagementFailure));
    if (parent.thread.deletedAt !== null) {
      return yield* failure("thread_not_found", "The calling thread was not found.");
    }

    let parentRun = ThreadManagementService.latestActiveRun(parent);
    if (parentRun === undefined) {
      const shell = yield* threadManagement
        .getThreadShell(scope.thread.threadId)
        .pipe(Effect.mapError(threadManagementFailure));
      if (
        shell !== null &&
        shell.activeRunId === null &&
        shell.latestRunId !== null &&
        isLiveCallerShell(shell, scope.thread.providerInstanceId)
      ) {
        const { runs } = yield* threadManagement
          .getThreadRecords(scope.thread.threadId, ["runs"], { runIds: [shell.latestRunId] })
          .pipe(Effect.mapError(threadManagementFailure));
        parentRun = runs[0];
      }
    }
    if (
      parent.thread.archivedAt !== null ||
      parentRun === undefined ||
      parentRun.rootNodeId === null ||
      parentRun.providerInstanceId !== scope.thread.providerInstanceId
    ) {
      return yield* failure(
        "parent_not_active",
        "Workflows require an active run owned by this MCP provider session.",
      );
    }

    if (input.target?.environmentId !== undefined) {
      return yield* failure("invalid_request", "Workflow agents run in this environment.");
    }
    if (
      input.target?.projectId !== undefined &&
      input.target.projectId !== parent.thread.projectId
    ) {
      return yield* failure(
        "invalid_request",
        "Workflow agents run in this thread's project. Omit target.projectId.",
      );
    }

    let scriptSource: string;
    if (input.scriptPath !== undefined) {
      scriptSource = yield* fileSystem
        .readFileString(input.scriptPath)
        .pipe(
          Effect.mapError(() =>
            failure("invalid_request", `Unable to read workflow script at ${input.scriptPath}.`),
          ),
        );
    } else if (input.script !== undefined) {
      scriptSource = input.script;
    } else {
      return yield* failure("invalid_request", "Provide either `script` or `scriptPath`.");
    }

    const meta = yield* Effect.try({
      try: () => parseWorkflowMeta(scriptSource),
      catch: (error) => failure("invalid_request", errorMessage(error)),
    });

    let cursor: ReplayCursor | undefined;
    if (input.resumeFromRunId !== undefined) {
      if (!isSafeRunId(input.resumeFromRunId)) {
        return yield* failure(
          "invalid_request",
          `Invalid resumeFromRunId "${input.resumeFromRunId}".`,
        );
      }
      const prevJournalPath = journalPathFor(input.resumeFromRunId);
      const prevExists = yield* fileSystem
        .exists(prevJournalPath)
        .pipe(Effect.orElseSucceed(() => false));
      if (!prevExists) {
        return yield* failure(
          "invalid_request",
          `Journal not found for workflow run "${input.resumeFromRunId}".`,
        );
      }
      const prevText = yield* fileSystem
        .readFileString(prevJournalPath)
        .pipe(
          Effect.mapError(() =>
            failure(
              "invalid_request",
              `Unable to read journal for workflow run "${input.resumeFromRunId}".`,
            ),
          ),
        );
      cursor = yield* Effect.try({
        try: () => replayCursor(decodeJournal(prevText)),
        catch: (error) => failure("invalid_request", errorMessage(error)),
      });
    }

    const runId = yield* generateRunId;
    const runDir = runDirFor(runId);
    const snapshotScriptPath = path.join(runDir, "script.js");
    const journalPath = journalPathFor(runId);

    yield* fileSystem
      .makeDirectory(runDir, { recursive: true })
      .pipe(
        Effect.mapError(() =>
          failure("orchestration_error", `Unable to create workflow directory ${runDir}.`),
        ),
      );
    yield* fileSystem
      .writeFileString(snapshotScriptPath, scriptSource)
      .pipe(
        Effect.mapError(() =>
          failure("orchestration_error", `Unable to write workflow script ${snapshotScriptPath}.`),
        ),
      );
    yield* fileSystem
      .writeFileString(journalPath, "")
      .pipe(
        Effect.mapError(() =>
          failure("orchestration_error", `Unable to create workflow journal ${journalPath}.`),
        ),
      );

    const initialPhases = meta.phases.map((p, index) => ({ index, title: p.title }));
    const initialWorkflow: OrchestrationV2SubagentWorkflow = {
      name: meta.name,
      runHandles: {
        runId,
        scriptPath: snapshotScriptPath,
        transcriptDir: runDir,
      },
      phases: initialPhases,
      agents: [],
    };

    const started = yield* threadManagement
      .dispatch({
        type: "workflow.start",
        commandId: stableCommandId({ scope, operation: "workflow-start", key: runId }),
        parentThreadId: scope.thread.threadId,
        parentRunId: parentRun.id,
        parentNodeId: parentRun.rootNodeId,
        title: meta.name,
        description: meta.description,
        modelSelection: parentRun.modelSelection,
        workflow: initialWorkflow,
      })
      .pipe(
        Effect.mapError((error) => {
          const message = userFacingDispatchErrorMessage(error) ?? errorMessage(error);
          return failure(
            /not active|stopping/iu.test(message) ? "parent_not_active" : "orchestration_error",
            `Unable to start workflow: ${message}`,
          );
        }),
      );

    const taskEvent = started.storedEvents.find(
      (stored) =>
        stored.event.type === "subagent.updated" && stored.event.payload.origin === "app_owned",
    );
    if (taskEvent?.event.type !== "subagent.updated") {
      return yield* failure(
        "orchestration_error",
        "Workflow start command did not produce a task projection.",
      );
    }
    const taskId = taskEvent.event.payload.id;
    const startedAt = yield* Clock.currentTimeMillis;

    yield* writePersistedRun({
      runId,
      threadId: scope.thread.threadId,
      taskId,
      name: meta.name,
      scriptPath: snapshotScriptPath,
      journalPath,
      startedAt,
      status: "running",
      phases: initialPhases,
      usage: {
        agents: 0,
        done: 0,
        errors: 0,
        empty: 0,
        cached: 0,
        tokens: 0,
        durationMs: 0,
      },
    });

    const done = yield* Deferred.make<void>();
    const registered = yield* Deferred.make<void>();

    yield* runEngine({
      scope,
      parentProjectId: parent.thread.projectId,
      parentThreadId: scope.thread.threadId,
      parentRunId: parentRun.id,
      taskId,
      runId,
      meta,
      scriptSource,
      scriptPath: snapshotScriptPath,
      journalPath,
      startedAt,
      afterSequence: started.sequence,
      args: input.args,
      runTarget: input.target,
      tokenBudget: input.tokenBudget,
      cursor,
      done,
      onRegisterLive: (live) => {
        liveRuns.set(runId, live);
        Deferred.doneUnsafe(registered, Exit.void);
      },
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("workflow.engine.defect", { runId, taskId, cause }),
      ),
      Effect.forkIn(layerScope),
    );
    yield* Deferred.await(registered);

    return {
      runId,
      taskId,
      status: "running",
      scriptPath: snapshotScriptPath,
      journalPath,
      next: ORCHESTRATOR_MCP_RUNNING_TASK_NEXT,
    } satisfies OrchestratorMcpWorkflowRunResult;
  });

  const status = Effect.fn("WorkflowService.status")(function* (
    scope: McpInvocationContext.McpInvocationScope,
    input: OrchestratorMcpWorkflowStatusInput,
  ): Effect.fn.Return<OrchestratorMcpWorkflowStatusResult, OrchestratorMcpFailure> {
    yield* requireCapability(scope);
    yield* Deferred.await(startupSweepDone);
    const live = liveRuns.get(input.runId);
    if (live !== undefined) {
      return yield* live.getStatus();
    }
    const persisted = yield* readPersistedRun(input.runId);
    if (Option.isNone(persisted)) {
      return yield* failure("task_not_found", `Workflow run "${input.runId}" was not found.`);
    }
    const journalText = yield* fileSystem
      .readFileString(persisted.value.journalPath)
      .pipe(Effect.orElseSucceed(() => ""));
    const entries = yield* Effect.try({
      try: () => decodeJournal(journalText),
      catch: () => [] as ReadonlyArray<JournalEntry>,
    }).pipe(Effect.orElseSucceed(() => [] as ReadonlyArray<JournalEntry>));
    return reconstructFromJournal(persisted.value, entries);
  });

  const loadCallerForStop = Effect.fn("WorkflowService.loadCallerForStop")(function* (
    scope: McpInvocationContext.McpInvocationScope,
  ): Effect.fn.Return<Caller, OrchestratorMcpFailure> {
    if (scope.client?.access === "read-only") {
      return yield* failure(
        "capability_denied",
        "This tool changes the environment, and this MCP client was approved for read-only access.",
      );
    }
    if (scope.thread === undefined) {
      return {
        scope,
        threads: threadManagement,
        caller: undefined,
        limits: McpInvocationContext.clientModeCeiling(scope.client),
      };
    }
    const caller = yield* threadManagement
      .getThreadShell(scope.thread.threadId)
      .pipe(Effect.mapError(threadManagementFailure));
    if (caller === null || caller.deletedAt !== null) {
      return yield* failure("thread_not_found", "The calling thread was not found.");
    }
    const resolved: Caller = {
      scope,
      threads: threadManagement,
      caller,
      limits: { runtimeMode: caller.runtimeMode, interactionMode: caller.interactionMode },
    };
    yield* assertLiveCaller(resolved);
    return resolved;
  });

  const stop = Effect.fn("WorkflowService.stop")(function* (
    scope: McpInvocationContext.McpInvocationScope,
    runId: string,
  ): Effect.fn.Return<
    { readonly runId: string; readonly status: OrchestratorMcpWorkflowRunStatus },
    OrchestratorMcpFailure
  > {
    yield* requireCapability(scope);
    yield* Deferred.await(startupSweepDone);
    const live = liveRuns.get(runId);
    const persisted = live === undefined ? yield* readPersistedRun(runId) : Option.none();
    if (live === undefined && Option.isNone(persisted)) {
      return yield* failure("task_not_found", `Workflow run "${runId}" was not found.`);
    }

    const targetThreadId =
      live !== undefined ? live.threadId : Option.getOrThrow(persisted).threadId;
    const targetTaskId = live !== undefined ? live.taskId : Option.getOrThrow(persisted).taskId;
    const knownStatus =
      live !== undefined ? live.currentStatus() : Option.getOrThrow(persisted).status;

    const caller = yield* loadCallerForStop(scope);
    if (targetThreadId !== scope.thread?.threadId) {
      const targetShell = yield* threadManagement
        .getThreadShell(targetThreadId)
        .pipe(Effect.mapError(threadManagementFailure));
      if (targetShell !== null && targetShell.deletedAt === null) {
        yield* assertTargetWithinLimits(caller.limits, targetShell);
        yield* assertSameLink(caller, targetShell);
      }
    }

    if (knownStatus !== "running") {
      return { runId, status: knownStatus };
    }

    const stopped = yield* threadManagement
      .stopWorkflow({ threadId: targetThreadId, subagentId: targetTaskId })
      .pipe(Effect.result);

    if (Result.isFailure(stopped)) {
      if (stopped.failure.reason === "not-running") {
        const latestLive = liveRuns.get(runId);
        if (latestLive !== undefined) {
          const latestStatus = latestLive.currentStatus();
          if (latestStatus !== "running") return { runId, status: latestStatus };
        }
        const latestPersisted = yield* readPersistedRun(runId);
        if (Option.isSome(latestPersisted) && latestPersisted.value.status !== "running") {
          return { runId, status: latestPersisted.value.status };
        }
        const { subagents } = yield* threadManagement
          .getThreadRecords(targetThreadId, ["subagents"], { subagentIds: [targetTaskId] })
          .pipe(
            Effect.orElseSucceed(() => ({
              subagents: [] as ReadonlyArray<OrchestrationV2Subagent>,
            })),
          );
        const task = subagents.find((candidate) => candidate.id === targetTaskId);
        return {
          runId,
          status: task === undefined ? "cancelled" : subagentStatusToWorkflowStatus(task.status),
        };
      }
      return yield* failure(
        stopped.failure.reason === "unavailable" || stopped.failure.reason === "stop-failed"
          ? "orchestration_error"
          : "invalid_request",
        stopped.failure.message,
      );
    }

    if (live !== undefined) {
      live.triggerExternalStop();
      yield* Deferred.await(live.done);
    } else if (Option.isSome(persisted)) {
      yield* writePersistedRun({ ...persisted.value, status: "cancelled" });
    }

    return { runId, status: "cancelled" };
  });

  return WorkflowService.of({
    run,
    status,
    stop,
  });
});

export const layer = Layer.effect(WorkflowService, make);
