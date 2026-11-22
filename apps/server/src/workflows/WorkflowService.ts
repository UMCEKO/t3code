// @effect-diagnostics nodeBuiltinImport:off -- Workflow script reads use O_NONBLOCK + fstat to reject special files without blocking.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
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
  type ModelSelection,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  type OrchestrationV2SubagentWorkflow,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
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
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  SUBAGENT_RESULT_TURN_ITEM_TYPES,
  subagentResultForRun,
} from "@t3tools/provider-core/server/subagentProjection";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { latestRootProviderFailure } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
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
import { DispatchModeLimit } from "../orchestration-v2/DispatchModeLimit.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import { checkOutput, type OutputRejection } from "../orchestration-v2/runOutput.ts";
import { userFacingDispatchErrorMessage } from "../orchestration-v2/UserFacingErrors.ts";
import {
  MAX_PENDING_JOURNAL_BYTES,
  MAX_PENDING_JOURNAL_ENTRIES,
  agentCallKey,
  decodeJournal,
  makeJournalWriter,
  replayCursor,
  type JournalEntry,
  type ReplayCursor,
} from "./journal.ts";
import {
  WORKFLOW_MESSAGE_LIMITS,
  parseWorkflowMeta,
  reapUnconfirmedSandboxChildren,
  runWorkflowScript,
  workflowConcurrency,
  WorkflowScriptError,
  type WorkflowAgentCall,
  type WorkflowHost,
  type WorkflowMeta,
} from "./sandbox.ts";

const MAX_PROGRESS_PREVIEW_CHARS = 160;
const MAX_COMPLETION_TEXT_BYTES = 12_000;
const MAX_COMPLETION_NAME_BYTES = 200;
const MAX_IN_MEMORY_LOGS = 50;
const MAX_DISTINCT_PHASES = 256;
const MAX_SCHEMA_REPROMPTS = 3;
const PROGRESS_COALESCE_INTERVAL_MS = 1_000;
const MAX_DEFAULT_TITLE_CHARS = 80;
const MAX_EXPLICIT_TITLE_CHARS = 512;
const RETRY_INITIAL_DELAY_MS = 50;
const RETRY_MAX_DELAY_MS = 2_000;
const STARTUP_RECOVERY_MAX_ATTEMPTS = 6;
const STOP_CONFIRM_WAIT_MS = 2_000;
const STOP_WAIT_BOUND_MS = 30_000;

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
  taskId: Schema.NullOr(NodeId),
  name: TrimmedNonEmptyString,
  scriptPath: TrimmedNonEmptyString,
  journalPath: TrimmedNonEmptyString,
  startedAt: NonNegativeInt,
  status: OrchestratorMcpWorkflowRunStatus,
  cleanupPending: Schema.optional(Schema.Boolean),
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
const decodeTargetOptionsSync = Schema.decodeUnknownSync(OrchestratorMcpTargetOptions);

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

class WorkflowStopNotConfirmedError extends Schema.TaggedError<WorkflowStopNotConfirmedError>()(
  "WorkflowStopNotConfirmedError",
  {
    messageText: Schema.String,
  },
) {}

class WorkflowScriptTooLargeError extends Schema.TaggedError<WorkflowScriptTooLargeError>()(
  "WorkflowScriptTooLargeError",
  {
    scriptPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workflow script at ${this.scriptPath} exceeds the 4 MiB limit.`;
  }
}

class WorkflowScriptNotRegularFileError extends Schema.TaggedError<WorkflowScriptNotRegularFileError>()(
  "WorkflowScriptNotRegularFileError",
  {
    scriptPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workflow script at ${this.scriptPath} must be a regular file.`;
  }
}

class WorkflowScriptReadError extends Schema.TaggedError<WorkflowScriptReadError>()(
  "WorkflowScriptReadError",
  {
    scriptPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Unable to read workflow script at ${this.scriptPath}.`;
  }
}

export type WorkflowScriptFileError = WorkflowScriptNotRegularFileError | WorkflowScriptReadError;

export interface OpenWorkflowScriptFile {
  readonly stat: Effect.Effect<
    { readonly isRegularFile: boolean; readonly size: number },
    WorkflowScriptFileError
  >;
  readonly readChunk: (
    maxBytes: number,
  ) => Effect.Effect<Option.Option<Uint8Array>, WorkflowScriptFileError>;
}

export type WorkflowScriptFileOpener = (
  scriptPath: string,
) => Effect.Effect<OpenWorkflowScriptFile, WorkflowScriptFileError, Scope.Scope>;

const errorCodeOf = (cause: unknown): string | undefined => {
  if (typeof cause === "object" && cause !== null && "code" in cause) {
    const code = (cause as { readonly code?: unknown }).code;
    if (typeof code === "string") return code;
  }
  return undefined;
};

const defaultOpenWorkflowScriptFile: WorkflowScriptFileOpener = (scriptPath) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: () =>
        NodeFSP.open(scriptPath, NodeFS.constants.O_RDONLY | (NodeFS.constants.O_NONBLOCK ?? 0)),
      catch: (cause): WorkflowScriptFileError =>
        errorCodeOf(cause) === "EISDIR"
          ? new WorkflowScriptNotRegularFileError({ scriptPath })
          : new WorkflowScriptReadError({ scriptPath, cause }),
    }),
    (handle) => Effect.promise(() => handle.close()).pipe(Effect.ignore),
  ).pipe(
    Effect.map((handle) => ({
      stat: Effect.tryPromise({
        try: async () => {
          const info = await handle.stat();
          return { isRegularFile: info.isFile(), size: info.size };
        },
        catch: (cause): WorkflowScriptFileError =>
          new WorkflowScriptReadError({ scriptPath, cause }),
      }),
      readChunk: (maxBytes) =>
        Effect.tryPromise({
          try: async () => {
            const buffer = Buffer.allocUnsafe(maxBytes);
            const { bytesRead } = await handle.read(buffer, 0, maxBytes, null);
            return bytesRead === 0
              ? Option.none<Uint8Array>()
              : Option.some(buffer.subarray(0, bytesRead));
          },
          catch: (cause): WorkflowScriptFileError =>
            new WorkflowScriptReadError({ scriptPath, cause }),
        }),
    })),
  );

export const WorkflowScriptFileOpener = Context.Reference<WorkflowScriptFileOpener>(
  "t3/workflows/WorkflowScriptFileOpener",
  { defaultValue: () => defaultOpenWorkflowScriptFile },
);

function createHostAbortController(): AbortController {
  return new AbortController();
}

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
  readonly triggerExternalStop: (stopRequestedAt?: number) => void;
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

function truncateUtf8Bytes(text: string, maxBytes: number): string {
  if (maxBytes <= 0) return "";
  if (Buffer.byteLength(text, "utf8") <= maxBytes) return text;
  let bytes = 0;
  let endIndex = 0;
  for (const ch of text) {
    const chBytes = Buffer.byteLength(ch, "utf8");
    if (bytes + chBytes > maxBytes) break;
    bytes += chBytes;
    endIndex += ch.length;
  }
  return text.slice(0, endIndex);
}

function isAppOwnedWorkflowTask(
  task: Pick<OrchestrationV2Subagent, "origin" | "childThreadId" | "workflow">,
): boolean {
  return task.origin === "app_owned" && task.childThreadId === null && task.workflow !== undefined;
}

function isLiveChildRun(run: OrchestrationV2Run): boolean {
  return !isTerminalRunStatus(run.status) && run.status !== "waiting";
}

function hasPendingBackgroundWork(state: {
  readonly thread: Pick<
    OrchestrationV2ThreadProjection["thread"],
    "activeProviderThreadId" | "pullRequests"
  >;
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly providerThreads: ReadonlyArray<OrchestrationV2ProviderThread>;
  readonly turnItems: ReadonlyArray<OrchestrationV2TurnItem>;
}): boolean {
  const latestRun = state.runs.findLast((run) => run.status !== "queued");
  if (
    derivePendingBackgroundWork({
      latestRun,
      providerThreads: state.providerThreads,
      turnItems: state.turnItems,
      activeProviderThreadId: state.thread.activeProviderThreadId,
      runs: state.runs,
      pullRequests: state.thread.pullRequests,
    }).length > 0
  ) {
    return true;
  }
  return state.providerThreads.some(
    (providerThread) => (providerThread.pendingBackgroundTasks?.length ?? 0) > 0,
  );
}

function withoutCleanupPending(record: PersistedWorkflowRun): PersistedWorkflowRun {
  const { cleanupPending: _cleanupPending, ...rest } = record;
  return rest;
}

function truncateWorkflowTitle(name: string): string {
  const trimmed = name.trim();
  if (Buffer.byteLength(trimmed, "utf8") <= MAX_COMPLETION_NAME_BYTES) return trimmed;
  const suffix = "...";
  return `${truncateUtf8Bytes(trimmed, MAX_COMPLETION_NAME_BYTES - suffix.length)}${suffix}`;
}

function formatCompletionResult(input: {
  readonly runId: string;
  readonly name: string;
  readonly result: unknown;
  readonly usage: OrchestratorMcpWorkflowUsage;
  readonly journalPath: string;
}): string {
  const rawJson = JSON.stringify(input.result ?? null) ?? "null";
  const runLine = `Run: ${input.runId} (full result available from t3_workflow_status with runId ${input.runId})`;
  const usageLine = `Usage: ${input.usage.agents} agents, ${input.usage.done} done, ${input.usage.errors} errors, ${input.usage.empty} empty, ${input.usage.tokens} tokens, ${formatDuration(input.usage.durationMs)}`;
  const journalLine = `Journal: ${input.journalPath}`;
  const fullHeader = `Workflow ${input.name} completed.`;
  const full = [fullHeader, runLine, `Result: ${rawJson}`, usageLine, journalLine].join("\n");
  if (Buffer.byteLength(full, "utf8") <= MAX_COMPLETION_TEXT_BYTES) {
    return full;
  }
  const headerLine = `Workflow ${truncateWorkflowTitle(input.name)} completed.`;
  const withBoundedName = [headerLine, runLine, `Result: ${rawJson}`, usageLine, journalLine].join(
    "\n",
  );
  if (Buffer.byteLength(withBoundedName, "utf8") <= MAX_COMPLETION_TEXT_BYTES) {
    return withBoundedName;
  }
  const suffix = "... (truncated)";
  const fixedBytes = Buffer.byteLength(
    [headerLine, runLine, `Result: ${suffix}`, usageLine, journalLine].join("\n"),
    "utf8",
  );
  if (fixedBytes <= MAX_COMPLETION_TEXT_BYTES) {
    const availableBytes = MAX_COMPLETION_TEXT_BYTES - fixedBytes;
    const resultJson = `${truncateUtf8Bytes(rawJson, availableBytes)}${suffix}`;
    return [headerLine, runLine, `Result: ${resultJson}`, usageLine, journalLine].join("\n");
  }
  const essential = [runLine, usageLine, headerLine].join("\n");
  if (Buffer.byteLength(essential, "utf8") <= MAX_COMPLETION_TEXT_BYTES) {
    return essential;
  }
  return truncateUtf8Bytes(essential, MAX_COMPLETION_TEXT_BYTES);
}

function formatFailureResult(input: { readonly runId: string; readonly error: string }): string {
  const runLine = `Run: ${input.runId} (details available from t3_workflow_status with runId ${input.runId})`;
  const full = `${input.error}\n${runLine}`;
  if (Buffer.byteLength(full, "utf8") <= MAX_COMPLETION_TEXT_BYTES) {
    return full;
  }
  const suffix = "... (truncated)";
  const overhead = Buffer.byteLength(`${suffix}\n${runLine}`, "utf8");
  if (overhead <= MAX_COMPLETION_TEXT_BYTES) {
    const availableBytes = MAX_COMPLETION_TEXT_BYTES - overhead;
    return `${truncateUtf8Bytes(input.error, availableBytes)}${suffix}\n${runLine}`;
  }
  return truncateUtf8Bytes(runLine, MAX_COMPLETION_TEXT_BYTES);
}

function resolveAgentTargetSync(
  call: WorkflowAgentCall,
  runTarget: OrchestratorMcpTarget | undefined,
): Result.Result<OrchestratorMcpTarget | undefined, OrchestratorMcpFailure> {
  const callTarget = call.opts.target;
  if (
    callTarget === undefined &&
    runTarget === undefined &&
    call.opts.model === undefined &&
    call.opts.effort === undefined
  ) {
    return Result.succeed(undefined);
  }

  // Replacing provider/driver starts a clean target instead of mixing options across providers.
  const switchesProvider =
    callTarget !== undefined &&
    (callTarget.providerInstanceId !== undefined || callTarget.driverKind !== undefined);
  const inheritedTarget = switchesProvider ? undefined : runTarget;

  let options: Array<ProviderOptionSelection> | undefined;
  if (callTarget?.options !== undefined) {
    try {
      options = [...decodeTargetOptionsSync(callTarget.options)];
    } catch {
      return Result.fail(failure("invalid_request", "agent() opts.target.options is invalid."));
    }
  } else if (inheritedTarget?.options !== undefined) {
    options = [...inheritedTarget.options];
  }

  if (call.opts.effort !== undefined) {
    const next = (options ?? []).filter((selection) => selection.id !== "effort");
    next.push({ id: "effort", value: call.opts.effort });
    options = next;
  }

  const providerInstanceId =
    callTarget?.providerInstanceId !== undefined
      ? ProviderInstanceId.make(callTarget.providerInstanceId)
      : inheritedTarget?.providerInstanceId;
  const driverKind =
    callTarget?.driverKind !== undefined
      ? ProviderDriverKind.make(callTarget.driverKind)
      : inheritedTarget?.driverKind;
  const model = call.opts.model ?? callTarget?.model ?? inheritedTarget?.model;
  const projectId = inheritedTarget?.projectId;
  const environmentId = inheritedTarget?.environmentId;

  return Result.succeed({
    ...(providerInstanceId === undefined ? {} : { providerInstanceId }),
    ...(driverKind === undefined ? {} : { driverKind }),
    ...(model === undefined ? {} : { model }),
    ...(options === undefined ? {} : { options }),
    ...(projectId === undefined ? {} : { projectId }),
    ...(environmentId === undefined ? {} : { environmentId }),
  });
}

function normalizeModelSelectionForKey(modelSelection: ModelSelection): Record<string, unknown> {
  const options =
    modelSelection.options !== undefined && modelSelection.options.length > 0
      ? [...modelSelection.options].toSorted((a, b) => a.id.localeCompare(b.id))
      : undefined;
  return {
    instanceId: modelSelection.instanceId,
    model: modelSelection.model,
    ...(options === undefined ? {} : { options }),
  };
}

function agentOptsForReplayKey(
  call: WorkflowAgentCall,
  resolvedTarget: Result.Result<OrchestratorMcpTarget | undefined, OrchestratorMcpFailure>,
  resolvedModelSelection: Result.Result<ModelSelection, OrchestratorMcpFailure>,
  runTarget: OrchestratorMcpTarget | undefined,
): Record<string, unknown> {
  const { model: _model, effort: _effort, target: _target, ...rest } = call.opts;
  if (Result.isFailure(resolvedTarget) || Result.isFailure(resolvedModelSelection)) {
    return {
      ...call.opts,
      ...(runTarget === undefined ? {} : { runTarget }),
    };
  }
  return {
    ...rest,
    modelSelection: normalizeModelSelectionForKey(resolvedModelSelection.success),
  };
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
    if (phases.length >= MAX_DISTINCT_PHASES) return;
    const index = phases.length;
    phaseIndexByTitle.set(trimmed, index);
    phases.push({ index, title: trimmed });
  };

  let currentPhase = persisted.currentPhase;
  const logs: Array<string> = [];
  const startedIndices = new Set<number>();
  const resultOutcomeByIndex = new Map<number, "ok" | "null" | "usage_limited" | "cancelled">();

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
    taskId: persisted.taskId ?? NodeId.make(`node:workflow:unstarted:${persisted.runId}`),
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

const retryDelayMs = (attempt: number): number =>
  Math.min(RETRY_MAX_DELAY_MS, RETRY_INITIAL_DELAY_MS * 2 ** attempt);

const withBoundedRetry = Effect.fn("WorkflowService.withBoundedRetry")(function* <A, E, R>(
  runAttempt: (attempt: number) => Effect.Effect<A, E, R>,
  options: {
    readonly maxAttempts: number;
    readonly onRetry: (attempt: number, error: E) => Effect.Effect<void>;
  },
): Effect.fn.Return<A, E, R> {
  for (let attempt = 0; ; attempt += 1) {
    const outcome = yield* runAttempt(attempt).pipe(Effect.result);
    if (Result.isSuccess(outcome)) return outcome.success;
    if (attempt + 1 >= options.maxAttempts) {
      return yield* Effect.fail(outcome.failure);
    }
    yield* options.onRetry(attempt, outcome.failure);
    yield* Effect.sleep(Duration.millis(retryDelayMs(attempt)));
  }
});

const runDirIn = (path: Path.Path, workflowsDir: string, runId: string): string =>
  path.join(workflowsDir, runId);
const runMetaPathIn = (path: Path.Path, workflowsDir: string, runId: string): string =>
  path.join(runDirIn(path, workflowsDir, runId), "run.json");
const journalPathIn = (path: Path.Path, workflowsDir: string, runId: string): string =>
  path.join(runDirIn(path, workflowsDir, runId), "journal.jsonl");

const isPlatformNotFound = (cause: unknown): boolean =>
  PlatformError.isPlatformError(cause) && cause.reason._tag === "NotFound";

const isOrchestratorProjectionError = Schema.is(Orchestrator.OrchestratorProjectionError);
const isProjectionStoreThreadNotFoundError = Schema.is(
  ProjectionStore.ProjectionStoreThreadNotFoundError,
);

const isThreadNotFound = (cause: unknown): boolean =>
  isProjectionStoreThreadNotFoundError(cause) ||
  (isOrchestratorProjectionError(cause) && isProjectionStoreThreadNotFoundError(cause.cause));

const readPersistedRunWith = Effect.fn("WorkflowService.readPersistedRun")(function* (
  deps: {
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
    readonly workflowsDir: string;
  },
  runId: string,
) {
  if (!isSafeRunId(runId)) return Option.none<PersistedWorkflowRun>();
  const dirPath = runDirIn(deps.path, deps.workflowsDir, runId);
  const dirStat = yield* deps.fileSystem.stat(dirPath).pipe(
    Effect.asSome,
    Effect.catchIf(isPlatformNotFound, () => Effect.succeed(Option.none<FileSystem.File.Info>())),
    Effect.mapError(() => failure("orchestration_error", `Unable to inspect ${dirPath}.`)),
  );
  if (Option.isNone(dirStat) || dirStat.value.type !== "Directory") {
    return Option.none<PersistedWorkflowRun>();
  }
  const metaPath = runMetaPathIn(deps.path, deps.workflowsDir, runId);
  const contentOption = yield* deps.fileSystem.readFileString(metaPath).pipe(
    Effect.asSome,
    Effect.catchIf(isPlatformNotFound, () => Effect.succeed(Option.none<string>())),
    Effect.mapError(() => failure("orchestration_error", `Unable to read ${metaPath}.`)),
  );
  if (Option.isNone(contentOption)) return Option.none<PersistedWorkflowRun>();
  const decoded = yield* decodePersistedWorkflowRun(contentOption.value).pipe(Effect.option);
  if (Option.isNone(decoded) || decoded.value.runId !== runId) {
    return Option.none<PersistedWorkflowRun>();
  }
  return decoded;
});

const writePersistedRunStrictWith = Effect.fn("WorkflowService.writePersistedRunStrict")(function* (
  deps: {
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
    readonly workflowsDir: string;
  },
  record: PersistedWorkflowRun,
) {
  const encoded = yield* encodePersistedWorkflowRun(record);
  const dir = runDirIn(deps.path, deps.workflowsDir, record.runId);
  const targetPath = runMetaPathIn(deps.path, deps.workflowsDir, record.runId);
  const nonce = (yield* Random.nextIntBetween(0, 0x1000000)).toString(16).padStart(6, "0");
  const tempPath = deps.path.join(dir, `.run.json.${nonce}.tmp`);
  yield* deps.fileSystem.writeFileString(tempPath, encoded);
  yield* deps.fileSystem
    .rename(tempPath, targetPath)
    .pipe(Effect.tapError(() => deps.fileSystem.remove(tempPath).pipe(Effect.ignore)));
});

const writePersistedRunWith = Effect.fn("WorkflowService.writePersistedRun")(function* (
  deps: {
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
    readonly workflowsDir: string;
  },
  record: PersistedWorkflowRun,
) {
  yield* writePersistedRunStrictWith(deps, record).pipe(
    Effect.catch((error) =>
      Effect.logWarning("workflow.run-meta.write-failed", { runId: record.runId, error }),
    ),
  );
});

const readWorkflowTaskStrict = Effect.fn("WorkflowService.readWorkflowTaskStrict")(function* (
  threadManagement: ThreadManagementService.ThreadManagementService["Service"],
  threadId: ThreadId,
  taskId: NodeId,
) {
  const shell = yield* threadManagement.getThreadShell(threadId);
  if (shell === null || shell.deletedAt !== null) return undefined;
  const records = yield* threadManagement
    .getThreadRecords(threadId, ["subagents"], {
      subagentIds: [taskId],
    })
    .pipe(
      Effect.asSome,
      Effect.catchIf(isThreadNotFound, () => Effect.succeedNone),
    );
  if (Option.isNone(records)) return undefined;
  return records.value.subagents.find((candidate) => candidate.id === taskId);
});

function buildStoppedSnapshotFromSql(
  workflow: OrchestrationV2SubagentWorkflow | undefined,
  stoppedAt: number,
): OrchestrationV2SubagentWorkflow | undefined {
  if (workflow === undefined) return undefined;
  return {
    ...workflow,
    agents: workflow.agents.map((agent) =>
      agent.state === "queued" || agent.state === "running"
        ? {
            ...agent,
            state: "failed" as const,
            completedAt: stoppedAt,
            durationMs: Math.max(0, stoppedAt - (agent.startedAt ?? agent.queuedAt ?? stoppedAt)),
          }
        : agent,
    ),
  };
}

interface OwnedChildTarget {
  readonly childThreadId: ThreadId;
  readonly childTaskId: NodeId | undefined;
}

const READ_SCRIPT_CHUNK_BYTES = 64 * 1024;

const readBoundedWorkflowScript = Effect.fn("WorkflowService.readBoundedWorkflowScript")(function* (
  scriptPath: string,
) {
  const openScriptFile = yield* WorkflowScriptFileOpener;
  const maxBytes = WORKFLOW_MESSAGE_LIMITS.maxValueBytes;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const file = yield* openScriptFile(scriptPath);
      const info = yield* file.stat;
      if (!info.isRegularFile) {
        return yield* new WorkflowScriptNotRegularFileError({ scriptPath });
      }
      if (info.size > maxBytes) {
        return yield* new WorkflowScriptTooLargeError({ scriptPath });
      }
      const chunks: Array<Uint8Array> = [];
      let total = 0;
      const limit = maxBytes + 1;
      while (total < limit) {
        const chunk = yield* file.readChunk(Math.min(limit - total, READ_SCRIPT_CHUNK_BYTES));
        if (Option.isNone(chunk) || chunk.value.byteLength === 0) break;
        total += chunk.value.byteLength;
        if (total > maxBytes) {
          return yield* new WorkflowScriptTooLargeError({ scriptPath });
        }
        chunks.push(chunk.value);
      }
      if (chunks.length === 0) return "";
      if (chunks.length === 1) return new TextDecoder("utf-8").decode(chunks[0]!);
      const bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return new TextDecoder("utf-8").decode(bytes);
    }),
  );
});

const readJournalEntriesSafe = (fileSystem: FileSystem.FileSystem, journalPath: string) =>
  fileSystem.readFileString(journalPath).pipe(
    Effect.orElseSucceed(() => ""),
    Effect.map((text) => {
      try {
        return decodeJournal(text);
      } catch {
        return [] as ReadonlyArray<JournalEntry>;
      }
    }),
  );

function collectJournalChildren(entries: ReadonlyArray<JournalEntry>): Array<OwnedChildTarget> {
  const journalChildren: Array<OwnedChildTarget> = [];
  const finishedIndices = new Set<number>();
  for (const item of entries) {
    if (item.type === "result" && item.outcome !== "cancelled") finishedIndices.add(item.index);
  }
  for (const item of entries) {
    if (
      item.type === "started" &&
      item.childThreadId !== null &&
      !finishedIndices.has(item.index)
    ) {
      journalChildren.push({
        childThreadId: ThreadId.make(item.childThreadId),
        childTaskId: item.agentId === null ? undefined : NodeId.make(item.agentId),
      });
    }
  }
  return journalChildren;
}

const discoverOwnedChildThreads = Effect.fn("WorkflowService.discoverOwnedChildThreads")(function* (
  threadManagement: ThreadManagementService.ThreadManagementService["Service"],
  parentThreadId: ThreadId,
  workflowTaskId: NodeId | null,
  extraChildren: ReadonlyArray<OwnedChildTarget> = [],
): Effect.fn.Return<ReadonlyArray<OwnedChildTarget>, Orchestrator.OrchestratorV2Error> {
  const byThread = new Map<ThreadId, NodeId | undefined>();
  for (const extra of extraChildren) {
    byThread.set(extra.childThreadId, extra.childTaskId ?? byThread.get(extra.childThreadId));
  }
  if (workflowTaskId === null && byThread.size === 0) {
    return [];
  }
  const parentShell = yield* threadManagement.getThreadShell(parentThreadId);
  if (parentShell !== null && parentShell.deletedAt === null) {
    const { subagents } = yield* threadManagement.getThreadRecords(parentThreadId, ["subagents"]);
    if (workflowTaskId !== null) {
      for (const subagent of subagents) {
        if (
          subagent.origin === "app_owned" &&
          subagent.childThreadId !== null &&
          subagent.parentNodeId === workflowTaskId
        ) {
          const existing = byThread.get(subagent.childThreadId);
          if (existing === undefined || !isTerminalSubagentStatus(subagent.status)) {
            byThread.set(subagent.childThreadId, subagent.id);
          }
        }
      }
    }
    for (const subagent of subagents) {
      if (
        subagent.origin === "app_owned" &&
        subagent.childThreadId !== null &&
        byThread.has(subagent.childThreadId) &&
        !isTerminalSubagentStatus(subagent.status)
      ) {
        byThread.set(subagent.childThreadId, subagent.id);
      }
    }
  }
  return [...byThread.entries()].map(([childThreadId, childTaskId]) => ({
    childThreadId,
    childTaskId,
  }));
});

const stopOwnedChildThreadOnce = Effect.fn("WorkflowService.stopOwnedChildThreadOnce")(
  function* (input: {
    readonly threadManagement: ThreadManagementService.ThreadManagementService["Service"];
    readonly parentThreadId: ThreadId;
    readonly runId: string;
    readonly childThreadId: ThreadId;
    readonly childTaskId: NodeId | undefined;
    readonly reason: string;
    readonly attempt: number;
    readonly visited: Set<ThreadId>;
  }): Effect.fn.Return<
    void,
    | Orchestrator.OrchestratorV2Error
    | ThreadManagementService.ThreadManagementProviderInstanceDisabledError
    | WorkflowStopNotConfirmedError
  > {
    const {
      threadManagement,
      parentThreadId,
      runId,
      childThreadId,
      childTaskId,
      reason,
      attempt,
      visited,
    } = input;
    if (visited.has(childThreadId)) return;
    visited.add(childThreadId);

    const childShell = yield* threadManagement.getThreadShell(childThreadId);
    if (childShell === null || childShell.deletedAt !== null) return;

    const readChildState = () =>
      threadManagement.getThreadRecords(
        childThreadId,
        ["runs", "providerThreads", "turnItems", "subagents"],
        {
          turnItemTypes: ["run_interrupt_request", "command_execution", "dynamic_tool", "subagent"],
        },
      );
    const isChildConfirmedStopped = (state: {
      readonly thread: Pick<
        OrchestrationV2ThreadProjection["thread"],
        "deletedAt" | "activeProviderThreadId" | "pullRequests"
      >;
      readonly runs: ReadonlyArray<OrchestrationV2Run>;
      readonly providerThreads: ReadonlyArray<OrchestrationV2ProviderThread>;
      readonly turnItems: ReadonlyArray<OrchestrationV2TurnItem>;
    }): boolean =>
      state.thread.deletedAt !== null ||
      (!state.runs.some(isLiveChildRun) && !hasPendingBackgroundWork(state));

    const initial = yield* readChildState();
    // Cancel queued runs first so stopping an active run cannot promote them.
    for (const childRun of initial.runs) {
      if (childRun.status !== "queued") continue;
      yield* threadManagement.dispatch({
        type: "queued-run.cancel",
        commandId: CommandId.make(
          `server:workflow-stop-child-queued:${runId}:${childThreadId}:${childRun.id}:a${attempt}`,
        ),
        threadId: childThreadId,
        runId: childRun.id,
      });
    }

    const beforeStopSeq = yield* threadManagement
      .getThreadEventSequence(childThreadId)
      .pipe(Effect.orElseSucceed(() => 0));

    // Never skip thread.stop: idle stops are idempotent, and terminal runs may still own pending provider work.
    const commandId = CommandId.make(
      `server:workflow-stop-child:${runId}:${childThreadId}:a${attempt}`,
    );
    const parentShell = yield* threadManagement.getThreadShell(parentThreadId);
    if (childTaskId !== undefined && parentShell !== null && parentShell.deletedAt === null) {
      yield* threadManagement.cancelDelegatedTask({
        parentThreadId,
        taskId: childTaskId,
        command: {
          type: "thread.stop",
          commandId,
          threadId: childThreadId,
          stopDelegatedTasks: false,
          reason,
        },
        stopDescendants: (remoteDispatch) =>
          threadManagement.stopDelegatedTasks({
            threadId: childThreadId,
            commandId,
            reason,
            remoteDispatch,
          }),
      });
    } else {
      yield* threadManagement.dispatch({
        type: "thread.stop",
        commandId,
        threadId: childThreadId,
        stopDelegatedTasks: true,
        reason,
      });
    }

    const afterStop = yield* readChildState();
    for (const childRun of afterStop.runs) {
      if (childRun.status !== "queued") continue;
      yield* threadManagement.dispatch({
        type: "queued-run.cancel",
        commandId: CommandId.make(
          `server:workflow-stop-child-queued-post:${runId}:${childThreadId}:${childRun.id}:a${attempt}`,
        ),
        threadId: childThreadId,
        runId: childRun.id,
      });
    }

    let firstDescendantError:
      | Orchestrator.OrchestratorV2Error
      | ThreadManagementService.ThreadManagementProviderInstanceDisabledError
      | WorkflowStopNotConfirmedError
      | undefined;
    for (const grandchild of afterStop.subagents) {
      if (grandchild.origin !== "app_owned" || grandchild.childThreadId === null) continue;
      const grandchildOutcome = yield* stopOwnedChildThreadOnce({
        threadManagement,
        parentThreadId: childThreadId,
        runId,
        childThreadId: grandchild.childThreadId,
        childTaskId: grandchild.id,
        reason,
        attempt,
        visited,
      }).pipe(Effect.result);
      if (Result.isFailure(grandchildOutcome) && firstDescendantError === undefined) {
        firstDescendantError = grandchildOutcome.failure;
      }
    }

    let confirmed = yield* readChildState();
    if (!isChildConfirmedStopped(confirmed)) {
      const waitSettled = threadManagement
        .streamStoredEventsFrom({
          threadId: childThreadId,
          afterSequence: beforeStopSeq,
        })
        .pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" ||
              event.type === "provider-thread.updated" ||
              event.type === "turn-item.updated" ||
              event.type === "thread.deleted",
          ),
          Stream.mapEffect(({ event }) => {
            if (event.type === "thread.deleted") return Effect.succeed("settled" as const);
            if (
              event.type === "turn-item.updated" &&
              event.payload.type === "run_interrupt_request" &&
              event.payload.unresponsiveAt !== undefined
            ) {
              return Effect.succeed("unresponsive" as const);
            }
            return readChildState().pipe(
              Effect.map((latest) =>
                isChildConfirmedStopped(latest) ? ("settled" as const) : ("pending" as const),
              ),
            );
          }),
          Stream.filter((state) => state !== "pending"),
          Stream.runHead,
          Effect.asVoid,
        );
      yield* Effect.raceFirst(waitSettled, Effect.sleep(Duration.millis(STOP_CONFIRM_WAIT_MS)));
      confirmed = yield* readChildState();
    }

    if (confirmed.thread.deletedAt !== null) {
      if (firstDescendantError !== undefined) {
        return yield* firstDescendantError;
      }
      return;
    }
    const liveRuns = confirmed.runs.filter(isLiveChildRun);
    if (liveRuns.length > 0) {
      return yield* new WorkflowStopNotConfirmedError({
        messageText: `Child thread ${childThreadId} still has ${liveRuns.length} live run(s).`,
      });
    }
    if (hasPendingBackgroundWork(confirmed)) {
      return yield* new WorkflowStopNotConfirmedError({
        messageText: `Child thread ${childThreadId} still has pending background work.`,
      });
    }
    if (firstDescendantError !== undefined) {
      return yield* firstDescendantError;
    }
  },
);

const stopOwnedChildThread = Effect.fn("WorkflowService.stopOwnedChildThread")(function* (input: {
  readonly threadManagement: ThreadManagementService.ThreadManagementService["Service"];
  readonly parentThreadId: ThreadId;
  readonly runId: string;
  readonly childThreadId: ThreadId;
  readonly childTaskId: NodeId | undefined;
  readonly reason: string;
  readonly maxAttempts: number;
}) {
  return yield* withBoundedRetry(
    (attempt) =>
      stopOwnedChildThreadOnce({
        ...input,
        attempt,
        visited: new Set<ThreadId>(),
      }),
    {
      maxAttempts: input.maxAttempts,
      onRetry: (attempt, error) =>
        Effect.logWarning("workflow.stop-child.retrying", {
          runId: input.runId,
          childThreadId: input.childThreadId,
          childTaskId: input.childTaskId,
          attempt: attempt + 1,
          error,
        }),
    },
  ).pipe(
    // Clear any ambient MCP DispatchModeLimit so lowered parent modes cannot block server-owned cleanup.
    Effect.provideService(DispatchModeLimit, undefined),
  );
});

const stopAllWorkflowChildren = Effect.fn("WorkflowService.stopAllWorkflowChildren")(
  function* (input: {
    readonly threadManagement: ThreadManagementService.ThreadManagementService["Service"];
    readonly parentThreadId: ThreadId;
    readonly workflowTaskId: NodeId | null;
    readonly runId: string;
    readonly reason: string;
    readonly extraChildren?: ReadonlyArray<OwnedChildTarget>;
    readonly maxAttempts: number;
    readonly attemptOffset?: number;
  }) {
    const attemptOffset = input.attemptOffset ?? 0;
    return yield* withBoundedRetry(
      (attempt) =>
        Effect.gen(function* () {
          const owned = yield* discoverOwnedChildThreads(
            input.threadManagement,
            input.parentThreadId,
            input.workflowTaskId,
            input.extraChildren,
          );
          let firstError:
            | Orchestrator.OrchestratorV2Error
            | ThreadManagementService.ThreadManagementProviderInstanceDisabledError
            | WorkflowStopNotConfirmedError
            | undefined;
          for (const child of owned) {
            const outcome = yield* stopOwnedChildThreadOnce({
              threadManagement: input.threadManagement,
              parentThreadId: input.parentThreadId,
              runId: input.runId,
              childThreadId: child.childThreadId,
              childTaskId: child.childTaskId,
              reason: input.reason,
              attempt: attemptOffset + attempt,
              visited: new Set<ThreadId>(),
            }).pipe(Effect.result);
            if (Result.isFailure(outcome) && firstError === undefined) {
              firstError = outcome.failure;
            }
          }
          if (firstError !== undefined) {
            return yield* firstError;
          }
        }),
      {
        maxAttempts: input.maxAttempts,
        onRetry: (attempt, error) =>
          Effect.logWarning("workflow.stop-children.retrying", {
            runId: input.runId,
            taskId: input.workflowTaskId,
            attempt: attemptOffset + attempt + 1,
            error,
          }),
      },
    ).pipe(Effect.provideService(DispatchModeLimit, undefined));
  },
);

const commitWorkflowComplete = Effect.fn("WorkflowService.commitWorkflowComplete")(
  function* (input: {
    readonly threadManagement: ThreadManagementService.ThreadManagementService["Service"];
    readonly parentThreadId: ThreadId;
    readonly taskId: NodeId;
    readonly runId: string;
    readonly commandIdBase: string;
    readonly status: "completed" | "failed" | "cancelled" | "interrupted";
    readonly result: string;
    readonly workflow?: OrchestrationV2SubagentWorkflow | undefined;
    readonly disposeDelivery?: boolean | undefined;
    readonly maxAttempts: number;
    readonly attemptOffset?: number;
  }) {
    const attemptOffset = input.attemptOffset ?? 0;
    return yield* withBoundedRetry(
      (attempt) =>
        Effect.gen(function* () {
          yield* input.threadManagement.dispatch({
            type: "workflow.complete",
            commandId: CommandId.make(`${input.commandIdBase}:a${attemptOffset + attempt}`),
            parentThreadId: input.parentThreadId,
            taskId: input.taskId,
            status: input.status,
            result: input.result,
            ...(input.workflow === undefined ? {} : { workflow: input.workflow }),
            ...(input.disposeDelivery === true ? { disposeDelivery: true } : {}),
          });
          const confirmed = yield* readWorkflowTaskStrict(
            input.threadManagement,
            input.parentThreadId,
            input.taskId,
          );
          if (confirmed === undefined || !isTerminalSubagentStatus(confirmed.status)) {
            return yield* new WorkflowStopNotConfirmedError({
              messageText: `Workflow task ${input.taskId} did not confirm terminal in SQL.`,
            });
          }
          return confirmed;
        }),
      {
        maxAttempts: input.maxAttempts,
        onRetry: (attempt, error) =>
          Effect.logWarning("workflow.complete.retrying", {
            runId: input.runId,
            taskId: input.taskId,
            attempt: attemptOffset + attempt + 1,
            error,
          }),
      },
    ).pipe(Effect.provideService(DispatchModeLimit, undefined));
  },
);

const reconcilePersistedFromSql = Effect.fn("WorkflowService.reconcilePersistedFromSql")(function* (
  deps: {
    readonly fileSystem: FileSystem.FileSystem;
    readonly path: Path.Path;
    readonly workflowsDir: string;
  },
  record: PersistedWorkflowRun,
  finalTask: OrchestrationV2Subagent | undefined,
  journalEntries: ReadonlyArray<JournalEntry>,
  restartMessage: string,
  options: { readonly clearCleanupPending: boolean },
) {
  const endEntry = journalEntries.findLast(
    (item): item is Extract<JournalEntry, { readonly type: "end" }> => item.type === "end",
  );
  const { error: _previousError, ...withoutError } = record;
  const baseRecord = options.clearCleanupPending
    ? withoutCleanupPending(withoutError)
    : withoutError;
  if (finalTask !== undefined && isTerminalSubagentStatus(finalTask.status)) {
    const finalStatus = subagentStatusToWorkflowStatus(finalTask.status);
    if (finalStatus === "completed") {
      const recoveredResult =
        record.result ??
        (endEntry?.status === "completed" && endEntry.result !== undefined
          ? (endEntry.result as Schema.Json)
          : undefined);
      yield* writePersistedRunWith(deps, {
        ...baseRecord,
        status: "completed",
        ...(recoveredResult === undefined ? {} : { result: recoveredResult }),
      });
      return;
    }
    if (finalStatus === "cancelled") {
      yield* writePersistedRunWith(deps, {
        ...baseRecord,
        status: "cancelled",
      });
      return;
    }
    yield* writePersistedRunWith(deps, {
      ...baseRecord,
      status: "failed",
      error:
        finalTask.result ??
        (endEntry?.status === "failed" ? endEntry.error : undefined) ??
        restartMessage,
    });
    return;
  }
  yield* writePersistedRunWith(deps, {
    ...baseRecord,
    status: "failed",
    error: restartMessage,
  });
});

interface RecoveryCandidate {
  readonly runId: string | undefined;
  readonly parentThreadId: ThreadId;
  readonly taskId: NodeId | null;
  readonly persisted: PersistedWorkflowRun | undefined;
  readonly sqlTask: OrchestrationV2Subagent | undefined;
}

/** Stops SQL-owned workflow children and reconciles SQL and run.json before provider recovery runs. */
export const recoverInterruptedWorkflows = Effect.gen(function* () {
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const storeDeps = { fileSystem, path, workflowsDir: serverConfig.workflowsDir };

  const candidatesByKey = new Map<string, RecoveryCandidate>();
  const activeSqlRunIds = new Set<string>();
  const keyFor = (parentThreadId: ThreadId, taskId: NodeId | null, runId: string | undefined) =>
    taskId !== null ? `task:${parentThreadId}:${taskId}` : `run:${runId ?? ""}`;

  // 1. Discover every SQL-running workflow; discovery failures must fail startup closed before provider recovery.
  const activeShells = yield* threadManagement
    .listActiveShellIndex()
    .pipe(
      Effect.tapError((error) =>
        Effect.logError("workflow.startup-sweep.list-shells-failed", { error }),
      ),
    );
  for (const shell of activeShells) {
    if (!shell.mayBeLive) continue;
    const records = yield* threadManagement.getThreadRecords(shell.id, ["subagents"]).pipe(
      Effect.asSome,
      Effect.catchIf(isThreadNotFound, () => Effect.succeedNone),
      Effect.tapError((error) =>
        Effect.logError("workflow.startup-sweep.read-subagents-failed", {
          threadId: shell.id,
          error,
        }),
      ),
    );
    if (Option.isNone(records)) continue;
    for (const subagent of records.value.subagents) {
      if (!isAppOwnedWorkflowTask(subagent) || isTerminalSubagentStatus(subagent.status)) continue;
      const runId = subagent.workflow?.runHandles?.runId;
      if (runId !== undefined) activeSqlRunIds.add(runId);
      const persisted =
        runId === undefined
          ? undefined
          : Option.getOrUndefined(yield* readPersistedRunWith(storeDeps, runId));
      candidatesByKey.set(keyFor(shell.id, subagent.id, runId), {
        runId,
        parentThreadId: shell.id,
        taskId: subagent.id,
        persisted,
        sqlTask: subagent,
      });
    }
  }

  // 2. Discover running run.json entries (covers SQL-terminal and crashed-before-start runs).
  const dirExists = yield* fileSystem
    .exists(serverConfig.workflowsDir)
    .pipe(
      Effect.tapError((error) =>
        Effect.logError("workflow.startup-sweep.workflows-dir-exists-failed", { error }),
      ),
    );
  if (dirExists) {
    const entries = yield* fileSystem.readDirectory(serverConfig.workflowsDir).pipe(
      Effect.catchIf(isPlatformNotFound, () => Effect.succeed([] as ReadonlyArray<string>)),
      Effect.tapError((error) =>
        Effect.logError("workflow.startup-sweep.workflows-dir-read-failed", { error }),
      ),
    );
    for (const entry of entries) {
      if (!isSafeRunId(entry) || activeSqlRunIds.has(entry)) continue;
      const entryDir = runDirIn(path, serverConfig.workflowsDir, entry);
      const entryStat = yield* fileSystem.stat(entryDir).pipe(
        Effect.asSome,
        Effect.catchIf(isPlatformNotFound, () =>
          Effect.succeed(Option.none<FileSystem.File.Info>()),
        ),
        Effect.catch((error) =>
          Effect.logWarning("workflow.startup-sweep.entry-stat-failed", {
            entry,
            error,
          }).pipe(Effect.as(Option.none<FileSystem.File.Info>())),
        ),
      );
      if (Option.isNone(entryStat) || entryStat.value.type !== "Directory") continue;
      const persisted = yield* readPersistedRunWith(storeDeps, entry).pipe(
        Effect.catch((error) =>
          Effect.logWarning("workflow.startup-sweep.orphan-meta-read-failed", {
            entry,
            error,
          }).pipe(Effect.as(Option.none<PersistedWorkflowRun>())),
        ),
      );
      if (Option.isNone(persisted)) continue;
      const record = persisted.value;
      const candidateKey = keyFor(record.threadId, record.taskId, record.runId);
      if (candidatesByKey.has(candidateKey)) continue;
      if (record.status !== "running" && record.cleanupPending !== true) continue;
      const sqlTask =
        record.taskId === null
          ? undefined
          : yield* readWorkflowTaskStrict(threadManagement, record.threadId, record.taskId).pipe(
              Effect.tapError((error) =>
                Effect.logError("workflow.startup-sweep.read-task-failed", {
                  runId: record.runId,
                  threadId: record.threadId,
                  taskId: record.taskId,
                  error,
                }),
              ),
            );
      candidatesByKey.set(candidateKey, {
        runId: record.runId,
        parentThreadId: record.threadId,
        taskId: record.taskId,
        persisted: record,
        sqlTask,
      });
    }
  }

  for (const candidate of candidatesByKey.values()) {
    yield* Effect.gen(function* () {
      const { parentThreadId, taskId, persisted, sqlTask } = candidate;
      const runId = candidate.runId ?? persisted?.runId ?? taskId ?? "unknown";
      const journalPath =
        persisted?.journalPath ?? journalPathIn(path, serverConfig.workflowsDir, runId);
      const journalEntries = yield* readJournalEntriesSafe(fileSystem, journalPath);

      const restartMessage = `The server restarted while this workflow ran. Resume it with t3_workflow_run resumeFromRunId=${runId}.`;

      const journalChildren = collectJournalChildren(journalEntries);

      const wasAlreadyStopRequested = sqlTask?.workflow?.stopRequestedAt !== undefined;

      const reconcilePersistedAfterCleanup = (
        finalTask: OrchestrationV2Subagent | undefined,
        childrenStoppedConfirmed: boolean,
      ) => {
        if (persisted === undefined) return Effect.void;
        if (persisted.status === "running") {
          return reconcilePersistedFromSql(
            storeDeps,
            persisted,
            finalTask,
            journalEntries,
            restartMessage,
            { clearCleanupPending: childrenStoppedConfirmed },
          );
        }
        if (persisted.cleanupPending === true && childrenStoppedConfirmed) {
          return writePersistedRunWith(storeDeps, withoutCleanupPending(persisted));
        }
        return Effect.void;
      };

      const completeRecoveredWorkflow = Effect.fn("WorkflowService.completeRecoveredWorkflow")(
        function* (targetTask: OrchestrationV2Subagent, maxAttempts: number, attemptOffset = 0) {
          const nowMs = yield* Clock.currentTimeMillis;
          let completeStatus: "completed" | "failed" | "cancelled";
          let completeResult: string;
          let disposeDelivery = false;
          let finalWorkflow: OrchestrationV2SubagentWorkflow | undefined;

          if (persisted !== undefined && persisted.status !== "running") {
            completeStatus = persisted.status;
            if (persisted.status === "completed") {
              completeResult = formatCompletionResult({
                runId: persisted.runId,
                name: persisted.name,
                result: persisted.result,
                usage: persisted.usage,
                journalPath: persisted.journalPath,
              });
            } else if (persisted.status === "cancelled") {
              completeResult = "Stopped.";
              disposeDelivery = true;
              finalWorkflow = buildStoppedSnapshotFromSql(targetTask.workflow, nowMs);
            } else {
              completeResult = formatFailureResult({
                runId: persisted.runId,
                error: persisted.error ?? restartMessage,
              });
              finalWorkflow = buildStoppedSnapshotFromSql(targetTask.workflow, nowMs);
            }
          } else if (wasAlreadyStopRequested) {
            completeStatus = "cancelled";
            completeResult = "Stopped.";
            disposeDelivery = true;
            finalWorkflow = buildStoppedSnapshotFromSql(targetTask.workflow, nowMs);
          } else {
            completeStatus = "failed";
            completeResult = restartMessage;
            finalWorkflow = buildStoppedSnapshotFromSql(targetTask.workflow, nowMs);
          }

          return yield* commitWorkflowComplete({
            threadManagement,
            parentThreadId,
            taskId: targetTask.id,
            runId,
            commandIdBase: `server:workflow-restart:${runId}`,
            status: completeStatus,
            result: completeResult,
            workflow: finalWorkflow,
            disposeDelivery,
            maxAttempts,
            attemptOffset,
          });
        },
      );

      const deferCleanupInBackground = Effect.fn("WorkflowService.deferCleanupInBackground")(
        function* (
          targetTaskId: NodeId | null,
          needsWorkflowCompletion: boolean,
          initialError: unknown,
        ) {
          yield* Effect.logWarning("workflow.startup-sweep.cleanup-deferred", {
            runId,
            taskId: targetTaskId,
            error: initialError,
          });
          // Persist the cleanup gate before touching SQL so a failed write fails startup closed.
          if (persisted !== undefined) {
            yield* writePersistedRunStrictWith(storeDeps, { ...persisted, cleanupPending: true });
          }
          if (needsWorkflowCompletion && targetTaskId !== null) {
            // Close admission and queued promotion in SQL before provider recovery runs.
            yield* withBoundedRetry(
              (attempt) =>
                threadManagement.dispatch({
                  type: "workflow.stop-request",
                  commandId: CommandId.make(
                    `server:workflow-restart-stop-request:${runId}:${targetTaskId}:a${attempt}`,
                  ),
                  parentThreadId,
                  taskId: targetTaskId,
                }),
              {
                maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
                onRetry: (attempt, error) =>
                  Effect.logWarning("workflow.startup-sweep.stop-request.retrying", {
                    runId,
                    taskId: targetTaskId,
                    attempt: attempt + 1,
                    error,
                  }),
              },
            ).pipe(
              Effect.provideService(DispatchModeLimit, undefined),
              // Provider recovery reads stopRequestedAt next, so an unwritten stop request must fail startup.
              Effect.tapError((error) =>
                Effect.logError("workflow.startup-sweep.stop-request-failed", {
                  runId,
                  taskId: targetTaskId,
                  error,
                }),
              ),
            );
          }

          yield* Effect.gen(function* () {
            for (let attempt = 0; ; attempt += 1) {
              yield* Effect.sleep(Duration.millis(retryDelayMs(attempt)));
              let currentTask: OrchestrationV2Subagent | undefined;
              if (targetTaskId !== null) {
                const latestTaskResult = yield* readWorkflowTaskStrict(
                  threadManagement,
                  parentThreadId,
                  targetTaskId,
                ).pipe(Effect.result);
                if (Result.isFailure(latestTaskResult)) {
                  yield* Effect.logWarning("workflow.startup-sweep.background-cleanup.retrying", {
                    runId,
                    taskId: targetTaskId,
                    attempt: attempt + 1,
                    error: latestTaskResult.failure,
                  });
                  continue;
                }
                currentTask = latestTaskResult.success;
                if (
                  currentTask !== undefined &&
                  !isTerminalSubagentStatus(currentTask.status) &&
                  currentTask.workflow?.stopRequestedAt === undefined
                ) {
                  yield* threadManagement
                    .dispatch({
                      type: "workflow.stop-request",
                      commandId: CommandId.make(
                        `server:workflow-restart-stop-request-bg:${runId}:${targetTaskId}:a${attempt}`,
                      ),
                      parentThreadId,
                      taskId: targetTaskId,
                    })
                    .pipe(Effect.provideService(DispatchModeLimit, undefined), Effect.ignore);
                }
              }
              const bgAttemptOffset = STARTUP_RECOVERY_MAX_ATTEMPTS + attempt;
              const stopOutcome = yield* stopAllWorkflowChildren({
                threadManagement,
                parentThreadId,
                workflowTaskId: targetTaskId,
                runId,
                reason: "Server restarted while parent workflow was running.",
                extraChildren: journalChildren,
                maxAttempts: 1,
                attemptOffset: bgAttemptOffset,
              }).pipe(Effect.result);
              if (Result.isFailure(stopOutcome)) {
                yield* Effect.logWarning("workflow.startup-sweep.background-cleanup.retrying", {
                  runId,
                  taskId: targetTaskId,
                  attempt: attempt + 1,
                  error: stopOutcome.failure,
                });
                continue;
              }
              if (currentTask !== undefined && !isTerminalSubagentStatus(currentTask.status)) {
                const completeOutcome = yield* completeRecoveredWorkflow(
                  currentTask,
                  1,
                  bgAttemptOffset,
                ).pipe(Effect.result);
                if (Result.isFailure(completeOutcome)) {
                  yield* Effect.logWarning("workflow.startup-sweep.background-complete.retrying", {
                    runId,
                    taskId: targetTaskId,
                    attempt: attempt + 1,
                    error: completeOutcome.failure,
                  });
                  continue;
                }
                yield* reconcilePersistedAfterCleanup(completeOutcome.success, true);
                return;
              }
              yield* reconcilePersistedAfterCleanup(currentTask, true);
              return;
            }
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("workflow.startup-sweep.background-cleanup.defect", {
                runId,
                taskId: targetTaskId,
                cause,
              }),
            ),
            Effect.forkScoped,
          );
        },
      );

      if (sqlTask !== undefined && !isTerminalSubagentStatus(sqlTask.status) && taskId !== null) {
        const bounded = yield* Effect.gen(function* () {
          yield* stopAllWorkflowChildren({
            threadManagement,
            parentThreadId,
            workflowTaskId: taskId,
            runId,
            reason: "Server restarted while parent workflow was running.",
            extraChildren: journalChildren,
            maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
          });
          const confirmedTask = yield* completeRecoveredWorkflow(
            sqlTask,
            STARTUP_RECOVERY_MAX_ATTEMPTS,
          );
          yield* reconcilePersistedAfterCleanup(confirmedTask, true);
        }).pipe(Effect.result);
        if (Result.isFailure(bounded)) {
          yield* deferCleanupInBackground(taskId, true, bounded.failure);
        }
        return;
      }

      // SQL is already terminal (or never started); confirm lingering children before clearing cleanupPending.
      const shouldConfirmChildren =
        persisted?.cleanupPending === true ||
        (sqlTask !== undefined && sqlTask.status !== "completed" && taskId !== null) ||
        (sqlTask === undefined && journalChildren.length > 0);
      if (shouldConfirmChildren) {
        const boundedStop = yield* stopAllWorkflowChildren({
          threadManagement,
          parentThreadId,
          workflowTaskId: taskId,
          runId,
          reason: "Server restarted while parent workflow was running.",
          extraChildren: journalChildren,
          maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
        }).pipe(Effect.result);
        if (Result.isFailure(boundedStop)) {
          yield* deferCleanupInBackground(taskId, false, boundedStop.failure);
          return;
        }
      }
      yield* reconcilePersistedAfterCleanup(sqlTask, shouldConfirmChildren);
    }).pipe(
      Effect.tapDefect((cause) =>
        Effect.logError("workflow.startup-sweep.entry-defect", {
          runId: candidate.runId,
          taskId: candidate.taskId,
          cause,
        }),
      ),
    );
  }
}).pipe(Effect.withSpan("WorkflowService.recoverInterruptedWorkflows"));

const make = Effect.gen(function* () {
  const layerScope = yield* Effect.scope;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const orchestratorMcp = yield* OrchestratorMcpService.OrchestratorMcpService;
  const storeDeps = { fileSystem, path, workflowsDir: serverConfig.workflowsDir };

  yield* Effect.addFinalizer(() =>
    Effect.promise(() => reapUnconfirmedSandboxChildren()).pipe(Effect.ignore),
  );

  const liveRuns = new Map<string, LiveWorkflowRun>();
  const concurrency = workflowConcurrency(NodeOS.cpus().length);

  const requireCapability = (scope: McpInvocationContext.McpInvocationScope) =>
    scope.capabilities.has("orchestration")
      ? Effect.void
      : Effect.fail(
          failure("capability_denied", "This MCP session cannot control orchestration tasks."),
        );

  const threadManagementFailure = (error: unknown) =>
    failure("orchestration_error", `Orchestration error: ${errorMessage(error)}`);

  const runDirFor = (runId: string): string => runDirIn(path, serverConfig.workflowRunsDir, runId);
  const journalPathFor = (runId: string): string =>
    journalPathIn(path, serverConfig.workflowRunsDir, runId);

  const readPersistedRun = (runId: string) => readPersistedRunWith(storeDeps, runId);
  const writePersistedRunStrict = (record: PersistedWorkflowRun) =>
    writePersistedRunStrictWith(storeDeps, record);
  const writePersistedRun = (record: PersistedWorkflowRun) =>
    writePersistedRunWith(storeDeps, record);

  const generateRunId = Effect.gen(function* () {
    const hi = yield* Random.nextIntBetween(0, 0x1000000);
    const lo = yield* Random.nextIntBetween(0, 0x1000000);
    return `wf_${hi.toString(16).padStart(6, "0")}${lo.toString(16).padStart(6, "0")}`;
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
    const agentsAbortDeferred = yield* Deferred.make<void>();
    const progressSignal = yield* Queue.dropping<void>(1);
    const runtimeContext = yield* Effect.context<never>();
    const runPromise = Effect.runPromiseWith(runtimeContext);

    yield* Effect.scoped(
      Effect.gen(function* () {
        const scriptReadsScope = yield* Scope.make();
        yield* Effect.addFinalizer(() =>
          Scope.close(scriptReadsScope, Exit.void).pipe(Effect.ignore),
        );
        let scriptReadsClosed = false;
        const closeScriptReads = (): void => {
          if (scriptReadsClosed) return;
          scriptReadsClosed = true;
          void runPromise(Scope.close(scriptReadsScope, Exit.void).pipe(Effect.ignore));
        };

        const journalWriter = yield* makeJournalWriter({
          writeLine: (line) => fileSystem.writeFileString(journalPath, line, { flag: "a" }),
          onWriteError: (error) =>
            Effect.logWarning("workflow.journal.append-failed", { runId, error }),
          maxPending: MAX_PENDING_JOURNAL_ENTRIES,
          maxPendingBytes: MAX_PENDING_JOURNAL_BYTES,
        });

        interface TrackedRepromptRun {
          readonly childThreadId: ThreadId;
          readonly runId: RunId;
          readonly followupTaskId: NodeId | undefined;
        }

        interface LiveAgentRecord {
          readonly index: number;
          key: string;
          readonly label: string;
          readonly phaseInfo: { readonly index: number; readonly title: string } | undefined;
          readonly done: Deferred.Deferred<void>;
          queuedAt: number | undefined;
          startedAt: number | undefined;
          countedInUsage: boolean;
          childTaskId: NodeId | undefined;
          childThreadId: ThreadId | undefined;
          initialTaskTerminal: boolean;
          readonly repromptRuns: Array<TrackedRepromptRun>;
          startedJournaled: boolean;
          resultJournaled: boolean;
          cancelled: boolean;
        }

        let status: OrchestratorMcpWorkflowRunStatus = "running";
        let finalResult: Schema.Json | undefined;
        let finalError: string | undefined;
        let endedByEngine = false;
        let stopRequested = false;
        let observedStopRequestedAt: number | undefined;
        let scriptSettled = false;
        let workerClosed = false;
        let hostLimitError: string | undefined;
        let cancelledUnawaitedCount = 0;
        let progressDirty = false;
        let progressSeq = 0;
        let lastDispatchedProgress: string | undefined;
        let replayGate: Promise<void> = Promise.resolve();

        const phases: Array<OrchestrationV2WorkflowPhase> = [];
        const phaseIndexByTitle = new Map<string, number>();
        for (const phase of meta.phases) {
          const trimmed = phase.title.trim();
          if (trimmed.length === 0 || phaseIndexByTitle.has(trimmed)) continue;
          const index = phases.length;
          phaseIndexByTitle.set(trimmed, index);
          phases.push({ index, title: trimmed });
        }
        let currentPhase: string | undefined;
        const logs: Array<string> = [];
        const agentsByIndex = new Map<number, OrchestrationV2WorkflowAgent>();
        const activeAgents = new Map<number, LiveAgentRecord>();
        const usage: MutableUsage = {
          agents: 0,
          done: 0,
          errors: 0,
          empty: 0,
          cached: 0,
          tokens: 0,
        };

        const failRunFromHostLimit = (message: string): never => {
          workerClosed = true;
          closeScriptReads();
          if (hostLimitError === undefined) {
            hostLimitError = message;
          }
          Deferred.doneUnsafe(agentsAbortDeferred, Exit.void);
          if (!abortController.signal.aborted) {
            abortController.abort(new WorkflowScriptError(hostLimitError));
          }
          throw new WorkflowScriptError(hostLimitError);
        };

        const ensurePhase = (title: string): { readonly index: number; readonly title: string } => {
          const trimmed = title.trim();
          const existing = phaseIndexByTitle.get(trimmed);
          if (existing !== undefined) return { index: existing, title: trimmed };
          if (phases.length >= MAX_DISTINCT_PHASES) {
            failRunFromHostLimit(
              `A workflow may record at most ${MAX_DISTINCT_PHASES} distinct phases.`,
            );
          }
          const index = phases.length;
          phaseIndexByTitle.set(trimmed, index);
          phases.push({ index, title: trimmed });
          return { index, title: trimmed };
        };

        const signalProgressSync = (): void => {
          progressDirty = true;
          Queue.offerUnsafe(progressSignal, undefined);
        };

        const appendJournal = (entry: JournalEntry): Effect.Effect<void> =>
          journalWriter.append(entry).pipe(Effect.asVoid);

        const pushLogInMemory = (message: string): void => {
          logs.push(message);
          if (logs.length > MAX_IN_MEMORY_LOGS) logs.shift();
        };

        const recordEngineLog = Effect.fn("WorkflowService.recordEngineLog")(function* (
          message: string,
        ) {
          pushLogInMemory(message);
          yield* appendJournal({ type: "log", message });
          signalProgressSync();
        });

        const flushJournal = journalWriter.flush;

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
          ...(observedStopRequestedAt === undefined
            ? {}
            : { stopRequestedAt: observedStopRequestedAt }),
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

        const triggerExternalStop = (stopRequestedAt?: number): void => {
          if (stopRequestedAt !== undefined && observedStopRequestedAt === undefined) {
            observedStopRequestedAt = stopRequestedAt;
          }
          if (endedByEngine || stopRequested) return;
          stopRequested = true;
          workerClosed = true;
          closeScriptReads();
          Deferred.doneUnsafe(agentsAbortDeferred, Exit.void);
          if (!abortController.signal.aborted) {
            abortController.abort(new WorkflowAbortedError());
          }
        };

        const checkSqlStopRequested = Effect.fn("WorkflowService.checkSqlStopRequested")(
          function* () {
            const sqlTask = yield* readWorkflowTaskStrict(
              threadManagement,
              parentThreadId,
              taskId,
            ).pipe(Effect.orElseSucceed(() => undefined));
            if (sqlTask === undefined) return false;
            if (sqlTask.workflow?.stopRequestedAt !== undefined) {
              triggerExternalStop(sqlTask.workflow.stopRequestedAt);
              return true;
            }
            if (isTerminalSubagentStatus(sqlTask.status)) {
              triggerExternalStop();
              return true;
            }
            return false;
          },
        );

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

        const cancelLiveAgent = Effect.fn("WorkflowService.cancelLiveAgent")(function* (
          record: LiveAgentRecord,
        ) {
          if (record.cancelled) return;
          record.cancelled = true;
          if (record.childThreadId === undefined) return;
          const latestFollowupTaskId = record.repromptRuns.findLast(
            (reprompt) => reprompt.followupTaskId !== undefined,
          )?.followupTaskId;
          const targetTaskId = record.initialTaskTerminal
            ? (latestFollowupTaskId ?? record.childTaskId)
            : (record.childTaskId ?? latestFollowupTaskId);
          yield* stopOwnedChildThread({
            threadManagement,
            parentThreadId,
            runId,
            childThreadId: record.childThreadId,
            childTaskId: targetTaskId,
            reason: "Workflow stopped.",
            maxAttempts: Number.POSITIVE_INFINITY,
          }).pipe(
            Effect.mapError(
              (error) => new WorkflowInternalError({ messageText: errorMessage(error) }),
            ),
          );
        });

        const journalCancelledAgent = (record: LiveAgentRecord): Effect.Effect<void> => {
          if (record.resultJournaled || !record.startedJournaled) return Effect.void;
          record.resultJournaled = true;
          return appendJournal({
            type: "result",
            index: record.index,
            key: record.key,
            outcome: "cancelled",
            value: null,
          });
        };

        const awaitAllActiveAgents = Effect.gen(function* () {
          while (activeAgents.size > 0) {
            const records = [...activeAgents.values()];
            yield* Effect.forEach(records, (record) => Deferred.await(record.done), {
              concurrency: "unbounded",
              discard: true,
            });
          }
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
              Deferred.await(agentsAbortDeferred).pipe(
                Effect.andThen(Effect.fail(new WorkflowAbortedError())),
              ),
            );
          },
        );

        const waitForChildRunTerminal = Effect.fn("WorkflowService.waitForChildRunTerminal")(
          function* (childThreadId: ThreadId, runIdToWait: RunId, fromSequence: number) {
            const readRun = threadManagement
              .getThreadRecords(childThreadId, ["runs"], { runIds: [runIdToWait] })
              .pipe(
                Effect.map(({ runs }) => runs.find((candidate) => candidate.id === runIdToWait)),
              );

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
                      ({ event }) =>
                        event.type === "run.updated" && event.payload.id === runIdToWait,
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
              Deferred.await(agentsAbortDeferred).pipe(
                Effect.andThen(Effect.fail(new WorkflowAbortedError())),
              ),
            );
          },
        );

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

        const cancelUnawaitedAgent = Effect.fn("WorkflowService.cancelUnawaitedAgent")(function* (
          record: LiveAgentRecord,
        ) {
          if (record.resultJournaled) return null;
          yield* cancelLiveAgent(record);
          record.resultJournaled = true;
          const cancelledAt = yield* Clock.currentTimeMillis;
          if (!record.countedInUsage) {
            record.countedInUsage = true;
            usage.agents += 1;
          }
          usage.errors += 1;
          const existing = agentsByIndex.get(record.index);
          const startedTime = record.startedAt ?? record.queuedAt ?? cancelledAt;
          agentsByIndex.set(record.index, {
            ...(existing ?? {
              index: record.index,
              label: record.label,
              ...(record.phaseInfo === undefined
                ? {}
                : { phaseIndex: record.phaseInfo.index, phaseTitle: record.phaseInfo.title }),
              attempt: 1,
              queuedAt: startedTime,
            }),
            state: "failed",
            completedAt: cancelledAt,
            durationMs: Math.max(0, cancelledAt - startedTime),
          });
          yield* appendJournal({
            type: "result",
            index: record.index,
            key: record.key,
            outcome: "cancelled",
            value: null,
          });
          cancelledUnawaitedCount += 1;
          signalProgressSync();
          yield* flushJournal;
          return null;
        });

        const abortStoppedAgent = Effect.fn("WorkflowService.abortStoppedAgent")(function* (
          record: LiveAgentRecord,
        ) {
          yield* cancelLiveAgent(record);
          yield* journalCancelledAgent(record);
          return yield* new WorkflowAbortedError();
        });

        const runAgentEffect = Effect.fn("WorkflowService.runAgent")(function* (
          call: WorkflowAgentCall,
          resolvedTarget: Result.Result<OrchestratorMcpTarget | undefined, OrchestratorMcpFailure>,
          liveRecord: LiveAgentRecord,
          prevReplayGate: Promise<void>,
          releaseReplayGate: () => void,
        ): Effect.fn.Return<unknown, WorkflowAbortedError | WorkflowInternalError> {
          const label = liveRecord.label;
          const phaseInfo = liveRecord.phaseInfo;
          const promptPreview = previewText(call.prompt);
          const effort = call.opts.effort?.trim();
          let gateReleased = false;
          const releaseGateOnce = () => {
            if (gateReleased) return;
            gateReleased = true;
            releaseReplayGate();
          };

          return yield* Effect.gen(function* () {
            if (stopRequested) {
              releaseGateOnce();
              return yield* abortStoppedAgent(liveRecord);
            }
            if (abortController.signal.aborted) {
              releaseGateOnce();
              return yield* new WorkflowAbortedError();
            }

            const resolvedModelSelection = Result.isFailure(resolvedTarget)
              ? Result.fail(resolvedTarget.failure)
              : yield* orchestratorMcp
                  .resolveWorkflowAgentModelSelection(scope, resolvedTarget.success)
                  .pipe(Effect.result);

            const key = agentCallKey(
              call.prompt,
              agentOptsForReplayKey(call, resolvedTarget, resolvedModelSelection, runTarget),
            );
            liveRecord.key = key;

            yield* Effect.promise(() => prevReplayGate);
            let replayed:
              | { readonly cached: true; readonly value: unknown }
              | { readonly cached: false };
            try {
              if (stopRequested || abortController.signal.aborted) {
                replayed = { cached: false };
              } else {
                replayed =
                  cursor === undefined
                    ? ({ cached: false } as const)
                    : cursor.take(call.index, key);
              }
            } finally {
              releaseGateOnce();
            }

            if (stopRequested) {
              return yield* abortStoppedAgent(liveRecord);
            }
            if (abortController.signal.aborted) {
              return yield* new WorkflowAbortedError();
            }

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
              yield* appendJournal({
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

            if (scriptSettled) {
              return yield* cancelUnawaitedAgent(liveRecord);
            }

            const queuedAt = yield* Clock.currentTimeMillis;
            liveRecord.queuedAt = queuedAt;
            liveRecord.countedInUsage = true;
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

            if (Result.isFailure(resolvedTarget) || Result.isFailure(resolvedModelSelection)) {
              liveRecord.startedJournaled = true;
              liveRecord.resultJournaled = true;
              const failedAt = yield* Clock.currentTimeMillis;
              usage.errors += 1;
              agentsByIndex.set(call.index, {
                ...agentsByIndex.get(call.index)!,
                state: "failed",
                completedAt: failedAt,
                durationMs: Math.max(0, failedAt - queuedAt),
              });
              yield* appendJournal({
                type: "started",
                index: call.index,
                key,
                label,
                phase: phaseInfo?.title ?? null,
                agentId: null,
                childThreadId: null,
              });
              yield* appendJournal({
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
            const outputSchema = call.opts.schema as
              | Readonly<Record<string, Schema.Json>>
              | undefined;

            const beforeDelegateSeq = yield* threadManagement
              .getThreadEventSequence(parentThreadId)
              .pipe(Effect.orElseSucceed(() => afterSequence));

            if (stopRequested || (yield* checkSqlStopRequested())) {
              return yield* abortStoppedAgent(liveRecord);
            }
            if (abortController.signal.aborted) {
              return yield* new WorkflowAbortedError();
            }
            if (scriptSettled) {
              return yield* cancelUnawaitedAgent(liveRecord);
            }

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
              if (
                stopRequested ||
                abortController.signal.aborted ||
                (yield* checkSqlStopRequested())
              ) {
                return yield* abortStoppedAgent(liveRecord);
              }
              if (scriptSettled) {
                return yield* cancelUnawaitedAgent(liveRecord);
              }
              liveRecord.startedJournaled = true;
              liveRecord.resultJournaled = true;
              const failedAt = yield* Clock.currentTimeMillis;
              usage.errors += 1;
              agentsByIndex.set(call.index, {
                ...agentsByIndex.get(call.index)!,
                state: "failed",
                completedAt: failedAt,
                durationMs: Math.max(0, failedAt - queuedAt),
              });
              yield* appendJournal({
                type: "started",
                index: call.index,
                key,
                label,
                phase: phaseInfo?.title ?? null,
                agentId: null,
                childThreadId: null,
              });
              yield* appendJournal({
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
            liveRecord.childTaskId = childTaskId;
            liveRecord.childThreadId = childThreadId;

            const startedAtAgent = yield* Clock.currentTimeMillis;
            liveRecord.startedAt = startedAtAgent;
            const requestedModel = resolvedModelSelection.success.model.trim();
            agentsByIndex.set(call.index, {
              ...agentsByIndex.get(call.index)!,
              agentId: childTaskId,
              childThreadId,
              state: "running",
              startedAt: startedAtAgent,
              ...(requestedModel.length > 0 ? { model: requestedModel } : {}),
            });
            liveRecord.startedJournaled = true;
            yield* appendJournal({
              type: "started",
              index: call.index,
              key,
              label,
              phase: phaseInfo?.title ?? null,
              agentId: childTaskId,
              childThreadId,
            });
            signalProgressSync();

            if (stopRequested) {
              return yield* abortStoppedAgent(liveRecord);
            }
            if (abortController.signal.aborted) {
              return yield* new WorkflowAbortedError();
            }
            if (scriptSettled) {
              return yield* cancelUnawaitedAgent(liveRecord);
            }

            const finishAgent = Effect.fnUntraced(function* (finish: {
              readonly state: "completed" | "failed";
              readonly outcome: "ok" | "null" | "usage_limited";
              readonly value: unknown;
              readonly countAs: "done" | "error" | "empty";
              readonly model?: string | undefined;
              readonly tokens: number;
            }) {
              liveRecord.resultJournaled = true;
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
              yield* appendJournal({
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

            const settledTaskResult = yield* waitForChildTaskTerminal(
              childTaskId,
              childThreadId,
              beforeDelegateSeq,
            ).pipe(Effect.result);
            if (Result.isFailure(settledTaskResult)) {
              if (settledTaskResult.failure._tag === "WorkflowAbortedError") {
                if (stopRequested) {
                  return yield* abortStoppedAgent(liveRecord);
                }
                if (scriptSettled) {
                  return yield* cancelUnawaitedAgent(liveRecord);
                }
                return yield* new WorkflowAbortedError();
              }
              return yield* new WorkflowInternalError({
                messageText: errorMessage(settledTaskResult.failure),
              });
            }
            const settledTask = settledTaskResult.success;
            liveRecord.initialTaskTerminal = true;

            if (stopRequested) {
              return yield* abortStoppedAgent(liveRecord);
            }
            if (abortController.signal.aborted) {
              return yield* new WorkflowAbortedError();
            }
            if (scriptSettled) {
              return yield* cancelUnawaitedAgent(liveRecord);
            }

            const initialSnapshot = yield* loadChildRunSnapshot(childThreadId, undefined).pipe(
              Effect.mapError(
                (error) => new WorkflowInternalError({ messageText: errorMessage(error) }),
              ),
            );
            const initialTokens = sumThreadTokens(initialSnapshot.providerTurns);
            const initialRun =
              initialSnapshot.runs.find((candidate) => candidate.delegatedTaskId === childTaskId) ??
              initialSnapshot.run;

            if (settledTask === undefined || settledTask.status !== "completed") {
              if (
                stopRequested ||
                abortController.signal.aborted ||
                (yield* checkSqlStopRequested())
              ) {
                return yield* abortStoppedAgent(liveRecord);
              }
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
                (initialRun === undefined
                  ? ""
                  : subagentResultForRun(initialSnapshot, initialRun).text);
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
                (currentRun === undefined
                  ? ""
                  : subagentResultForRun(currentSnapshot, currentRun).text);
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

              if (stopRequested || (yield* checkSqlStopRequested())) {
                return yield* abortStoppedAgent(liveRecord);
              }
              if (abortController.signal.aborted) {
                return yield* new WorkflowAbortedError();
              }
              if (scriptSettled) {
                return yield* cancelUnawaitedAgent(liveRecord);
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

              if (stopRequested || (yield* checkSqlStopRequested())) {
                return yield* abortStoppedAgent(liveRecord);
              }
              if (abortController.signal.aborted) {
                return yield* new WorkflowAbortedError();
              }
              if (scriptSettled) {
                return yield* cancelUnawaitedAgent(liveRecord);
              }

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
                if (
                  stopRequested ||
                  abortController.signal.aborted ||
                  (yield* checkSqlStopRequested())
                ) {
                  return yield* abortStoppedAgent(liveRecord);
                }
                if (scriptSettled) {
                  return yield* cancelUnawaitedAgent(liveRecord);
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

              liveRecord.repromptRuns.push({
                childThreadId,
                runId: sent.success.run.id,
                followupTaskId: sent.success.run.delegatedTaskId,
              });

              if (stopRequested) {
                return yield* abortStoppedAgent(liveRecord);
              }
              if (abortController.signal.aborted) {
                return yield* new WorkflowAbortedError();
              }
              if (scriptSettled) {
                return yield* cancelUnawaitedAgent(liveRecord);
              }

              const settledRepromptResult = yield* waitForChildRunTerminal(
                childThreadId,
                sent.success.run.id,
                beforeRepromptSeq,
              ).pipe(Effect.result);
              if (Result.isFailure(settledRepromptResult)) {
                if (settledRepromptResult.failure._tag === "WorkflowAbortedError") {
                  if (stopRequested) {
                    return yield* abortStoppedAgent(liveRecord);
                  }
                  if (scriptSettled) {
                    return yield* cancelUnawaitedAgent(liveRecord);
                  }
                  return yield* new WorkflowAbortedError();
                }
                return yield* new WorkflowInternalError({
                  messageText: errorMessage(settledRepromptResult.failure),
                });
              }
              const settledReprompt = settledRepromptResult.success;

              if (stopRequested) {
                return yield* abortStoppedAgent(liveRecord);
              }
              if (abortController.signal.aborted) {
                return yield* new WorkflowAbortedError();
              }
              if (scriptSettled) {
                return yield* cancelUnawaitedAgent(liveRecord);
              }

              currentSnapshot = yield* loadChildRunSnapshot(
                childThreadId,
                sent.success.run.id,
              ).pipe(
                Effect.mapError(
                  (error) => new WorkflowInternalError({ messageText: errorMessage(error) }),
                ),
              );
              currentRun = currentSnapshot.run ?? settledReprompt;
              latestTaskResult = null;

              if (currentRun === undefined || currentRun.status !== "completed") {
                if (
                  stopRequested ||
                  abortController.signal.aborted ||
                  (yield* checkSqlStopRequested())
                ) {
                  return yield* abortStoppedAgent(liveRecord);
                }
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
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                releaseGateOnce();
                activeAgents.delete(call.index);
                Deferred.doneUnsafe(liveRecord.done, Exit.void);
              }),
            ),
          );
        });

        const host: WorkflowHost = {
          runAgent: (call) => {
            if (workerClosed || scriptSettled) {
              return Promise.resolve(null);
            }
            if (stopRequested || abortController.signal.aborted) {
              return Promise.reject(new WorkflowAbortedError());
            }
            const resolvedTarget = resolveAgentTargetSync(call, runTarget);
            const phaseInfo =
              call.phase !== undefined && call.phase.trim().length > 0
                ? ensurePhase(call.phase)
                : undefined;
            const record: LiveAgentRecord = {
              index: call.index,
              key: "",
              label: agentLabel(call),
              phaseInfo,
              done: Deferred.makeUnsafe<void>(),
              queuedAt: undefined,
              startedAt: undefined,
              countedInUsage: false,
              childTaskId: undefined,
              childThreadId: undefined,
              initialTaskTerminal: false,
              repromptRuns: [],
              startedJournaled: false,
              resultJournaled: false,
              cancelled: false,
            };
            activeAgents.set(call.index, record);
            // Serialize cursor.take in synchronous call order even while model resolution runs asynchronously.
            const prevReplayGate = replayGate;
            let releaseReplayGate!: () => void;
            replayGate = new Promise<void>((resolve) => {
              releaseReplayGate = resolve;
            });
            return runPromise(
              runAgentEffect(call, resolvedTarget, record, prevReplayGate, releaseReplayGate).pipe(
                Effect.catchTags({
                  WorkflowInternalError: () => Effect.succeed(null),
                }),
              ),
            );
          },
          onPhase: (title) => {
            if (workerClosed) return;
            const phaseInfo = ensurePhase(title);
            if (!journalWriter.tryAppend({ type: "phase", title: phaseInfo.title })) {
              failRunFromHostLimit(
                `Workflow journal write backlog exceeded its ${MAX_PENDING_JOURNAL_ENTRIES}-entry or ${MAX_PENDING_JOURNAL_BYTES / (1024 * 1024)} MiB limit.`,
              );
            }
            currentPhase = phaseInfo.title;
            signalProgressSync();
          },
          onLog: (message) => {
            if (workerClosed) return;
            if (!journalWriter.tryAppend({ type: "log", message })) {
              failRunFromHostLimit(
                `Workflow journal write backlog exceeded its ${MAX_PENDING_JOURNAL_ENTRIES}-entry or ${MAX_PENDING_JOURNAL_BYTES / (1024 * 1024)} MiB limit.`,
              );
            }
            pushLogInMemory(message);
            signalProgressSync();
          },
          loadScript: ({ scriptPath: nestedPath }) => {
            if (workerClosed || scriptReadsClosed || abortController.signal.aborted) {
              return Promise.reject(new WorkflowAbortedError());
            }
            return runPromise(
              readBoundedWorkflowScript(nestedPath).pipe(
                Effect.forkIn(scriptReadsScope),
                Effect.flatMap(Fiber.await),
              ),
            ).then((exit) => {
              if (Exit.isSuccess(exit)) return exit.value;
              if (Cause.hasInterruptsOnly(exit.cause)) {
                throw new WorkflowAbortedError();
              }
              const err = Cause.squash(exit.cause);
              if (
                typeof err === "object" &&
                err !== null &&
                "_tag" in err &&
                (err._tag === "WorkflowScriptTooLargeError" ||
                  err._tag === "WorkflowScriptNotRegularFileError") &&
                "message" in err &&
                typeof err.message === "string"
              ) {
                throw new WorkflowScriptError(err.message);
              }
              throw new WorkflowScriptError(`Unable to read workflow script at ${nestedPath}`);
            });
          },
          budget: {
            total: tokenBudget ?? null,
            spent: () => usage.tokens,
          },
          signal: abortController.signal,
        };

        const coalescerStop = yield* Deferred.make<void>();
        const dispatchProgress = Effect.fn("WorkflowService.dispatchProgress")(
          function* (options?: { readonly forceStopped?: boolean; readonly atMs?: number }) {
            const forceStopped = options?.forceStopped === true;
            if (!forceStopped && (!progressDirty || stopRequested)) return;
            progressDirty = false;
            const sentAt = options?.atMs ?? (yield* Clock.currentTimeMillis);
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
                workflow: buildWorkflowSnapshot(sentAt),
                ...(latestLog === undefined ? {} : { progress: latestLog }),
              })
              .pipe(
                Effect.provideService(DispatchModeLimit, undefined),
                Effect.tap(() =>
                  Effect.sync(() => {
                    if (latestLog !== undefined) lastDispatchedProgress = latestLog;
                  }),
                ),
                Effect.catch((error) =>
                  Effect.logWarning("workflow.progress.dispatch-failed", {
                    runId,
                    taskId,
                    error,
                  }),
                ),
              );
          },
        );

        // Coalesce workflow.progress updates to at most once per second via Effect Clock.
        const coalescerFiber = yield* Effect.gen(function* () {
          let lastSentAt: number | undefined;
          while (true) {
            const event = yield* Effect.raceFirst(
              Queue.take(progressSignal).pipe(Effect.as("signal" as const)),
              Deferred.await(coalescerStop).pipe(Effect.as("stop" as const)),
            );
            if (event === "stop") {
              yield* dispatchProgress();
              return;
            }
            if (!progressDirty) continue;
            const now = yield* Clock.currentTimeMillis;
            if (lastSentAt !== undefined) {
              const remaining = PROGRESS_COALESCE_INTERVAL_MS - (now - lastSentAt);
              if (remaining > 0) {
                const slept = yield* Effect.raceFirst(
                  Effect.sleep(Duration.millis(remaining)).pipe(Effect.as("tick" as const)),
                  Deferred.await(coalescerStop).pipe(Effect.as("stop" as const)),
                );
                if (slept === "stop") {
                  yield* dispatchProgress();
                  return;
                }
              }
            }
            yield* dispatchProgress();
            lastSentAt = yield* Clock.currentTimeMillis;
            if (yield* Deferred.isDone(coalescerStop)) {
              yield* dispatchProgress();
              return;
            }
          }
        }).pipe(Effect.forkScoped);

        const flushCoalescer = Effect.gen(function* () {
          Deferred.doneUnsafe(coalescerStop, Exit.void);
          yield* Fiber.join(coalescerFiber);
        });

        // Watch our own task row so workflow.stop-request or terminal status aborts the script.
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
                (event.payload.workflow?.stopRequestedAt !== undefined ||
                  isTerminalSubagentStatus(event.payload.status)),
            ),
            Stream.runHead,
            Effect.tap((matched) => {
              if (Option.isNone(matched) || endedByEngine) return Effect.void;
              const { event } = matched.value;
              if (event.type !== "subagent.updated") return Effect.void;
              return Effect.sync(() =>
                triggerExternalStop(event.payload.workflow?.stopRequestedAt),
              );
            }),
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
                workerClosed = true;
                closeScriptReads();
                Deferred.doneUnsafe(agentsAbortDeferred, Exit.void);
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
              onCleanupFailure: (cleanupFailure) => {
                void runPromise(
                  Effect.logError("workflow.sandbox.cleanup-failed", {
                    runId,
                    taskId,
                    pid: cleanupFailure.pid,
                    attempts: cleanupFailure.attempts,
                    cause: cleanupFailure.cause,
                  }),
                );
              },
            }).finally(() => {
              workerClosed = true;
              closeScriptReads();
            });
          },
          catch: (err) => new WorkflowExecutionError({ messageText: errorMessage(err) }),
        }).pipe(Effect.result);
        workerClosed = true;
        scriptReadsClosed = true;
        yield* Scope.close(scriptReadsScope, Exit.void);

        if (!stopRequested) {
          scriptSettled = true;
          Deferred.doneUnsafe(agentsAbortDeferred, Exit.void);
          yield* awaitAllActiveAgents;
          if (cancelledUnawaitedCount > 0) {
            yield* recordEngineLog(
              `${cancelledUnawaitedCount} unawaited ${cancelledUnawaitedCount === 1 ? "agent was" : "agents were"} cancelled when the script ended`,
            );
          }
        }

        const trackedOwnedChildren = (): ReadonlyArray<OwnedChildTarget> =>
          [...agentsByIndex.values()].flatMap((agent) =>
            agent.childThreadId === undefined
              ? []
              : [
                  {
                    childThreadId: agent.childThreadId,
                    childTaskId:
                      agent.agentId === undefined ? undefined : NodeId.make(agent.agentId),
                  },
                ],
          );

        if (stopRequested) {
          Deferred.doneUnsafe(agentsAbortDeferred, Exit.void);
          yield* awaitAllActiveAgents;
          yield* stopAllWorkflowChildren({
            threadManagement,
            parentThreadId,
            workflowTaskId: taskId,
            runId,
            reason: "Workflow stopped.",
            extraChildren: trackedOwnedChildren(),
            maxAttempts: Number.POSITIVE_INFINITY,
          });
          const stoppedAt = yield* Clock.currentTimeMillis;
          for (const [index, agent] of agentsByIndex) {
            if (agent.state === "queued" || agent.state === "running") {
              agentsByIndex.set(index, {
                ...agent,
                state: "failed",
                completedAt: stoppedAt,
                durationMs: Math.max(
                  0,
                  stoppedAt - (agent.startedAt ?? agent.queuedAt ?? stoppedAt),
                ),
              });
            }
          }
          yield* flushCoalescer;
          const finalStopLog = logs.at(-1);
          if (finalStopLog !== undefined && finalStopLog !== lastDispatchedProgress) {
            yield* dispatchProgress({ forceStopped: true, atMs: stoppedAt });
          }
          yield* appendJournal({ type: "end", status: "cancelled" });
          yield* flushJournal;
          yield* journalWriter.close;
          endedByEngine = true;
          yield* commitWorkflowComplete({
            threadManagement,
            parentThreadId,
            taskId,
            runId,
            commandIdBase: stableCommandId({
              scope,
              operation: "workflow-complete",
              key: runId,
            }),
            status: "cancelled",
            result: "Stopped.",
            workflow: buildWorkflowSnapshot(stoppedAt),
            disposeDelivery: true,
            maxAttempts: Number.POSITIVE_INFINITY,
          });
          status = "cancelled";
          yield* writePersistedRun(buildPersistedRecord(stoppedAt));
          return;
        }

        // Confirm every SQL-owned child is stopped or terminal before reporting completion.
        yield* stopAllWorkflowChildren({
          threadManagement,
          parentThreadId,
          workflowTaskId: taskId,
          runId,
          reason: "Workflow completed.",
          extraChildren: trackedOwnedChildren(),
          maxAttempts: Number.POSITIVE_INFINITY,
        });

        endedByEngine = true;
        if (hostLimitError === undefined && Result.isSuccess(scriptOutcome)) {
          const completedAt = yield* Clock.currentTimeMillis;
          const finalUsage = buildUsage(completedAt);
          yield* flushCoalescer;
          yield* appendJournal({
            type: "end",
            status: "completed",
            result: scriptOutcome.success,
          });
          yield* flushJournal;
          yield* journalWriter.close;
          yield* commitWorkflowComplete({
            threadManagement,
            parentThreadId,
            taskId,
            runId,
            commandIdBase: stableCommandId({
              scope,
              operation: "workflow-complete",
              key: runId,
            }),
            status: "completed",
            result: formatCompletionResult({
              runId,
              name: meta.name,
              result: scriptOutcome.success,
              usage: finalUsage,
              journalPath,
            }),
            workflow: buildWorkflowSnapshot(completedAt),
            maxAttempts: Number.POSITIVE_INFINITY,
          });
          status = "completed";
          finalResult = scriptOutcome.success as Schema.Json;
          yield* writePersistedRun(buildPersistedRecord(completedAt));
          return;
        }

        const errText =
          hostLimitError ??
          (Result.isFailure(scriptOutcome)
            ? scriptOutcome.failure.messageText
            : "Workflow failed.");
        const failedAt = yield* Clock.currentTimeMillis;
        yield* flushCoalescer;
        yield* appendJournal({
          type: "end",
          status: "failed",
          error: errText,
        });
        yield* flushJournal;
        yield* journalWriter.close;
        yield* commitWorkflowComplete({
          threadManagement,
          parentThreadId,
          taskId,
          runId,
          commandIdBase: stableCommandId({
            scope,
            operation: "workflow-complete",
            key: runId,
          }),
          status: "failed",
          result: formatFailureResult({ runId, error: errText }),
          workflow: buildWorkflowSnapshot(failedAt),
          maxAttempts: Number.POSITIVE_INFINITY,
        });
        status = "failed";
        finalError = errText;
        yield* writePersistedRun(buildPersistedRecord(failedAt));
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
      scriptSource = yield* readBoundedWorkflowScript(input.scriptPath).pipe(
        Effect.mapError((error) =>
          error._tag === "WorkflowScriptTooLargeError" ||
          error._tag === "WorkflowScriptNotRegularFileError"
            ? failure("invalid_request", error.message)
            : failure("invalid_request", `Unable to read workflow script at ${input.scriptPath}.`),
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
    const declaredPhaseCount = new Set(
      meta.phases.map((phase) => phase.title.trim()).filter((title) => title.length > 0),
    ).size;
    if (declaredPhaseCount > MAX_DISTINCT_PHASES) {
      return yield* failure(
        "invalid_request",
        `A workflow may declare at most ${MAX_DISTINCT_PHASES} distinct phases.`,
      );
    }

    let cursor: ReplayCursor | undefined;
    if (input.resumeFromRunId !== undefined) {
      const sourceRunId = input.resumeFromRunId;
      if (!isSafeRunId(sourceRunId)) {
        return yield* failure("invalid_request", `Invalid resumeFromRunId "${sourceRunId}".`);
      }
      const liveSource = liveRuns.get(sourceRunId);
      if (liveSource !== undefined) {
        const liveSqlTask = yield* readWorkflowTaskStrict(
          threadManagement,
          liveSource.threadId,
          liveSource.taskId,
        ).pipe(Effect.mapError(threadManagementFailure));
        if (
          liveSource.currentStatus() === "running" &&
          (liveSqlTask === undefined || !isTerminalSubagentStatus(liveSqlTask.status))
        ) {
          return yield* failure(
            "invalid_request",
            `Workflow run "${sourceRunId}" is still running and cannot be resumed until it finishes.`,
          );
        }
        // Live engine already committed terminal SQL after confirming child stops; await run.json flush.
        yield* Deferred.await(liveSource.done);
      }
      const sourcePersisted = yield* readPersistedRun(sourceRunId);
      if (Option.isNone(sourcePersisted)) {
        return yield* failure("invalid_request", `Workflow run "${sourceRunId}" was not found.`);
      }
      if (sourcePersisted.value.cleanupPending === true) {
        return yield* failure(
          "invalid_request",
          `Workflow run "${sourceRunId}" still has child cleanup pending and cannot be resumed until its child stops are confirmed.`,
        );
      }
      if (sourcePersisted.value.status === "running") {
        return yield* failure(
          "invalid_request",
          `Workflow run "${sourceRunId}" is still running and cannot be resumed until it finishes.`,
        );
      }
      if (sourcePersisted.value.taskId !== null) {
        const sourceSqlTask = yield* readWorkflowTaskStrict(
          threadManagement,
          sourcePersisted.value.threadId,
          sourcePersisted.value.taskId,
        ).pipe(Effect.mapError(threadManagementFailure));
        if (sourceSqlTask !== undefined && !isTerminalSubagentStatus(sourceSqlTask.status)) {
          return yield* failure(
            "invalid_request",
            `Workflow run "${sourceRunId}" is still running in orchestration state and cannot be resumed until it finishes.`,
          );
        }
      }
      const prevJournalPath = journalPathFor(sourceRunId);
      const prevExists = yield* fileSystem
        .exists(prevJournalPath)
        .pipe(Effect.orElseSucceed(() => false));
      if (!prevExists) {
        return yield* failure(
          "invalid_request",
          `Journal not found for workflow run "${sourceRunId}".`,
        );
      }
      const prevText = yield* fileSystem
        .readFileString(prevJournalPath)
        .pipe(
          Effect.mapError(() =>
            failure("invalid_request", `Unable to read journal for workflow run "${sourceRunId}".`),
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

    const startCommandId = stableCommandId({ scope, operation: "workflow-start", key: runId });
    const derivedTaskId = IdAllocator.derive.delegatedTaskNode({ commandId: startCommandId });
    const startedAt = yield* Clock.currentTimeMillis;
    const initialPersistedRun: PersistedWorkflowRun = {
      runId,
      threadId: scope.thread.threadId,
      taskId: derivedTaskId,
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
    };
    yield* writePersistedRunStrict(initialPersistedRun).pipe(
      Effect.mapError(() =>
        failure("orchestration_error", `Unable to persist workflow metadata for ${runId}.`),
      ),
    );

    const started = yield* threadManagement
      .dispatch({
        type: "workflow.start",
        commandId: startCommandId,
        parentThreadId: scope.thread.threadId,
        parentRunId: parentRun.id,
        parentNodeId: parentRun.rootNodeId,
        title: truncateWorkflowTitle(meta.name),
        description: meta.description,
        modelSelection: parentRun.modelSelection,
        workflow: initialWorkflow,
      })
      .pipe(
        Effect.tapError(() => fileSystem.remove(runDir, { recursive: true }).pipe(Effect.ignore)),
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
    if (taskId !== derivedTaskId) {
      yield* writePersistedRun({ ...initialPersistedRun, taskId });
    }

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
    const live = liveRuns.get(runId);
    const persisted = live === undefined ? yield* readPersistedRun(runId) : Option.none();
    if (live === undefined && Option.isNone(persisted)) {
      return yield* failure("task_not_found", `Workflow run "${runId}" was not found.`);
    }

    const targetThreadId =
      live !== undefined ? live.threadId : Option.getOrThrow(persisted).threadId;
    const targetTaskId = live !== undefined ? live.taskId : Option.getOrThrow(persisted).taskId;

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

    if (live !== undefined) {
      const liveStatus = live.currentStatus();
      if (liveStatus !== "running") {
        return { runId, status: liveStatus };
      }
      const stopRequestResult = yield* threadManagement
        .dispatch({
          type: "workflow.stop-request",
          commandId: CommandId.make(`server:workflow-stop-request:${runId}:${live.taskId}`),
          parentThreadId: live.threadId,
          taskId: live.taskId,
        })
        .pipe(
          Effect.mapError((error) =>
            failure(
              "orchestration_error",
              `Unable to request workflow stop: ${userFacingDispatchErrorMessage(error) ?? errorMessage(error)}`,
            ),
          ),
        );
      const stopEvent = stopRequestResult.storedEvents.find(
        (stored) =>
          stored.event.type === "subagent.updated" && stored.event.payload.id === live.taskId,
      );
      const stopRequestedAt =
        stopEvent?.event.type === "subagent.updated"
          ? stopEvent.event.payload.workflow?.stopRequestedAt
          : undefined;
      live.triggerExternalStop(stopRequestedAt);
      yield* Deferred.await(live.done).pipe(
        Effect.timeoutOption(Duration.millis(STOP_WAIT_BOUND_MS)),
      );
      return { runId, status: live.currentStatus() };
    }

    const record = Option.getOrThrow(persisted);
    if (targetTaskId === null) {
      const nextStatus = record.status === "running" ? "cancelled" : record.status;
      if (record.status === "running" || record.cleanupPending === true) {
        const journalEntries = yield* readJournalEntriesSafe(fileSystem, record.journalPath);
        yield* stopAllWorkflowChildren({
          threadManagement,
          parentThreadId: targetThreadId,
          workflowTaskId: null,
          runId,
          reason: "Workflow stopped.",
          extraChildren: collectJournalChildren(journalEntries),
          maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
        }).pipe(Effect.mapError(threadManagementFailure));
        yield* writePersistedRun({ ...withoutCleanupPending(record), status: nextStatus });
      }
      return { runId, status: nextStatus };
    }

    const sqlTask = yield* readWorkflowTaskStrict(
      threadManagement,
      targetThreadId,
      targetTaskId,
    ).pipe(Effect.mapError(threadManagementFailure));
    if (sqlTask === undefined) {
      const nextStatus = record.status === "running" ? "cancelled" : record.status;
      if (record.status === "running" || record.cleanupPending === true) {
        const journalEntries = yield* readJournalEntriesSafe(fileSystem, record.journalPath);
        yield* stopAllWorkflowChildren({
          threadManagement,
          parentThreadId: targetThreadId,
          workflowTaskId: targetTaskId,
          runId,
          reason: "Workflow stopped.",
          extraChildren: collectJournalChildren(journalEntries),
          maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
        }).pipe(Effect.mapError(threadManagementFailure));
        yield* writePersistedRun({ ...withoutCleanupPending(record), status: nextStatus });
      }
      return { runId, status: nextStatus };
    }
    if (isTerminalSubagentStatus(sqlTask.status)) {
      const sqlStatus = subagentStatusToWorkflowStatus(sqlTask.status);
      if (record.status === "running" || record.cleanupPending === true) {
        const journalEntries = yield* readJournalEntriesSafe(fileSystem, record.journalPath);
        const confirmedChildren = record.cleanupPending === true || sqlTask.status !== "completed";
        if (confirmedChildren) {
          yield* stopAllWorkflowChildren({
            threadManagement,
            parentThreadId: targetThreadId,
            workflowTaskId: targetTaskId,
            runId,
            reason: "Workflow stopped.",
            extraChildren: collectJournalChildren(journalEntries),
            maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
          }).pipe(Effect.mapError(threadManagementFailure));
        }
        yield* reconcilePersistedFromSql(storeDeps, record, sqlTask, journalEntries, "Stopped.", {
          clearCleanupPending: confirmedChildren,
        });
      }
      return { runId, status: sqlStatus };
    }

    // No live engine owns this SQL-running workflow; close admission, stop children, and complete it here.
    yield* threadManagement
      .dispatch({
        type: "workflow.stop-request",
        commandId: CommandId.make(`server:workflow-stop-request:${runId}:${targetTaskId}`),
        parentThreadId: targetThreadId,
        taskId: targetTaskId,
      })
      .pipe(
        Effect.mapError((error) =>
          failure(
            "orchestration_error",
            `Unable to request workflow stop: ${userFacingDispatchErrorMessage(error) ?? errorMessage(error)}`,
          ),
        ),
      );

    const journalEntries = yield* readJournalEntriesSafe(fileSystem, record.journalPath);
    yield* stopAllWorkflowChildren({
      threadManagement,
      parentThreadId: targetThreadId,
      workflowTaskId: targetTaskId,
      runId,
      reason: "Workflow stopped.",
      extraChildren: collectJournalChildren(journalEntries),
      maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
    }).pipe(Effect.mapError(threadManagementFailure));

    const stoppedAt = yield* Clock.currentTimeMillis;
    const latestTask = yield* readWorkflowTaskStrict(
      threadManagement,
      targetThreadId,
      targetTaskId,
    ).pipe(Effect.mapError(threadManagementFailure));
    const confirmedTask = yield* commitWorkflowComplete({
      threadManagement,
      parentThreadId: targetThreadId,
      taskId: targetTaskId,
      runId,
      commandIdBase: `server:workflow-stop-complete:${runId}:${targetTaskId}`,
      status: "cancelled",
      result: "Stopped.",
      workflow: buildStoppedSnapshotFromSql(latestTask?.workflow ?? sqlTask.workflow, stoppedAt),
      disposeDelivery: true,
      maxAttempts: STARTUP_RECOVERY_MAX_ATTEMPTS,
    }).pipe(Effect.mapError(threadManagementFailure));

    const finalStatus = subagentStatusToWorkflowStatus(confirmedTask.status);
    if (record.status === "running" || record.cleanupPending === true) {
      yield* writePersistedRun({ ...withoutCleanupPending(record), status: finalStatus });
    }
    return { runId, status: finalStatus };
  });

  return WorkflowService.of({
    run,
    status,
    stop,
  });
});

export const layer = Layer.effect(WorkflowService, make);
