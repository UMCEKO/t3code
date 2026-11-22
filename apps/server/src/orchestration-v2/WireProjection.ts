import type {
  CheckpointId,
  MessageId,
  NodeId,
  OrchestrationV2Checkpoint,
  OrchestrationV2DomainEvent,
  OrchestrationV2ContextHandoff,
  OrchestrationV2StoredEvent,
  OrchestrationV2Subagent,
  OrchestrationV2SubagentWorkflow,
  OrchestrationV2ThreadProjection,
  OrchestrationV2ThreadStreamItem,
  OrchestrationV2TurnItem,
  OrchestrationV2WorkflowAgent,
} from "@t3tools/contracts";
import {
  dedupeSubagentBodies,
  dedupeTurnItemBodies,
} from "@t3tools/shared/orchestrationV2DedupedBodies";
import { MCP_APP_OUTPUT_KEY, readMcpAppReference } from "@t3tools/shared/mcpApp";
import {
  commandIndicatesFailure,
  compactDynamicToolOutput,
  omitToolOutputImageData,
  toolOutputImages,
} from "@t3tools/shared/toolOutput";

// A checkpoint over vendored or generated trees can list 15k+ files; the card needs a prefix.
const MAX_WIRE_CHECKPOINT_FILES = 200;
const MAX_WIRE_WORKFLOW_AGENTS = 200;
const MAX_WIRE_WORKFLOW_LOGS = 50;
const MAX_WIRE_WORKFLOW_LOG_CHARS = 2_000;
// Workflow members render from the parent workflow's previews, not their own subagent rows.
const MAX_WORKFLOW_MEMBER_PREVIEW_CHARS = 500;
const MAX_WORKFLOW_AGENT_LABEL_CHARS = 200;
const MAX_DETAIL_STRING_BYTES = 32_768;
const MAX_DYNAMIC_VALUE_BYTES = 16_384;
const MAX_ON_DEMAND_BYTES = 256 * 1024;
const TRUNCATION_MARKER = "\n… output truncated for transport";
const TRUNCATION_MARKER_BYTES = Buffer.byteLength(TRUNCATION_MARKER, "utf8");

function truncateDetail(
  value: string | undefined,
  maxBytes = MAX_DETAIL_STRING_BYTES,
): string | undefined {
  if (
    value === undefined ||
    (value.length <= maxBytes && Buffer.byteLength(value, "utf8") <= maxBytes)
  ) {
    return value;
  }
  // The marker counts toward the cap, so projecting truncated text again leaves it unchanged.
  const budget = maxBytes - TRUNCATION_MARKER_BYTES;
  // UTF-8 needs at least one byte per UTF-16 unit; encode only a potentially fitting prefix.
  const prefix = Buffer.from(value.slice(0, budget), "utf8")
    .subarray(0, budget)
    .toString("utf8")
    .replace(/\uFFFD$/u, "");
  return `${prefix}${TRUNCATION_MARKER}`;
}

function truncateChars(value: string, maxChars: number): string {
  if (value.length <= maxChars) return value;
  let end = maxChars - 1;
  const last = value.charCodeAt(end - 1);
  if (last >= 0xd800 && last <= 0xdbff) {
    end -= 1;
  }
  return `${value.slice(0, end)}…`;
}

function projectWorkflowAgentForWire(
  agent: OrchestrationV2WorkflowAgent,
): OrchestrationV2WorkflowAgent {
  const label = truncateChars(agent.label, MAX_WORKFLOW_AGENT_LABEL_CHARS);
  const prompt =
    agent.prompt === undefined
      ? undefined
      : truncateChars(agent.prompt, MAX_WORKFLOW_MEMBER_PREVIEW_CHARS);
  const result =
    agent.result === undefined
      ? undefined
      : truncateChars(agent.result, MAX_WORKFLOW_MEMBER_PREVIEW_CHARS);
  const error =
    agent.error === undefined
      ? undefined
      : truncateChars(agent.error, MAX_WORKFLOW_MEMBER_PREVIEW_CHARS);
  if (
    label === agent.label &&
    prompt === agent.prompt &&
    result === agent.result &&
    error === agent.error
  ) {
    return agent;
  }
  return {
    ...agent,
    label,
    ...(prompt === undefined ? {} : { prompt }),
    ...(result === undefined ? {} : { result }),
    ...(error === undefined ? {} : { error }),
  };
}

function boundWorkflowLogsForWire(
  logs: ReadonlyArray<string> | undefined,
): ReadonlyArray<string> | undefined {
  if (logs === undefined || logs.length === 0) return logs;
  const sliced = logs.length > MAX_WIRE_WORKFLOW_LOGS ? logs.slice(-MAX_WIRE_WORKFLOW_LOGS) : logs;
  let changed = sliced !== logs;
  const bounded = sliced.map((entry) => {
    const next = truncateChars(entry, MAX_WIRE_WORKFLOW_LOG_CHARS);
    if (next !== entry) changed = true;
    return next;
  });
  return changed ? bounded : logs;
}

function settledAgentRecency(agent: OrchestrationV2WorkflowAgent): number {
  if (agent.completedAt !== undefined) return agent.completedAt;
  if (agent.startedAt !== undefined) return agent.startedAt + (agent.durationMs ?? 0);
  return agent.queuedAt ?? -1;
}

function boundWorkflowAgentsForWire(
  agents: ReadonlyArray<OrchestrationV2WorkflowAgent>,
): ReadonlyArray<OrchestrationV2WorkflowAgent> {
  if (agents.length <= MAX_WIRE_WORKFLOW_AGENTS) {
    let changed = false;
    const projected = agents.map((agent) => {
      const next = projectWorkflowAgentForWire(agent);
      if (next !== agent) changed = true;
      return next;
    });
    return changed ? projected : agents;
  }

  const running: OrchestrationV2WorkflowAgent[] = [];
  const queued: OrchestrationV2WorkflowAgent[] = [];
  const settled: OrchestrationV2WorkflowAgent[] = [];
  for (const agent of agents) {
    if (agent.state === "running") running.push(agent);
    else if (agent.state === "queued") queued.push(agent);
    else settled.push(agent);
  }

  const selected: OrchestrationV2WorkflowAgent[] = [];
  const take = (list: ReadonlyArray<OrchestrationV2WorkflowAgent>) => {
    const budget = MAX_WIRE_WORKFLOW_AGENTS - selected.length;
    if (budget <= 0 || list.length === 0) return;
    const limit = Math.min(budget, list.length);
    for (let index = 0; index < limit; index += 1) {
      selected.push(projectWorkflowAgentForWire(list[index]!));
    }
  };

  if (running.length > MAX_WIRE_WORKFLOW_AGENTS) {
    running.sort((left, right) => left.index - right.index);
  }
  take(running);

  if (selected.length < MAX_WIRE_WORKFLOW_AGENTS) {
    if (queued.length > MAX_WIRE_WORKFLOW_AGENTS - selected.length) {
      queued.sort((left, right) => left.index - right.index);
    }
    take(queued);
  }

  if (selected.length < MAX_WIRE_WORKFLOW_AGENTS && settled.length > 0) {
    settled.sort(
      (left, right) =>
        settledAgentRecency(right) - settledAgentRecency(left) || right.index - left.index,
    );
    take(settled);
  }

  return selected;
}

function selectChangedWorkflowAgents(
  agents: ReadonlyArray<OrchestrationV2WorkflowAgent>,
  changedAgentIndices: ReadonlyArray<number>,
): ReadonlyArray<OrchestrationV2WorkflowAgent> {
  if (changedAgentIndices.length === 0) return [];
  if (changedAgentIndices.length === 1) {
    const target = changedAgentIndices[0]!;
    const direct = agents[target];
    if (direct !== undefined && direct.index === target) return [direct];
    const found = agents.find((agent) => agent.index === target);
    return found === undefined ? [] : [found];
  }
  const wanted = new Set(changedAgentIndices);
  const selected: OrchestrationV2WorkflowAgent[] = [];
  for (const agent of agents) {
    if (wanted.has(agent.index)) selected.push(agent);
  }
  return selected;
}

function projectWorkflowForWire(
  workflow: OrchestrationV2SubagentWorkflow,
  allowDelta: boolean,
): OrchestrationV2SubagentWorkflow {
  const agentCount = Math.max(workflow.agentCount ?? 0, workflow.agents.length);
  const logs = boundWorkflowLogsForWire(workflow.logs);
  // A delta past the row cap would drop rows the client keeps stale, so send a replacement instead.
  if (
    allowDelta &&
    workflow.changedAgentIndices !== undefined &&
    workflow.changedAgentIndices.length <= MAX_WIRE_WORKFLOW_AGENTS
  ) {
    const { changedAgentIndices, ...rest } = workflow;
    const agents = boundWorkflowAgentsForWire(
      selectChangedWorkflowAgents(workflow.agents, changedAgentIndices),
    );
    return {
      ...rest,
      ...(logs === undefined ? {} : { logs }),
      agents,
      agentCount,
      agentsDelta: true,
    };
  }

  const agents = boundWorkflowAgentsForWire(workflow.agents);
  const stripChanged = workflow.changedAgentIndices !== undefined;
  const stripDelta = !allowDelta && workflow.agentsDelta !== undefined;
  if (
    !stripChanged &&
    !stripDelta &&
    agents === workflow.agents &&
    logs === workflow.logs &&
    workflow.agentCount === agentCount
  ) {
    return workflow;
  }

  const { changedAgentIndices: _changed, agentsDelta, ...rest } = workflow;
  return {
    ...rest,
    ...(logs === undefined ? {} : { logs }),
    agents,
    agentCount,
    ...(allowDelta && agentsDelta === true ? { agentsDelta: true as const } : {}),
  };
}

/** Bound stored diffs to the client wire limit to avoid retaining invisible data in SQLite and heap. */
export function boundFileChangeForStorage(item: OrchestrationV2TurnItem): OrchestrationV2TurnItem {
  if (item.type !== "file_change") return item;
  const { diffStr, oldStr, newStr } = item;
  const bounded = {
    ...item,
    ...(diffStr === undefined ? {} : { diffStr: truncateDetail(diffStr) }),
    ...(oldStr === undefined ? {} : { oldStr: truncateDetail(oldStr) }),
    ...(newStr === undefined ? {} : { newStr: truncateDetail(newStr) }),
  };
  return bounded.diffStr === diffStr && bounded.oldStr === oldStr && bounded.newStr === newStr
    ? item
    : bounded;
}

function summarizeDynamicValue(value: unknown): unknown {
  let serialized: string;
  try {
    if (typeof value === "string" && value.length > MAX_DYNAMIC_VALUE_BYTES) {
      serialized = value;
    } else {
      const json = JSON.stringify(value) ?? String(value);
      if (Buffer.byteLength(json, "utf8") <= MAX_DYNAMIC_VALUE_BYTES) {
        return value;
      }
      serialized = typeof value === "string" ? value : json;
    }
  } catch {
    serialized = "Unserializable tool output";
  }

  // Splitting and normalizing every line can allocate far more than the input.
  const start = /\S/u.exec(serialized)?.index;
  let firstLine = start === undefined ? "Large tool output" : "";
  let pendingSpace = false;
  for (let index = start ?? serialized.length; index < serialized.length; index += 1) {
    const character = serialized[index]!;
    if (character === "\n") break;
    if (/\s/u.test(character)) {
      pendingSpace = true;
      continue;
    }
    if (pendingSpace) firstLine += " ";
    firstLine += character;
    pendingSpace = false;
    if (firstLine.length > 160) break;
  }
  const last = firstLine.charCodeAt(158);
  const end = last >= 0xd800 && last <= 0xdbff ? 158 : 159;
  return {
    summary: firstLine.length <= 160 ? firstLine : `${firstLine.slice(0, end).trimEnd()}…`,
    truncated: true,
  };
}

type CheckpointFiles = Pick<OrchestrationV2Checkpoint, "files" | "filesTotal">;

// Same input, same output object: snapshot compaction relies on shared item identity.
const cappedCheckpointFiles = new WeakMap<object, object>();

function capCheckpointFiles<T extends CheckpointFiles>(value: T): T {
  if (value.files.length <= MAX_WIRE_CHECKPOINT_FILES || value.filesTotal !== undefined) {
    return value;
  }
  const cached = cappedCheckpointFiles.get(value);
  if (cached !== undefined) return cached as T;
  let additions = 0;
  let deletions = 0;
  for (const file of value.files) {
    additions += file.additions;
    deletions += file.deletions;
  }
  const capped = {
    ...value,
    files: value.files.slice(0, MAX_WIRE_CHECKPOINT_FILES),
    filesTotal: { count: value.files.length, additions, deletions },
  };
  cappedCheckpointFiles.set(value, capped);
  return capped;
}

/** Idempotent; bounded snapshots that skip `projectThreadProjectionForWire` still need it. */
export function capProjectionCheckpointFilesForWire(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadProjection {
  const checkpoints = projection.checkpoints.map(capCheckpointFiles);
  const turnItems = projection.turnItems.map(capCheckpointItemFiles);
  const visibleTurnItems = projection.visibleTurnItems.map((row) => {
    const item = capCheckpointItemFiles(row.item);
    return item === row.item ? row : { ...row, item };
  });
  const unchanged = (next: ReadonlyArray<unknown>, previous: ReadonlyArray<unknown>) =>
    next.every((value, index) => value === previous[index]);
  return unchanged(checkpoints, projection.checkpoints) &&
    unchanged(turnItems, projection.turnItems) &&
    unchanged(visibleTurnItems, projection.visibleTurnItems)
    ? projection
    : { ...projection, checkpoints, turnItems, visibleTurnItems };
}

function capCheckpointItemFiles(item: OrchestrationV2TurnItem): OrchestrationV2TurnItem {
  return item.type === "checkpoint" ? capCheckpointFiles(item) : item;
}

export function projectTurnItemForWire(item: OrchestrationV2TurnItem): OrchestrationV2TurnItem {
  switch (item.type) {
    case "checkpoint":
      return capCheckpointFiles(item);
    case "handoff": {
      const { summary: _summary, ...projected } = item;
      return projected;
    }
    case "command_execution": {
      const { output, ...projected } = item;
      // A successful search can print "ENOENT" or "exit code 2", so output text
      // decides only when the provider reported no exit code.
      const failed = commandIndicatesFailure(item);
      return {
        ...projected,
        ...(failed ? { outputIndicatesFailure: true } : {}),
        ...(output?.trim() ? { outputOmitted: true } : {}),
      };
    }
    case "file_change": {
      const { diffStr, oldStr: _old, newStr: _new, ...projected } = item;
      return item.status === "failed" && diffStr?.trim()
        ? { ...projected, diffStr: truncateDetail(diffStr) }
        : projected;
    }
    case "subagent":
      return {
        ...item,
        prompt: truncateDetail(item.prompt) ?? "",
        progress: truncateDetail(item.progress),
        result: item.result === null ? null : (truncateDetail(item.result) ?? null),
      };
    case "dynamic_tool": {
      const { output: rawOutput, ...projected } = item;
      const output = compactDynamicToolOutput(rawOutput);
      return {
        ...projected,
        input: summarizeDynamicValue(item.input),
        ...(output === undefined ? {} : { output }),
        ...(hasDynamicValue(rawOutput) ? { outputOmitted: true } : {}),
      };
    }
    default:
      return item;
  }
}

function hasDynamicValue(value: unknown): boolean {
  if (value === undefined || value === null) return false;
  if (typeof value === "string") return value.trim().length > 0;
  if (Array.isArray(value)) return value.length > 0;
  return typeof value !== "object" || Object.keys(value).length > 0;
}

function boundDynamicValue(value: unknown): unknown {
  if (value === undefined) return value;
  if (typeof value === "string") return truncateDetail(value, MAX_ON_DEMAND_BYTES);
  let json: string;
  try {
    json = JSON.stringify(value) ?? String(value);
  } catch {
    return "Unserializable tool value";
  }
  return Buffer.byteLength(json, "utf8") <= MAX_ON_DEMAND_BYTES
    ? value
    : truncateDetail(json, MAX_ON_DEMAND_BYTES);
}

/** Preserve ordered image markers in truncated output so clients can load images by index. */
function boundToolResult(output: unknown): unknown {
  const omitted = omitToolOutputImageData(output);
  const bounded = boundDynamicValue(omitted);
  if (typeof bounded !== "string" || typeof omitted === "string") return bounded;
  const images = toolOutputImages(omitted);
  return images.length === 0
    ? bounded
    : [
        { type: "text", text: bounded },
        ...images.map((image) => ({ type: "image", mimeType: image.mimeType })),
      ];
}

function boundToolOutput(output: unknown): unknown {
  if (typeof output !== "object" || output === null || !("result" in output)) {
    return boundToolResult(output);
  }
  const app =
    MCP_APP_OUTPUT_KEY in output ? readMcpAppReference(output[MCP_APP_OUTPUT_KEY]) : undefined;
  if (app === undefined) return boundToolResult(output);
  // The app receives this result, so its image data stays unless it is too large to send.
  const fits = boundDynamicValue(output.result) === output.result;
  return {
    [MCP_APP_OUTPUT_KEY]: app,
    result: fits ? output.result : boundToolResult(output.result),
  };
}

export function projectTurnItemForDetail(item: OrchestrationV2TurnItem): OrchestrationV2TurnItem {
  switch (item.type) {
    case "command_execution":
      return {
        ...item,
        input: truncateDetail(item.input, MAX_ON_DEMAND_BYTES) ?? "",
        output: truncateDetail(item.output, MAX_ON_DEMAND_BYTES),
      };
    case "dynamic_tool":
      return {
        ...item,
        input: boundDynamicValue(item.input),
        output: boundToolOutput(item.output),
      };
    case "subagent":
      return {
        ...item,
        prompt: truncateDetail(item.prompt, MAX_ON_DEMAND_BYTES) ?? "",
        progress: truncateDetail(item.progress, MAX_ON_DEMAND_BYTES),
        result:
          item.result === null ? null : (truncateDetail(item.result, MAX_ON_DEMAND_BYTES) ?? null),
      };
    case "file_change":
      return {
        ...projectTurnItemForWire(item),
        ...(item.diffStr === undefined
          ? {}
          : { diffStr: truncateDetail(item.diffStr, MAX_ON_DEMAND_BYTES) }),
        ...(!item.diffStr?.trim() && item.oldStr !== undefined
          ? { oldStr: truncateDetail(item.oldStr, MAX_ON_DEMAND_BYTES) }
          : {}),
        ...(!item.diffStr?.trim() && item.newStr !== undefined
          ? { newStr: truncateDetail(item.newStr, MAX_ON_DEMAND_BYTES) }
          : {}),
      };
    case "handoff":
      return projectTurnItemForWire(item);
    default:
      return item;
  }
}

export function projectContextHandoffForWire(
  handoff: OrchestrationV2ContextHandoff,
): OrchestrationV2ContextHandoff {
  const { history: _history, delivery: _delivery, ...projected } = handoff;
  return { ...projected, summaryText: "" };
}

function projectSubagentRecordForWire(
  subagent: OrchestrationV2Subagent,
  allowDelta: boolean,
): OrchestrationV2Subagent {
  if (subagent.completionWake !== "never") {
    if (subagent.workflow === undefined) return subagent;
    const workflow = projectWorkflowForWire(subagent.workflow, allowDelta);
    return workflow === subagent.workflow ? subagent : { ...subagent, workflow };
  }
  const title =
    subagent.title === null ? null : truncateChars(subagent.title, MAX_WORKFLOW_AGENT_LABEL_CHARS);
  const prompt = truncateChars(subagent.prompt, MAX_WORKFLOW_MEMBER_PREVIEW_CHARS);
  const progress = truncateDetail(subagent.progress);
  const result =
    subagent.result === null
      ? null
      : truncateChars(subagent.result, MAX_WORKFLOW_MEMBER_PREVIEW_CHARS);
  const workflow =
    subagent.workflow === undefined
      ? undefined
      : projectWorkflowForWire(subagent.workflow, allowDelta);
  if (
    title === subagent.title &&
    prompt === subagent.prompt &&
    progress === subagent.progress &&
    result === subagent.result &&
    workflow === subagent.workflow
  ) {
    return subagent;
  }
  return {
    ...subagent,
    title,
    prompt,
    ...(progress === undefined ? {} : { progress }),
    result,
    ...(workflow === undefined ? {} : { workflow }),
  };
}

export function projectSubagentForWire(subagent: OrchestrationV2Subagent): OrchestrationV2Subagent {
  return projectSubagentRecordForWire(subagent, false);
}

export function projectThreadProjectionForWire(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadProjection {
  const subagents = projection.subagents.map(projectSubagentForWire);
  const projectedById = new Map<string, OrchestrationV2TurnItem>();
  const project = (item: OrchestrationV2TurnItem) => {
    const key = `${item.threadId}:${item.id}`;
    const existing = projectedById.get(key);
    if (existing !== undefined) return existing;
    const projected = projectTurnItemForWire(item);
    projectedById.set(key, projected);
    return projected;
  };
  return {
    ...projection,
    checkpoints: projection.checkpoints.map(capCheckpointFiles),
    contextHandoffs: projection.contextHandoffs.map(projectContextHandoffForWire),
    subagents,
    turnItems: projection.turnItems.map(project),
    visibleTurnItems: projection.visibleTurnItems.map((row) => ({
      ...row,
      item: project(row.item),
    })),
  };
}

export function projectDomainEventForWire(
  event: OrchestrationV2DomainEvent,
): OrchestrationV2DomainEvent {
  switch (event.type) {
    case "turn-item.updated": {
      const payload = projectTurnItemForWire(event.payload);
      return payload === event.payload ? event : { ...event, payload };
    }
    case "context-handoff.updated":
      return { ...event, payload: projectContextHandoffForWire(event.payload) };
    case "checkpoint.captured": {
      const payload = capCheckpointFiles(event.payload);
      return payload === event.payload ? event : { ...event, payload };
    }
    case "subagent.updated": {
      const payload = projectSubagentRecordForWire(event.payload, true);
      return payload === event.payload ? event : { ...event, payload };
    }
    default:
      return event;
  }
}

// Keyed on the stored event PubSub hands every subscriber, so all of them share one projection
// and LiveStreamBudget sizes it once. Consumers must not mutate the shared result.
const projectedStoredEvents = new WeakMap<OrchestrationV2StoredEvent, OrchestrationV2StoredEvent>();

export function projectStoredEventForWire(
  stored: OrchestrationV2StoredEvent,
): OrchestrationV2StoredEvent {
  const cached = projectedStoredEvents.get(stored);
  if (cached !== undefined) return cached;
  const event = projectDomainEventForWire(stored.event);
  if (event === stored.event) {
    projectedStoredEvents.set(stored, stored);
    return stored;
  }
  const projected = { ...stored, event };
  projectedStoredEvents.set(stored, projected);
  projectedStoredEvents.set(projected, projected);
  return projected;
}

type ThreadStreamEvent = Extract<OrchestrationV2ThreadStreamItem, { readonly kind: "event" }>;

const threadStreamEvents = new WeakMap<OrchestrationV2StoredEvent, ThreadStreamEvent>();

/** For an event already projected for the wire; subscribers share the item and its cached size. */
export function threadStreamEventItem(stored: OrchestrationV2StoredEvent): ThreadStreamEvent {
  const cached = threadStreamEvents.get(stored);
  if (cached !== undefined) return cached;
  const item = { kind: "event" as const, sequence: stored.sequence, event: stored.event };
  threadStreamEvents.set(stored, item);
  return item;
}

const LIVE_DEDUPE_MEMORY = 32;

function remember<K, V>(memory: Map<K, V>, key: K, value: V): void {
  memory.delete(key);
  memory.set(key, value);
  if (memory.size > LIVE_DEDUPE_MEMORY) memory.delete(memory.keys().next().value as K);
}

/**
 * One per subscription, after coalescing, so it remembers only records the client received. A
 * record event comes before the turn item that repeats its body, so the item (or a repeated
 * subagent record) ships blank and marked. The other order, or a forgotten record, only costs
 * bytes: the repeat ships whole.
 */
export function makeLiveBodyDeduper(): (
  item: OrchestrationV2ThreadStreamItem,
) => OrchestrationV2ThreadStreamItem {
  const messageText = new Map<MessageId, string>();
  const checkpointFiles = new Map<CheckpointId, OrchestrationV2Checkpoint["files"]>();
  const subagents = new Map<NodeId, Pick<OrchestrationV2Subagent, "prompt" | "result">>();
  const source = {
    messageText: (id: MessageId) => messageText.get(id),
    checkpointFiles: (id: CheckpointId) => checkpointFiles.get(id),
    subagent: (id: NodeId) => subagents.get(id),
  };
  const rememberSubagent = ({ id, prompt, result }: OrchestrationV2Subagent) => {
    remember(subagents, id, { prompt, result });
  };
  return (item) => {
    if (item.kind === "snapshot") {
      // The client now holds exactly these records; their bodies are never blanked.
      const { messages, checkpoints, subagents: records } = item.projection;
      for (const message of messages) remember(messageText, message.id, message.text);
      for (const checkpoint of checkpoints)
        remember(checkpointFiles, checkpoint.id, checkpoint.files);
      for (const subagent of records) rememberSubagent(subagent);
      return item;
    }
    if (item.kind !== "event") return item;
    const { event } = item;
    switch (event.type) {
      case "message.updated":
        remember(messageText, event.payload.id, event.payload.text);
        return item;
      case "checkpoint.captured":
        remember(checkpointFiles, event.payload.id, event.payload.files);
        return item;
      case "subagent.updated": {
        const payload = dedupeSubagentBodies(event.payload, subagents.get(event.payload.id));
        rememberSubagent(event.payload);
        return payload === event.payload ? item : { ...item, event: { ...event, payload } };
      }
      case "turn-item.updated": {
        const payload = dedupeTurnItemBodies(event.payload, source);
        return payload === event.payload ? item : { ...item, event: { ...event, payload } };
      }
      default:
        return item;
    }
  };
}
