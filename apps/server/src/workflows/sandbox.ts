import * as NodeVM from "node:vm";

export interface WorkflowMeta {
  readonly name: string;
  readonly description: string;
  readonly whenToUse?: string;
  readonly phases: ReadonlyArray<{
    readonly title: string;
    readonly detail?: string;
    readonly model?: string;
  }>;
}

// @effect-diagnostics-next-line extendsNativeError:off - the sandbox is Promise code that runs outside Effect.
export class WorkflowScriptError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "WorkflowScriptError";
  }
}

export interface WorkflowAgentOpts {
  readonly label?: string;
  readonly phase?: string;
  readonly schema?: Record<string, unknown>;
  readonly model?: string;
  readonly effort?: string;
  readonly isolation?: "worktree";
  readonly agentType?: string;
  readonly target?: {
    readonly providerInstanceId?: string;
    readonly driverKind?: string;
    readonly model?: string;
    readonly options?: unknown;
  };
}

export interface WorkflowAgentCall {
  readonly index: number;
  readonly prompt: string;
  readonly opts: WorkflowAgentOpts;
  readonly phase: string | undefined;
}

export interface WorkflowHost {
  /** Resolves to the agent's value (text, or the validated object with a schema), or null when skipped/dead. Rejects only for budget/abort/caps. */
  readonly runAgent: (call: WorkflowAgentCall) => Promise<unknown>;
  readonly onPhase: (title: string) => void;
  readonly onLog: (message: string) => void;
  /** Loads a `{scriptPath}` for workflow(); names of saved workflows are unsupported and must throw. */
  readonly loadScript: (ref: { readonly scriptPath: string }) => Promise<string>;
  readonly budget: { readonly total: number | null; readonly spent: () => number };
  readonly signal: AbortSignal;
}

export const WORKFLOW_LIMITS: { readonly maxAgents: 1000; readonly maxItems: 4096 } = {
  maxAgents: 1000,
  maxItems: 4096,
};

export function workflowConcurrency(cpus: number): number {
  if (!Number.isFinite(cpus)) return 1;
  return Math.min(16, Math.max(1, Math.trunc(cpus) - 2));
}

const MAX_LOG_CHARS = 2000;
const DEFAULT_SYNC_TIMEOUT_MS = 5_000;

const ALLOWED_AGENT_OPT_KEYS = new Set([
  "label",
  "phase",
  "schema",
  "model",
  "effort",
  "isolation",
  "agentType",
  "target",
]);

const ALLOWED_TARGET_KEYS = new Set(["providerInstanceId", "driverKind", "model", "options"]);

const IDENT_START = /^[\p{ID_Start}_$]/u;
const IDENT_CONTINUE = /^[\p{ID_Continue}$]/u;

function isLineTerminator(ch: string): boolean {
  return ch === "\n" || ch === "\r" || ch === "\u2028" || ch === "\u2029";
}

function isWhitespace(ch: string): boolean {
  return (
    ch === " " ||
    ch === "\t" ||
    ch === "\v" ||
    ch === "\f" ||
    ch === "\u00a0" ||
    ch === "\ufeff" ||
    isLineTerminator(ch)
  );
}

interface ParsedMetaScript {
  readonly meta: WorkflowMeta;
  readonly body: string;
  readonly bodyStartLine: number;
}

class MetaScanner {
  readonly source: string;
  pos: number;

  constructor(source: string) {
    this.source = source;
    this.pos = source.charCodeAt(0) === 0xfeff ? 1 : 0;
  }

  skipTrivia(): { readonly sawNewline: boolean } {
    let sawNewline = false;
    while (this.pos < this.source.length) {
      const ch = this.source[this.pos]!;
      if (isWhitespace(ch)) {
        if (isLineTerminator(ch)) sawNewline = true;
        this.pos += 1;
        continue;
      }
      if (ch === "/" && this.source[this.pos + 1] === "/") {
        this.pos += 2;
        while (this.pos < this.source.length && !isLineTerminator(this.source[this.pos]!)) {
          this.pos += 1;
        }
        continue;
      }
      if (ch === "/" && this.source[this.pos + 1] === "*") {
        const end = this.source.indexOf("*/", this.pos + 2);
        if (end === -1) {
          throw new WorkflowScriptError("Unterminated block comment in workflow meta");
        }
        for (let i = this.pos + 2; i < end; i += 1) {
          if (isLineTerminator(this.source[i]!)) {
            sawNewline = true;
            break;
          }
        }
        this.pos = end + 2;
        continue;
      }
      break;
    }
    return { sawNewline };
  }

  readIdentifier(): string | null {
    if (this.pos >= this.source.length) return null;
    const startMatch = IDENT_START.exec(this.source.slice(this.pos));
    if (startMatch === null) return null;
    const start = this.pos;
    this.pos += startMatch[0].length;
    while (this.pos < this.source.length) {
      const contMatch = IDENT_CONTINUE.exec(this.source.slice(this.pos));
      if (contMatch === null) break;
      this.pos += contMatch[0].length;
    }
    return this.source.slice(start, this.pos);
  }

  consumeKeyword(expected: string): boolean {
    const saved = this.pos;
    const ident = this.readIdentifier();
    if (ident === expected) return true;
    this.pos = saved;
    return false;
  }

  scanQuotedString(quote: "'" | '"'): void {
    this.pos += 1;
    while (this.pos < this.source.length) {
      const ch = this.source[this.pos]!;
      if (ch === "\\") {
        this.pos += 2;
        continue;
      }
      if (ch === quote) {
        this.pos += 1;
        return;
      }
      if (ch === "\n" || ch === "\r") {
        throw new WorkflowScriptError("Unterminated string literal in meta");
      }
      this.pos += 1;
    }
    throw new WorkflowScriptError("Unterminated string literal in meta");
  }

  scanPureTemplateLiteral(): void {
    this.pos += 1;
    while (this.pos < this.source.length) {
      const ch = this.source[this.pos]!;
      if (ch === "\\") {
        this.pos += 2;
        continue;
      }
      if (ch === "$" && this.source[this.pos + 1] === "{") {
        throw new WorkflowScriptError("meta must be a pure literal");
      }
      if (ch === "`") {
        this.pos += 1;
        return;
      }
      this.pos += 1;
    }
    throw new WorkflowScriptError("Unterminated template literal in meta");
  }

  scanNumber(): boolean {
    const rest = this.source.slice(this.pos);
    const match =
      /^(?:0[xX][0-9a-fA-F](?:_?[0-9a-fA-F])*|0[bB][01](?:_?[01])*|0[oO][0-7](?:_?[0-7])*|(?:(?:0|[1-9](?:_?[0-9])*)(?:\.(?:[0-9](?:_?[0-9])*)?)?|\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?)/.exec(
        rest,
      );
    if (match === null) return false;
    const end = this.pos + match[0].length;
    const next = this.source[end];
    if (next !== undefined && IDENT_START.test(next)) {
      throw new WorkflowScriptError("meta must be a pure literal");
    }
    this.pos = end;
    return true;
  }

  parseObjectLiteral(): void {
    if (this.source[this.pos] !== "{") {
      throw new WorkflowScriptError("meta must be a pure literal");
    }
    this.pos += 1;

    while (true) {
      this.skipTrivia();
      if (this.pos >= this.source.length) {
        throw new WorkflowScriptError("Unterminated object literal in meta");
      }
      if (this.source[this.pos] === "}") {
        this.pos += 1;
        return;
      }

      const ch = this.source[this.pos]!;
      if (ch === "'" || ch === '"') {
        this.scanQuotedString(ch);
      } else if (this.readIdentifier() !== null) {
        // Identifier key validated; `:` must follow so shorthand/methods/getters are rejected.
      } else if (!this.scanNumber()) {
        throw new WorkflowScriptError("meta must be a pure literal");
      }

      this.skipTrivia();
      if (this.source[this.pos] !== ":") {
        throw new WorkflowScriptError("meta must be a pure literal");
      }
      this.pos += 1;

      this.parseValue();
      this.skipTrivia();

      const sep = this.source[this.pos];
      if (sep === ",") {
        this.pos += 1;
        continue;
      }
      if (sep === "}") {
        this.pos += 1;
        return;
      }
      throw new WorkflowScriptError("meta must be a pure literal");
    }
  }

  parseArrayLiteral(): void {
    this.pos += 1;
    while (true) {
      this.skipTrivia();
      if (this.pos >= this.source.length) {
        throw new WorkflowScriptError("Unterminated array literal in meta");
      }
      if (this.source[this.pos] === "]") {
        this.pos += 1;
        return;
      }
      this.parseValue();
      this.skipTrivia();
      const sep = this.source[this.pos];
      if (sep === ",") {
        this.pos += 1;
        continue;
      }
      if (sep === "]") {
        this.pos += 1;
        return;
      }
      throw new WorkflowScriptError("meta must be a pure literal");
    }
  }

  parseValue(): void {
    this.skipTrivia();
    if (this.pos >= this.source.length) {
      throw new WorkflowScriptError("meta must be a pure literal");
    }
    const ch = this.source[this.pos]!;
    if (ch === "{") {
      this.parseObjectLiteral();
      return;
    }
    if (ch === "[") {
      this.parseArrayLiteral();
      return;
    }
    if (ch === "'" || ch === '"') {
      this.scanQuotedString(ch);
      return;
    }
    if (ch === "`") {
      this.scanPureTemplateLiteral();
      return;
    }
    if (ch === "-" || ch === "+") {
      this.pos += 1;
      this.skipTrivia();
      if (!this.scanNumber()) {
        throw new WorkflowScriptError("meta must be a pure literal");
      }
      return;
    }
    if (this.scanNumber()) {
      return;
    }
    const ident = this.readIdentifier();
    if (ident === "true" || ident === "false" || ident === "null") {
      return;
    }
    throw new WorkflowScriptError("meta must be a pure literal");
  }
}

function extractErrorMessage(err: unknown): string {
  if (typeof err === "object" && err !== null && "message" in err) {
    const msg = (err as { readonly message?: unknown }).message;
    if (typeof msg === "string") return msg;
  }
  return String(err);
}

function extractErrorCode(err: unknown): string | undefined {
  if (typeof err === "object" && err !== null && "code" in err) {
    const code = (err as { readonly code?: unknown }).code;
    if (typeof code === "string") return code;
  }
  return undefined;
}

function ownProperty(obj: object, key: string): unknown {
  return Object.prototype.hasOwnProperty.call(obj, key)
    ? (obj as Record<string, unknown>)[key]
    : undefined;
}

function validateMetaObject(raw: unknown): WorkflowMeta {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new WorkflowScriptError("meta must be a plain object");
  }

  const name = ownProperty(raw, "name");
  if (typeof name !== "string" || name.trim().length === 0) {
    throw new WorkflowScriptError("meta.name must be a non-empty string");
  }

  const description = ownProperty(raw, "description");
  if (typeof description !== "string" || description.trim().length === 0) {
    throw new WorkflowScriptError("meta.description must be a non-empty string");
  }

  const whenToUse = ownProperty(raw, "whenToUse");
  if (whenToUse !== undefined && typeof whenToUse !== "string") {
    throw new WorkflowScriptError("meta.whenToUse must be a string");
  }

  const rawPhases = ownProperty(raw, "phases");
  const phases: Array<{
    readonly title: string;
    readonly detail?: string;
    readonly model?: string;
  }> = [];

  if (rawPhases !== undefined) {
    if (!Array.isArray(rawPhases)) {
      throw new WorkflowScriptError("meta.phases must be an array");
    }
    const seenTitles = new Set<string>();
    for (let i = 0; i < rawPhases.length; i += 1) {
      const item: unknown = rawPhases[i];
      if (typeof item !== "object" || item === null || Array.isArray(item)) {
        throw new WorkflowScriptError(`meta.phases[${i}] must be an object`);
      }
      const title = ownProperty(item, "title");
      if (typeof title !== "string" || title.trim().length === 0) {
        throw new WorkflowScriptError(`meta.phases[${i}].title must be a non-empty string`);
      }
      if (seenTitles.has(title)) {
        throw new WorkflowScriptError(`meta.phases contains duplicate title "${title}"`);
      }
      seenTitles.add(title);

      const detail = ownProperty(item, "detail");
      if (detail !== undefined && typeof detail !== "string") {
        throw new WorkflowScriptError(`meta.phases[${i}].detail must be a string`);
      }

      const model = ownProperty(item, "model");
      if (model !== undefined && typeof model !== "string") {
        throw new WorkflowScriptError(`meta.phases[${i}].model must be a string`);
      }

      phases.push({
        title,
        ...(detail !== undefined ? { detail } : {}),
        ...(model !== undefined ? { model } : {}),
      });
    }
  }

  return {
    name,
    description,
    ...(whenToUse !== undefined ? { whenToUse } : {}),
    phases,
  };
}

function parseMetaAndBody(script: string): ParsedMetaScript {
  if (typeof script !== "string") {
    throw new WorkflowScriptError("Workflow script must be a string");
  }
  const scanner = new MetaScanner(script);
  scanner.skipTrivia();

  if (!scanner.consumeKeyword("export")) {
    throw new WorkflowScriptError("Workflow script must start with `export const meta = { ... }`");
  }
  scanner.skipTrivia();
  if (!scanner.consumeKeyword("const")) {
    throw new WorkflowScriptError("Workflow script must start with `export const meta = { ... }`");
  }
  scanner.skipTrivia();
  if (!scanner.consumeKeyword("meta")) {
    throw new WorkflowScriptError("Workflow script must start with `export const meta = { ... }`");
  }
  scanner.skipTrivia();
  if (script[scanner.pos] !== "=") {
    throw new WorkflowScriptError("Workflow script must start with `export const meta = { ... }`");
  }
  scanner.pos += 1;
  scanner.skipTrivia();

  const literalStart = scanner.pos;
  scanner.parseObjectLiteral();
  const literalEnd = scanner.pos;
  const metaLiteralText = script.slice(literalStart, literalEnd);

  const afterMetaTrivia = scanner.skipTrivia();
  if (scanner.pos < script.length) {
    const nextChar = script[scanner.pos]!;
    if (nextChar === ";") {
      scanner.pos += 1;
    } else if (
      !afterMetaTrivia.sawNewline ||
      nextChar === "." ||
      nextChar === "(" ||
      nextChar === "?"
    ) {
      throw new WorkflowScriptError("meta must be a pure literal");
    }
  }

  let evaluated: unknown;
  try {
    const metaContext = NodeVM.createContext(Object.create(null), {
      codeGeneration: { strings: false, wasm: false },
      microtaskMode: "afterEvaluate",
    });
    evaluated = NodeVM.runInContext(`(${metaLiteralText})`, metaContext, {
      timeout: DEFAULT_SYNC_TIMEOUT_MS,
    });
  } catch (err) {
    throw new WorkflowScriptError(`Failed to evaluate meta literal: ${extractErrorMessage(err)}`);
  }

  const meta = validateMetaObject(evaluated);
  const bodyStart = scanner.pos;
  let bodyStartLine = 0;
  for (let i = 0; i < bodyStart; i += 1) {
    if (script[i] === "\n") bodyStartLine += 1;
  }

  return {
    meta,
    body: script.slice(bodyStart),
    bodyStartLine,
  };
}

export function parseWorkflowMeta(script: string): WorkflowMeta {
  return parseMetaAndBody(script).meta;
}

interface AbortHub {
  readonly aborted: boolean;
  readonly reason: () => unknown;
  readonly onAbort: (listener: (reason: unknown) => void) => () => void;
  readonly dispose: () => void;
}

function createAbortHub(signal: AbortSignal): AbortHub {
  const listeners = new Set<(reason: unknown) => void>();
  const getReason = (): unknown =>
    signal.reason !== undefined ? signal.reason : new Error("Workflow aborted");

  const handleAbort = (): void => {
    const reason = getReason();
    for (const listener of Array.from(listeners)) {
      listener(reason);
    }
    listeners.clear();
  };

  if (!signal.aborted) {
    signal.addEventListener("abort", handleAbort, { once: true });
  }

  return {
    get aborted() {
      return signal.aborted;
    },
    reason: getReason,
    onAbort(listener) {
      if (signal.aborted) {
        listener(getReason());
        return () => {};
      }
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    dispose() {
      signal.removeEventListener("abort", handleAbort);
      listeners.clear();
    },
  };
}

function raceWithAbort<T>(promise: Promise<T>, abortHub: AbortHub): Promise<T> {
  if (abortHub.aborted) {
    promise.catch(() => {});
    return Promise.reject(abortHub.reason());
  }
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const unsub = abortHub.onAbort((reason) => {
      if (settled) return;
      settled = true;
      promise.catch(() => {});
      reject(reason);
    });
    promise.then(
      (value) => {
        if (settled) return;
        settled = true;
        unsub();
        resolve(value);
      },
      (err) => {
        if (settled) return;
        settled = true;
        unsub();
        reject(err);
      },
    );
  });
}

interface SemaphoreWaiter {
  readonly resolve: () => void;
  readonly reject: (reason: unknown) => void;
  unsubAbort: () => void;
}

class FifoSemaphore {
  private readonly limit: number;
  private readonly abortHub: AbortHub;
  private active = 0;
  private readonly queue: Array<SemaphoreWaiter> = [];

  constructor(limit: number, abortHub: AbortHub) {
    this.limit = limit;
    this.abortHub = abortHub;
  }

  tryAcquire(): boolean {
    if (this.abortHub.aborted || this.active >= this.limit) {
      return false;
    }
    this.active += 1;
    return true;
  }

  acquire(): Promise<void> {
    if (this.abortHub.aborted) {
      return Promise.reject(this.abortHub.reason());
    }
    if (this.active < this.limit) {
      this.active += 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      const waiter: SemaphoreWaiter = {
        resolve,
        reject,
        unsubAbort: () => {},
      };
      waiter.unsubAbort = this.abortHub.onAbort((reason) => {
        const idx = this.queue.indexOf(waiter);
        if (idx !== -1) {
          this.queue.splice(idx, 1);
        }
        reject(reason);
      });
      this.queue.push(waiter);
    });
  }

  release(): void {
    while (this.queue.length > 0) {
      const next = this.queue.shift()!;
      next.unsubAbort();
      if (this.abortHub.aborted) {
        next.reject(this.abortHub.reason());
        continue;
      }
      // Transfer the slot directly to the next FIFO waiter so new arrivals cannot barge ahead.
      next.resolve();
      return;
    }
    this.active -= 1;
  }
}

function isPlainObject(
  value: unknown,
  realmObjectProto: unknown,
): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === null || proto === Object.prototype || proto === realmObjectProto;
}

function strictJsonStringify(value: unknown, label: string): string {
  try {
    const serialized = JSON.stringify(value, (_key, v: unknown) => {
      if (typeof v === "function" || typeof v === "symbol" || typeof v === "bigint") {
        throw new WorkflowScriptError(`${label} is not JSON-serializable`);
      }
      return v;
    });
    if (serialized === undefined) {
      throw new WorkflowScriptError(`${label} is not JSON-serializable`);
    }
    return serialized;
  } catch (err) {
    if (err instanceof WorkflowScriptError) throw err;
    throw new WorkflowScriptError(`${label} is not JSON-serializable: ${extractErrorMessage(err)}`);
  }
}

function cloneJsonToHost(value: unknown, label: string): unknown {
  return JSON.parse(strictJsonStringify(value, label));
}

function validateAgentOpts(rawOpts: unknown, realmObjectProto: unknown): WorkflowAgentOpts {
  if (rawOpts === undefined) {
    return {};
  }
  if (!isPlainObject(rawOpts, realmObjectProto)) {
    throw new TypeError("agent() opts must be a plain object");
  }

  for (const key of Object.keys(rawOpts)) {
    if (!ALLOWED_AGENT_OPT_KEYS.has(key)) {
      throw new TypeError(`Unknown agent() option: "${key}"`);
    }
  }

  const label = ownProperty(rawOpts, "label");
  if (label !== undefined && typeof label !== "string") {
    throw new TypeError("agent() opts.label must be a string");
  }

  const phase = ownProperty(rawOpts, "phase");
  if (phase !== undefined && typeof phase !== "string") {
    throw new TypeError("agent() opts.phase must be a string");
  }

  const model = ownProperty(rawOpts, "model");
  if (model !== undefined && typeof model !== "string") {
    throw new TypeError("agent() opts.model must be a string");
  }

  const effort = ownProperty(rawOpts, "effort");
  if (effort !== undefined && typeof effort !== "string") {
    throw new TypeError("agent() opts.effort must be a string");
  }

  const isolation = ownProperty(rawOpts, "isolation");
  if (isolation !== undefined && isolation !== "worktree") {
    throw new TypeError('agent() opts.isolation must be "worktree"');
  }

  const agentType = ownProperty(rawOpts, "agentType");
  if (agentType !== undefined && typeof agentType !== "string") {
    throw new TypeError("agent() opts.agentType must be a string");
  }

  let schema: Record<string, unknown> | undefined;
  const rawSchema = ownProperty(rawOpts, "schema");
  if (rawSchema !== undefined) {
    if (
      !isPlainObject(rawSchema, realmObjectProto) ||
      ownProperty(rawSchema, "type") !== "object"
    ) {
      throw new TypeError('agent() opts.schema must be an object schema with type: "object"');
    }
    schema = cloneJsonToHost(rawSchema, "agent() opts.schema") as Record<string, unknown>;
  }

  let target: WorkflowAgentOpts["target"];
  const rawTarget = ownProperty(rawOpts, "target");
  if (rawTarget !== undefined) {
    if (!isPlainObject(rawTarget, realmObjectProto)) {
      throw new TypeError("agent() opts.target must be a plain object");
    }
    for (const key of Object.keys(rawTarget)) {
      if (!ALLOWED_TARGET_KEYS.has(key)) {
        throw new TypeError(`Unknown agent() opts.target key: "${key}"`);
      }
    }
    const providerInstanceId = ownProperty(rawTarget, "providerInstanceId");
    if (providerInstanceId !== undefined && typeof providerInstanceId !== "string") {
      throw new TypeError("agent() opts.target.providerInstanceId must be a string");
    }
    const driverKind = ownProperty(rawTarget, "driverKind");
    if (driverKind !== undefined && typeof driverKind !== "string") {
      throw new TypeError("agent() opts.target.driverKind must be a string");
    }
    const targetModel = ownProperty(rawTarget, "model");
    if (targetModel !== undefined && typeof targetModel !== "string") {
      throw new TypeError("agent() opts.target.model must be a string");
    }
    const rawOptions = ownProperty(rawTarget, "options");
    const clonedOptions =
      rawOptions !== undefined
        ? cloneJsonToHost(rawOptions, "agent() opts.target.options")
        : undefined;

    target = {
      ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
      ...(driverKind !== undefined ? { driverKind } : {}),
      ...(targetModel !== undefined ? { model: targetModel } : {}),
      ...(clonedOptions !== undefined ? { options: clonedOptions } : {}),
    };
  }

  return {
    ...(label !== undefined ? { label } : {}),
    ...(phase !== undefined ? { phase } : {}),
    ...(schema !== undefined ? { schema } : {}),
    ...(model !== undefined ? { model } : {}),
    ...(effort !== undefined ? { effort } : {}),
    ...(isolation !== undefined ? { isolation } : {}),
    ...(agentType !== undefined ? { agentType } : {}),
    ...(target !== undefined ? { target } : {}),
  };
}

interface SharedRunState {
  readonly host: WorkflowHost;
  readonly semaphore: FifoSemaphore;
  readonly abortHub: AbortHub;
  readonly syncTimeoutMs: number;
  agentCount: number;
}

type RealmErrorKind = "Error" | "TypeError" | "WorkflowScriptError";

interface RealmErrorSpec {
  readonly kind: RealmErrorKind;
  readonly message: string;
}

type BridgeSyncOutcome<T> =
  | { readonly ok: true; readonly value: T }
  | {
      readonly ok: false;
      readonly errorKind: RealmErrorKind;
      readonly errorMessage: string;
    };

type SerializedReturnOutcome =
  | { readonly ok: true; readonly json: string | undefined }
  | { readonly ok: false; readonly errorMessage: string };

interface ContextDeferred {
  readonly rejectSpecSync: (kind: RealmErrorKind, message: string) => void;
  readonly enqueueResolveJson: (json: string | undefined) => void;
  readonly enqueueRejectSpec: (kind: RealmErrorKind, message: string) => void;
}

interface ContextHostBridge {
  readonly startAgent: (prompt: unknown, rawOpts: unknown, deferred: ContextDeferred) => void;
  readonly startWorkflow: (ref: unknown, rawArgs: unknown, deferred: ContextDeferred) => void;
  readonly onPhase: (title: string) => BridgeSyncOutcome<void>;
  readonly onLog: (message: string) => BridgeSyncOutcome<void>;
  readonly budgetSpent: () => BridgeSyncOutcome<number>;
  readonly budgetRemaining: () => BridgeSyncOutcome<number>;
}

interface ContextSetupConfig {
  readonly argsJson: string | undefined;
  readonly budgetTotal: number | null;
  readonly maxItems: number;
}

interface ContextRuntime {
  readonly objectPrototype: unknown;
  readonly hasQueuedSteps: () => boolean;
  readonly isContextTypeError: (err: unknown) => boolean;
  readonly enqueueAbort: (message: string) => void;
  readonly enqueueStartBody: (
    bodyFn: unknown,
    onResolve: (outcome: SerializedReturnOutcome) => void,
    onReject: (err: unknown) => void,
  ) => void;
}

const CONTEXT_SETUP_SOURCE = `
const __t3StepQueue = [];
const __t3ShiftStep = Array.prototype.shift.bind(__t3StepQueue);
const __t3DrainSteps = () => {
  while (__t3StepQueue.length > 0) {
    __t3ShiftStep()();
  }
};
(function(bridge, config) {
  "use strict";
  const ContextError = Error;
  const ContextTypeError = TypeError;
  const ContextPromise = Promise;
  const ContextArray = Array;
  const ContextObject = Object;
  const ContextReflect = Reflect;
  const ContextString = String;
  const jsonParse = JSON.parse;
  const jsonStringify = JSON.stringify;
  const objectPrototype = ContextObject.prototype;
  const isArray = ContextArray.isArray;
  const promiseThen = ContextPromise.prototype.then;
  const promiseCatch = ContextPromise.prototype.catch;
  const promiseAll = ContextPromise.all.bind(ContextPromise);
  const promiseRace = ContextPromise.race.bind(ContextPromise);
  const promiseReject = ContextPromise.reject.bind(ContextPromise);
  const defineProperty = ContextObject.defineProperty;
  const freeze = ContextObject.freeze;
  const pushStep = ContextArray.prototype.push.bind(__t3StepQueue);
  const maxItems = config.maxItems;

  const makeContextError = (kind, message) => {
    if (kind === "TypeError") {
      return new ContextTypeError(message);
    }
    const err = new ContextError(message);
    if (kind === "WorkflowScriptError") {
      err.name = "WorkflowScriptError";
    }
    return err;
  };

  const OriginalDate = globalThis.Date;
  const GuardedDate = new Proxy(OriginalDate, {
    apply() {
      throw makeContextError(
        "WorkflowScriptError",
        "Date() is not available in workflow scripts: it would break resume.",
      );
    },
    construct(target, args, newTarget) {
      if (args.length === 0) {
        throw makeContextError(
          "WorkflowScriptError",
          "new Date() is not available in workflow scripts: it would break resume.",
        );
      }
      return ContextReflect.construct(target, args, newTarget === GuardedDate ? target : newTarget);
    },
  });
  defineProperty(OriginalDate, "now", {
    value: function now() {
      throw makeContextError(
        "WorkflowScriptError",
        "Date.now() is not available in workflow scripts: it would break resume.",
      );
    },
    configurable: false,
    writable: false,
  });
  defineProperty(OriginalDate.prototype, "constructor", {
    value: GuardedDate,
    configurable: false,
    writable: false,
  });
  defineProperty(globalThis, "Date", {
    value: GuardedDate,
    configurable: false,
    writable: false,
  });
  defineProperty(Math, "random", {
    value: function random() {
      throw makeContextError(
        "WorkflowScriptError",
        "Math.random() is not available in workflow scripts: it would break resume.",
      );
    },
    configurable: false,
    writable: false,
  });

  let aborted = false;
  let abortError = null;
  let rejectAbort;
  const abortPromise = new ContextPromise((_, rej) => {
    rejectAbort = rej;
  });
  promiseCatch.call(abortPromise, () => {});

  const makeDeferred = () => {
    let resolve;
    let reject;
    const promise = new ContextPromise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    return {
      promise,
      rejectSpecSync(kind, message) {
        reject(makeContextError(kind, message));
      },
      enqueueResolveJson(json) {
        pushStep(() => {
          resolve(json === undefined ? undefined : jsonParse(json));
        });
      },
      enqueueRejectSpec(kind, message) {
        pushStep(() => {
          reject(makeContextError(kind, message));
        });
      },
    };
  };

  const callSync = (fn, arg) => {
    const outcome = fn(arg);
    if (outcome.ok) return outcome.value;
    throw makeContextError(outcome.errorKind, outcome.errorMessage);
  };

  const raceAbort = (promise) => {
    promiseCatch.call(promise, () => {});
    if (aborted) {
      return promiseReject(abortError);
    }
    return promiseRace([promise, abortPromise]);
  };

  const runParallel = async (thunks) => {
    if (!isArray(thunks)) {
      throw new ContextTypeError("parallel() requires an array of functions");
    }
    if (thunks.length > maxItems) {
      throw makeContextError(
        "WorkflowScriptError",
        "parallel() accepts at most " + maxItems + " items",
      );
    }
    const fns = new ContextArray(thunks.length);
    for (let i = 0; i < thunks.length; i += 1) {
      const thunk = thunks[i];
      if (typeof thunk !== "function") {
        throw new ContextTypeError("parallel() item at index " + i + " must be a function");
      }
      fns[i] = thunk;
    }
    if (aborted) {
      throw abortError;
    }

    const promises = new ContextArray(fns.length);
    for (let i = 0; i < fns.length; i += 1) {
      const thunk = fns[i];
      promises[i] = (async () => {
        try {
          return await thunk();
        } catch {
          if (aborted) {
            throw abortError;
          }
          return null;
        }
      })();
    }

    return await raceAbort(promiseAll(promises));
  };

  const runPipeline = async (items, rawStages) => {
    if (!isArray(items)) {
      throw new ContextTypeError("pipeline() requires an array of items");
    }
    if (items.length > maxItems) {
      throw makeContextError(
        "WorkflowScriptError",
        "pipeline() accepts at most " + maxItems + " items",
      );
    }
    if (rawStages.length === 0) {
      throw new ContextTypeError("pipeline() requires at least one stage function");
    }
    const stages = new ContextArray(rawStages.length);
    for (let i = 0; i < rawStages.length; i += 1) {
      const stage = rawStages[i];
      if (typeof stage !== "function") {
        throw new ContextTypeError("pipeline() stage at index " + i + " must be a function");
      }
      stages[i] = stage;
    }
    if (aborted) {
      throw abortError;
    }

    const itemPromises = new ContextArray(items.length);
    for (let i = 0; i < items.length; i += 1) {
      const item = items[i];
      itemPromises[i] = (async () => {
        let current = item;
        for (let s = 0; s < stages.length; s += 1) {
          if (aborted) {
            throw abortError;
          }
          try {
            current = await stages[s](current, item, i);
          } catch {
            if (aborted) {
              throw abortError;
            }
            return null;
          }
        }
        return current;
      })();
    }

    return await raceAbort(promiseAll(itemPromises));
  };

  // Regular functions (not async) so their constructor is the context's Function.
  function agent(prompt, rawOpts) {
    const deferred = makeDeferred();
    bridge.startAgent(prompt, rawOpts, deferred);
    return deferred.promise;
  }

  function parallel(thunks) {
    return runParallel(thunks);
  }

  function pipeline(items, ...rawStages) {
    return runPipeline(items, rawStages);
  }

  function phase(title) {
    if (typeof title !== "string" || title.trim().length === 0) {
      throw new ContextTypeError("phase() title must be a non-empty string");
    }
    callSync(bridge.onPhase, title);
  }

  function log(message) {
    callSync(bridge.onLog, ContextString(message));
  }

  function workflow(ref, rawArgs) {
    const deferred = makeDeferred();
    bridge.startWorkflow(ref, rawArgs, deferred);
    return deferred.promise;
  }

  globalThis.agent = agent;
  globalThis.parallel = parallel;
  globalThis.pipeline = pipeline;
  globalThis.phase = phase;
  globalThis.log = log;
  globalThis.workflow = workflow;
  globalThis.args = config.argsJson === undefined ? undefined : jsonParse(config.argsJson);
  globalThis.budget = freeze({
    total: config.budgetTotal,
    spent: function spent() {
      return callSync(bridge.budgetSpent);
    },
    remaining: function remaining() {
      return callSync(bridge.budgetRemaining);
    },
  });

  const stringifyReturnValue = (value) => {
    if (value === undefined) {
      return { ok: true, json: undefined };
    }
    try {
      const serialized = jsonStringify(value, (_key, v) => {
        if (typeof v === "function" || typeof v === "symbol" || typeof v === "bigint") {
          throw makeContextError(
            "WorkflowScriptError",
            "Workflow return value is not JSON-serializable",
          );
        }
        return v;
      });
      if (serialized === undefined) {
        return { ok: false, errorMessage: "Workflow return value is not JSON-serializable" };
      }
      return { ok: true, json: serialized };
    } catch (err) {
      const msg =
        typeof err === "object" && err !== null && typeof err.message === "string"
          ? err.message
          : ContextString(err);
      return {
        ok: false,
        errorMessage:
          msg === "Workflow return value is not JSON-serializable"
            ? msg
            : "Workflow return value is not JSON-serializable: " + msg,
      };
    }
  };

  return {
    objectPrototype,
    hasQueuedSteps: () => __t3StepQueue.length > 0,
    isContextTypeError: (err) => err instanceof ContextTypeError,
    enqueueAbort: (message) => {
      pushStep(() => {
        if (aborted) return;
        aborted = true;
        abortError = makeContextError("Error", message);
        rejectAbort(abortError);
      });
    },
    enqueueStartBody: (bodyFn, onResolve, onReject) => {
      pushStep(() => {
        try {
          const runPromise = bodyFn();
          // Context-realm reactions ensure completion runs inside the context's afterEvaluate drain.
          promiseThen.call(
            runPromise,
            (value) => {
              onResolve(stringifyReturnValue(value));
            },
            (err) => {
              onReject(err);
            },
          );
        } catch (err) {
          onReject(err);
        }
      });
    },
  };
})`;

const CONTEXT_SETUP_SCRIPT = new NodeVM.Script(CONTEXT_SETUP_SOURCE);
const DRAIN_STEPS_SCRIPT = new NodeVM.Script("__t3DrainSteps()");

function toRealmErrorSpec(err: unknown, abortHub: AbortHub): RealmErrorSpec {
  if (abortHub.aborted && err === abortHub.reason()) {
    return { kind: "Error", message: extractErrorMessage(err) };
  }
  if (err instanceof WorkflowScriptError) {
    return { kind: "WorkflowScriptError", message: err.message };
  }
  if (err instanceof TypeError) {
    return { kind: "TypeError", message: err.message };
  }
  return { kind: "Error", message: extractErrorMessage(err) };
}

function serializeHostValue(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  const serialized = JSON.stringify(value);
  if (serialized === undefined) {
    throw new WorkflowScriptError("Host return value is not JSON-serializable");
  }
  return serialized;
}

function normalizeScriptFailure(
  err: unknown,
  abortHub: AbortHub,
  isContextTypeError: (value: unknown) => boolean,
): unknown {
  if (abortHub.aborted) {
    return abortHub.reason();
  }
  if (err instanceof WorkflowScriptError) {
    return err;
  }
  if (extractErrorCode(err) === "ERR_VM_DYNAMIC_IMPORT_CALLBACK_MISSING") {
    return new WorkflowScriptError("import() is not available in workflow scripts.");
  }
  if (err instanceof TypeError || isContextTypeError(err)) {
    return new TypeError(extractErrorMessage(err));
  }
  if (err instanceof Error) {
    return err;
  }
  return new WorkflowScriptError(extractErrorMessage(err));
}

async function executeScriptInContext(
  script: string,
  args: unknown,
  shared: SharedRunState,
  depth: number,
): Promise<unknown> {
  if (shared.abortHub.aborted) {
    throw shared.abortHub.reason();
  }

  const parsed = parseMetaAndBody(script);
  const argsJson = args === undefined ? undefined : strictJsonStringify(args, "Workflow args");

  let compiled: NodeVM.Script;
  try {
    compiled = new NodeVM.Script(`(async () => { "use strict"; ${parsed.body}\n})`, {
      filename: `${parsed.meta.name}.workflow.js`,
      lineOffset: parsed.bodyStartLine,
    });
  } catch (err) {
    const message = extractErrorMessage(err);
    if (/Cannot use import statement/i.test(message)) {
      throw new WorkflowScriptError("import statements are not available in workflow scripts.");
    }
    throw new WorkflowScriptError(`Syntax error in workflow script: ${message}`);
  }

  // vm is a determinism guard, not a security boundary; script authors already have shell access.
  const context = NodeVM.createContext(Object.create(null), {
    codeGeneration: { strings: false, wasm: false },
    microtaskMode: "afterEvaluate",
  });

  let currentPhase: string | undefined;
  let runtime!: ContextRuntime;
  let contextClosed = false;
  let inSlice = false;
  let postSliceDrainScheduled = false;
  const runDeferred = Promise.withResolvers<unknown>();

  const finishWithValue = (value: unknown): void => {
    if (contextClosed) return;
    contextClosed = true;
    runDeferred.resolve(value);
  };

  const finishWithError = (err: unknown): void => {
    if (contextClosed) return;
    contextClosed = true;
    runDeferred.reject(err);
  };

  const schedulePostSliceDrain = (): void => {
    if (contextClosed || postSliceDrainScheduled) return;
    postSliceDrainScheduled = true;
    // Dynamic import() rejects on Node's host microtask queue two hops after invocation.
    queueMicrotask(() => {
      queueMicrotask(() => {
        postSliceDrainScheduled = false;
        if (!contextClosed && !inSlice) {
          drainContext(false);
        }
      });
    });
  };

  const isClosed = (): boolean => contextClosed;

  const drainContext = (scheduleFollowUp = true): void => {
    if (contextClosed || inSlice) return;
    inSlice = true;
    try {
      while (true) {
        DRAIN_STEPS_SCRIPT.runInContext(context, { timeout: shared.syncTimeoutMs });
        if (isClosed() || !runtime.hasQueuedSteps()) break;
      }
    } catch (err) {
      if (extractErrorCode(err) === "ERR_SCRIPT_EXECUTION_TIMEOUT") {
        finishWithError(
          new WorkflowScriptError(
            `The workflow script ran for more than ${shared.syncTimeoutMs} ms without waiting; it was stopped.`,
          ),
        );
      } else {
        finishWithError(normalizeScriptFailure(err, shared.abortHub, runtime.isContextTypeError));
      }
      return;
    } finally {
      inSlice = false;
    }
    if (!contextClosed && shared.abortHub.aborted) {
      finishWithError(shared.abortHub.reason());
      return;
    }
    if (scheduleFollowUp && !contextClosed) {
      schedulePostSliceDrain();
    }
  };

  const settleDeferredWithValue = (deferred: ContextDeferred, value: unknown): void => {
    if (contextClosed) return;
    try {
      const json = serializeHostValue(value);
      deferred.enqueueResolveJson(json);
    } catch (err) {
      const spec = toRealmErrorSpec(err, shared.abortHub);
      deferred.enqueueRejectSpec(spec.kind, spec.message);
    }
    drainContext();
  };

  const settleDeferredWithError = (deferred: ContextDeferred, err: unknown): void => {
    if (contextClosed) return;
    const spec = toRealmErrorSpec(err, shared.abortHub);
    deferred.enqueueRejectSpec(spec.kind, spec.message);
    drainContext();
  };

  const runBridgeSync = <T>(fn: () => T): BridgeSyncOutcome<T> => {
    try {
      return { ok: true, value: fn() };
    } catch (err) {
      const spec = toRealmErrorSpec(err, shared.abortHub);
      return { ok: false, errorKind: spec.kind, errorMessage: spec.message };
    }
  };

  const checkBudget = (): void => {
    const { total, spent } = shared.host.budget;
    if (total !== null && spent() >= total) {
      throw new Error("The workflow's token budget is spent.");
    }
  };

  const startAgent = (prompt: unknown, rawOpts: unknown, deferred: ContextDeferred): void => {
    try {
      if (shared.abortHub.aborted) {
        deferred.rejectSpecSync("Error", extractErrorMessage(shared.abortHub.reason()));
        return;
      }
      if (typeof prompt !== "string" || prompt.trim().length === 0) {
        deferred.rejectSpecSync("TypeError", "agent() prompt must be a non-empty string");
        return;
      }
      const opts = validateAgentOpts(rawOpts, runtime.objectPrototype);
      checkBudget();

      if (shared.agentCount >= WORKFLOW_LIMITS.maxAgents) {
        deferred.rejectSpecSync(
          "WorkflowScriptError",
          `A workflow runs at most ${WORKFLOW_LIMITS.maxAgents} agents`,
        );
        return;
      }
      const index = shared.agentCount;
      shared.agentCount += 1;

      const call: WorkflowAgentCall = {
        index,
        prompt,
        opts,
        phase: opts.phase ?? currentPhase,
      };

      const runWithSlot = (): void => {
        if (contextClosed) {
          shared.semaphore.release();
          return;
        }
        let agentPromise: Promise<unknown>;
        try {
          if (shared.abortHub.aborted) {
            throw shared.abortHub.reason();
          }
          checkBudget();
          agentPromise = raceWithAbort(shared.host.runAgent(call), shared.abortHub);
        } catch (err) {
          shared.semaphore.release();
          if (inSlice) {
            const spec = toRealmErrorSpec(err, shared.abortHub);
            deferred.rejectSpecSync(spec.kind, spec.message);
          } else {
            settleDeferredWithError(deferred, err);
          }
          return;
        }

        agentPromise.then(
          (result) => {
            shared.semaphore.release();
            settleDeferredWithValue(deferred, result);
          },
          (err) => {
            shared.semaphore.release();
            settleDeferredWithError(deferred, err);
          },
        );
      };

      if (shared.semaphore.tryAcquire()) {
        runWithSlot();
        return;
      }

      shared.semaphore.acquire().then(
        () => {
          runWithSlot();
        },
        (err) => {
          settleDeferredWithError(deferred, err);
        },
      );
    } catch (err) {
      const spec = toRealmErrorSpec(err, shared.abortHub);
      deferred.rejectSpecSync(spec.kind, spec.message);
    }
  };

  const startWorkflow = (ref: unknown, rawArgs: unknown, deferred: ContextDeferred): void => {
    try {
      if (shared.abortHub.aborted) {
        deferred.rejectSpecSync("Error", extractErrorMessage(shared.abortHub.reason()));
        return;
      }
      if (depth >= 1) {
        deferred.rejectSpecSync(
          "WorkflowScriptError",
          "Nested workflow() calls are supported one level only.",
        );
        return;
      }
      if (typeof ref === "string") {
        deferred.rejectSpecSync(
          "WorkflowScriptError",
          "Saved workflows are not supported; pass {scriptPath}.",
        );
        return;
      }
      if (!isPlainObject(ref, runtime.objectPrototype)) {
        deferred.rejectSpecSync(
          "WorkflowScriptError",
          "workflow() requires { scriptPath: string }",
        );
        return;
      }
      const scriptPath = ownProperty(ref, "scriptPath");
      if (typeof scriptPath !== "string" || scriptPath.trim().length === 0) {
        deferred.rejectSpecSync(
          "WorkflowScriptError",
          "workflow() requires { scriptPath: string }",
        );
        return;
      }
      const childArgs =
        rawArgs === undefined ? undefined : cloneJsonToHost(rawArgs, "workflow() args");

      void (async () => {
        try {
          const childScript = await raceWithAbort(
            shared.host.loadScript({ scriptPath }),
            shared.abortHub,
          );
          const childResult = await executeScriptInContext(
            childScript,
            childArgs,
            shared,
            depth + 1,
          );
          settleDeferredWithValue(deferred, childResult);
        } catch (err) {
          settleDeferredWithError(deferred, err);
        }
      })();
    } catch (err) {
      const spec = toRealmErrorSpec(err, shared.abortHub);
      deferred.rejectSpecSync(spec.kind, spec.message);
    }
  };

  const budgetTotal = shared.host.budget.total;
  const bridge: ContextHostBridge = {
    startAgent,
    startWorkflow,
    onPhase: (title) =>
      runBridgeSync(() => {
        currentPhase = title;
        shared.host.onPhase(title);
      }),
    onLog: (message) =>
      runBridgeSync(() => {
        shared.host.onLog(
          message.length > MAX_LOG_CHARS ? message.slice(0, MAX_LOG_CHARS) : message,
        );
      }),
    budgetSpent: () => runBridgeSync(() => shared.host.budget.spent()),
    budgetRemaining: () =>
      runBridgeSync(() =>
        budgetTotal === null
          ? Number.POSITIVE_INFINITY
          : Math.max(0, budgetTotal - shared.host.budget.spent()),
      ),
  };

  const setupContext = CONTEXT_SETUP_SCRIPT.runInContext(context, {
    timeout: shared.syncTimeoutMs,
  }) as (bridge: ContextHostBridge, config: ContextSetupConfig) => ContextRuntime;

  runtime = setupContext(bridge, {
    argsJson,
    budgetTotal,
    maxItems: WORKFLOW_LIMITS.maxItems,
  });

  const unsubAbort = shared.abortHub.onAbort((reason) => {
    if (contextClosed) return;
    runtime.enqueueAbort(extractErrorMessage(reason));
    if (!inSlice) {
      drainContext(false);
      if (!contextClosed) {
        finishWithError(reason);
      }
    }
  });

  try {
    const bodyFn: unknown = compiled.runInContext(context, { timeout: shared.syncTimeoutMs });
    runtime.enqueueStartBody(
      bodyFn,
      (outcome) => {
        if (shared.abortHub.aborted) {
          finishWithError(shared.abortHub.reason());
          return;
        }
        if (!outcome.ok) {
          finishWithError(new WorkflowScriptError(outcome.errorMessage));
          return;
        }
        if (outcome.json === undefined) {
          finishWithValue(null);
          return;
        }
        finishWithValue(JSON.parse(outcome.json));
      },
      (err) => {
        finishWithError(normalizeScriptFailure(err, shared.abortHub, runtime.isContextTypeError));
      },
    );
    drainContext();
    return await runDeferred.promise;
  } finally {
    unsubAbort();
  }
}

export async function runWorkflowScript(input: {
  readonly script: string;
  readonly args: unknown;
  readonly host: WorkflowHost;
  readonly concurrency: number;
  readonly syncTimeoutMs?: number;
}): Promise<unknown> {
  const limit = Number.isFinite(input.concurrency) ? Math.max(1, Math.trunc(input.concurrency)) : 1;
  const syncTimeoutMs =
    input.syncTimeoutMs !== undefined && Number.isFinite(input.syncTimeoutMs)
      ? Math.max(1, Math.trunc(input.syncTimeoutMs))
      : DEFAULT_SYNC_TIMEOUT_MS;
  const abortHub = createAbortHub(input.host.signal);
  try {
    const shared: SharedRunState = {
      host: input.host,
      semaphore: new FifoSemaphore(limit, abortHub),
      abortHub,
      syncTimeoutMs,
      agentCount: 0,
    };
    return await executeScriptInContext(input.script, input.args, shared, 0);
  } finally {
    abortHub.dispose();
  }
}
