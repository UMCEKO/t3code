// @effect-diagnostics globalTimers:off -- The sandbox host owns its syncTimeoutMs deadline timer outside Effect.
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The sandbox runs outside Effect and spawns its isolated child process directly.
import * as NodeChildProcess from "node:child_process";

import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  MAX_ERROR_MESSAGE_CHARS,
  MAX_PHASE_MESSAGE_BYTES,
  MAX_START_MESSAGE_BYTES,
  makeWorkflowProgressBudget,
  WORKFLOW_MESSAGE_LIMITS,
  WORKFLOW_PROGRESS_LIMITS,
  WORKFLOW_SANDBOX_BOOTSTRAP,
  WORKFLOW_SANDBOX_PROCESS_LIMITS,
  WORKFLOW_SANDBOX_WORKER_SOURCE,
  type RealmErrorKind,
  type WorkflowProgressLimits,
} from "./sandboxWorker.ts";

export {
  MAX_PHASE_MESSAGE_BYTES,
  WORKFLOW_MESSAGE_LIMITS,
  WORKFLOW_PROGRESS_LIMITS,
  WORKFLOW_SANDBOX_PROCESS_LIMITS,
};

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

// @effect-diagnostics-next-line extendsNativeError:off -- the sandbox is Promise code that runs outside Effect.
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

const DEFAULT_SYNC_TIMEOUT_MS = 5_000;
const MAX_META_DEPTH = 64;

const IDENT_START = /[\p{ID_Start}_$]/uy;
const IDENT_CONTINUE = /[\p{ID_Continue}$]/uy;
const NUMBER_RE =
  /(?:0[xX][0-9a-fA-F](?:_?[0-9a-fA-F])*|0[bB][01](?:_?[01])*|0[oO][0-7](?:_?[0-7])*|(?:(?:0|[1-9](?:_?[0-9])*)(?:\.(?:[0-9](?:_?[0-9])*)?)?|\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?)/y;
const HEX_DIGITS = /^[0-9a-fA-F]+$/;

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

function truncateErrorMessage(message: string): string {
  return message.length > MAX_ERROR_MESSAGE_CHARS
    ? message.slice(0, MAX_ERROR_MESSAGE_CHARS)
    : message;
}

function ownProperty(obj: object, key: string): unknown {
  return Object.prototype.hasOwnProperty.call(obj, key)
    ? (obj as Record<string, unknown>)[key]
    : undefined;
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
    IDENT_START.lastIndex = this.pos;
    const startMatch = IDENT_START.exec(this.source);
    if (startMatch === null) return null;
    const start = this.pos;
    this.pos += startMatch[0].length;
    while (this.pos < this.source.length) {
      IDENT_CONTINUE.lastIndex = this.pos;
      const contMatch = IDENT_CONTINUE.exec(this.source);
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

  readEscapeSequence(): string {
    if (this.pos >= this.source.length) {
      throw new WorkflowScriptError("Unterminated string literal in meta");
    }
    const esc = this.source[this.pos]!;
    this.pos += 1;
    switch (esc) {
      case "\n":
      case "\u2028":
      case "\u2029":
        return "";
      case "\r":
        if (this.source[this.pos] === "\n") this.pos += 1;
        return "";
      case "n":
        return "\n";
      case "r":
        return "\r";
      case "t":
        return "\t";
      case "b":
        return "\b";
      case "f":
        return "\f";
      case "v":
        return "\v";
      case "\\":
        return "\\";
      case "'":
        return "'";
      case '"':
        return '"';
      case "0": {
        const next = this.source[this.pos];
        if (next !== undefined && next >= "0" && next <= "9") {
          throw new WorkflowScriptError("meta must be a pure literal");
        }
        return "\0";
      }
      case "1":
      case "2":
      case "3":
      case "4":
      case "5":
      case "6":
      case "7":
        throw new WorkflowScriptError("meta must be a pure literal");
      case "x": {
        const hex = this.source.slice(this.pos, this.pos + 2);
        if (hex.length !== 2 || !HEX_DIGITS.test(hex)) {
          throw new WorkflowScriptError("Invalid hex escape in meta");
        }
        this.pos += 2;
        return String.fromCharCode(Number.parseInt(hex, 16));
      }
      case "u": {
        if (this.source[this.pos] === "{") {
          const close = this.source.indexOf("}", this.pos + 1);
          if (close === -1) {
            throw new WorkflowScriptError("Invalid unicode escape in meta");
          }
          const hex = this.source.slice(this.pos + 1, close);
          if (hex.length === 0 || !HEX_DIGITS.test(hex)) {
            throw new WorkflowScriptError("Invalid unicode escape in meta");
          }
          const codePoint = Number.parseInt(hex, 16);
          if (codePoint > 0x10ffff) {
            throw new WorkflowScriptError("Invalid unicode escape in meta");
          }
          this.pos = close + 1;
          return String.fromCodePoint(codePoint);
        }
        const hex = this.source.slice(this.pos, this.pos + 4);
        if (hex.length !== 4 || !HEX_DIGITS.test(hex)) {
          throw new WorkflowScriptError("Invalid unicode escape in meta");
        }
        this.pos += 4;
        return String.fromCharCode(Number.parseInt(hex, 16));
      }
      default:
        return esc;
    }
  }

  parseQuotedString(quote: "'" | '"'): string {
    this.pos += 1;
    let out = "";
    while (this.pos < this.source.length) {
      const ch = this.source[this.pos]!;
      if (ch === "\\") {
        this.pos += 1;
        out += this.readEscapeSequence();
        continue;
      }
      if (ch === quote) {
        this.pos += 1;
        return out;
      }
      if (ch === "\n" || ch === "\r") {
        throw new WorkflowScriptError("Unterminated string literal in meta");
      }
      out += ch;
      this.pos += 1;
    }
    throw new WorkflowScriptError("Unterminated string literal in meta");
  }

  parsePureTemplateLiteral(): string {
    this.pos += 1;
    let out = "";
    while (this.pos < this.source.length) {
      const ch = this.source[this.pos]!;
      if (ch === "\\") {
        this.pos += 1;
        out += this.readEscapeSequence();
        continue;
      }
      if (ch === "$" && this.source[this.pos + 1] === "{") {
        throw new WorkflowScriptError("meta must be a pure literal");
      }
      if (ch === "`") {
        this.pos += 1;
        return out;
      }
      if (ch === "\r") {
        if (this.source[this.pos + 1] === "\n") this.pos += 1;
        out += "\n";
        this.pos += 1;
        continue;
      }
      out += ch;
      this.pos += 1;
    }
    throw new WorkflowScriptError("Unterminated template literal in meta");
  }

  parseNumber(): number | undefined {
    NUMBER_RE.lastIndex = this.pos;
    const match = NUMBER_RE.exec(this.source);
    if (match === null) return undefined;
    const end = this.pos + match[0].length;
    if (end < this.source.length) {
      IDENT_START.lastIndex = end;
      if (IDENT_START.test(this.source)) {
        throw new WorkflowScriptError("meta must be a pure literal");
      }
    }
    const value = Number(match[0].replaceAll("_", ""));
    if (Number.isNaN(value)) {
      throw new WorkflowScriptError("meta must be a pure literal");
    }
    this.pos = end;
    return value;
  }

  parseObjectLiteral(depth: number): Record<string, unknown> {
    if (depth > MAX_META_DEPTH) {
      throw new WorkflowScriptError("meta literal is too deeply nested");
    }
    if (this.source[this.pos] !== "{") {
      throw new WorkflowScriptError("meta must be a pure literal");
    }
    this.pos += 1;
    const obj = Object.create(null) as Record<string, unknown>;

    while (true) {
      this.skipTrivia();
      if (this.pos >= this.source.length) {
        throw new WorkflowScriptError("Unterminated object literal in meta");
      }
      if (this.source[this.pos] === "}") {
        this.pos += 1;
        return obj;
      }

      let key: string;
      const ch = this.source[this.pos]!;
      if (ch === "'" || ch === '"') {
        key = this.parseQuotedString(ch);
      } else {
        const ident = this.readIdentifier();
        if (ident !== null) {
          key = ident;
        } else {
          const num = this.parseNumber();
          if (num === undefined) {
            throw new WorkflowScriptError("meta must be a pure literal");
          }
          key = String(num);
        }
      }

      this.skipTrivia();
      if (this.source[this.pos] !== ":") {
        throw new WorkflowScriptError("meta must be a pure literal");
      }
      this.pos += 1;

      const val = this.parseValue(depth + 1);
      Object.defineProperty(obj, key, {
        value: val,
        writable: true,
        enumerable: true,
        configurable: true,
      });
      this.skipTrivia();

      const sep = this.source[this.pos];
      if (sep === ",") {
        this.pos += 1;
        continue;
      }
      if (sep === "}") {
        this.pos += 1;
        return obj;
      }
      throw new WorkflowScriptError("meta must be a pure literal");
    }
  }

  parseArrayLiteral(depth: number): Array<unknown> {
    if (depth > MAX_META_DEPTH) {
      throw new WorkflowScriptError("meta literal is too deeply nested");
    }
    this.pos += 1;
    const arr: Array<unknown> = [];
    while (true) {
      this.skipTrivia();
      if (this.pos >= this.source.length) {
        throw new WorkflowScriptError("Unterminated array literal in meta");
      }
      if (this.source[this.pos] === "]") {
        this.pos += 1;
        return arr;
      }
      arr.push(this.parseValue(depth + 1));
      this.skipTrivia();
      const sep = this.source[this.pos];
      if (sep === ",") {
        this.pos += 1;
        continue;
      }
      if (sep === "]") {
        this.pos += 1;
        return arr;
      }
      throw new WorkflowScriptError("meta must be a pure literal");
    }
  }

  parseValue(depth: number): unknown {
    this.skipTrivia();
    if (this.pos >= this.source.length) {
      throw new WorkflowScriptError("meta must be a pure literal");
    }
    const ch = this.source[this.pos]!;
    if (ch === "{") return this.parseObjectLiteral(depth);
    if (ch === "[") return this.parseArrayLiteral(depth);
    if (ch === "'" || ch === '"') return this.parseQuotedString(ch);
    if (ch === "`") return this.parsePureTemplateLiteral();
    if (ch === "-" || ch === "+") {
      this.pos += 1;
      this.skipTrivia();
      const num = this.parseNumber();
      if (num === undefined) {
        throw new WorkflowScriptError("meta must be a pure literal");
      }
      return ch === "-" ? -num : num;
    }
    const num = this.parseNumber();
    if (num !== undefined) return num;
    const ident = this.readIdentifier();
    if (ident === "true") return true;
    if (ident === "false") return false;
    if (ident === "null") return null;
    throw new WorkflowScriptError("meta must be a pure literal");
  }
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
    if (rawPhases.length > WORKFLOW_PROGRESS_LIMITS.maxPhaseTitles) {
      throw new WorkflowScriptError(
        `meta.phases has at most ${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitles} entries.`,
      );
    }
    const seenTitles = new Set<string>();
    let totalPhaseBytes = 0;
    for (let i = 0; i < rawPhases.length; i += 1) {
      const item: unknown = rawPhases[i];
      if (typeof item !== "object" || item === null || Array.isArray(item)) {
        throw new WorkflowScriptError(`meta.phases[${i}] must be an object`);
      }
      const title = ownProperty(item, "title");
      if (typeof title !== "string" || title.trim().length === 0) {
        throw new WorkflowScriptError(`meta.phases[${i}].title must be a non-empty string`);
      }
      const titleBytes = Buffer.byteLength(title, "utf8");
      if (titleBytes > WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes) {
        throw new WorkflowScriptError(
          `meta.phases[${i}].title is at most ${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes} bytes.`,
        );
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

      totalPhaseBytes +=
        titleBytes +
        (detail !== undefined ? Buffer.byteLength(detail, "utf8") : 0) +
        (model !== undefined ? Buffer.byteLength(model, "utf8") : 0);
      if (totalPhaseBytes > WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleTotalBytes) {
        throw new WorkflowScriptError("meta.phases exceeds the 64 KiB limit.");
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

export function parseWorkflowMeta(script: string): WorkflowMeta {
  if (typeof script !== "string") {
    throw new WorkflowScriptError("Workflow script must be a string");
  }
  if (Buffer.byteLength(script, "utf8") > WORKFLOW_MESSAGE_LIMITS.maxValueBytes) {
    throw new WorkflowScriptError("Workflow script exceeds the 4 MiB limit.");
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

  const evaluated = scanner.parseObjectLiteral(0);
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

  return validateMetaObject(evaluated);
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

function serializeHostValue(value: unknown): string {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) {
    throw new WorkflowScriptError("Host return value is not JSON-serializable");
  }
  return serialized;
}

function getAbortReason(signal: AbortSignal): unknown {
  return signal.reason !== undefined ? signal.reason : new Error("Workflow aborted");
}

function toRealmErrorSpec(err: unknown): {
  readonly kind: RealmErrorKind;
  readonly message: string;
} {
  if (err instanceof WorkflowScriptError) {
    return { kind: "WorkflowScriptError", message: truncateErrorMessage(err.message) };
  }
  if (err instanceof TypeError) {
    return { kind: "TypeError", message: truncateErrorMessage(err.message) };
  }
  return { kind: "Error", message: truncateErrorMessage(extractErrorMessage(err)) };
}

function fromRealmErrorSpec(kind: unknown, message: unknown): Error {
  const text = typeof message === "string" ? message : String(message ?? "Workflow script failed");
  if (kind === "TypeError") {
    return new TypeError(text);
  }
  return new WorkflowScriptError(text);
}

function readBudgetSpent(host: WorkflowHost): number {
  const spent = host.budget.spent();
  return Number.isFinite(spent) ? spent : 0;
}

const WORKFLOW_SANDBOX_WORKER_BOOTSTRAP_LINE = `${JSON.stringify(WORKFLOW_SANDBOX_WORKER_SOURCE)}\n`;
const WORKFLOW_SANDBOX_WORKER_BOOTSTRAP_LINE_BYTES = Buffer.byteLength(
  WORKFLOW_SANDBOX_WORKER_BOOTSTRAP_LINE,
  "utf8",
);
const MAX_STDIN_BUFFERED_BYTES = MAX_START_MESSAGE_BYTES * 2;

export function buildWorkflowSandboxCommand(options?: {
  readonly platform?: NodeJS.Platform;
  readonly execPath?: string;
  readonly bootstrap?: string;
}): { readonly command: string; readonly args: ReadonlyArray<string> } {
  const platform = options?.platform ?? HostProcess.Platform.defaultValue();
  const execPath = options?.execPath ?? process.execPath;
  const bootstrap = options?.bootstrap ?? WORKFLOW_SANDBOX_BOOTSTRAP;
  const nodeArgs = [...WORKFLOW_SANDBOX_PROCESS_LIMITS.nodeFlags, "-e", bootstrap];
  if (platform === "win32") {
    return { command: execPath, args: nodeArgs };
  }
  return {
    command: "/bin/sh",
    args: [
      "-c",
      `ulimit -d ${WORKFLOW_SANDBOX_PROCESS_LIMITS.rlimitDataKiB} && exec "$0" "$@"`,
      execPath,
      ...nodeArgs,
    ],
  };
}

export interface BoundedLineSplitter {
  readonly push: (chunk: Buffer) => void;
  readonly reset: () => void;
}

export function makeBoundedLineSplitter(options: {
  readonly maxLineBytes: number;
  readonly onLine: (line: string, bytes: number) => void;
  readonly onOverflow: () => void;
  readonly onInvalidUtf8?: () => void;
}): BoundedLineSplitter {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let chunks: Array<Buffer> = [];
  let pendingBytes = 0;
  let stopped = false;

  const reset = (): void => {
    chunks = [];
    pendingBytes = 0;
  };

  return {
    reset,
    push: (chunk: Buffer) => {
      if (stopped) return;
      let offset = 0;
      while (offset < chunk.length && !stopped) {
        const nl = chunk.indexOf(0x0a, offset);
        if (nl === -1) {
          const remaining = chunk.length - offset;
          if (pendingBytes + remaining > options.maxLineBytes) {
            reset();
            stopped = true;
            options.onOverflow();
            return;
          }
          chunks.push(offset === 0 ? chunk : Buffer.from(chunk.subarray(offset)));
          pendingBytes += remaining;
          return;
        }
        const segmentBytes = nl - offset;
        const lineBytes = pendingBytes + segmentBytes;
        if (lineBytes > options.maxLineBytes) {
          reset();
          stopped = true;
          options.onOverflow();
          return;
        }
        const lineBuf =
          chunks.length === 0
            ? chunk.subarray(offset, nl)
            : Buffer.concat([...chunks, chunk.subarray(offset, nl)]);
        reset();
        offset = nl + 1;
        let line: string;
        try {
          line = decoder.decode(lineBuf);
        } catch {
          stopped = true;
          if (options.onInvalidUtf8) {
            options.onInvalidUtf8();
          } else {
            options.onOverflow();
          }
          return;
        }
        options.onLine(line, lineBytes);
      }
    },
  };
}

export interface WorkflowHostMessageReceiverOptions {
  readonly script: string;
  readonly argsJson: string | undefined;
  readonly host: WorkflowHost;
  readonly concurrency: number;
  readonly syncTimeoutMs: number;
  readonly initialSpent: number;
  readonly postToChild: (message: string, maxBytes: number, capMessage: string) => boolean;
  readonly onIdleSettled: (allIdle: boolean) => void;
  readonly onSucceed: (value: unknown) => void;
  readonly onFail: (err: unknown) => void;
  readonly isSettled: () => boolean;
  readonly progressLimits?: WorkflowProgressLimits;
}

export function createWorkflowHostMessageReceiver(
  options: WorkflowHostMessageReceiverOptions,
): BoundedLineSplitter {
  const progressLimits = options.progressLimits ?? WORKFLOW_PROGRESS_LIMITS;
  const maxPhaseMessageBytes = progressLimits.maxPhaseTitleBytes + 256;
  const progressBudget = makeWorkflowProgressBudget(progressLimits);
  let started = false;
  let lastSentSeq = 0;
  let lastIdleSeq = 0;
  let hostAgentCount = 0;

  const handleLine = (raw: string, bytes: number): void => {
    if (options.isSettled()) return;
    if (bytes > WORKFLOW_MESSAGE_LIMITS.maxValueBytes) {
      options.onFail(new WorkflowScriptError("Workflow message exceeded the 4 MiB limit."));
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      options.onFail(new WorkflowScriptError("Workflow sandbox sent invalid JSON."));
      return;
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      options.onFail(new WorkflowScriptError("Workflow sandbox sent a malformed message."));
      return;
    }
    const msg = parsed as Record<string, unknown>;
    const msgType = msg.type;

    if (msgType === "ready") {
      if (started) return;
      started = true;
      lastSentSeq += 1;
      const base = JSON.stringify({
        type: "start",
        seq: lastSentSeq,
        script: options.script,
        hasArgs: options.argsJson !== undefined,
        concurrency: options.concurrency,
        syncTimeoutMs: options.syncTimeoutMs,
        budgetTotal: options.host.budget.total,
        budgetSpent: options.initialSpent,
        maxAgents: WORKFLOW_LIMITS.maxAgents,
        maxItems: WORKFLOW_LIMITS.maxItems,
        maxPromptBytes: WORKFLOW_MESSAGE_LIMITS.maxPromptBytes,
        maxValueBytes: WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
        maxLogChars: WORKFLOW_MESSAGE_LIMITS.maxLogChars,
        progressLimits,
      });
      const startMsg =
        options.argsJson === undefined ? base : `${base.slice(0, -1)},"args":${options.argsJson}}`;
      options.postToChild(
        startMsg,
        MAX_START_MESSAGE_BYTES,
        "Workflow start payload exceeds size limit.",
      );
      return;
    }

    if (msgType === "idle") {
      if (bytes > WORKFLOW_MESSAGE_LIMITS.maxPromptBytes) {
        options.onFail(
          new WorkflowScriptError("Workflow sandbox message exceeded the 1 MiB limit."),
        );
        return;
      }
      const seq = msg.seq;
      if (typeof seq !== "number" || !Number.isInteger(seq) || seq < 0 || seq > lastSentSeq) {
        options.onFail(new WorkflowScriptError("Workflow sandbox sent an invalid idle sequence."));
        return;
      }
      if (seq > lastIdleSeq) {
        lastIdleSeq = seq;
        options.onIdleSettled(lastIdleSeq === lastSentSeq);
      }
      return;
    }

    if (msgType === "phase") {
      if (bytes > maxPhaseMessageBytes) {
        options.onFail(
          new WorkflowScriptError(
            `Workflow phase() message exceeds the ${maxPhaseMessageBytes}-byte limit.`,
          ),
        );
        return;
      }
      if (typeof msg.title !== "string" || msg.title.trim().length === 0) {
        options.onFail(new WorkflowScriptError("Workflow sandbox sent an invalid phase title."));
        return;
      }
      const overPhase = progressBudget.phase(msg.title);
      if (overPhase !== undefined) {
        options.onFail(new WorkflowScriptError(overPhase));
        return;
      }
      try {
        options.host.onPhase(msg.title);
      } catch (err) {
        options.onFail(err);
      }
      return;
    }

    if (msgType === "log") {
      if (bytes > WORKFLOW_MESSAGE_LIMITS.maxPromptBytes) {
        options.onFail(new WorkflowScriptError("Workflow log() message exceeds the 1 MiB limit."));
        return;
      }
      if (typeof msg.message !== "string") {
        options.onFail(new WorkflowScriptError("Workflow sandbox sent an invalid log message."));
        return;
      }
      const text =
        msg.message.length > WORKFLOW_MESSAGE_LIMITS.maxLogChars
          ? msg.message.slice(0, WORKFLOW_MESSAGE_LIMITS.maxLogChars)
          : msg.message;
      const overLog = progressBudget.log(text);
      if (overLog !== undefined) {
        options.onFail(new WorkflowScriptError(overLog));
        return;
      }
      try {
        options.host.onLog(text);
      } catch (err) {
        options.onFail(err);
      }
      return;
    }

    if (msgType === "agent") {
      if (bytes > WORKFLOW_MESSAGE_LIMITS.maxPromptBytes) {
        options.onFail(
          new WorkflowScriptError("Workflow agent() prompt and options exceed the 1 MiB limit."),
        );
        return;
      }
      const id = msg.id;
      const index = msg.index;
      const prompt = msg.prompt;
      const opts = msg.opts;
      const phase = msg.phase;
      if (
        typeof id !== "number" ||
        !Number.isInteger(id) ||
        typeof index !== "number" ||
        !Number.isInteger(index) ||
        index < 0 ||
        typeof prompt !== "string" ||
        typeof opts !== "object" ||
        opts === null ||
        Array.isArray(opts) ||
        (phase !== undefined && typeof phase !== "string")
      ) {
        options.onFail(new WorkflowScriptError("Workflow sandbox sent an invalid agent request."));
        return;
      }
      const optsPhase = ownProperty(opts, "phase");
      if (optsPhase !== undefined && typeof optsPhase !== "string") {
        options.onFail(new WorkflowScriptError("Workflow sandbox sent an invalid agent request."));
        return;
      }
      if (phase !== undefined) {
        const overPhase = progressBudget.phaseTitle(phase);
        if (overPhase !== undefined) {
          options.onFail(new WorkflowScriptError(overPhase));
          return;
        }
      }
      if (typeof optsPhase === "string" && optsPhase !== phase) {
        const overOptsPhase = progressBudget.phaseTitle(optsPhase);
        if (overOptsPhase !== undefined) {
          options.onFail(new WorkflowScriptError(overOptsPhase));
          return;
        }
      }

      hostAgentCount += 1;
      if (hostAgentCount > WORKFLOW_LIMITS.maxAgents) {
        options.onFail(
          new WorkflowScriptError(`A workflow runs at most ${WORKFLOW_LIMITS.maxAgents} agents`),
        );
        return;
      }

      const call: WorkflowAgentCall = {
        index,
        prompt,
        opts: opts as WorkflowAgentOpts,
        phase,
      };

      void Promise.resolve()
        .then(() => options.host.runAgent(call))
        .then(
          (value) => {
            if (options.isSettled()) return;
            let spent: number;
            try {
              spent = readBudgetSpent(options.host);
            } catch (err) {
              options.onFail(err);
              return;
            }
            const seq = (lastSentSeq += 1);
            if (value === undefined) {
              options.postToChild(
                JSON.stringify({ type: "agentResult", seq, id, ok: true, spent, hasValue: false }),
                WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
                "Workflow agent() result exceeds the 4 MiB limit.",
              );
              return;
            }
            let valueJson: string;
            try {
              valueJson = serializeHostValue(value);
            } catch (err) {
              const spec = toRealmErrorSpec(err);
              options.postToChild(
                JSON.stringify({
                  type: "agentResult",
                  seq,
                  id,
                  ok: false,
                  spent,
                  errorKind: spec.kind,
                  errorMessage: spec.message,
                }),
                WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
                "Workflow agent() result exceeds the 4 MiB limit.",
              );
              return;
            }
            const resultMsg = `{"type":"agentResult","seq":${seq},"id":${id},"ok":true,"spent":${spent},"hasValue":true,"value":${valueJson}}`;
            options.postToChild(
              resultMsg,
              WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
              "Workflow agent() result exceeds the 4 MiB limit.",
            );
          },
          (err) => {
            if (options.isSettled() || options.host.signal.aborted) return;
            let spent: number;
            try {
              spent = readBudgetSpent(options.host);
            } catch (budgetErr) {
              options.onFail(budgetErr);
              return;
            }
            const seq = (lastSentSeq += 1);
            const spec = toRealmErrorSpec(err);
            options.postToChild(
              JSON.stringify({
                type: "agentResult",
                seq,
                id,
                ok: false,
                spent,
                errorKind: spec.kind,
                errorMessage: spec.message,
              }),
              WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
              "Workflow agent() result exceeds the 4 MiB limit.",
            );
          },
        );
      return;
    }

    if (msgType === "loadScript") {
      if (bytes > WORKFLOW_MESSAGE_LIMITS.maxPromptBytes) {
        options.onFail(new WorkflowScriptError("workflow() scriptPath exceeds the 1 MiB limit."));
        return;
      }
      const id = msg.id;
      const scriptPath = msg.scriptPath;
      if (
        typeof id !== "number" ||
        !Number.isInteger(id) ||
        typeof scriptPath !== "string" ||
        scriptPath.trim().length === 0
      ) {
        options.onFail(
          new WorkflowScriptError("Workflow sandbox sent an invalid loadScript request."),
        );
        return;
      }

      void Promise.resolve()
        .then(() => options.host.loadScript({ scriptPath }))
        .then(
          (childScript) => {
            if (options.isSettled()) return;
            const seq = (lastSentSeq += 1);
            if (typeof childScript !== "string") {
              options.postToChild(
                JSON.stringify({
                  type: "loadScriptResult",
                  seq,
                  id,
                  ok: false,
                  errorKind: "WorkflowScriptError",
                  errorMessage: "Workflow script must be a string",
                }),
                WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
                "Nested workflow script exceeds the 4 MiB limit.",
              );
              return;
            }
            options.postToChild(
              JSON.stringify({
                type: "loadScriptResult",
                seq,
                id,
                ok: true,
                script: childScript,
              }),
              WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
              "Nested workflow script exceeds the 4 MiB limit.",
            );
          },
          (err) => {
            if (options.isSettled() || options.host.signal.aborted) return;
            const seq = (lastSentSeq += 1);
            const spec = toRealmErrorSpec(err);
            options.postToChild(
              JSON.stringify({
                type: "loadScriptResult",
                seq,
                id,
                ok: false,
                errorKind: spec.kind,
                errorMessage: spec.message,
              }),
              WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
              "Nested workflow script exceeds the 4 MiB limit.",
            );
          },
        );
      return;
    }

    if (msgType === "done") {
      options.onSucceed(msg.value);
      return;
    }

    if (msgType === "error") {
      if (bytes > WORKFLOW_MESSAGE_LIMITS.maxPromptBytes) {
        options.onFail(new WorkflowScriptError("Workflow error message exceeds the 1 MiB limit."));
        return;
      }
      options.onFail(fromRealmErrorSpec(msg.kind, msg.message));
      return;
    }

    options.onFail(
      new WorkflowScriptError(`Unknown workflow sandbox message type: ${String(msgType)}`),
    );
  };

  return makeBoundedLineSplitter({
    maxLineBytes: WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
    onLine: handleLine,
    onOverflow: () => {
      options.onFail(new WorkflowScriptError("Workflow message exceeded the 4 MiB limit."));
    },
    onInvalidUtf8: () => {
      options.onFail(new WorkflowScriptError("Workflow sandbox sent invalid UTF-8."));
    },
  });
}

export async function runWorkflowScript(input: {
  readonly script: string;
  readonly args: unknown;
  readonly host: WorkflowHost;
  readonly concurrency: number;
  readonly syncTimeoutMs?: number;
}): Promise<unknown> {
  if (input.host.signal.aborted) {
    throw getAbortReason(input.host.signal);
  }
  if (typeof input.script !== "string") {
    throw new WorkflowScriptError("Workflow script must be a string");
  }
  if (Buffer.byteLength(input.script, "utf8") > WORKFLOW_MESSAGE_LIMITS.maxValueBytes) {
    throw new WorkflowScriptError("Workflow script exceeds the 4 MiB limit.");
  }
  const argsJson =
    input.args === undefined ? undefined : strictJsonStringify(input.args, "Workflow args");
  if (
    argsJson !== undefined &&
    Buffer.byteLength(argsJson, "utf8") > WORKFLOW_MESSAGE_LIMITS.maxValueBytes
  ) {
    throw new WorkflowScriptError("Workflow args exceed the 4 MiB limit.");
  }

  const limit = Number.isFinite(input.concurrency) ? Math.max(1, Math.trunc(input.concurrency)) : 1;
  const syncTimeoutMs =
    input.syncTimeoutMs !== undefined && Number.isFinite(input.syncTimeoutMs)
      ? Math.max(1, Math.trunc(input.syncTimeoutMs))
      : DEFAULT_SYNC_TIMEOUT_MS;
  const initialSpent = readBudgetSpent(input.host);

  const { command, args } = buildWorkflowSandboxCommand();
  const child = NodeChildProcess.spawn(command, args, {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ELECTRON_RUN_AS_NODE: "1" },
    windowsHide: true,
  });
  child.stdin.on("error", () => {});
  child.stdout.on("error", () => {});
  child.stderr.on("error", () => {});
  child.stderr.resume();

  const runDeferred = Promise.withResolvers<unknown>();
  const exitDeferred = Promise.withResolvers<void>();
  let exited = false;
  let settled = false;
  let deadlineTimer: ReturnType<typeof setTimeout> | null = null;
  let terminatePromise: Promise<void> | null = null;

  const markExited = (): void => {
    if (exited) return;
    exited = true;
    exitDeferred.resolve();
  };

  const clearDeadline = (): void => {
    if (deadlineTimer !== null) {
      clearTimeout(deadlineTimer);
      deadlineTimer = null;
    }
  };

  const failRun = (err: unknown): void => {
    if (settled) return;
    settled = true;
    clearDeadline();
    void terminateChild();
    runDeferred.reject(err);
  };

  const succeedRun = (value: unknown): void => {
    if (settled) return;
    settled = true;
    clearDeadline();
    runDeferred.resolve(value);
  };

  const onDeadlineExpired = (): void => {
    deadlineTimer = null;
    if (settled) return;
    failRun(
      new WorkflowScriptError(
        `The workflow script ran for more than ${syncTimeoutMs} ms without waiting; it was stopped.`,
      ),
    );
  };

  const armDeadlineIfIdle = (): void => {
    if (settled || deadlineTimer !== null) return;
    deadlineTimer = setTimeout(onDeadlineExpired, syncTimeoutMs);
  };

  const resetDeadline = (): void => {
    clearDeadline();
    if (settled) return;
    deadlineTimer = setTimeout(onDeadlineExpired, syncTimeoutMs);
  };

  const writeToChildStdin = (line: string, lineBytes: number): boolean => {
    if (settled) return false;
    if (child.stdin.destroyed || !child.stdin.writable) {
      failRun(new WorkflowScriptError("Workflow sandbox stopped reading its input."));
      return false;
    }
    if (child.stdin.writableLength + lineBytes > MAX_STDIN_BUFFERED_BYTES) {
      failRun(new WorkflowScriptError("Workflow sandbox stopped reading its input."));
      return false;
    }
    child.stdin.write(line);
    return true;
  };

  const postToChild = (message: string, maxBytes: number, capMessage: string): boolean => {
    if (settled) return false;
    const msgBytes = Buffer.byteLength(message, "utf8");
    if (msgBytes > maxBytes) {
      failRun(new WorkflowScriptError(capMessage));
      return false;
    }
    if (!writeToChildStdin(`${message}\n`, msgBytes + 1)) {
      return false;
    }
    armDeadlineIfIdle();
    return true;
  };

  const receiver = createWorkflowHostMessageReceiver({
    script: input.script,
    argsJson,
    host: input.host,
    concurrency: limit,
    syncTimeoutMs,
    initialSpent,
    postToChild,
    onIdleSettled: (allIdle) => {
      if (allIdle) {
        clearDeadline();
      } else {
        resetDeadline();
      }
    },
    onSucceed: succeedRun,
    onFail: failRun,
    isSettled: () => settled,
  });

  const terminateChild = (): Promise<void> => {
    terminatePromise ??= (async () => {
      receiver.reset();
      child.stdin.destroy();
      child.stdout.destroy();
      child.stderr.destroy();
      if (
        exited ||
        child.exitCode !== null ||
        child.signalCode !== null ||
        child.pid === undefined
      ) {
        markExited();
        return;
      }
      try {
        child.kill("SIGKILL");
      } catch {
        markExited();
        return;
      }
      await exitDeferred.promise;
    })();
    return terminatePromise;
  };

  const onAbort = (): void => {
    failRun(getAbortReason(input.host.signal));
  };
  input.host.signal.addEventListener("abort", onAbort, { once: true });

  child.once("error", (err: unknown) => {
    markExited();
    if (settled) return;
    const code = extractErrorCode(err);
    const message = extractErrorMessage(err);
    failRun(
      new WorkflowScriptError(
        `Workflow sandbox failed to start${code !== undefined ? ` (${code})` : ""}: ${message}`,
      ),
    );
  });

  child.once("exit", () => {
    markExited();
  });

  child.once("close", (code: number | null, signal: NodeJS.Signals | null) => {
    markExited();
    if (settled) return;
    if (signal !== null) {
      failRun(
        new WorkflowScriptError(
          `Workflow sandbox exceeded its memory limit or crashed (${signal})`,
        ),
      );
      return;
    }
    if (code !== null && code !== 0) {
      failRun(
        new WorkflowScriptError(
          `Workflow sandbox exceeded its memory limit or crashed (code ${code})`,
        ),
      );
      return;
    }
    failRun(new WorkflowScriptError(`Workflow sandbox exited unexpectedly (code ${code ?? 0})`));
  });

  child.stdout.on("data", (chunk: Buffer) => {
    receiver.push(chunk);
  });

  if (child.pid !== undefined) {
    writeToChildStdin(
      WORKFLOW_SANDBOX_WORKER_BOOTSTRAP_LINE,
      WORKFLOW_SANDBOX_WORKER_BOOTSTRAP_LINE_BYTES,
    );
  }

  try {
    return await runDeferred.promise;
  } finally {
    settled = true;
    clearDeadline();
    input.host.signal.removeEventListener("abort", onAbort);
    await terminateChild();
  }
}
