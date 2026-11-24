import * as NodeModule from "node:module";

// Linux enforces RLIMIT_DATA (brk + private mmap); macOS and Windows enforce only V8 heap flags and deadline kill.
export const WORKFLOW_SANDBOX_PROCESS_LIMITS = {
  maxOldSpaceSizeMb: 256,
  maxSemiSpaceSizeMb: 16,
  rlimitDataKiB: 1024 * 1024,
  nodeFlags: ["--max-old-space-size=256", "--max-semi-space-size=16"],
} as const;

export const WORKFLOW_SANDBOX_WORKER_ARG = "__workflow-sandbox-worker" as const;

export const WORKFLOW_MESSAGE_LIMITS = {
  maxPromptBytes: 1024 * 1024,
  maxValueBytes: 4 * 1024 * 1024,
  maxLogChars: 2000,
} as const;

export const WORKFLOW_PROGRESS_LIMITS = {
  maxLogs: 10_000,
  maxLogBytes: 4 * 1024 * 1024,
  maxPhaseTitles: 256,
  maxPhaseTitleBytes: 1024,
  maxPhaseTitleTotalBytes: 64 * 1024,
  maxPhaseSwitches: 100_000,
  maxPhaseEventBytes: 4 * 1024 * 1024,
} as const;

// JSON.stringify expands ASCII control characters (1 UTF-8 byte) to \uXXXX (6 UTF-8 bytes).
export const JSON_STRING_MAX_EXPANSION = 6;
export const MAX_PHASE_MESSAGE_BYTES =
  WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes * JSON_STRING_MAX_EXPANSION + 256;
export const MAX_VALUE_MESSAGE_BYTES = WORKFLOW_MESSAGE_LIMITS.maxValueBytes + 4_096;
export const MAX_SCRIPT_MESSAGE_BYTES =
  WORKFLOW_MESSAGE_LIMITS.maxValueBytes * JSON_STRING_MAX_EXPANSION + 16_384;
export const MAX_START_MESSAGE_BYTES =
  WORKFLOW_MESSAGE_LIMITS.maxValueBytes * (JSON_STRING_MAX_EXPANSION + 1) + 16_384;
export const MAX_WORKER_STDOUT_BUFFERED_BYTES = 4 * 1024 * 1024;
export const MIN_WORKER_STDOUT_BUFFERED_BYTES =
  Buffer.byteLength(
    JSON.stringify({ type: "error", kind: "WorkflowScriptError", message: "" }),
    "utf8",
  ) + 1;
export const MAX_ERROR_MESSAGE_CHARS = 16_384;

export type WorkflowProgressLimits = {
  readonly [K in keyof typeof WORKFLOW_PROGRESS_LIMITS]: number;
};

export interface WorkflowProgressBudget {
  /** Returns a cap-failure message once a per-run progress budget is exceeded. */
  readonly phase: (title: string, frameBytes?: number) => string | undefined;
  readonly phaseTitle: (title: string) => string | undefined;
  readonly log: (message: string) => string | undefined;
}

export function makeWorkflowProgressBudget(
  limits: WorkflowProgressLimits = WORKFLOW_PROGRESS_LIMITS,
): WorkflowProgressBudget {
  const titles = new Set<string>();
  let phaseTitleBytes = 0;
  let phaseEventBytes = 0;
  let switches = 0;
  let logs = 0;
  let logBytes = 0;

  const phaseTitle = (title: string): string | undefined => {
    if (titles.has(title)) return undefined;
    const bytes = Buffer.byteLength(title, "utf8");
    if (bytes > limits.maxPhaseTitleBytes) {
      return `A workflow phase title is at most ${limits.maxPhaseTitleBytes} bytes.`;
    }
    if (titles.size >= limits.maxPhaseTitles) {
      return `A workflow has at most ${limits.maxPhaseTitles} distinct phases.`;
    }
    if (phaseTitleBytes + bytes > limits.maxPhaseTitleTotalBytes) {
      return "Workflow phase titles exceed the 64 KiB limit.";
    }
    titles.add(title);
    phaseTitleBytes += bytes;
    return undefined;
  };

  return {
    phase: (title, frameBytes) => {
      switches += 1;
      if (switches > limits.maxPhaseSwitches) {
        return `A workflow switches phase at most ${limits.maxPhaseSwitches} times.`;
      }
      const titleOver = phaseTitle(title);
      if (titleOver !== undefined) return titleOver;
      const emittedBytes =
        frameBytes ?? Buffer.byteLength(JSON.stringify({ type: "phase", title }), "utf8") + 1;
      if (phaseEventBytes + emittedBytes > limits.maxPhaseEventBytes) {
        return "Workflow phase() output exceeds the 4 MiB limit.";
      }
      phaseEventBytes += emittedBytes;
      return undefined;
    },
    phaseTitle,
    log: (message) => {
      logs += 1;
      logBytes += Buffer.byteLength(message, "utf8");
      if (logs > limits.maxLogs) return `A workflow logs at most ${limits.maxLogs} lines.`;
      if (logBytes > limits.maxLogBytes) return "Workflow log() output exceeds the 4 MiB limit.";
      return undefined;
    },
  };
}

export type RealmErrorKind = "Error" | "TypeError" | "WorkflowScriptError";

// Self-contained CommonJS source executed by the internal __workflow-sandbox-worker subcommand.
export const WORKFLOW_SANDBOX_WORKER_SOURCE = String.raw`
"use strict";
(() => {
  const vm = require("node:vm");
  const stdin = process.stdin;
  const stdout = process.stdout;
  const utf8Decoder = new TextDecoder("utf-8", { fatal: true });
  stdin.on("error", () => {});
  stdout.on("error", () => {});
  const writeRawMessage = (msg) => {
    stdout.write(msg + "\n");
  };
  const byteLength = Buffer.byteLength;
  const scheduleImmediate = setImmediate;
  const scheduleMicrotask = queueMicrotask;

  // Strip low-level builtin/native loaders from the worker realm before any script runs.
  for (const key of ["getBuiltinModule", "binding", "_linkedBinding", "dlopen"]) {
    try {
      delete process[key];
    } catch {
      // Fall through to defineProperty when non-deletable.
    }
    if (key in process) {
      try {
        Object.defineProperty(process, key, {
          value: undefined,
          writable: false,
          configurable: false,
        });
      } catch {
        // Ignore if already locked down.
      }
    }
  }
  for (const key of ["fetch", "WebSocket"]) {
    if (key in globalThis) {
      try {
        delete globalThis[key];
      } catch {
        // Ignore if non-deletable.
      }
    }
  }

  const JSON_STRING_MAX_EXPANSION = ${JSON_STRING_MAX_EXPANSION};
  const MAX_START_BYTES = ${MAX_START_MESSAGE_BYTES};
  const MAX_SCRIPT_BYTES = ${MAX_SCRIPT_MESSAGE_BYTES};
  const DEFAULT_MAX_STDOUT_BUFFERED_BYTES = ${MAX_WORKER_STDOUT_BUFFERED_BYTES};
  const MIN_STDOUT_BUFFERED_BYTES = ${MIN_WORKER_STDOUT_BUFFERED_BYTES};
  const MAX_ERROR_CHARS = ${MAX_ERROR_MESSAGE_CHARS};
  const MAX_META_DEPTH = 64;
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
  const IDENT_START = /[\p{ID_Start}_$]/uy;
  const IDENT_CONTINUE = /[\p{ID_Continue}$]/uy;
  const NUMBER_RE =
    /(?:0[xX][0-9a-fA-F](?:_?[0-9a-fA-F])*|0[bB][01](?:_?[01])*|0[oO][0-7](?:_?[0-7])*|(?:(?:0|[1-9](?:_?[0-9])*)(?:\.(?:[0-9](?:_?[0-9])*)?)?|\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?)/y;
  const HEX_DIGITS = /^[0-9a-fA-F]+$/;

  class WorkerScriptError extends Error {
    constructor(message) {
      super(message);
      this.name = "WorkflowScriptError";
    }
  }

  function truncateErrorMessage(message) {
    return message.length > MAX_ERROR_CHARS ? message.slice(0, MAX_ERROR_CHARS) : message;
  }

  function extractErrorMessage(err) {
    if (typeof err === "object" && err !== null && "message" in err) {
      const msg = err.message;
      if (typeof msg === "string") return msg;
    }
    return String(err);
  }

  function extractErrorCode(err) {
    if (typeof err === "object" && err !== null && "code" in err) {
      const code = err.code;
      if (typeof code === "string") return code;
    }
    return undefined;
  }

  function extractErrorName(err) {
    if (typeof err === "object" && err !== null && "name" in err) {
      const name = err.name;
      if (typeof name === "string") return name;
    }
    return undefined;
  }

  function ownProperty(obj, key) {
    return Object.prototype.hasOwnProperty.call(obj, key) ? obj[key] : undefined;
  }

  function isLineTerminator(ch) {
    return ch === "\n" || ch === "\r" || ch === "\u2028" || ch === "\u2029";
  }

  function isWhitespace(ch) {
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

  class MetaScanner {
    constructor(source) {
      this.source = source;
      this.pos = source.charCodeAt(0) === 0xfeff ? 1 : 0;
    }

    skipTrivia() {
      let sawNewline = false;
      while (this.pos < this.source.length) {
        const ch = this.source[this.pos];
        if (isWhitespace(ch)) {
          if (isLineTerminator(ch)) sawNewline = true;
          this.pos += 1;
          continue;
        }
        if (ch === "/" && this.source[this.pos + 1] === "/") {
          this.pos += 2;
          while (this.pos < this.source.length && !isLineTerminator(this.source[this.pos])) {
            this.pos += 1;
          }
          continue;
        }
        if (ch === "/" && this.source[this.pos + 1] === "*") {
          const end = this.source.indexOf("*/", this.pos + 2);
          if (end === -1) {
            throw new WorkerScriptError("Unterminated block comment in workflow meta");
          }
          for (let i = this.pos + 2; i < end; i += 1) {
            if (isLineTerminator(this.source[i])) {
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

    readIdentifier() {
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

    consumeKeyword(expected) {
      const saved = this.pos;
      const ident = this.readIdentifier();
      if (ident === expected) return true;
      this.pos = saved;
      return false;
    }

    readEscapeSequence() {
      if (this.pos >= this.source.length) {
        throw new WorkerScriptError("Unterminated string literal in meta");
      }
      const esc = this.source[this.pos];
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
            throw new WorkerScriptError("meta must be a pure literal");
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
          throw new WorkerScriptError("meta must be a pure literal");
        case "x": {
          const hex = this.source.slice(this.pos, this.pos + 2);
          if (hex.length !== 2 || !HEX_DIGITS.test(hex)) {
            throw new WorkerScriptError("Invalid hex escape in meta");
          }
          this.pos += 2;
          return String.fromCharCode(Number.parseInt(hex, 16));
        }
        case "u": {
          if (this.source[this.pos] === "{") {
            const close = this.source.indexOf("}", this.pos + 1);
            if (close === -1) {
              throw new WorkerScriptError("Invalid unicode escape in meta");
            }
            const hex = this.source.slice(this.pos + 1, close);
            if (hex.length === 0 || !HEX_DIGITS.test(hex)) {
              throw new WorkerScriptError("Invalid unicode escape in meta");
            }
            const codePoint = Number.parseInt(hex, 16);
            if (codePoint > 0x10ffff) {
              throw new WorkerScriptError("Invalid unicode escape in meta");
            }
            this.pos = close + 1;
            return String.fromCodePoint(codePoint);
          }
          const hex = this.source.slice(this.pos, this.pos + 4);
          if (hex.length !== 4 || !HEX_DIGITS.test(hex)) {
            throw new WorkerScriptError("Invalid unicode escape in meta");
          }
          this.pos += 4;
          return String.fromCharCode(Number.parseInt(hex, 16));
        }
        default:
          return esc;
      }
    }

    parseQuotedString(quote) {
      this.pos += 1;
      let out = "";
      while (this.pos < this.source.length) {
        const ch = this.source[this.pos];
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
          throw new WorkerScriptError("Unterminated string literal in meta");
        }
        out += ch;
        this.pos += 1;
      }
      throw new WorkerScriptError("Unterminated string literal in meta");
    }

    parsePureTemplateLiteral() {
      this.pos += 1;
      let out = "";
      while (this.pos < this.source.length) {
        const ch = this.source[this.pos];
        if (ch === "\\") {
          this.pos += 1;
          out += this.readEscapeSequence();
          continue;
        }
        if (ch === "$" && this.source[this.pos + 1] === "{") {
          throw new WorkerScriptError("meta must be a pure literal");
        }
        if (ch === "\x60") {
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
      throw new WorkerScriptError("Unterminated template literal in meta");
    }

    parseNumber() {
      NUMBER_RE.lastIndex = this.pos;
      const match = NUMBER_RE.exec(this.source);
      if (match === null) return undefined;
      const end = this.pos + match[0].length;
      if (end < this.source.length) {
        IDENT_START.lastIndex = end;
        if (IDENT_START.test(this.source)) {
          throw new WorkerScriptError("meta must be a pure literal");
        }
      }
      const value = Number(match[0].replaceAll("_", ""));
      if (Number.isNaN(value)) {
        throw new WorkerScriptError("meta must be a pure literal");
      }
      this.pos = end;
      return value;
    }

    parseObjectLiteral(depth) {
      if (depth > MAX_META_DEPTH) {
        throw new WorkerScriptError("meta literal is too deeply nested");
      }
      if (this.source[this.pos] !== "{") {
        throw new WorkerScriptError("meta must be a pure literal");
      }
      this.pos += 1;
      const obj = Object.create(null);

      while (true) {
        this.skipTrivia();
        if (this.pos >= this.source.length) {
          throw new WorkerScriptError("Unterminated object literal in meta");
        }
        if (this.source[this.pos] === "}") {
          this.pos += 1;
          return obj;
        }

        let key;
        const ch = this.source[this.pos];
        if (ch === "'" || ch === '"') {
          key = this.parseQuotedString(ch);
        } else {
          const ident = this.readIdentifier();
          if (ident !== null) {
            key = ident;
          } else {
            const num = this.parseNumber();
            if (num === undefined) {
              throw new WorkerScriptError("meta must be a pure literal");
            }
            key = String(num);
          }
        }

        this.skipTrivia();
        if (this.source[this.pos] !== ":") {
          throw new WorkerScriptError("meta must be a pure literal");
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
        throw new WorkerScriptError("meta must be a pure literal");
      }
    }

    parseArrayLiteral(depth) {
      if (depth > MAX_META_DEPTH) {
        throw new WorkerScriptError("meta literal is too deeply nested");
      }
      this.pos += 1;
      const arr = [];
      while (true) {
        this.skipTrivia();
        if (this.pos >= this.source.length) {
          throw new WorkerScriptError("Unterminated array literal in meta");
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
        throw new WorkerScriptError("meta must be a pure literal");
      }
    }

    parseValue(depth) {
      this.skipTrivia();
      if (this.pos >= this.source.length) {
        throw new WorkerScriptError("meta must be a pure literal");
      }
      const ch = this.source[this.pos];
      if (ch === "{") return this.parseObjectLiteral(depth);
      if (ch === "[") return this.parseArrayLiteral(depth);
      if (ch === "'" || ch === '"') return this.parseQuotedString(ch);
      if (ch === "\x60") return this.parsePureTemplateLiteral();
      if (ch === "-" || ch === "+") {
        this.pos += 1;
        this.skipTrivia();
        const num = this.parseNumber();
        if (num === undefined) {
          throw new WorkerScriptError("meta must be a pure literal");
        }
        return ch === "-" ? -num : num;
      }
      const num = this.parseNumber();
      if (num !== undefined) return num;
      const ident = this.readIdentifier();
      if (ident === "true") return true;
      if (ident === "false") return false;
      if (ident === "null") return null;
      throw new WorkerScriptError("meta must be a pure literal");
    }
  }

  function validateMetaObject(raw, progressLimits) {
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      throw new WorkerScriptError("meta must be a plain object");
    }
    const name = ownProperty(raw, "name");
    if (typeof name !== "string" || name.trim().length === 0) {
      throw new WorkerScriptError("meta.name must be a non-empty string");
    }
    const description = ownProperty(raw, "description");
    if (typeof description !== "string" || description.trim().length === 0) {
      throw new WorkerScriptError("meta.description must be a non-empty string");
    }
    const whenToUse = ownProperty(raw, "whenToUse");
    if (whenToUse !== undefined && typeof whenToUse !== "string") {
      throw new WorkerScriptError("meta.whenToUse must be a string");
    }
    const rawConcurrency = ownProperty(raw, "concurrency");
    if (
      rawConcurrency !== undefined &&
      (typeof rawConcurrency !== "number" ||
        !Number.isInteger(rawConcurrency) ||
        rawConcurrency < 1)
    ) {
      throw new WorkerScriptError("meta.concurrency must be a positive integer");
    }
    const rawPhases = ownProperty(raw, "phases");
    const phases = [];
    if (rawPhases !== undefined) {
      if (!Array.isArray(rawPhases)) {
        throw new WorkerScriptError("meta.phases must be an array");
      }
      if (rawPhases.length > progressLimits.maxPhaseTitles) {
        throw new WorkerScriptError(
          "meta.phases has at most " + progressLimits.maxPhaseTitles + " entries.",
        );
      }
      const seenTitles = new Set();
      let totalPhaseBytes = 0;
      for (let i = 0; i < rawPhases.length; i += 1) {
        const item = rawPhases[i];
        if (typeof item !== "object" || item === null || Array.isArray(item)) {
          throw new WorkerScriptError("meta.phases[" + i + "] must be an object");
        }
        const title = ownProperty(item, "title");
        if (typeof title !== "string" || title.trim().length === 0) {
          throw new WorkerScriptError("meta.phases[" + i + "].title must be a non-empty string");
        }
        const titleBytes = byteLength(title, "utf8");
        if (titleBytes > progressLimits.maxPhaseTitleBytes) {
          throw new WorkerScriptError(
            "meta.phases[" + i + "].title is at most " + progressLimits.maxPhaseTitleBytes + " bytes.",
          );
        }
        if (seenTitles.has(title)) {
          throw new WorkerScriptError('meta.phases contains duplicate title "' + title + '"');
        }
        seenTitles.add(title);
        const detail = ownProperty(item, "detail");
        if (detail !== undefined && typeof detail !== "string") {
          throw new WorkerScriptError("meta.phases[" + i + "].detail must be a string");
        }
        const model = ownProperty(item, "model");
        if (model !== undefined && typeof model !== "string") {
          throw new WorkerScriptError("meta.phases[" + i + "].model must be a string");
        }
        totalPhaseBytes +=
          titleBytes +
          (detail !== undefined ? byteLength(detail, "utf8") : 0) +
          (model !== undefined ? byteLength(model, "utf8") : 0);
        if (totalPhaseBytes > progressLimits.maxPhaseTitleTotalBytes) {
          throw new WorkerScriptError("meta.phases exceeds the 64 KiB limit.");
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
      ...(rawConcurrency !== undefined ? { concurrency: rawConcurrency } : {}),
      phases,
    };
  }

  function parseMetaAndBody(script, progressLimits) {
    if (typeof script !== "string") {
      throw new WorkerScriptError("Workflow script must be a string");
    }
    const scanner = new MetaScanner(script);
    scanner.skipTrivia();
    if (!scanner.consumeKeyword("export")) {
      throw new WorkerScriptError(
        "Workflow script must start with \x60export const meta = { ... }\x60",
      );
    }
    scanner.skipTrivia();
    if (!scanner.consumeKeyword("const")) {
      throw new WorkerScriptError(
        "Workflow script must start with \x60export const meta = { ... }\x60",
      );
    }
    scanner.skipTrivia();
    if (!scanner.consumeKeyword("meta")) {
      throw new WorkerScriptError(
        "Workflow script must start with \x60export const meta = { ... }\x60",
      );
    }
    scanner.skipTrivia();
    if (script[scanner.pos] !== "=") {
      throw new WorkerScriptError(
        "Workflow script must start with \x60export const meta = { ... }\x60",
      );
    }
    scanner.pos += 1;
    scanner.skipTrivia();

    const evaluated = scanner.parseObjectLiteral(0);
    const afterMetaTrivia = scanner.skipTrivia();
    if (scanner.pos < script.length) {
      const nextChar = script[scanner.pos];
      if (nextChar === ";") {
        scanner.pos += 1;
      } else if (
        !afterMetaTrivia.sawNewline ||
        nextChar === "." ||
        nextChar === "(" ||
        nextChar === "?"
      ) {
        throw new WorkerScriptError("meta must be a pure literal");
      }
    }

    const meta = validateMetaObject(evaluated, progressLimits);
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

  function isPlainObject(value, realmObjectProto) {
    if (typeof value !== "object" || value === null) return false;
    const proto = Object.getPrototypeOf(value);
    return proto === null || proto === Object.prototype || proto === realmObjectProto;
  }

  function strictJsonStringify(value, label) {
    try {
      const serialized = JSON.stringify(value, (_key, v) => {
        if (typeof v === "function" || typeof v === "symbol" || typeof v === "bigint") {
          throw new WorkerScriptError(label + " is not JSON-serializable");
        }
        return v;
      });
      if (serialized === undefined) {
        throw new WorkerScriptError(label + " is not JSON-serializable");
      }
      return serialized;
    } catch (err) {
      if (err instanceof WorkerScriptError) throw err;
      throw new WorkerScriptError(label + " is not JSON-serializable: " + extractErrorMessage(err));
    }
  }

  function cloneJsonValue(value, label) {
    return JSON.parse(strictJsonStringify(value, label));
  }

  function validateAgentOpts(rawOpts, realmObjectProto) {
    if (rawOpts === undefined) return {};
    if (!isPlainObject(rawOpts, realmObjectProto)) {
      throw new TypeError("agent() opts must be a plain object");
    }
    for (const key of Object.keys(rawOpts)) {
      if (!ALLOWED_AGENT_OPT_KEYS.has(key)) {
        throw new TypeError('Unknown agent() option: "' + key + '"');
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
    let schema;
    const rawSchema = ownProperty(rawOpts, "schema");
    if (rawSchema !== undefined) {
      if (
        !isPlainObject(rawSchema, realmObjectProto) ||
        ownProperty(rawSchema, "type") !== "object"
      ) {
        throw new TypeError('agent() opts.schema must be an object schema with type: "object"');
      }
      schema = cloneJsonValue(rawSchema, "agent() opts.schema");
    }
    let target;
    const rawTarget = ownProperty(rawOpts, "target");
    if (rawTarget !== undefined) {
      if (!isPlainObject(rawTarget, realmObjectProto)) {
        throw new TypeError("agent() opts.target must be a plain object");
      }
      for (const key of Object.keys(rawTarget)) {
        if (!ALLOWED_TARGET_KEYS.has(key)) {
          throw new TypeError('Unknown agent() opts.target key: "' + key + '"');
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
          ? cloneJsonValue(rawOptions, "agent() opts.target.options")
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

  function makeProgressBudget(limits) {
    const titles = new Set();
    let phaseTitleBytes = 0;
    let phaseEventBytes = 0;
    let switches = 0;
    let logs = 0;
    let logBytes = 0;
    const phaseTitle = (title) => {
      if (titles.has(title)) return undefined;
      const bytes = byteLength(title, "utf8");
      if (bytes > limits.maxPhaseTitleBytes) {
        return "A workflow phase title is at most " + limits.maxPhaseTitleBytes + " bytes.";
      }
      if (titles.size >= limits.maxPhaseTitles) {
        return "A workflow has at most " + limits.maxPhaseTitles + " distinct phases.";
      }
      if (phaseTitleBytes + bytes > limits.maxPhaseTitleTotalBytes) {
        return "Workflow phase titles exceed the 64 KiB limit.";
      }
      titles.add(title);
      phaseTitleBytes += bytes;
      return undefined;
    };
    return {
      phase(title, frameBytes) {
        switches += 1;
        if (switches > limits.maxPhaseSwitches) {
          return "A workflow switches phase at most " + limits.maxPhaseSwitches + " times.";
        }
        const titleOver = phaseTitle(title);
        if (titleOver !== undefined) return titleOver;
        const emitted = frameBytes ?? byteLength(JSON.stringify({ type: "phase", title }), "utf8") + 1;
        if (phaseEventBytes + emitted > limits.maxPhaseEventBytes) {
          return "Workflow phase() output exceeds the 4 MiB limit.";
        }
        phaseEventBytes += emitted;
        return undefined;
      },
      phaseTitle,
      log(message) {
        logs += 1;
        logBytes += byteLength(message, "utf8");
        if (logs > limits.maxLogs) return "A workflow logs at most " + limits.maxLogs + " lines.";
        if (logBytes > limits.maxLogBytes) return "Workflow log() output exceeds the 4 MiB limit.";
        return undefined;
      },
    };
  }

  const CONTEXT_SETUP_SOURCE = [
    'const __t3DrainBridge = (() => {',
    '  let drainFn = null;',
    '  return Object.freeze({',
    '    bind(fn) { if (drainFn === null) drainFn = fn; },',
    '    run() { if (drainFn !== null) drainFn(); },',
    '  });',
    '})();',
    '(function(bridge, config) {',
    '  "use strict";',
    '  const ContextError = Error;',
    '  const ContextTypeError = TypeError;',
    '  const ContextPromise = Promise;',
    '  const ContextArray = Array;',
    '  const ContextObject = Object;',
    '  const ContextReflect = Reflect;',
    '  const ContextString = String;',
    '  const jsonParse = JSON.parse;',
    '  const jsonStringify = JSON.stringify;',
    '  const objectPrototype = ContextObject.prototype;',
    '  const createNullObj = ContextObject.create;',
    '  const isArray = ContextArray.isArray;',
    '  const promiseThen = ContextPromise.prototype.then;',
    '  const promiseCatch = ContextPromise.prototype.catch;',
    '  const promiseAll = ContextPromise.all.bind(ContextPromise);',
    '  const promiseRace = ContextPromise.race.bind(ContextPromise);',
    '  const promiseReject = ContextPromise.reject.bind(ContextPromise);',
    '  const defineProperty = ContextObject.defineProperty;',
    '  const freeze = ContextObject.freeze;',
    '  const maxItems = config.maxItems;',
    '',
    '  let stepHead = null;',
    '  let stepTail = null;',
    '  const pushStep = (fn) => {',
    '    const node = createNullObj(null);',
    '    node.fn = fn;',
    '    node.next = null;',
    '    if (stepTail === null) {',
    '      stepHead = node;',
    '      stepTail = node;',
    '    } else {',
    '      stepTail.next = node;',
    '      stepTail = node;',
    '    }',
    '  };',
    '  __t3DrainBridge.bind(() => {',
    '    while (stepHead !== null) {',
    '      const node = stepHead;',
    '      stepHead = node.next;',
    '      if (stepHead === null) stepTail = null;',
    '      node.fn();',
    '    }',
    '  });',
    '',
    '  const makeContextError = (kind, message) => {',
    '    if (kind === "TypeError") return new ContextTypeError(message);',
    '    const err = new ContextError(message);',
    '    if (kind === "WorkflowScriptError") err.name = "WorkflowScriptError";',
    '    return err;',
    '  };',
    '',
    '  const OriginalDate = globalThis.Date;',
    '  const GuardedDate = new Proxy(OriginalDate, {',
    '    apply() {',
    '      throw makeContextError("WorkflowScriptError", "Date() is not available in workflow scripts: it would break resume.");',
    '    },',
    '    construct(target, args, newTarget) {',
    '      if (args.length === 0) {',
    '        throw makeContextError("WorkflowScriptError", "new Date() is not available in workflow scripts: it would break resume.");',
    '      }',
    '      return ContextReflect.construct(target, args, newTarget === GuardedDate ? target : newTarget);',
    '    },',
    '  });',
    '  defineProperty(OriginalDate, "now", {',
    '    value: function now() {',
    '      throw makeContextError("WorkflowScriptError", "Date.now() is not available in workflow scripts: it would break resume.");',
    '    },',
    '    configurable: false,',
    '    writable: false,',
    '  });',
    '  defineProperty(OriginalDate.prototype, "constructor", {',
    '    value: GuardedDate,',
    '    configurable: false,',
    '    writable: false,',
    '  });',
    '  defineProperty(globalThis, "Date", {',
    '    value: GuardedDate,',
    '    configurable: false,',
    '    writable: false,',
    '  });',
    '  defineProperty(Math, "random", {',
    '    value: function random() {',
    '      throw makeContextError("WorkflowScriptError", "Math.random() is not available in workflow scripts: it would break resume.");',
    '    },',
    '    configurable: false,',
    '    writable: false,',
    '  });',
    '  if (typeof Intl === "object" && Intl !== null && "DateTimeFormat" in Intl) {',
    '    defineProperty(Intl, "DateTimeFormat", {',
    '      value: function DateTimeFormat() {',
    '        throw makeContextError("WorkflowScriptError", "Intl.DateTimeFormat is not available in workflow scripts: it would break resume.");',
    '      },',
    '      configurable: false,',
    '      writable: false,',
    '    });',
    '  }',
    '  for (const name of ["ArrayBuffer", "SharedArrayBuffer", "DataView", "Atomics", "WebAssembly", "Int8Array", "Uint8Array", "Uint8ClampedArray", "Int16Array", "Uint16Array", "Int32Array", "Uint32Array", "Float16Array", "Float32Array", "Float64Array", "BigInt64Array", "BigUint64Array", "WeakRef", "FinalizationRegistry"]) {',
    '    if (!(name in globalThis)) continue;',
    '    const message = name + " is not available in workflow scripts: it allocates memory outside the sandbox limit.";',
    '    defineProperty(globalThis, name, {',
    '      get() {',
    '        throw makeContextError("WorkflowScriptError", message);',
    '      },',
    '      configurable: false,',
    '    });',
    '  }',
    '',
    '  let capError = null;',
    '  let rejectCap;',
    '  const capPromise = new ContextPromise((_, rej) => { rejectCap = rej; });',
    '  promiseCatch.call(capPromise, () => {});',
    '  const tripCap = (message) => {',
    '    if (capError === null) {',
    '      capError = makeContextError("WorkflowScriptError", message);',
    '      rejectCap(capError);',
    '    }',
    '    return capError;',
    '  };',
    '',
    '  const makeDeferred = () => {',
    '    let resolve;',
    '    let reject;',
    '    const promise = new ContextPromise((res, rej) => {',
    '      resolve = res;',
    '      reject = rej;',
    '    });',
    '    return {',
    '      promise,',
    '      rejectSpecSync(kind, message) {',
    '        reject(makeContextError(kind, message));',
    '      },',
    '      enqueueResolveJson(json) {',
    '        pushStep(() => {',
    '          resolve(json === undefined ? undefined : jsonParse(json));',
    '        });',
    '      },',
    '      enqueueRejectSpec(kind, message) {',
    '        pushStep(() => {',
    '          reject(makeContextError(kind, message));',
    '        });',
    '      },',
    '    };',
    '  };',
    '',
    '  const callSync = (fn, arg) => {',
    '    const outcome = fn(arg);',
    '    if (outcome.ok) return outcome.value;',
    '    throw makeContextError(outcome.errorKind, outcome.errorMessage);',
    '  };',
    '',
    '  const raceCap = (promise) => {',
    '    promiseCatch.call(promise, () => {});',
    '    if (capError !== null) return promiseReject(capError);',
    '    return promiseRace([promise, capPromise]);',
    '  };',
    '',
    '  const runParallel = async (thunks) => {',
    '    if (!isArray(thunks)) {',
    '      throw new ContextTypeError("parallel() requires an array of functions");',
    '    }',
    '    if (thunks.length > maxItems) {',
    '      const msg = "parallel() accepts at most " + maxItems + " items";',
    '      const err = tripCap(msg);',
    '      bridge.onCapExceeded(msg);',
    '      throw err;',
    '    }',
    '    const fns = new ContextArray(thunks.length);',
    '    for (let i = 0; i < thunks.length; i += 1) {',
    '      const thunk = thunks[i];',
    '      if (typeof thunk !== "function") {',
    '        throw new ContextTypeError("parallel() item at index " + i + " must be a function");',
    '      }',
    '      fns[i] = thunk;',
    '    }',
    '    if (capError !== null) throw capError;',
    '',
    '    const promises = new ContextArray(fns.length);',
    '    for (let i = 0; i < fns.length; i += 1) {',
    '      const thunk = fns[i];',
    '      promises[i] = (async () => {',
    '        try {',
    '          return await thunk();',
    '        } catch {',
    '          if (capError !== null) throw capError;',
    '          return null;',
    '        }',
    '      })();',
    '    }',
    '    return await raceCap(promiseAll(promises));',
    '  };',
    '',
    '  const runPipeline = async (items, rawStages) => {',
    '    if (!isArray(items)) {',
    '      throw new ContextTypeError("pipeline() requires an array of items");',
    '    }',
    '    if (items.length > maxItems) {',
    '      const msg = "pipeline() accepts at most " + maxItems + " items";',
    '      const err = tripCap(msg);',
    '      bridge.onCapExceeded(msg);',
    '      throw err;',
    '    }',
    '    if (rawStages.length === 0) {',
    '      throw new ContextTypeError("pipeline() requires at least one stage function");',
    '    }',
    '    const stages = new ContextArray(rawStages.length);',
    '    for (let i = 0; i < rawStages.length; i += 1) {',
    '      const stage = rawStages[i];',
    '      if (typeof stage !== "function") {',
    '        throw new ContextTypeError("pipeline() stage at index " + i + " must be a function");',
    '      }',
    '      stages[i] = stage;',
    '    }',
    '    if (capError !== null) throw capError;',
    '',
    '    const itemPromises = new ContextArray(items.length);',
    '    for (let i = 0; i < items.length; i += 1) {',
    '      const item = items[i];',
    '      itemPromises[i] = (async () => {',
    '        let current = item;',
    '        for (let s = 0; s < stages.length; s += 1) {',
    '          if (capError !== null) throw capError;',
    '          try {',
    '            current = await stages[s](current, item, i);',
    '          } catch {',
    '            if (capError !== null) throw capError;',
    '            return null;',
    '          }',
    '        }',
    '        return current;',
    '      })();',
    '    }',
    '    return await raceCap(promiseAll(itemPromises));',
    '  };',
    '',
    '  // Regular functions (not async) so their constructor is the context Function.',
    '  function agent(prompt, rawOpts) {',
    '    const deferred = makeDeferred();',
    '    bridge.startAgent(prompt, rawOpts, deferred);',
    '    return deferred.promise;',
    '  }',
    '  function parallel(thunks) {',
    '    return runParallel(thunks);',
    '  }',
    '  function pipeline(items, ...rawStages) {',
    '    return runPipeline(items, rawStages);',
    '  }',
    '  function phase(title) {',
    '    if (typeof title !== "string" || title.trim().length === 0) {',
    '      throw new ContextTypeError("phase() title must be a non-empty string");',
    '    }',
    '    callSync(bridge.onPhase, title);',
    '  }',
    '  function log(message) {',
    '    callSync(bridge.onLog, ContextString(message));',
    '  }',
    '  function workflow(ref, rawArgs) {',
    '    const deferred = makeDeferred();',
    '    bridge.startWorkflow(ref, rawArgs, deferred);',
    '    return deferred.promise;',
    '  }',
    '',
    '  globalThis.agent = agent;',
    '  globalThis.parallel = parallel;',
    '  globalThis.pipeline = pipeline;',
    '  globalThis.phase = phase;',
    '  globalThis.log = log;',
    '  globalThis.workflow = workflow;',
    '  globalThis.args = config.argsJson === undefined ? undefined : jsonParse(config.argsJson);',
    '  globalThis.budget = freeze({',
    '    total: config.budgetTotal,',
    '    spent: function spent() {',
    '      return callSync(bridge.budgetSpent);',
    '    },',
    '    remaining: function remaining() {',
    '      return callSync(bridge.budgetRemaining);',
    '    },',
    '  });',
    '',
    '  const stringifyReturnValue = (value) => {',
    '    if (value === undefined) {',
    '      return { ok: true, json: "null" };',
    '    }',
    '    try {',
    '      const serialized = jsonStringify(value, (_key, v) => {',
    '        if (typeof v === "function" || typeof v === "symbol" || typeof v === "bigint") {',
    '          throw makeContextError("WorkflowScriptError", "Workflow return value is not JSON-serializable");',
    '        }',
    '        return v;',
    '      });',
    '      if (serialized === undefined) {',
    '        return { ok: false, errorMessage: "Workflow return value is not JSON-serializable" };',
    '      }',
    '      return { ok: true, json: serialized };',
    '    } catch (err) {',
    '      const msg =',
    '        typeof err === "object" && err !== null && typeof err.message === "string"',
    '          ? err.message',
    '          : ContextString(err);',
    '      return {',
    '        ok: false,',
    '        errorMessage:',
    '          msg === "Workflow return value is not JSON-serializable"',
    '            ? msg',
    '            : "Workflow return value is not JSON-serializable: " + msg,',
    '      };',
    '    }',
    '  };',
    '',
    '  return {',
    '    objectPrototype,',
    '    hasQueuedSteps: () => stepHead !== null,',
    '    isContextTypeError: (err) => err instanceof ContextTypeError,',
    '    setCapError: (message) => { tripCap(message); },',
    '    enqueueStartBody: (bodyFn, onResolve, onReject) => {',
    '      pushStep(() => {',
    '        try {',
    '          const runPromise = bodyFn();',
    '          promiseThen.call(',
    '            runPromise,',
    '            (value) => { onResolve(stringifyReturnValue(value)); },',
    '            (err) => { onReject(err); },',
    '          );',
    '        } catch (err) {',
    '          onReject(err);',
    '        }',
    '      });',
    '    },',
    '  };',
    '})',
  ].join("\n");

  const CONTEXT_SETUP_SCRIPT = new vm.Script(CONTEXT_SETUP_SOURCE);
  const DRAIN_STEPS_SCRIPT = new vm.Script("__t3DrainBridge.run()");

  let started = false;
  let runClosed = false;
  let stickyCapFailure = null;
  let latestSeq = 0;
  let idleScheduled = false;

  let config = null;
  let progressBudget = null;
  let agentCount = 0;
  let budgetSpent = 0;
  let nextRequestId = 1;
  const pendingAgents = new Map();
  const pendingLoads = new Map();
  const activeContexts = new Set();

  const maxStdoutBufferedBytes = () =>
    config && typeof config.maxStdoutBufferedBytes === "number"
      ? config.maxStdoutBufferedBytes
      : DEFAULT_MAX_STDOUT_BUFFERED_BYTES;

  const checkStdoutBacklog = (frameBytes) => {
    const queued = typeof stdout.writableLength === "number" ? stdout.writableLength : 0;
    const cap = maxStdoutBufferedBytes();
    if (queued + frameBytes > cap) {
      return "Workflow sandbox output backlog exceeded the limit.";
    }
    return undefined;
  };

  function writeMessage(msg, frameBytes) {
    if (runClosed) return false;
    const bytes = frameBytes ?? byteLength(msg, "utf8") + 1;
    const backlogOver = checkStdoutBacklog(bytes);
    if (backlogOver !== undefined) {
      failCap(backlogOver);
      return false;
    }
    writeRawMessage(msg);
    return true;
  }

  function closeRunState() {
    runClosed = true;
    for (const closeContext of activeContexts) {
      closeContext();
    }
    activeContexts.clear();
    pendingAgents.clear();
    pendingLoads.clear();
  }

  function abortOversizedTerminalFrame() {
    stdin.destroy();
    if (!stdout.destroyed && !stdout.writableEnded) {
      stdout.destroy();
    }
    process.exit(1);
  }

  function writeTerminalFrame(msg, frameBytes) {
    const bytes = frameBytes ?? byteLength(msg, "utf8") + 1;
    const cap = maxStdoutBufferedBytes();
    if (!Number.isFinite(cap) || bytes > cap) {
      abortOversizedTerminalFrame();
      return false;
    }
    const tryWrite = () => {
      if (stdout.destroyed || stdout.writableEnded) return;
      const queued = typeof stdout.writableLength === "number" ? stdout.writableLength : 0;
      if (queued + bytes <= cap) {
        writeRawMessage(msg);
        return;
      }
      if (stdout.writableNeedDrain) {
        stdout.once("drain", tryWrite);
      } else {
        stdout.write("", tryWrite);
      }
    };
    tryWrite();
    return true;
  }

  function buildBoundedErrorFrame(kind, message, cap) {
    const safe = truncateErrorMessage(message);
    const full = JSON.stringify({ type: "error", kind, message: safe });
    const fullBytes = byteLength(full, "utf8") + 1;
    if (fullBytes <= cap) {
      return { msg: full, frameBytes: fullBytes };
    }
    const empty = JSON.stringify({ type: "error", kind, message: "" });
    const emptyBytes = byteLength(empty, "utf8") + 1;
    if (emptyBytes > cap) {
      return null;
    }
    if (emptyBytes === cap) {
      return { msg: empty, frameBytes: emptyBytes };
    }
    const available = cap - emptyBytes;
    let lo = 0;
    let hi = safe.length;
    while (lo < hi) {
      const mid = (lo + hi + 1) >>> 1;
      const encodedLen = byteLength(JSON.stringify(safe.slice(0, mid)), "utf8") - 2;
      if (encodedLen <= available) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    if (lo > 0 && safe.charCodeAt(lo - 1) >= 0xd800 && safe.charCodeAt(lo - 1) <= 0xdbff) {
      lo -= 1;
    }
    const trimmed = JSON.stringify({ type: "error", kind, message: safe.slice(0, lo) });
    return { msg: trimmed, frameBytes: byteLength(trimmed, "utf8") + 1 };
  }

  function postErrorAndClose(kind, message) {
    if (runClosed) return;
    closeRunState();
    const frame = buildBoundedErrorFrame(kind, message, maxStdoutBufferedBytes());
    if (frame === null) {
      abortOversizedTerminalFrame();
      return;
    }
    writeTerminalFrame(frame.msg, frame.frameBytes);
  }

  function failCap(message) {
    if (stickyCapFailure === null) {
      stickyCapFailure = message;
    }
    postErrorAndClose("WorkflowScriptError", stickyCapFailure);
  }

  function postDoneAndClose(valueJson) {
    if (runClosed) return;
    if (stickyCapFailure !== null) {
      postErrorAndClose("WorkflowScriptError", stickyCapFailure);
      return;
    }
    const valueBytes = byteLength(valueJson, "utf8");
    const msg = '{"type":"done","value":' + valueJson + "}";
    const frameBytes = byteLength(msg, "utf8") + 1;
    const cap = maxStdoutBufferedBytes();
    if (valueBytes > config.maxValueBytes) {
      failCap("Workflow return value exceeds the 4 MiB limit.");
      return;
    }
    if (frameBytes > cap) {
      failCap("Workflow return value frame exceeds the stdout backlog limit.");
      return;
    }
    closeRunState();
    writeTerminalFrame(msg, frameBytes);
  }

  function postSmallMessage(payload, capLabel) {
    if (runClosed) return undefined;
    const msg = JSON.stringify(payload);
    const bytes = byteLength(msg, "utf8");
    if (bytes > config.maxPromptBytes) {
      const capMessage = capLabel + " exceeds the 1 MiB limit.";
      failCap(capMessage);
      return capMessage;
    }
    const backlogOver = checkStdoutBacklog(bytes + 1);
    if (backlogOver !== undefined) {
      failCap(backlogOver);
      return backlogOver;
    }
    writeRawMessage(msg);
    return undefined;
  }

  function scheduleIdle() {
    if (runClosed || idleScheduled) return;
    idleScheduled = true;
    scheduleImmediate(() => {
      idleScheduled = false;
      if (runClosed) return;
      writeMessage(JSON.stringify({ type: "idle", seq: latestSeq }));
    });
  }

  function toErrorSpec(err, isContextTypeError) {
    if (extractErrorCode(err) === "ERR_VM_DYNAMIC_IMPORT_CALLBACK_MISSING") {
      return { kind: "WorkflowScriptError", message: "import() is not available in workflow scripts." };
    }
    if (err instanceof WorkerScriptError || extractErrorName(err) === "WorkflowScriptError") {
      return { kind: "WorkflowScriptError", message: extractErrorMessage(err) };
    }
    if (err instanceof TypeError || (isContextTypeError && isContextTypeError(err))) {
      return { kind: "TypeError", message: extractErrorMessage(err) };
    }
    return { kind: "Error", message: extractErrorMessage(err) };
  }

  function checkBudget() {
    if (config.budgetTotal !== null && budgetSpent >= config.budgetTotal) {
      throw new Error("The workflow's token budget is spent.");
    }
  }

  function executeScriptInContext(script, argsJson, depth) {
    return new Promise((resolveRun, rejectRun) => {
      if (runClosed) {
        rejectRun(new WorkerScriptError(stickyCapFailure ?? "Workflow run closed"));
        return;
      }

      let parsed;
      try {
        parsed = parseMetaAndBody(script, config.progressLimits);
      } catch (err) {
        rejectRun(err);
        return;
      }

      let compiled;
      try {
        compiled = new vm.Script('(async () => { "use strict"; ' + parsed.body + "\n})", {
          filename: parsed.meta.name + ".workflow.js",
          lineOffset: parsed.bodyStartLine,
        });
      } catch (err) {
        const message = extractErrorMessage(err);
        if (/Cannot use import statement/i.test(message)) {
          rejectRun(new WorkerScriptError("import statements are not available in workflow scripts."));
        } else {
          rejectRun(new WorkerScriptError("Syntax error in workflow script: " + message));
        }
        return;
      }

      const context = vm.createContext(Object.create(null), {
        codeGeneration: { strings: false, wasm: false },
        microtaskMode: "afterEvaluate",
      });

      let currentPhase;
      let runtime;
      let contextClosed = false;
      let inSlice = false;
      let postSliceDrainScheduled = false;

      const closeContext = () => {
        contextClosed = true;
      };
      activeContexts.add(closeContext);

      const finishWithJson = (json) => {
        if (contextClosed) return;
        contextClosed = true;
        activeContexts.delete(closeContext);
        resolveRun(json);
      };

      const finishWithError = (err) => {
        if (contextClosed) return;
        contextClosed = true;
        activeContexts.delete(closeContext);
        rejectRun(err);
      };

      const schedulePostSliceDrain = () => {
        if (contextClosed || runClosed || postSliceDrainScheduled) return;
        postSliceDrainScheduled = true;
        // Dynamic import() rejects on the worker microtask queue two hops after invocation.
        scheduleMicrotask(() => {
          scheduleMicrotask(() => {
            postSliceDrainScheduled = false;
            if (!contextClosed && !runClosed && !inSlice) {
              drainContext(false);
            }
          });
        });
      };

      const drainContext = (scheduleFollowUp = true) => {
        if (contextClosed || runClosed || inSlice) return;
        inSlice = true;
        try {
          while (true) {
            DRAIN_STEPS_SCRIPT.runInContext(context, { timeout: config.syncTimeoutMs });
            if (contextClosed || runClosed || !runtime.hasQueuedSteps()) break;
          }
        } catch (err) {
          if (extractErrorCode(err) === "ERR_SCRIPT_EXECUTION_TIMEOUT") {
            const timeoutMessage =
              "The workflow script ran for more than " +
              config.syncTimeoutMs +
              " ms without waiting; it was stopped.";
            postErrorAndClose("WorkflowScriptError", timeoutMessage);
            finishWithError(new WorkerScriptError(timeoutMessage));
          } else {
            const spec = toErrorSpec(err, runtime.isContextTypeError);
            finishWithError(spec);
          }
          return;
        } finally {
          inSlice = false;
        }
        if (scheduleFollowUp && !contextClosed && !runClosed) {
          schedulePostSliceDrain();
        }
      };

      const settleDeferredWithJson = (deferred, json) => {
        if (contextClosed || runClosed) return;
        deferred.enqueueResolveJson(json);
        drainContext();
      };

      const settleDeferredWithSpec = (deferred, kind, message) => {
        if (contextClosed || runClosed) return;
        deferred.enqueueRejectSpec(kind, message);
        drainContext();
      };

      const runBridgeSync = (fn) => {
        try {
          return { ok: true, value: fn() };
        } catch (err) {
          const spec = toErrorSpec(err, runtime.isContextTypeError);
          return { ok: false, errorKind: spec.kind, errorMessage: spec.message };
        }
      };

      const startAgent = (prompt, rawOpts, deferred) => {
        try {
          if (runClosed || contextClosed) return;
          if (typeof prompt !== "string" || prompt.trim().length === 0) {
            deferred.rejectSpecSync("TypeError", "agent() prompt must be a non-empty string");
            return;
          }
          const opts = validateAgentOpts(rawOpts, runtime.objectPrototype);
          checkBudget();

          if (agentCount >= config.maxAgents) {
            const capMessage = "A workflow runs at most " + config.maxAgents + " agents";
            runtime.setCapError(capMessage);
            deferred.rejectSpecSync("WorkflowScriptError", capMessage);
            failCap(capMessage);
            return;
          }

          if (opts.phase !== undefined) {
            const overPhase = progressBudget.phaseTitle(opts.phase);
            if (overPhase !== undefined) {
              runtime.setCapError(overPhase);
              deferred.rejectSpecSync("WorkflowScriptError", overPhase);
              failCap(overPhase);
              return;
            }
          }

          const index = agentCount;
          const requestId = nextRequestId;
          const callPhase = opts.phase ?? currentPhase;
          const agentMsg = JSON.stringify({
            type: "agent",
            id: requestId,
            index,
            prompt,
            opts,
            ...(callPhase !== undefined ? { phase: callPhase } : {}),
          });
          if (byteLength(agentMsg, "utf8") > config.maxPromptBytes) {
            const capMessage = "Workflow agent() prompt and options exceed the 1 MiB limit.";
            runtime.setCapError(capMessage);
            deferred.rejectSpecSync("WorkflowScriptError", capMessage);
            failCap(capMessage);
            return;
          }

          agentCount += 1;
          nextRequestId += 1;

          // Send immediately in call-index order; the host enforces concurrency in strict FIFO order.
          pendingAgents.set(requestId, {
            onResolveJson: (json) => settleDeferredWithJson(deferred, json),
            onRejectSpec: (kind, message) => settleDeferredWithSpec(deferred, kind, message),
          });
          if (!writeMessage(agentMsg)) {
            const capMessage = stickyCapFailure ?? "Workflow sandbox output backlog exceeded the limit.";
            runtime.setCapError(capMessage);
            deferred.rejectSpecSync("WorkflowScriptError", capMessage);
          }
        } catch (err) {
          const spec = toErrorSpec(err, runtime.isContextTypeError);
          deferred.rejectSpecSync(spec.kind, spec.message);
        }
      };

      const startWorkflow = (ref, rawArgs, deferred) => {
        try {
          if (runClosed || contextClosed) return;
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
          const childArgsJson =
            rawArgs === undefined ? undefined : strictJsonStringify(rawArgs, "workflow() args");
          if (childArgsJson !== undefined && byteLength(childArgsJson, "utf8") > config.maxValueBytes) {
            const capMessage = "workflow() args exceed the 4 MiB limit.";
            runtime.setCapError(capMessage);
            deferred.rejectSpecSync("WorkflowScriptError", capMessage);
            failCap(capMessage);
            return;
          }

          const requestId = nextRequestId;
          nextRequestId += 1;
          const loadMsg = JSON.stringify({ type: "loadScript", id: requestId, scriptPath });
          if (byteLength(loadMsg, "utf8") > config.maxPromptBytes) {
            const capMessage = "workflow() scriptPath exceeds the 1 MiB limit.";
            runtime.setCapError(capMessage);
            deferred.rejectSpecSync("WorkflowScriptError", capMessage);
            failCap(capMessage);
            return;
          }

          pendingLoads.set(requestId, {
            onLoaded: (childScript) => {
              if (runClosed || contextClosed) return;
              executeScriptInContext(childScript, childArgsJson, depth + 1).then(
                (childResultJson) => {
                  if (runClosed || contextClosed) return;
                  if (byteLength(childResultJson, "utf8") > config.maxValueBytes) {
                    failCap("Nested workflow return value exceeds the 4 MiB limit.");
                    return;
                  }
                  settleDeferredWithJson(deferred, childResultJson);
                },
                (err) => {
                  if (runClosed || contextClosed) return;
                  const spec =
                    err && typeof err === "object" && "kind" in err && "message" in err
                      ? err
                      : toErrorSpec(err, null);
                  settleDeferredWithSpec(deferred, spec.kind, spec.message);
                },
              );
            },
            onRejectSpec: (kind, message) => settleDeferredWithSpec(deferred, kind, message),
          });
          if (!writeMessage(loadMsg)) {
            const capMessage = stickyCapFailure ?? "Workflow sandbox output backlog exceeded the limit.";
            runtime.setCapError(capMessage);
            deferred.rejectSpecSync("WorkflowScriptError", capMessage);
          }
        } catch (err) {
          const spec = toErrorSpec(err, runtime.isContextTypeError);
          deferred.rejectSpecSync(spec.kind, spec.message);
        }
      };

      const bridge = {
        startAgent,
        startWorkflow,
        onCapExceeded: (message) => {
          failCap(message);
        },
        onPhase: (title) =>
          runBridgeSync(() => {
            if (runClosed) {
              throw new WorkerScriptError(stickyCapFailure ?? "Workflow run closed");
            }
            const msg = JSON.stringify({ type: "phase", title });
            const msgBytes = byteLength(msg, "utf8");
            const over = progressBudget.phase(title, msgBytes + 1);
            if (over !== undefined) {
              runtime.setCapError(over);
              failCap(over);
              throw new WorkerScriptError(over);
            }
            const maxPhaseBytes =
              config.progressLimits.maxPhaseTitleBytes * JSON_STRING_MAX_EXPANSION + 256;
            if (msgBytes > maxPhaseBytes) {
              const capMessage =
                "Workflow phase() message exceeds the " + maxPhaseBytes + "-byte limit.";
              runtime.setCapError(capMessage);
              failCap(capMessage);
              throw new WorkerScriptError(capMessage);
            }
            const backlogOver = checkStdoutBacklog(msgBytes + 1);
            if (backlogOver !== undefined) {
              runtime.setCapError(backlogOver);
              failCap(backlogOver);
              throw new WorkerScriptError(backlogOver);
            }
            currentPhase = title;
            writeRawMessage(msg);
          }),
        onLog: (message) =>
          runBridgeSync(() => {
            if (runClosed) {
              throw new WorkerScriptError(stickyCapFailure ?? "Workflow run closed");
            }
            const trimmed =
              message.length > config.maxLogChars ? message.slice(0, config.maxLogChars) : message;
            const over = progressBudget.log(trimmed);
            if (over !== undefined) {
              runtime.setCapError(over);
              failCap(over);
              throw new WorkerScriptError(over);
            }
            const sendOver = postSmallMessage({ type: "log", message: trimmed }, "Workflow log() message");
            if (sendOver !== undefined) {
              runtime.setCapError(sendOver);
              throw new WorkerScriptError(sendOver);
            }
          }),
        budgetSpent: () => runBridgeSync(() => budgetSpent),
        budgetRemaining: () =>
          runBridgeSync(() =>
            config.budgetTotal === null
              ? Number.POSITIVE_INFINITY
              : Math.max(0, config.budgetTotal - budgetSpent),
          ),
      };

      try {
        const setupContext = CONTEXT_SETUP_SCRIPT.runInContext(context, {
          timeout: config.syncTimeoutMs,
        });
        runtime = setupContext(bridge, {
          argsJson,
          budgetTotal: config.budgetTotal,
          maxItems: config.maxItems,
        });
        const bodyFn = compiled.runInContext(context, { timeout: config.syncTimeoutMs });
        runtime.enqueueStartBody(
          bodyFn,
          (outcome) => {
            if (stickyCapFailure !== null) {
              finishWithError({ kind: "WorkflowScriptError", message: stickyCapFailure });
              return;
            }
            if (!outcome.ok) {
              finishWithError({ kind: "WorkflowScriptError", message: outcome.errorMessage });
              return;
            }
            finishWithJson(outcome.json);
          },
          (err) => {
            if (stickyCapFailure !== null) {
              finishWithError({ kind: "WorkflowScriptError", message: stickyCapFailure });
              return;
            }
            finishWithError(toErrorSpec(err, runtime.isContextTypeError));
          },
        );
        drainContext();
      } catch (err) {
        if (extractErrorCode(err) === "ERR_SCRIPT_EXECUTION_TIMEOUT") {
          const timeoutMessage =
            "The workflow script ran for more than " +
            config.syncTimeoutMs +
            " ms without waiting; it was stopped.";
          postErrorAndClose("WorkflowScriptError", timeoutMessage);
          finishWithError(new WorkerScriptError(timeoutMessage));
        } else {
          finishWithError(toErrorSpec(err, runtime ? runtime.isContextTypeError : null));
        }
      }
    });
  }

  let stdinChunks = [];
  let stdinBytes = 0;

  function handleHostLine(raw, bytes) {
    if (runClosed) return;
    const maxIncoming = started ? MAX_SCRIPT_BYTES : MAX_START_BYTES;
    if (bytes > maxIncoming) {
      failCap("Workflow host message exceeded the size limit.");
      return;
    }

    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      failCap("Workflow host sent an invalid JSON message.");
      return;
    }
    if (typeof msg !== "object" || msg === null) {
      failCap("Workflow host sent a malformed message.");
      return;
    }
    if (typeof msg.seq === "number" && Number.isInteger(msg.seq) && msg.seq > latestSeq) {
      latestSeq = msg.seq;
    }

    try {
      if (msg.type === "start") {
        if (started) return;
        started = true;
        const configuredCap =
          msg.maxStdoutBufferedBytes === undefined
            ? DEFAULT_MAX_STDOUT_BUFFERED_BYTES
            : typeof msg.maxStdoutBufferedBytes === "number" &&
                Number.isFinite(msg.maxStdoutBufferedBytes)
              ? Math.trunc(msg.maxStdoutBufferedBytes)
              : -1;
        config = {
          concurrency: msg.concurrency,
          syncTimeoutMs: msg.syncTimeoutMs,
          budgetTotal: msg.budgetTotal,
          maxAgents: msg.maxAgents,
          maxItems: msg.maxItems,
          maxPromptBytes: msg.maxPromptBytes,
          maxValueBytes: msg.maxValueBytes,
          maxLogChars: msg.maxLogChars,
          progressLimits: msg.progressLimits,
          maxStdoutBufferedBytes: Math.max(0, configuredCap),
        };
        if (configuredCap < MIN_STDOUT_BUFFERED_BYTES) {
          postErrorAndClose(
            "WorkflowScriptError",
            "Workflow sandbox stdout backlog cap must be at least " +
              MIN_STDOUT_BUFFERED_BYTES +
              " bytes.",
          );
          return;
        }
        budgetSpent = msg.budgetSpent;
        progressBudget = makeProgressBudget(config.progressLimits);

        if (typeof msg.script !== "string") {
          postErrorAndClose("WorkflowScriptError", "Workflow script must be a string");
          return;
        }
        if (byteLength(msg.script, "utf8") > config.maxValueBytes) {
          failCap("Workflow script exceeds the 4 MiB limit.");
          return;
        }
        const argsJson = msg.hasArgs ? JSON.stringify(msg.args) : undefined;
        if (argsJson !== undefined && byteLength(argsJson, "utf8") > config.maxValueBytes) {
          failCap("Workflow args exceed the 4 MiB limit.");
          return;
        }

        executeScriptInContext(msg.script, argsJson, 0).then(
          (resultJson) => {
            postDoneAndClose(resultJson);
          },
          (err) => {
            if (runClosed) return;
            if (stickyCapFailure !== null) {
              postErrorAndClose("WorkflowScriptError", stickyCapFailure);
              return;
            }
            const spec =
              err && typeof err === "object" && "kind" in err && "message" in err
                ? err
                : toErrorSpec(err, null);
            postErrorAndClose(spec.kind, spec.message);
          },
        );
        return;
      }

      if (!started) {
        failCap("Workflow worker received message before start.");
        return;
      }

      if (msg.type === "agentResult") {
        if (typeof msg.spent === "number" && Number.isFinite(msg.spent)) {
          budgetSpent = msg.spent;
        }
        const pending = pendingAgents.get(msg.id);
        if (!pending) return;
        pendingAgents.delete(msg.id);
        if (msg.ok) {
          const valueJson = msg.hasValue ? JSON.stringify(msg.value) : undefined;
          if (valueJson !== undefined && byteLength(valueJson, "utf8") > config.maxValueBytes) {
            failCap("Workflow agent() result exceeds the 4 MiB limit.");
            return;
          }
          pending.onResolveJson(valueJson);
        } else {
          pending.onRejectSpec(msg.errorKind || "Error", String(msg.errorMessage ?? "Agent failed"));
        }
        return;
      }

      if (msg.type === "loadScriptResult") {
        const pending = pendingLoads.get(msg.id);
        if (!pending) return;
        pendingLoads.delete(msg.id);
        if (msg.ok) {
          if (typeof msg.script !== "string") {
            pending.onRejectSpec("WorkflowScriptError", "Workflow script must be a string");
            return;
          }
          if (byteLength(msg.script, "utf8") > config.maxValueBytes) {
            failCap("Nested workflow script exceeds the 4 MiB limit.");
            return;
          }
          pending.onLoaded(msg.script);
        } else {
          pending.onRejectSpec(
            msg.errorKind || "WorkflowScriptError",
            String(msg.errorMessage ?? "Failed to load workflow script"),
          );
        }
      }
    } finally {
      scheduleIdle();
    }
  }

  stdin.on("data", (chunk) => {
    if (runClosed) return;
    let offset = 0;
    while (offset < chunk.length && !runClosed) {
      const maxIncoming = started ? MAX_SCRIPT_BYTES : MAX_START_BYTES;
      const nl = chunk.indexOf(10, offset);
      if (nl === -1) {
        const remaining = chunk.length - offset;
        if (stdinBytes + remaining > maxIncoming) {
          stdinChunks = [];
          stdinBytes = 0;
          failCap("Workflow host message exceeded the size limit.");
          return;
        }
        stdinChunks.push(offset === 0 ? chunk : Buffer.from(chunk.subarray(offset)));
        stdinBytes += remaining;
        return;
      }
      const segmentBytes = nl - offset;
      const lineBytes = stdinBytes + segmentBytes;
      if (lineBytes > maxIncoming) {
        stdinChunks = [];
        stdinBytes = 0;
        failCap("Workflow host message exceeded the size limit.");
        return;
      }
      const lineBuf =
        stdinChunks.length === 0
          ? chunk.subarray(offset, nl)
          : Buffer.concat([...stdinChunks, chunk.subarray(offset, nl)]);
      stdinChunks = [];
      stdinBytes = 0;
      offset = nl + 1;
      let raw;
      try {
        raw = utf8Decoder.decode(lineBuf);
      } catch {
        failCap("Workflow host sent invalid UTF-8.");
        return;
      }
      handleHostLine(raw, lineBytes);
    }
  });

  stdin.on("end", () => {
    stdinChunks = [];
    stdinBytes = 0;
  });

  writeRawMessage('{"type":"ready"}');
})();
`;

export function runWorkflowSandboxWorker(): void {
  const require = NodeModule.createRequire(import.meta.url);
  new Function("require", WORKFLOW_SANDBOX_WORKER_SOURCE)(require);
}
