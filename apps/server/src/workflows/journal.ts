// @effect-diagnostics-next-line nodeBuiltinImport:off -- deterministic SHA-256 hashing has no synchronous Effect equivalent.
import * as NodeCrypto from "node:crypto";

export type JournalEntry =
  | {
      readonly type: "started";
      readonly index: number;
      readonly key: string;
      readonly label: string;
      readonly phase: string | null;
      readonly agentId: string | null;
      readonly childThreadId: string | null;
    }
  | {
      readonly type: "result";
      readonly index: number;
      readonly key: string;
      readonly outcome: "ok" | "null" | "usage_limited";
      readonly value: unknown;
      readonly cached?: true;
    }
  | { readonly type: "phase"; readonly title: string }
  | { readonly type: "log"; readonly message: string }
  | {
      readonly type: "end";
      readonly status: "completed" | "failed" | "cancelled";
      readonly result?: unknown;
      readonly error?: string;
    };

export interface ReplayCursor {
  readonly take: (
    index: number,
    key: string,
  ) => { readonly cached: true; readonly value: unknown } | { readonly cached: false };
}

function unwrapToJson(value: unknown): unknown {
  if (typeof value === "object" && value !== null && "toJSON" in value) {
    const candidate = (value as { readonly toJSON?: unknown }).toJSON;
    if (typeof candidate === "function") {
      return (candidate as () => unknown).call(value);
    }
  }
  return value;
}

function canonicalJson(value: unknown, seen: Set<object>): string | undefined {
  const unwrapped = unwrapToJson(value);
  if (unwrapped === null) return "null";
  if (typeof unwrapped === "boolean" || typeof unwrapped === "string") {
    return JSON.stringify(unwrapped);
  }
  if (typeof unwrapped === "number") {
    return Number.isFinite(unwrapped) ? JSON.stringify(unwrapped) : "null";
  }
  if (typeof unwrapped === "bigint") {
    throw new TypeError("Do not know how to serialize a BigInt");
  }
  if (
    typeof unwrapped === "undefined" ||
    typeof unwrapped === "function" ||
    typeof unwrapped === "symbol"
  ) {
    return undefined;
  }

  if (seen.has(unwrapped)) {
    throw new TypeError("Converting circular structure to JSON");
  }
  seen.add(unwrapped);
  try {
    if (Array.isArray(unwrapped)) {
      const parts: Array<string> = [];
      for (const item of unwrapped) {
        parts.push(canonicalJson(item, seen) ?? "null");
      }
      return `[${parts.join(",")}]`;
    }

    const record = unwrapped as Record<string, unknown>;
    const keys = Object.keys(record).sort();
    const parts: Array<string> = [];
    for (const key of keys) {
      const serialized = canonicalJson(record[key], seen);
      if (serialized !== undefined) {
        parts.push(`${JSON.stringify(key)}:${serialized}`);
      }
    }
    return `{${parts.join(",")}}`;
  } finally {
    seen.delete(unwrapped);
  }
}

export function agentCallKey(prompt: string, opts: unknown): string {
  const payload = canonicalJson([prompt, opts], new Set()) ?? "null";
  return NodeCrypto.createHash("sha256").update(payload).digest("hex");
}

export function encodeJournalEntry(entry: JournalEntry): string {
  const line = JSON.stringify(entry);
  if (typeof line !== "string") {
    throw new TypeError("Journal entry is not JSON-serializable");
  }
  return `${line}\n`;
}

function isNonNegativeInt(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function isJournalEntry(value: unknown): value is JournalEntry {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  switch (record.type) {
    case "started":
      return (
        isNonNegativeInt(record.index) &&
        typeof record.key === "string" &&
        typeof record.label === "string" &&
        (record.phase === null || typeof record.phase === "string") &&
        (record.agentId === null || typeof record.agentId === "string") &&
        (record.childThreadId === null || typeof record.childThreadId === "string")
      );
    case "result":
      return (
        isNonNegativeInt(record.index) &&
        typeof record.key === "string" &&
        (record.outcome === "ok" ||
          record.outcome === "null" ||
          record.outcome === "usage_limited") &&
        (record.cached === undefined || record.cached === true)
      );
    case "phase":
      return typeof record.title === "string";
    case "log":
      return typeof record.message === "string";
    case "end":
      return (
        (record.status === "completed" ||
          record.status === "failed" ||
          record.status === "cancelled") &&
        (record.error === undefined || typeof record.error === "string")
      );
    default:
      return false;
  }
}

export function decodeJournal(text: string): ReadonlyArray<JournalEntry> {
  const rawLines = text.split("\n");
  let lastNonEmpty = -1;
  for (let i = rawLines.length - 1; i >= 0; i -= 1) {
    if (rawLines[i]!.trim().length > 0) {
      lastNonEmpty = i;
      break;
    }
  }
  if (lastNonEmpty === -1) {
    return [];
  }

  const entries: Array<JournalEntry> = [];
  for (let i = 0; i <= lastNonEmpty; i += 1) {
    const trimmed = rawLines[i]!.trim();
    if (trimmed.length === 0) continue;

    let parsed: unknown;
    let valid = false;
    try {
      parsed = JSON.parse(trimmed);
      valid = isJournalEntry(parsed);
    } catch {
      valid = false;
    }

    if (valid) {
      entries.push(parsed as JournalEntry);
      continue;
    }

    if (i === lastNonEmpty) {
      // Tolerate a truncated final line from a crash mid-append.
      break;
    }
    throw new Error(`Corrupt journal entry at line ${i + 1}`);
  }
  return entries;
}

interface RecordedResult {
  readonly key: string;
  readonly outcome: "ok" | "null" | "usage_limited";
  readonly value: unknown;
}

export function replayCursor(previous: ReadonlyArray<JournalEntry>): ReplayCursor {
  const results = new Map<number, RecordedResult>();
  for (const entry of previous) {
    if (entry.type === "result") {
      results.set(entry.index, {
        key: entry.key,
        outcome: entry.outcome,
        value: entry.value,
      });
    }
  }

  let nextIndex = 0;
  let broken = false;

  return {
    take(index: number, key: string) {
      if (index !== nextIndex) {
        throw new Error(`Replay cursor expected index ${nextIndex}, got ${index}`);
      }
      nextIndex += 1;

      if (broken) {
        return { cached: false };
      }

      const previousResult = results.get(index);
      if (previousResult === undefined || previousResult.key !== key) {
        broken = true;
        return { cached: false };
      }

      if (previousResult.outcome === "usage_limited") {
        return { cached: false };
      }

      return {
        cached: true,
        value: previousResult.outcome === "null" ? null : previousResult.value,
      };
    },
  };
}
