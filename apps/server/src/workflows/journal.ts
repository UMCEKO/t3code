// @effect-diagnostics-next-line nodeBuiltinImport:off -- deterministic SHA-256 hashing has no synchronous Effect equivalent.
import * as NodeCrypto from "node:crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export const MAX_JOURNAL_LINE_BYTES = 8 * 1024 * 1024;
export const MAX_PENDING_JOURNAL_ENTRIES = 1_024;
export const MAX_PENDING_JOURNAL_BYTES = 16 * 1024 * 1024;
export const DEFAULT_REPLAY_CACHE_MAX_ENTRIES = 8;
export const DEFAULT_REPLAY_CACHE_MAX_BYTES = 8 * 1024 * 1024;

export class JournalEncodeError extends Schema.TaggedError<JournalEncodeError>()(
  "JournalEncodeError",
  {
    cause: Schema.Defect(),
  },
) {}

export class JournalRecordTooLargeError extends Schema.TaggedError<JournalRecordTooLargeError>()(
  "JournalRecordTooLargeError",
  {
    entryType: Schema.String,
    bytes: Schema.Number,
    maxBytes: Schema.Number,
  },
) {
  override get message(): string {
    return `Workflow journal "${this.entryType}" record (${this.bytes} bytes) exceeds the ${this.maxBytes}-byte limit.`;
  }
}

export const isJournalRecordTooLargeError = Schema.is(JournalRecordTooLargeError);

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
      readonly outcome: "ok" | "null" | "usage_limited" | "cancelled";
      readonly value: unknown;
      readonly cached?: true;
      /** Why the agent failed, when it did. */
      readonly error?: string;
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

export interface JournalLineSpan {
  readonly offset: number;
  readonly length: number;
}

export interface ReplayIndexEntry {
  readonly key: string;
  readonly outcome: "ok" | "null" | "usage_limited" | "cancelled";
  readonly offset: number;
  readonly length: number;
}

export interface ReplayIndex {
  readonly entries: ReadonlyMap<number, ReplayIndexEntry>;
}

export interface ReplayIndexAccumulator {
  readonly push: (entry: JournalEntry, span: JournalLineSpan) => void;
  readonly build: () => ReplayIndex;
}

export interface ReplayCacheStats {
  readonly indexedCount: number;
  readonly cachedCount: number;
  readonly cachedBytes: number;
}

export interface IndexedReplayCursor<E = never> {
  readonly take: (
    index: number,
    key: string,
  ) => Effect.Effect<
    { readonly cached: true; readonly value: unknown } | { readonly cached: false },
    E
  >;
  readonly stats: () => ReplayCacheStats;
  readonly clear: () => void;
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

function encodeJournalEntryWithLimit(entry: JournalEntry, maxLineBytes: number): string {
  const line = JSON.stringify(entry);
  if (typeof line !== "string") {
    throw new TypeError("Journal entry is not JSON-serializable");
  }
  const bytes = Buffer.byteLength(line, "utf8");
  if (bytes > maxLineBytes) {
    throw new JournalRecordTooLargeError({
      entryType: entry.type,
      bytes,
      maxBytes: maxLineBytes,
    });
  }
  return `${line}\n`;
}

export function encodeJournalEntry(entry: JournalEntry): string {
  return encodeJournalEntryWithLimit(entry, MAX_JOURNAL_LINE_BYTES);
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
          record.outcome === "usage_limited" ||
          record.outcome === "cancelled") &&
        (record.cached === undefined || record.cached === true) &&
        (record.error === undefined || typeof record.error === "string")
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

export function parseJournalLine(trimmedLine: string): JournalEntry | undefined {
  try {
    const parsed: unknown = JSON.parse(trimmedLine);
    return isJournalEntry(parsed) ? parsed : undefined;
  } catch {
    return undefined;
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
    const rawLine = rawLines[i]!;
    const trimmed = rawLine.trim();
    if (trimmed.length === 0) continue;

    const bytes = Buffer.byteLength(rawLine, "utf8");
    if (bytes > MAX_JOURNAL_LINE_BYTES) {
      throw new JournalRecordTooLargeError({
        entryType: "unknown",
        bytes,
        maxBytes: MAX_JOURNAL_LINE_BYTES,
      });
    }

    const parsed = parseJournalLine(trimmed);
    if (parsed !== undefined) {
      entries.push(parsed);
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

export function createReplayIndexAccumulator(): ReplayIndexAccumulator {
  const entries = new Map<number, ReplayIndexEntry>();
  return {
    push(entry: JournalEntry, span: JournalLineSpan) {
      if (entry.type === "result") {
        entries.set(entry.index, {
          key: entry.key,
          outcome: entry.outcome,
          offset: span.offset,
          length: span.length,
        });
      }
    },
    build() {
      return { entries };
    },
  };
}

export function createIndexedReplayCursor<E = never>(input: {
  readonly index: ReplayIndex;
  readonly loadValue: (entry: ReplayIndexEntry, expectedIndex: number) => Effect.Effect<unknown, E>;
  readonly maxCacheEntries?: number;
  readonly maxCacheBytes?: number;
  readonly onBreak?: () => void;
}): IndexedReplayCursor<E> {
  const maxCacheEntries = Math.max(
    0,
    Math.trunc(input.maxCacheEntries ?? DEFAULT_REPLAY_CACHE_MAX_ENTRIES),
  );
  const maxCacheBytes = Math.max(
    0,
    Math.trunc(input.maxCacheBytes ?? DEFAULT_REPLAY_CACHE_MAX_BYTES),
  );
  const cache = new Map<number, { readonly value: unknown; readonly bytes: number }>();
  let cachedBytes = 0;
  let nextIndex = 0;
  let broken = false;

  const clear = (): void => {
    cache.clear();
    cachedBytes = 0;
  };

  const remember = (index: number, value: unknown, bytes: number): void => {
    if (maxCacheEntries === 0 || bytes > maxCacheBytes) return;
    const existing = cache.get(index);
    if (existing !== undefined) {
      cachedBytes = Math.max(0, cachedBytes - existing.bytes);
      cache.delete(index);
    }
    while (
      cache.size > 0 &&
      (cache.size >= maxCacheEntries || cachedBytes + bytes > maxCacheBytes)
    ) {
      const oldestKey = cache.keys().next().value;
      if (oldestKey === undefined) break;
      const oldest = cache.get(oldestKey);
      cache.delete(oldestKey);
      if (oldest !== undefined) {
        cachedBytes = Math.max(0, cachedBytes - oldest.bytes);
      }
    }
    cache.set(index, { value, bytes });
    cachedBytes += bytes;
  };

  return {
    take(index: number, key: string) {
      return Effect.suspend(
        (): Effect.Effect<
          { readonly cached: true; readonly value: unknown } | { readonly cached: false },
          E
        > => {
          if (index !== nextIndex) {
            return Effect.die(new Error(`Replay cursor expected index ${nextIndex}, got ${index}`));
          }
          nextIndex += 1;

          if (broken) {
            return Effect.succeed({ cached: false } as const);
          }

          const previousResult = input.index.entries.get(index);
          if (
            previousResult === undefined ||
            previousResult.key !== key ||
            previousResult.outcome === "cancelled"
          ) {
            broken = true;
            clear();
            input.onBreak?.();
            return Effect.succeed({ cached: false } as const);
          }

          if (previousResult.outcome === "usage_limited") {
            return Effect.succeed({ cached: false } as const);
          }

          if (previousResult.outcome === "null") {
            return Effect.succeed({ cached: true, value: null } as const);
          }

          const hit = cache.get(index);
          if (hit !== undefined) {
            cache.delete(index);
            cache.set(index, hit);
            return Effect.succeed({ cached: true, value: hit.value } as const);
          }

          return input.loadValue(previousResult, index).pipe(
            Effect.map((value) => {
              remember(index, value, previousResult.length);
              return { cached: true, value } as const;
            }),
          );
        },
      );
    },
    stats() {
      return {
        indexedCount: input.index.entries.size,
        cachedCount: cache.size,
        cachedBytes,
      };
    },
    clear,
  };
}

interface RecordedResult {
  readonly key: string;
  readonly outcome: "ok" | "null" | "usage_limited" | "cancelled";
  readonly value: unknown;
}

export function replayCursor(previous: ReadonlyArray<JournalEntry>): ReplayCursor {
  const results = new Map<number, RecordedResult>();
  for (const entry of previous) {
    if (entry.type === "result") {
      results.set(entry.index, {
        key: entry.key,
        outcome: entry.outcome,
        value: entry.outcome === "ok" ? entry.value : null,
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
      if (
        previousResult === undefined ||
        previousResult.key !== key ||
        previousResult.outcome === "cancelled"
      ) {
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

type JournalQueueItem =
  | { readonly _tag: "entry"; readonly line: string; readonly bytes: number }
  | { readonly _tag: "encodeError"; readonly error: JournalEncodeError }
  | { readonly _tag: "flush"; readonly done: Deferred.Deferred<void> };

export interface JournalWriter {
  readonly append: (entry: JournalEntry) => Effect.Effect<boolean, JournalRecordTooLargeError>;
  readonly tryAppend: (entry: JournalEntry) => boolean;
  readonly flush: Effect.Effect<void>;
  readonly close: Effect.Effect<void>;
  readonly pendingCount: Effect.Effect<number>;
  readonly pendingBytes: Effect.Effect<number>;
}

function tryEncode(
  entry: JournalEntry,
  maxRecordBytes: number,
):
  | { readonly ok: true; readonly line: string; readonly bytes: number }
  | { readonly ok: false; readonly error: JournalEncodeError | JournalRecordTooLargeError } {
  try {
    const line = encodeJournalEntryWithLimit(entry, maxRecordBytes);
    return { ok: true, line, bytes: Buffer.byteLength(line, "utf8") };
  } catch (cause) {
    if (isJournalRecordTooLargeError(cause)) {
      return { ok: false, error: cause };
    }
    return { ok: false, error: new JournalEncodeError({ cause }) };
  }
}

export const makeJournalWriter = Effect.fn("WorkflowJournal.makeJournalWriter")(function* <
  E,
>(input: {
  readonly writeLine: (line: string) => Effect.Effect<void, E>;
  readonly onWriteError?: (error: E | JournalEncodeError) => Effect.Effect<void>;
  readonly maxPending?: number;
  readonly maxPendingBytes?: number;
  readonly maxRecordBytes?: number;
}): Effect.fn.Return<JournalWriter, never, Scope.Scope> {
  const capacity = Math.max(1, Math.trunc(input.maxPending ?? MAX_PENDING_JOURNAL_ENTRIES));
  const maxPendingBytes = Math.max(
    1,
    Math.trunc(input.maxPendingBytes ?? MAX_PENDING_JOURNAL_BYTES),
  );
  const maxRecordBytes = Math.max(1, Math.trunc(input.maxRecordBytes ?? MAX_JOURNAL_LINE_BYTES));
  const queue = yield* Queue.bounded<JournalQueueItem>(capacity);
  let closed = false;
  let inFlight = 0;
  let waitingBytes = 0;
  const pendingFlushes = new Set<Deferred.Deferred<void>>();
  interface ByteWaiter {
    readonly needed: number;
    readonly ready: Deferred.Deferred<boolean>;
    granted: boolean;
  }
  const byteWaiters: Array<ByteWaiter> = [];

  const canAdmitBytes = (bytes: number): boolean =>
    waitingBytes + bytes <= maxPendingBytes || (bytes > maxPendingBytes && waitingBytes === 0);

  const wakeByteWaiters = (): void => {
    if (closed) {
      while (byteWaiters.length > 0) {
        const waiter = byteWaiters.shift()!;
        Deferred.doneUnsafe(waiter.ready, Exit.succeed(false));
      }
      return;
    }
    while (byteWaiters.length > 0 && canAdmitBytes(byteWaiters[0]!.needed)) {
      const waiter = byteWaiters.shift()!;
      waiter.granted = true;
      waitingBytes += waiter.needed;
      Deferred.doneUnsafe(waiter.ready, Exit.succeed(true));
    }
  };

  const releaseBytes = (bytes: number): void => {
    waitingBytes = Math.max(0, waitingBytes - bytes);
    wakeByteWaiters();
  };

  const acquireBytes = (bytes: number): Effect.Effect<boolean> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (closed) return false;
        if (byteWaiters.length === 0 && canAdmitBytes(bytes)) {
          waitingBytes += bytes;
          return true;
        }
        const waiter: ByteWaiter = {
          needed: bytes,
          ready: yield* Deferred.make<boolean>(),
          granted: false,
        };
        byteWaiters.push(waiter);
        const exit = yield* Effect.exit(restore(Deferred.await(waiter.ready)));
        if (Exit.isSuccess(exit)) {
          if (exit.value) {
            if (closed) {
              releaseBytes(bytes);
              return false;
            }
            return true;
          }
          return false;
        }
        const index = byteWaiters.indexOf(waiter);
        if (index !== -1) {
          byteWaiters.splice(index, 1);
          wakeByteWaiters();
        } else if (waiter.granted) {
          releaseBytes(bytes);
        }
        return yield* exit;
      }),
    );

  const resolvePendingFlushes = Effect.sync(() => {
    for (const done of pendingFlushes) {
      Deferred.doneUnsafe(done, Exit.void);
    }
    pendingFlushes.clear();
  });

  const writerFiber = yield* Effect.gen(function* () {
    while (true) {
      const taken = yield* Queue.take(queue).pipe(Effect.exit);
      if (Exit.isFailure(taken)) return;
      const item = taken.value;
      if (item._tag === "flush") {
        pendingFlushes.delete(item.done);
        Deferred.doneUnsafe(item.done, Exit.void);
        continue;
      }
      if (item._tag === "encodeError") {
        inFlight += 1;
        yield* (
          input.onWriteError !== undefined ? input.onWriteError(item.error) : Effect.void
        ).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              inFlight -= 1;
            }),
          ),
        );
        continue;
      }
      inFlight += 1;
      yield* input.writeLine(item.line).pipe(
        Effect.catch((error) =>
          input.onWriteError !== undefined ? input.onWriteError(error) : Effect.void,
        ),
        Effect.ensuring(
          Effect.sync(() => {
            inFlight -= 1;
            releaseBytes(item.bytes);
          }),
        ),
      );
    }
  }).pipe(Effect.ensuring(resolvePendingFlushes), Effect.forkScoped);

  const close = Effect.gen(function* () {
    if (closed) return;
    closed = true;
    wakeByteWaiters();
    yield* Queue.shutdown(queue);
    yield* Fiber.interrupt(writerFiber);
    waitingBytes = 0;
    yield* resolvePendingFlushes;
  });

  yield* Effect.addFinalizer(() => close);

  const append = (entry: JournalEntry): Effect.Effect<boolean, JournalRecordTooLargeError> =>
    Effect.suspend(() => {
      if (closed) return Effect.succeed(false);
      const encoded = tryEncode(entry, maxRecordBytes);
      if (!encoded.ok) {
        if (encoded.error._tag === "JournalRecordTooLargeError") {
          return Effect.fail(encoded.error);
        }
        return Queue.offer(queue, { _tag: "encodeError", error: encoded.error });
      }
      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const acquired = yield* restore(acquireBytes(encoded.bytes));
          if (!acquired) return false;
          const offeredExit = yield* Effect.exit(
            restore(
              Queue.offer(queue, {
                _tag: "entry",
                line: encoded.line,
                bytes: encoded.bytes,
              }),
            ),
          );
          if (Exit.isFailure(offeredExit)) {
            releaseBytes(encoded.bytes);
            return yield* offeredExit;
          }
          if (!offeredExit.value) {
            releaseBytes(encoded.bytes);
            return false;
          }
          return true;
        }),
      );
    });

  const tryAppend = (entry: JournalEntry): boolean => {
    if (closed) return false;
    const encoded = tryEncode(entry, maxRecordBytes);
    if (!encoded.ok) {
      if (encoded.error._tag === "JournalRecordTooLargeError") {
        throw encoded.error;
      }
      return Queue.offerUnsafe(queue, { _tag: "encodeError", error: encoded.error });
    }
    if (byteWaiters.length > 0 || waitingBytes + encoded.bytes > maxPendingBytes) {
      return false;
    }
    if (!Queue.offerUnsafe(queue, { _tag: "entry", line: encoded.line, bytes: encoded.bytes })) {
      return false;
    }
    waitingBytes += encoded.bytes;
    return true;
  };

  const flush: Effect.Effect<void> = Effect.gen(function* () {
    if (closed) return;
    const done = yield* Deferred.make<void>();
    pendingFlushes.add(done);
    const offered = yield* Queue.offer(queue, { _tag: "flush", done });
    if (!offered) {
      pendingFlushes.delete(done);
      return;
    }
    yield* Deferred.await(done);
  });

  const pendingCount = Queue.size(queue).pipe(Effect.map((size) => size + inFlight));
  const pendingBytes = Effect.sync(() => waitingBytes);

  return {
    append,
    tryAppend,
    flush,
    close,
    pendingCount,
    pendingBytes,
  };
});
