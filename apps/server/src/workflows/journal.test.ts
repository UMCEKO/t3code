import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";

import {
  JournalEncodeError,
  MAX_PENDING_JOURNAL_BYTES,
  MAX_PENDING_JOURNAL_ENTRIES,
  agentCallKey,
  decodeJournal,
  encodeJournalEntry,
  makeJournalWriter,
  replayCursor,
  type JournalEntry,
} from "./journal.ts";

describe("journal", () => {
  describe("agentCallKey", () => {
    it("is stable for reordered object keys at any nesting depth", () => {
      const a = agentCallKey("analyze", {
        label: "a",
        schema: {
          type: "object",
          properties: { z: { type: "number" }, a: { type: "string" } },
        },
        target: { model: "m", providerInstanceId: "p" },
      });
      const b = agentCallKey("analyze", {
        target: { providerInstanceId: "p", model: "m" },
        schema: {
          properties: { a: { type: "string" }, z: { type: "number" } },
          type: "object",
        },
        label: "a",
      });
      expect(a).toBe(b);
      expect(a).toMatch(/^[0-9a-f]{64}$/);
    });

    it("ignores undefined object properties consistently with JSON", () => {
      const a = agentCallKey("analyze", { label: "a", model: undefined });
      const b = agentCallKey("analyze", { label: "a" });
      expect(a).toBe(b);
    });

    it("changes when prompt or opts change", () => {
      const base = agentCallKey("prompt A", { label: "x", effort: "high" });
      expect(agentCallKey("prompt B", { label: "x", effort: "high" })).not.toBe(base);
      expect(agentCallKey("prompt A", { label: "y", effort: "high" })).not.toBe(base);
      expect(agentCallKey("prompt A", { label: "x", effort: "low" })).not.toBe(base);
      expect(agentCallKey("prompt A", {})).not.toBe(base);
    });
  });

  describe("encodeJournalEntry and decodeJournal", () => {
    it("round-trips all entry types", () => {
      const entries: ReadonlyArray<JournalEntry> = [
        { type: "phase", title: "Scan" },
        { type: "log", message: "Starting scan" },
        {
          type: "started",
          index: 0,
          key: "k0",
          label: "scan-a",
          phase: "Scan",
          agentId: "agent-1",
          childThreadId: "thread-1",
        },
        { type: "result", index: 0, key: "k0", outcome: "ok", value: { files: ["a.ts"] } },
        { type: "result", index: 1, key: "k1", outcome: "null", value: null, cached: true },
        { type: "result", index: 2, key: "k2", outcome: "usage_limited", value: null },
        { type: "result", index: 3, key: "k3", outcome: "cancelled", value: null },
        { type: "end", status: "completed", result: { ok: true } },
      ];

      const encoded = entries.map(encodeJournalEntry).join("");
      expect(decodeJournal(encoded)).toEqual(entries);
    });

    it("returns empty array for empty text", () => {
      expect(decodeJournal("")).toEqual([]);
      expect(decodeJournal("\n  \n")).toEqual([]);
    });

    it("tolerates a torn last line without failing", () => {
      const first: JournalEntry = { type: "phase", title: "Scan" };
      const second: JournalEntry = {
        type: "result",
        index: 0,
        key: "k0",
        outcome: "ok",
        value: "done",
      };
      const text =
        encodeJournalEntry(first) +
        encodeJournalEntry(second) +
        '{"type":"result","index":1,"key":"k1","out';

      expect(decodeJournal(text)).toEqual([first, second]);
    });

    it("throws on a corrupt middle line", () => {
      const first: JournalEntry = { type: "phase", title: "Scan" };
      const last: JournalEntry = { type: "log", message: "after" };
      const text = `${encodeJournalEntry(first)}{corrupt json\n${encodeJournalEntry(last)}`;

      expect(() => decodeJournal(text)).toThrow(/line 2/i);
    });

    it("throws on a schema-invalid middle line", () => {
      const first: JournalEntry = { type: "phase", title: "Scan" };
      const last: JournalEntry = { type: "log", message: "after" };
      const text = `${encodeJournalEntry(first)}{"type":"unknown"}\n${encodeJournalEntry(last)}`;

      expect(() => decodeJournal(text)).toThrow(/line 2/i);
    });
  });

  describe("replayCursor", () => {
    it("replays the unchanged prefix and goes live from the first changed call onward", () => {
      const previous: ReadonlyArray<JournalEntry> = [
        { type: "result", index: 0, key: "k0", outcome: "ok", value: "r0" },
        { type: "result", index: 1, key: "k1", outcome: "null", value: null },
        { type: "result", index: 2, key: "k2", outcome: "ok", value: "r2" },
        { type: "result", index: 3, key: "k3", outcome: "ok", value: "r3" },
      ];

      const cursor = replayCursor(previous);
      expect(cursor.take(0, "k0")).toEqual({ cached: true, value: "r0" });
      expect(cursor.take(1, "k1")).toEqual({ cached: true, value: null });
      expect(cursor.take(2, "k2-edited")).toEqual({ cached: false });
      expect(cursor.take(3, "k3")).toEqual({ cached: false });
      expect(cursor.take(4, "k4")).toEqual({ cached: false });
    });

    it("goes live from an unfinished call that only has a started entry", () => {
      const previous: ReadonlyArray<JournalEntry> = [
        { type: "result", index: 0, key: "k0", outcome: "ok", value: "r0" },
        {
          type: "started",
          index: 1,
          key: "k1",
          label: "unfinished",
          phase: null,
          agentId: "a1",
          childThreadId: "t1",
        },
        { type: "result", index: 2, key: "k2", outcome: "ok", value: "r2" },
      ];

      const cursor = replayCursor(previous);
      expect(cursor.take(0, "k0")).toEqual({ cached: true, value: "r0" });
      expect(cursor.take(1, "k1")).toEqual({ cached: false });
      expect(cursor.take(2, "k2")).toEqual({ cached: false });
    });

    it("treats cancelled results as unfinished and ends cache reuse for later calls", () => {
      const previous: ReadonlyArray<JournalEntry> = [
        { type: "result", index: 0, key: "k0", outcome: "ok", value: "r0" },
        { type: "result", index: 1, key: "k1", outcome: "cancelled", value: null },
        { type: "result", index: 2, key: "k2", outcome: "ok", value: "r2" },
      ];

      const cursor = replayCursor(previous);
      expect(cursor.take(0, "k0")).toEqual({ cached: true, value: "r0" });
      expect(cursor.take(1, "k1")).toEqual({ cached: false });
      expect(cursor.take(2, "k2")).toEqual({ cached: false });
    });

    it("reruns usage_limited calls without ending cache reuse for later calls", () => {
      const previous: ReadonlyArray<JournalEntry> = [
        { type: "result", index: 0, key: "k0", outcome: "ok", value: "r0" },
        { type: "result", index: 1, key: "k1", outcome: "usage_limited", value: null },
        { type: "result", index: 2, key: "k2", outcome: "ok", value: "r2" },
      ];

      const cursor = replayCursor(previous);
      expect(cursor.take(0, "k0")).toEqual({ cached: true, value: "r0" });
      expect(cursor.take(1, "k1")).toEqual({ cached: false });
      expect(cursor.take(2, "k2")).toEqual({ cached: true, value: "r2" });
    });

    it("throws when indices are not requested sequentially from 0", () => {
      const cursor = replayCursor([]);
      expect(() => cursor.take(1, "k1")).toThrow(/expected index 0, got 1/i);
      expect(cursor.take(0, "k0")).toEqual({ cached: false });
      expect(() => cursor.take(0, "k0")).toThrow(/expected index 1, got 0/i);
      expect(() => cursor.take(2, "k2")).toThrow(/expected index 1, got 2/i);
    });
  });

  describe("makeJournalWriter", () => {
    it.effect(
      "bounds pending entries under a burst beyond 1,024 without losing ordering and rejects entries after close",
      () =>
        Effect.gen(function* () {
          const total = MAX_PENDING_JOURNAL_ENTRIES + 500;
          const firstWriteStarted = yield* Deferred.make<void>();
          const releaseWrites = yield* Deferred.make<void>();
          const queueFilled = yield* Deferred.make<void>();
          const written: Array<string> = [];
          let blockedPastCap = false;

          const writer = yield* makeJournalWriter({
            writeLine: (line) =>
              Effect.gen(function* () {
                if (written.length === 0) {
                  yield* Deferred.succeed(firstWriteStarted, undefined);
                  yield* Deferred.await(releaseWrites);
                }
                written.push(line);
              }),
          });

          const producer = yield* Effect.gen(function* () {
            for (let i = 0; i < total; i += 1) {
              const accepted = yield* writer.append({ type: "log", message: `msg-${i}` });
              expect(accepted).toBe(true);
              if (i === MAX_PENDING_JOURNAL_ENTRIES) {
                yield* Deferred.succeed(queueFilled, undefined);
              }
              if (i === MAX_PENDING_JOURNAL_ENTRIES + 1) {
                blockedPastCap = true;
              }
            }
          }).pipe(Effect.forkScoped);

          yield* Deferred.await(firstWriteStarted);
          yield* Deferred.await(queueFilled);
          yield* Effect.yieldNow;

          // Pending entries stay bounded to 1 in-flight + MAX_PENDING_JOURNAL_ENTRIES queued.
          expect(yield* writer.pendingCount).toBe(MAX_PENDING_JOURNAL_ENTRIES + 1);
          expect(blockedPastCap).toBe(false);
          expect(writer.tryAppend({ type: "log", message: "overflow" })).toBe(false);

          yield* Deferred.succeed(releaseWrites, undefined);
          yield* Fiber.join(producer);
          yield* writer.flush;

          expect(blockedPastCap).toBe(true);
          expect(written).toHaveLength(total);
          const decoded = decodeJournal(written.join(""));
          expect(decoded).toHaveLength(total);
          for (let i = 0; i < total; i += 1) {
            expect(decoded[i]).toEqual({ type: "log", message: `msg-${i}` });
          }

          yield* writer.close;
          expect(yield* writer.append({ type: "log", message: "after-close" })).toBe(false);
          expect(writer.tryAppend({ type: "log", message: "after-close-sync" })).toBe(false);
          expect(written).toHaveLength(total);
        }),
    );

    it.effect(
      "bounds pending bytes for tryAppend and blocks append until an in-flight write completes",
      () =>
        Effect.gen(function* () {
          expect(MAX_PENDING_JOURNAL_BYTES).toBe(16 * 1024 * 1024);
          const firstEntry: JournalEntry = { type: "log", message: "a".repeat(60) };
          const secondEntry: JournalEntry = { type: "log", message: "b".repeat(60) };
          const firstBytes = Buffer.byteLength(encodeJournalEntry(firstEntry), "utf8");
          const secondBytes = Buffer.byteLength(encodeJournalEntry(secondEntry), "utf8");
          const firstWriteStarted = yield* Deferred.make<void>();
          const releaseFirstWrite = yield* Deferred.make<void>();
          const written: Array<string> = [];
          let secondEnqueued = false;

          const writer = yield* makeJournalWriter({
            maxPendingBytes: firstBytes + secondBytes - 1,
            writeLine: (line) =>
              Effect.gen(function* () {
                if (written.length === 0) {
                  yield* Deferred.succeed(firstWriteStarted, undefined);
                  yield* Deferred.await(releaseFirstWrite);
                }
                written.push(line);
              }),
          });

          expect(writer.tryAppend(firstEntry)).toBe(true);
          yield* Deferred.await(firstWriteStarted);
          expect(yield* writer.pendingBytes).toBe(firstBytes);
          expect(writer.tryAppend(secondEntry)).toBe(false);

          const blockedAppend = yield* Effect.gen(function* () {
            const accepted = yield* writer.append(secondEntry);
            secondEnqueued = true;
            return accepted;
          }).pipe(Effect.forkScoped);

          yield* Effect.yieldNow;
          expect(secondEnqueued).toBe(false);
          expect(blockedAppend.pollUnsafe()).toBeUndefined();

          yield* Deferred.succeed(releaseFirstWrite, undefined);
          expect(yield* Fiber.join(blockedAppend)).toBe(true);
          yield* writer.flush;

          expect(secondEnqueued).toBe(true);
          expect(yield* writer.pendingBytes).toBe(0);
          expect(decodeJournal(written.join(""))).toEqual([firstEntry, secondEntry]);
        }),
    );

    it.effect("admits a single oversized entry when nothing else waits", () =>
      Effect.gen(function* () {
        const smallEntry: JournalEntry = { type: "log", message: "small" };
        const smallBytes = Buffer.byteLength(encodeJournalEntry(smallEntry), "utf8");
        const oversizedEntry: JournalEntry = {
          type: "result",
          index: 0,
          key: "k-big",
          outcome: "ok",
          value: "x".repeat(200),
        };
        const oversizedBytes = Buffer.byteLength(encodeJournalEntry(oversizedEntry), "utf8");
        expect(oversizedBytes).toBeGreaterThan(smallBytes + 10);

        const smallWriteStarted = yield* Deferred.make<void>();
        const releaseSmallWrite = yield* Deferred.make<void>();
        const written: Array<string> = [];
        let blockNextSmall = false;

        const writer = yield* makeJournalWriter({
          maxPendingBytes: smallBytes + 10,
          writeLine: (line) =>
            Effect.gen(function* () {
              if (blockNextSmall) {
                blockNextSmall = false;
                yield* Deferred.succeed(smallWriteStarted, undefined);
                yield* Deferred.await(releaseSmallWrite);
              }
              written.push(line);
            }),
        });

        // Empty queue: tryAppend rejects over-cap, while append admits the oversized entry.
        expect(writer.tryAppend(oversizedEntry)).toBe(false);
        expect(yield* writer.append(oversizedEntry)).toBe(true);
        yield* writer.flush;
        expect(yield* writer.pendingBytes).toBe(0);
        expect(decodeJournal(written.join(""))).toEqual([oversizedEntry]);

        // Non-empty queue: oversized append waits for the in-flight entry to complete.
        blockNextSmall = true;
        expect(writer.tryAppend(smallEntry)).toBe(true);
        yield* Deferred.await(smallWriteStarted);

        const waitingOversized = yield* writer.append(oversizedEntry).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(waitingOversized.pollUnsafe()).toBeUndefined();

        yield* Deferred.succeed(releaseSmallWrite, undefined);
        expect(yield* Fiber.join(waitingOversized)).toBe(true);
        yield* writer.flush;
        expect(yield* writer.pendingBytes).toBe(0);
        expect(decodeJournal(written.join(""))).toEqual([
          oversizedEntry,
          smallEntry,
          oversizedEntry,
        ]);
      }),
    );

    it.effect("routes enqueue-time encode failures through onWriteError", () =>
      Effect.gen(function* () {
        const errors: Array<unknown> = [];
        const written: Array<string> = [];
        const writer = yield* makeJournalWriter({
          writeLine: (line) => Effect.sync(() => void written.push(line)),
          onWriteError: (error) => Effect.sync(() => void errors.push(error)),
        });

        const unserializable = {
          type: "result",
          index: 0,
          key: "k-bad",
          outcome: "ok",
          value: 1n,
        } as unknown as JournalEntry;

        expect(writer.tryAppend(unserializable)).toBe(true);
        expect(yield* writer.append(unserializable)).toBe(true);
        yield* writer.flush;

        expect(written).toHaveLength(0);
        expect(errors).toHaveLength(2);
        expect(errors[0]).toBeInstanceOf(JournalEncodeError);
        expect(errors[1]).toBeInstanceOf(JournalEncodeError);
        expect(yield* writer.pendingBytes).toBe(0);
      }),
    );
  });
});
