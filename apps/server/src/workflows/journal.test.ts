import { describe, expect, it } from "vite-plus/test";

import {
  agentCallKey,
  decodeJournal,
  encodeJournalEntry,
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
});
