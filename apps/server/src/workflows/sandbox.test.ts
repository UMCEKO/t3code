import { describe, expect, it } from "vite-plus/test";

import {
  WORKFLOW_LIMITS,
  WorkflowScriptError,
  parseWorkflowMeta,
  runWorkflowScript,
  workflowConcurrency,
  type WorkflowAgentCall,
  type WorkflowHost,
} from "./sandbox.ts";

function makeScript(body: string, meta = `{ name: "test", description: "test workflow" }`): string {
  return `export const meta = ${meta};\n${body}`;
}

function makeHost(overrides: Partial<WorkflowHost> = {}): WorkflowHost {
  const controller = new AbortController();
  return {
    runAgent: async (call) => `res:${call.index}:${call.prompt}`,
    onPhase: () => {},
    onLog: () => {},
    loadScript: async () => {
      throw new Error("unexpected loadScript");
    },
    budget: { total: null, spent: () => 0 },
    signal: controller.signal,
    ...overrides,
  };
}

describe("sandbox", () => {
  describe("workflowConcurrency", () => {
    it("clamps cpus - 2 between 1 and 16", () => {
      expect(workflowConcurrency(1)).toBe(1);
      expect(workflowConcurrency(2)).toBe(1);
      expect(workflowConcurrency(3)).toBe(1);
      expect(workflowConcurrency(4)).toBe(2);
      expect(workflowConcurrency(10)).toBe(8);
      expect(workflowConcurrency(18)).toBe(16);
      expect(workflowConcurrency(64)).toBe(16);
    });
  });

  describe("parseWorkflowMeta", () => {
    it("parses a pure literal with phases and leading comments", () => {
      const script = `
        // leading comment
        /* block comment */
        export const meta = {
          name: "review-repo",
          description: \`Reviews {braces} and \\$\{escaped}\`,
          whenToUse: "before merge",
          phases: [
            { title: "Scan", detail: "collect files", model: "sonnet" },
            { title: "Synthesize" },
          ],
        };
        return 1;
      `;

      expect(parseWorkflowMeta(script)).toEqual({
        name: "review-repo",
        description: "Reviews {braces} and ${escaped}",
        whenToUse: "before merge",
        phases: [
          { title: "Scan", detail: "collect files", model: "sonnet" },
          { title: "Synthesize" },
        ],
      });
    });

    it("defaults phases to an empty array when omitted", () => {
      expect(parseWorkflowMeta(makeScript("return 1;"))).toEqual({
        name: "test",
        description: "test workflow",
        phases: [],
      });
    });

    it("rejects identifier values, calls, spread, computed keys, and template interpolation", () => {
      const invalidCases = [
        `export const meta = { name: someVar, description: "d" };`,
        `export const meta = { name: makeName(), description: "d" };`,
        `export const meta = { ...base, name: "a", description: "d" };`,
        `export const meta = { [key]: "a", name: "a", description: "d" };`,
        "export const meta = { name: `hello ${user}`, description: 'd' };",
        `export const meta = { name, description: "d" };`,
        `export const meta = buildMeta();`,
      ];

      for (const script of invalidCases) {
        expect(() => parseWorkflowMeta(script)).toThrow(WorkflowScriptError);
        expect(() => parseWorkflowMeta(script)).toThrow(/pure literal/i);
      }
    });

    it("rejects missing/empty name, missing description, and duplicate phase titles", () => {
      expect(() => parseWorkflowMeta(`export const meta = { description: "d" };`)).toThrow(
        WorkflowScriptError,
      );
      expect(() =>
        parseWorkflowMeta(`export const meta = { name: "   ", description: "d" };`),
      ).toThrow(WorkflowScriptError);
      expect(() => parseWorkflowMeta(`export const meta = { name: "a" };`)).toThrow(
        WorkflowScriptError,
      );
      expect(() =>
        parseWorkflowMeta(
          `export const meta = { name: "a", description: "d", phases: [{ title: "A" }, { title: "A" }] };`,
        ),
      ).toThrow(WorkflowScriptError);
    });
  });

  describe("determinism and isolation", () => {
    it("throws resume-breaking error on Date.now(), new Date(), Date(), and Math.random()", async () => {
      const cases = [
        { code: "return Date.now();", api: "Date.now" },
        { code: "return new Date();", api: "new Date()" },
        { code: "return Date();", api: "Date()" },
        { code: "return Math.random();", api: "Math.random" },
      ];

      for (const testCase of cases) {
        await expect(
          runWorkflowScript({
            script: makeScript(testCase.code),
            args: {},
            host: makeHost(),
            concurrency: 2,
          }),
        ).rejects.toThrow(
          new RegExp(
            `${testCase.api.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}.*is not available in workflow scripts: it would break resume\\.`,
          ),
        );
      }
    });

    it("allows deterministic dated new Date(x), Date.parse, and Date.UTC", async () => {
      const result = await runWorkflowScript({
        script: makeScript(`
          const epoch = new Date(0);
          return {
            iso: epoch.toISOString(),
            isDate: epoch instanceof Date,
            parsed: Date.parse("2025-01-01T00:00:00.000Z"),
            utc: Date.UTC(2025, 0, 1),
          };
        `),
        args: {},
        host: makeHost(),
        concurrency: 2,
      });

      expect(result).toEqual({
        iso: "1970-01-01T00:00:00.000Z",
        isDate: true,
        parsed: 1735689600000,
        utc: 1735689600000,
      });
    });

    it("omits Node globals and rejects static and dynamic imports", async () => {
      const globals = await runWorkflowScript({
        script: makeScript(`
          return {
            process: typeof process,
            require: typeof require,
            fetch: typeof fetch,
            setTimeout: typeof setTimeout,
            setInterval: typeof setInterval,
            performance: typeof performance,
          };
        `),
        args: {},
        host: makeHost(),
        concurrency: 2,
      });
      expect(globals).toEqual({
        process: "undefined",
        require: "undefined",
        fetch: "undefined",
        setTimeout: "undefined",
        setInterval: "undefined",
        performance: "undefined",
      });

      await expect(
        runWorkflowScript({
          script: makeScript(`import fs from "node:fs"; return fs;`),
          args: {},
          host: makeHost(),
          concurrency: 2,
        }),
      ).rejects.toThrow(WorkflowScriptError);

      await expect(
        runWorkflowScript({
          script: makeScript(`return await import("node:fs");`),
          args: {},
          host: makeHost(),
          concurrency: 2,
        }),
      ).rejects.toThrow(WorkflowScriptError);
    });

    it("clones args into context and converts undefined return to null, rejecting non-JSON returns", async () => {
      const inputArgs = { nested: { count: 1 } };
      const mutated = await runWorkflowScript({
        script: makeScript(`
          args.nested.count = 99;
          return { mutated: args.nested.count, isObj: args instanceof Object };
        `),
        args: inputArgs,
        host: makeHost(),
        concurrency: 2,
      });
      expect(mutated).toEqual({ mutated: 99, isObj: true });
      expect(inputArgs.nested.count).toBe(1);

      const empty = await runWorkflowScript({
        script: makeScript(`const x = 1;`),
        args: undefined,
        host: makeHost(),
        concurrency: 2,
      });
      expect(empty).toBeNull();

      await expect(
        runWorkflowScript({
          script: makeScript(`return () => 1;`),
          args: {},
          host: makeHost(),
          concurrency: 2,
        }),
      ).rejects.toThrow(WorkflowScriptError);

      await expect(
        runWorkflowScript({
          script: makeScript(`const a = {}; a.self = a; return a;`),
          args: {},
          host: makeHost(),
          concurrency: 2,
        }),
      ).rejects.toThrow(WorkflowScriptError);
    });
  });

  describe("hooks: agent, parallel, pipeline, phase, log", () => {
    it("sets default opts.phase via phase() and truncates log() to 2000 chars", async () => {
      const calls: Array<WorkflowAgentCall> = [];
      const phases: Array<string> = [];
      const logs: Array<string> = [];

      await runWorkflowScript({
        script: makeScript(`
          phase("Scan");
          await agent("p0");
          await agent("p1", { phase: "Override", label: "custom", isolation: "worktree" });
          log("x".repeat(2500));
        `),
        args: {},
        host: makeHost({
          runAgent: async (call) => {
            calls.push(call);
            return "ok";
          },
          onPhase: (title) => phases.push(title),
          onLog: (msg) => logs.push(msg),
        }),
        concurrency: 2,
      });

      expect(phases).toEqual(["Scan"]);
      expect(calls.map((c) => ({ index: c.index, phase: c.phase, opts: c.opts }))).toEqual([
        { index: 0, phase: "Scan", opts: {} },
        {
          index: 1,
          phase: "Override",
          opts: { phase: "Override", label: "custom", isolation: "worktree" },
        },
      ]);
      expect(logs).toHaveLength(1);
      expect(logs[0]).toHaveLength(2000);
    });

    it("rejects unknown agent opts and non-object schema roots with TypeError", async () => {
      await expect(
        runWorkflowScript({
          script: makeScript(`await agent("hi", { bogus: true });`),
          args: {},
          host: makeHost(),
          concurrency: 2,
        }),
      ).rejects.toThrow(TypeError);

      await expect(
        runWorkflowScript({
          script: makeScript(`await agent("hi", { schema: { type: "array" } });`),
          args: {},
          host: makeHost(),
          concurrency: 2,
        }),
      ).rejects.toThrow(TypeError);
    });

    it("parallel resolves throwing thunks to null and preserves order", async () => {
      const deferred0 = Promise.withResolvers<string>();
      const deferred2 = Promise.withResolvers<string>();

      const runPromise = runWorkflowScript({
        script: makeScript(`
          return await parallel([
            () => agent("slow"),
            () => { throw new Error("sync boom"); },
            () => agent("fast"),
            async () => { throw new Error("async boom"); },
          ]);
        `),
        args: {},
        host: makeHost({
          runAgent: (call) => (call.prompt === "slow" ? deferred0.promise : deferred2.promise),
        }),
        concurrency: 4,
      });

      deferred2.resolve("fast-done");
      deferred0.resolve("slow-done");

      await expect(runPromise).resolves.toEqual(["slow-done", null, "fast-done", null]);
    });

    it("pipeline has no barrier between stages and drops throwing items to null", async () => {
      const gates = new Map<string, PromiseWithResolvers<string>>();
      const started: Array<string> = [];
      const stage2Item0Started = Promise.withResolvers<void>();
      const stage2Item1Started = Promise.withResolvers<void>();

      const runPromise = runWorkflowScript({
        script: makeScript(`
          return await pipeline(
            ["a", "b", "c"],
            async (item) => {
              if (item === "c") throw new Error("drop c");
              return await agent("s1:" + item);
            },
            async (prev, item, index) => {
              return await agent("s2:" + item + ":" + prev + ":" + index);
            },
          );
        `),
        args: {},
        host: makeHost({
          runAgent: (call) => {
            started.push(call.prompt);
            const gate = Promise.withResolvers<string>();
            gates.set(call.prompt, gate);
            if (call.prompt.startsWith("s2:a:")) {
              stage2Item0Started.resolve();
            }
            if (call.prompt.startsWith("s2:b:")) stage2Item1Started.resolve();
            return gate.promise;
          },
        }),
        concurrency: 4,
      });

      // Resolve item 0's stage 1 while item 1 is still blocked in stage 1.
      await Promise.resolve();
      expect(started).toEqual(["s1:a", "s1:b"]);
      gates.get("s1:a")!.resolve("r1a");

      await stage2Item0Started.promise;
      expect(started).toEqual(["s1:a", "s1:b", "s2:a:r1a:0"]);

      gates.get("s2:a:r1a:0")!.resolve("final-a");
      gates.get("s1:b")!.resolve("r1b");
      await stage2Item1Started.promise;
      gates.get("s2:b:r1b:1")!.resolve("final-b");

      await expect(runPromise).resolves.toEqual(["final-a", "final-b", null]);
      expect(started.some((p) => p.startsWith("s2:c:"))).toBe(false);
    });
  });

  describe("concurrency, caps, budget, abort, and nested workflow", () => {
    it("enforces concurrency cap and starts queued calls in FIFO order", async () => {
      let inFlight = 0;
      let maxInFlight = 0;
      const startOrder: Array<number> = [];
      const gates: Array<PromiseWithResolvers<string>> = [];
      const allStartedCount = (target: number): Promise<void> => {
        if (startOrder.length >= target) return Promise.resolve();
        const waiter = Promise.withResolvers<void>();
        waiters.push({ target, waiter });
        return waiter.promise;
      };
      const waiters: Array<{ target: number; waiter: PromiseWithResolvers<void> }> = [];

      const runPromise = runWorkflowScript({
        script: makeScript(`
          return await parallel([0, 1, 2, 3, 4].map((i) => () => agent("p" + i)));
        `),
        args: {},
        host: makeHost({
          runAgent: (call) => {
            inFlight += 1;
            maxInFlight = Math.max(maxInFlight, inFlight);
            startOrder.push(call.index);
            const gate = Promise.withResolvers<string>();
            gates[call.index] = gate;
            for (const w of waiters) {
              if (startOrder.length >= w.target) w.waiter.resolve();
            }
            return gate.promise.finally(() => {
              inFlight -= 1;
            });
          },
        }),
        concurrency: 2,
      });

      await allStartedCount(2);
      expect(startOrder).toEqual([0, 1]);
      expect(inFlight).toBe(2);

      gates[1]!.resolve("done-1");
      await allStartedCount(3);
      expect(startOrder).toEqual([0, 1, 2]);
      expect(inFlight).toBe(2);

      gates[0]!.resolve("done-0");
      await allStartedCount(4);
      expect(startOrder).toEqual([0, 1, 2, 3]);

      gates[2]!.resolve("done-2");
      await allStartedCount(5);
      expect(startOrder).toEqual([0, 1, 2, 3, 4]);

      gates[3]!.resolve("done-3");
      gates[4]!.resolve("done-4");

      await expect(runPromise).resolves.toEqual(["done-0", "done-1", "done-2", "done-3", "done-4"]);
      expect(maxInFlight).toBe(2);
    });

    it("enforces the 1000-agent cap and 4096-item cap", async () => {
      await expect(
        runWorkflowScript({
          script: makeScript(`
            for (let i = 0; i < ${WORKFLOW_LIMITS.maxAgents + 1}; i += 1) {
              await agent("p" + i);
            }
          `),
          args: {},
          host: makeHost({ runAgent: async () => "ok" }),
          concurrency: 4,
        }),
      ).rejects.toThrow("A workflow runs at most 1000 agents");

      await expect(
        runWorkflowScript({
          script: makeScript(`
            const thunks = Array.from({ length: ${WORKFLOW_LIMITS.maxItems + 1} }, () => () => 1);
            await parallel(thunks);
          `),
          args: {},
          host: makeHost(),
          concurrency: 4,
        }),
      ).rejects.toThrow(WorkflowScriptError);

      await expect(
        runWorkflowScript({
          script: makeScript(`
            const items = Array.from({ length: ${WORKFLOW_LIMITS.maxItems + 1} }, (_, i) => i);
            await pipeline(items, (x) => x);
          `),
          args: {},
          host: makeHost(),
          concurrency: 4,
        }),
      ).rejects.toThrow(WorkflowScriptError);
    });

    it("exposes budget and throws when token budget is exhausted", async () => {
      let spentTokens = 40;
      const result = await runWorkflowScript({
        script: makeScript(`
          const before = { total: budget.total, spent: budget.spent(), remaining: budget.remaining() };
          await agent("first");
          const after = { spent: budget.spent(), remaining: budget.remaining() };
          let budgetError = null;
          try {
            await agent("second");
          } catch (e) {
            budgetError = e.message;
          }
          return { before, after, budgetError };
        `),
        args: {},
        host: makeHost({
          budget: { total: 100, spent: () => spentTokens },
          runAgent: async () => {
            spentTokens = 100;
            return "ok";
          },
        }),
        concurrency: 2,
      });

      expect(result).toEqual({
        before: { total: 100, spent: 40, remaining: 60 },
        after: { spent: 100, remaining: 0 },
        budgetError: "The workflow's token budget is spent.",
      });
    });

    it("rejects pending agent and parallel calls with the abort reason", async () => {
      const controller = new AbortController();
      const started = Promise.withResolvers<void>();
      const neverSettled = Promise.withResolvers<string>();

      const runPromise = runWorkflowScript({
        script: makeScript(`
          return await parallel([
            () => agent("first"),
            () => agent("queued"),
          ]);
        `),
        args: {},
        host: makeHost({
          signal: controller.signal,
          runAgent: () => {
            started.resolve();
            return neverSettled.promise;
          },
        }),
        concurrency: 1,
      });

      await started.promise;
      const reason = new Error("stopped by user");
      controller.abort(reason);

      await expect(runPromise).rejects.toBe(reason);
    });

    it("supports one-level nested workflow({scriptPath}) sharing the agent counter and rejects deeper nesting or string names", async () => {
      const seenIndices: Array<number> = [];
      const childScript = makeScript(
        `
          const a = await agent("child-1");
          const b = await agent("child-2");
          return { childArgs: args, a, b };
        `,
        `{ name: "child", description: "child workflow" }`,
      );
      const grandchildCaller = makeScript(
        `return await workflow({ scriptPath: "/grandchild.js" });`,
        `{ name: "child-nesting", description: "tries nested workflow" }`,
      );

      const loadScript = async (ref: { readonly scriptPath: string }): Promise<string> => {
        if (ref.scriptPath === "/child.js") return childScript;
        if (ref.scriptPath === "/nest.js") return grandchildCaller;
        throw new Error(`missing ${ref.scriptPath}`);
      };

      const result = await runWorkflowScript({
        script: makeScript(`
          const first = await agent("parent-0");
          const nested = await workflow({ scriptPath: "/child.js" }, { x: 42 });
          const last = await agent("parent-3");
          return { first, nested, last };
        `),
        args: { root: true },
        host: makeHost({
          loadScript,
          runAgent: async (call) => {
            seenIndices.push(call.index);
            return `${call.index}:${call.prompt}`;
          },
        }),
        concurrency: 2,
      });

      expect(seenIndices).toEqual([0, 1, 2, 3]);
      expect(result).toEqual({
        first: "0:parent-0",
        nested: {
          childArgs: { x: 42 },
          a: "1:child-1",
          b: "2:child-2",
        },
        last: "3:parent-3",
      });

      await expect(
        runWorkflowScript({
          script: makeScript(`return await workflow("saved-name");`),
          args: {},
          host: makeHost({ loadScript }),
          concurrency: 2,
        }),
      ).rejects.toThrow("Saved workflows are not supported; pass {scriptPath}.");

      await expect(
        runWorkflowScript({
          script: makeScript(`return await workflow({ scriptPath: "/nest.js" });`),
          args: {},
          host: makeHost({ loadScript }),
          concurrency: 2,
        }),
      ).rejects.toThrow(/one level only/i);
    });
  });
});
