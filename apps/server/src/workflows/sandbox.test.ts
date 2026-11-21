import { describe, expect, it } from "vite-plus/test";

import {
  WORKFLOW_LIMITS,
  WORKFLOW_MESSAGE_LIMITS,
  WORKFLOW_WORKER_RESOURCE_LIMITS,
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
    it("throws resume-breaking error on Date.now(), new Date(), Date(), new Date(0).constructor, Math.random(), and Intl.DateTimeFormat", async () => {
      const cases = [
        { code: "return Date.now();", api: "Date.now" },
        { code: "return new Date();", api: "new Date()" },
        { code: "return Date();", api: "Date()" },
        { code: "return new Date(0).constructor.now();", api: "Date.now" },
        { code: "return new (new Date(0).constructor)();", api: "new Date()" },
        { code: "return (new Date(0).constructor)();", api: "Date()" },
        { code: "return Math.random();", api: "Math.random" },
        { code: "return Intl.DateTimeFormat();", api: "Intl.DateTimeFormat" },
        { code: "return new Intl.DateTimeFormat();", api: "Intl.DateTimeFormat" },
        { code: "return Intl.DateTimeFormat().format();", api: "Intl.DateTimeFormat" },
        { code: 'return new Intl.DateTimeFormat("en-US").format();', api: "Intl.DateTimeFormat" },
        { code: "return new Intl.DateTimeFormat().formatToParts();", api: "Intl.DateTimeFormat" },
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

    it("keeps hooks, promises, errors, Date.prototype.constructor, and codegen guards inside the script realm", async () => {
      const result = await runWorkflowScript({
        script: makeScript(`
          const agentPromise = agent("hello");
          const parallelPromise = parallel([() => 1]);
          const pipelinePromise = pipeline([1], (x) => x + 1);
          const agentVal = await agentPromise;
          const parallelVal = await parallelPromise;
          const pipelineVal = await pipelinePromise;

          const catchKind = async (fn) => {
            try {
              await fn();
              return null;
            } catch (e) {
              return {
                isError: e instanceof Error,
                isTypeError: e instanceof TypeError,
                ctorMatches: e.constructor === Error || e.constructor === TypeError,
                ctorCtorIsFunction: e.constructor.constructor === Function,
                message: e.message,
              };
            }
          };

          const badOptsErr = await catchKind(() => agent("x", { bogus: true }));
          const hostFailErr = await catchKind(() => agent("fail-host"));
          const badPhaseErr = await catchKind(() => phase(""));
          const dateErr = await catchKind(() => new Date(0).constructor.now());

          let asyncFnCtorThrew = false;
          try {
            (async () => {}).constructor("return 1")();
          } catch {
            asyncFnCtorThrew = true;
          }

          let agentCtorThrew = false;
          try {
            agent.constructor("return 1")();
          } catch {
            agentCtorThrew = true;
          }

          return {
            constructors: {
              agent: agent.constructor === Function,
              parallel: parallel.constructor === Function,
              pipeline: pipeline.constructor === Function,
              phase: phase.constructor === Function,
              log: log.constructor === Function,
              workflow: workflow.constructor === Function,
              budgetSpent: budget.spent.constructor === Function,
              budgetRemaining: budget.remaining.constructor === Function,
              dateCtorIsDate: new Date(0).constructor === Date,
            },
            promises: {
              agent: agentPromise instanceof Promise,
              parallel: parallelPromise instanceof Promise,
              pipeline: pipelinePromise instanceof Promise,
            },
            values: { agentVal, parallelVal, pipelineVal },
            errors: { badOptsErr, hostFailErr, badPhaseErr, dateErr },
            codegenBlocked: { asyncFnCtorThrew, agentCtorThrew },
          };
        `),
        args: {},
        host: makeHost({
          runAgent: async (call) => {
            if (call.prompt === "fail-host") {
              throw new Error("host boom");
            }
            return `ok:${call.prompt}`;
          },
        }),
        concurrency: 2,
      });

      expect(result).toEqual({
        constructors: {
          agent: true,
          parallel: true,
          pipeline: true,
          phase: true,
          log: true,
          workflow: true,
          budgetSpent: true,
          budgetRemaining: true,
          dateCtorIsDate: true,
        },
        promises: {
          agent: true,
          parallel: true,
          pipeline: true,
        },
        values: {
          agentVal: "ok:hello",
          parallelVal: [1],
          pipelineVal: [2],
        },
        errors: {
          badOptsErr: {
            isError: true,
            isTypeError: true,
            ctorMatches: true,
            ctorCtorIsFunction: true,
            message: 'Unknown agent() option: "bogus"',
          },
          hostFailErr: {
            isError: true,
            isTypeError: false,
            ctorMatches: true,
            ctorCtorIsFunction: true,
            message: "host boom",
          },
          badPhaseErr: {
            isError: true,
            isTypeError: true,
            ctorMatches: true,
            ctorCtorIsFunction: true,
            message: "phase() title must be a non-empty string",
          },
          dateErr: {
            isError: true,
            isTypeError: false,
            ctorMatches: true,
            ctorCtorIsFunction: true,
            message: "Date.now() is not available in workflow scripts: it would break resume.",
          },
        },
        codegenBlocked: {
          asyncFnCtorThrew: true,
          agentCtorThrew: true,
        },
      });
    });

    it("stops top-level and post-await infinite loops while normal multi-await, parallel, and pipeline scripts complete", async () => {
      const timeoutError =
        "The workflow script ran for more than 50 ms without waiting; it was stopped.";

      await expect(
        runWorkflowScript({
          script: makeScript(`while (true) {}`),
          args: {},
          host: makeHost(),
          concurrency: 2,
          syncTimeoutMs: 50,
        }),
      ).rejects.toThrow(new WorkflowScriptError(timeoutError));

      await expect(
        runWorkflowScript({
          script: makeScript(`
            await agent("first");
            while (true) {}
          `),
          args: {},
          host: makeHost(),
          concurrency: 2,
          syncTimeoutMs: 50,
        }),
      ).rejects.toThrow(new WorkflowScriptError(timeoutError));

      await expect(
        runWorkflowScript({
          script: makeScript(`
            return await parallel([
              async () => {
                await agent("inside-parallel");
                while (true) {}
              },
            ]);
          `),
          args: {},
          host: makeHost(),
          concurrency: 2,
          syncTimeoutMs: 50,
        }),
      ).rejects.toThrow(new WorkflowScriptError(timeoutError));

      await expect(
        runWorkflowScript({
          script: makeScript(`
            await agent("first");
            Object.defineProperty(Object.prototype, "then", {
              get() {
                while (true) {}
              },
            });
            return {};
          `),
          args: {},
          host: makeHost(),
          concurrency: 2,
          syncTimeoutMs: 50,
        }),
      ).rejects.toThrow(new WorkflowScriptError(timeoutError));

      const multiAwait = await runWorkflowScript({
        script: makeScript(`
          const first = await agent("step-1");
          const second = await agent("step-2");
          const third = await agent("step-3");
          const par = await parallel([() => agent("par-a"), () => agent("par-b")]);
          const pipe = await pipeline(
            ["x", "y"],
            async (item) => await agent("pipe-1:" + item),
            async (prev) => await agent("pipe-2:" + prev),
          );
          return { first, second, third, par, pipe };
        `),
        args: {},
        host: makeHost(),
        concurrency: 2,
        syncTimeoutMs: 50,
      });

      expect(multiAwait).toEqual({
        first: "res:0:step-1",
        second: "res:1:step-2",
        third: "res:2:step-3",
        par: ["res:3:par-a", "res:4:par-b"],
        pipe: ["res:7:pipe-2:res:5:pipe-1:x", "res:8:pipe-2:res:6:pipe-1:y"],
      });
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
      const stage1Started = Promise.withResolvers<void>();
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
            if (started.length === 2) {
              stage1Started.resolve();
            }
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
      await stage1Started.promise;
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

    it("fails the workflow on sticky cap exhaustion in parallel, pipeline, nested workflows, and try/catch", async () => {
      const overCapMsg = `A workflow runs at most ${WORKFLOW_LIMITS.maxAgents} agents`;

      await expect(
        runWorkflowScript({
          script: makeScript(`
            const thunks = Array.from({ length: ${WORKFLOW_LIMITS.maxAgents + 1} }, (_, i) => () => agent("p" + i));
            return await parallel(thunks);
          `),
          args: {},
          host: makeHost({ runAgent: async () => "ok" }),
          concurrency: 8,
        }),
      ).rejects.toThrow(new WorkflowScriptError(overCapMsg));

      await expect(
        runWorkflowScript({
          script: makeScript(`
            const items = Array.from({ length: ${WORKFLOW_LIMITS.maxAgents + 1} }, (_, i) => i);
            return await pipeline(items, (i) => agent("p" + i));
          `),
          args: {},
          host: makeHost({ runAgent: async () => "ok" }),
          concurrency: 8,
        }),
      ).rejects.toThrow(new WorkflowScriptError(overCapMsg));

      await expect(
        runWorkflowScript({
          script: makeScript(`
            const items = Array.from({ length: 501 }, (_, i) => i);
            return await pipeline(
              items,
              (i) => agent("s1:" + i),
              (_prev, i) => agent("s2:" + i),
            );
          `),
          args: {},
          host: makeHost({ runAgent: async () => "ok" }),
          concurrency: 16,
        }),
      ).rejects.toThrow(new WorkflowScriptError(overCapMsg));

      await expect(
        runWorkflowScript({
          script: makeScript(`
            for (let i = 0; i < ${WORKFLOW_LIMITS.maxAgents}; i += 1) {
              await agent("p" + i);
            }
            try {
              await agent("over-cap");
            } catch {
              return "swallowed";
            }
            return "ok";
          `),
          args: {},
          host: makeHost({ runAgent: async () => "ok" }),
          concurrency: 4,
        }),
      ).rejects.toThrow(new WorkflowScriptError(overCapMsg));

      await expect(
        runWorkflowScript({
          script: makeScript(`
            try {
              await parallel(Array.from({ length: ${WORKFLOW_LIMITS.maxItems + 1} }, () => () => 1));
            } catch {
              return "swallowed";
            }
            return "ok";
          `),
          args: {},
          host: makeHost(),
          concurrency: 4,
        }),
      ).rejects.toThrow(
        new WorkflowScriptError(`parallel() accepts at most ${WORKFLOW_LIMITS.maxItems} items`),
      );

      await expect(
        runWorkflowScript({
          script: makeScript(`
            try {
              await pipeline(Array.from({ length: ${WORKFLOW_LIMITS.maxItems + 1} }, (_, i) => i), (x) => x);
            } catch {
              return "swallowed";
            }
            return "ok";
          `),
          args: {},
          host: makeHost(),
          concurrency: 4,
        }),
      ).rejects.toThrow(
        new WorkflowScriptError(`pipeline() accepts at most ${WORKFLOW_LIMITS.maxItems} items`),
      );

      await expect(
        runWorkflowScript({
          script: makeScript(`
            await agent("parent-0");
            try {
              await workflow({ scriptPath: "/over-cap-child.js" });
            } catch {
              return "swallowed";
            }
            return "ok";
          `),
          args: {},
          host: makeHost({
            runAgent: async () => "ok",
            loadScript: async () =>
              makeScript(
                `return await parallel(Array.from({ length: ${WORKFLOW_LIMITS.maxAgents} }, (_, i) => () => agent("c" + i)));`,
                `{ name: "over-cap-child", description: "exceeds parent agent cap" }`,
              ),
          }),
          concurrency: 8,
        }),
      ).rejects.toThrow(new WorkflowScriptError(overCapMsg));
    });

    it("rejects out-of-memory allocations without crashing the host process", async () => {
      expect(WORKFLOW_WORKER_RESOURCE_LIMITS.maxOldGenerationSizeMb).toBeLessThanOrEqual(256);
      expect(WORKFLOW_WORKER_RESOURCE_LIMITS.maxYoungGenerationSizeMb).toBeLessThanOrEqual(32);
      expect(WORKFLOW_WORKER_RESOURCE_LIMITS.codeRangeSizeMb).toBeLessThanOrEqual(16);
      expect(WORKFLOW_WORKER_RESOURCE_LIMITS.stackSizeMb).toBeLessThanOrEqual(4);

      await expect(
        runWorkflowScript({
          script: makeScript(`
            const chunks = [];
            const base = "x".repeat(1024 * 1024);
            while (true) {
              chunks.push(chunks.length + base);
            }
          `),
          args: {},
          host: makeHost(),
          concurrency: 1,
          syncTimeoutMs: 5_000,
        }),
      ).rejects.toThrow(/ERR_WORKER_OUT_OF_MEMORY|memory limit/i);
    });

    it("rejects oversized messages on both sides of the worker boundary", async () => {
      const overPrompt = WORKFLOW_MESSAGE_LIMITS.maxPromptBytes + 1;
      const overValue = WORKFLOW_MESSAGE_LIMITS.maxValueBytes + 1;

      await expect(
        runWorkflowScript({
          script: makeScript(`
            try {
              await agent("a".repeat(${overPrompt}));
            } catch {
              return "swallowed";
            }
            return "ok";
          `),
          args: {},
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(WorkflowScriptError);

      await expect(
        runWorkflowScript({
          script: makeScript(`
            await agent("ok", {
              schema: { type: "object", description: "d".repeat(${overPrompt}) },
            });
          `),
          args: {},
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(WorkflowScriptError);

      await expect(
        runWorkflowScript({
          script: makeScript(`return "b".repeat(${overValue});`),
          args: {},
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(WorkflowScriptError);

      await expect(
        runWorkflowScript({
          script: makeScript(`return await agent("big-result");`),
          args: {},
          host: makeHost({
            runAgent: async () => "c".repeat(overValue),
          }),
          concurrency: 1,
        }),
      ).rejects.toThrow(WorkflowScriptError);
    });

    it("terminates a busy CPU-spinning worker immediately when host.signal aborts", async () => {
      const controller = new AbortController();
      const reason = new Error("abort busy worker");

      const runPromise = runWorkflowScript({
        script: makeScript(`
          phase("busy-loop");
          while (true) {}
        `),
        args: {},
        host: makeHost({
          signal: controller.signal,
          onPhase: (title) => {
            if (title === "busy-loop") {
              controller.abort(reason);
            }
          },
        }),
        concurrency: 1,
        syncTimeoutMs: 10_000,
      });

      await expect(runPromise).rejects.toBe(reason);
    });

    it("blocks classic VM escape attempts including values returned from agent()", async () => {
      const result = await runWorkflowScript({
        script: makeScript(`
          const tryEscape = (fn) => {
            try {
              const out = fn();
              return { escaped: true, type: typeof out };
            } catch (err) {
              return { escaped: false, name: err?.name ?? null };
            }
          };

          const agentObj = await agent("obj");
          const agentStr = await agent("str");

          return {
            globals: {
              process: typeof process,
              require: typeof require,
              fs: typeof fs,
              fetch: typeof fetch,
            },
            thisEscape: tryEscape(() => this.constructor.constructor("return process")()),
            methodThisEscape: tryEscape(() =>
              ({ run() { return this.constructor.constructor("return process")(); } }).run(),
            ),
            globalThisEscape: tryEscape(() => globalThis.constructor.constructor("return process")()),
            objectLiteralProcess: tryEscape(() => ({}).constructor.constructor("return process")()),
            objectLiteralFs: tryEscape(() =>
              ({}).constructor.constructor('return require("node:fs")')(),
            ),
            agentObjProcess: tryEscape(() => agentObj.constructor.constructor("return process")()),
            agentObjNestedFs: tryEscape(() =>
              agentObj.nested.constructor.constructor('return require("node:fs")')(),
            ),
            agentStrProcess: tryEscape(() => agentStr.constructor.constructor("return process")()),
            argsProcess: tryEscape(() => args.constructor.constructor("return process")()),
          };
        `),
        args: { seed: 1 },
        host: makeHost({
          runAgent: async (call) =>
            call.prompt === "obj" ? { ok: true, nested: [1, 2] } : "plain-text",
        }),
        concurrency: 2,
      });

      expect(result).toEqual({
        globals: {
          process: "undefined",
          require: "undefined",
          fs: "undefined",
          fetch: "undefined",
        },
        thisEscape: { escaped: false, name: "EvalError" },
        methodThisEscape: { escaped: false, name: "EvalError" },
        globalThisEscape: { escaped: false, name: "EvalError" },
        objectLiteralProcess: { escaped: false, name: "EvalError" },
        objectLiteralFs: { escaped: false, name: "EvalError" },
        agentObjProcess: { escaped: false, name: "EvalError" },
        agentObjNestedFs: { escaped: false, name: "EvalError" },
        agentStrProcess: { escaped: false, name: "EvalError" },
        argsProcess: { escaped: false, name: "EvalError" },
      });
    });
  });
});
