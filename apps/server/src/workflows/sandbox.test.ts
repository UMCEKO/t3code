// @effect-diagnostics nodeBuiltinImport:off -- Probes the real child process ulimit and handshake on Linux.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";

import * as HostProcess from "@t3tools/shared/HostProcess";
import { describe, expect, it } from "vite-plus/test";

import {
  MAX_PHASE_MESSAGE_BYTES,
  MAX_VALUE_MESSAGE_BYTES,
  MIN_WORKER_STDOUT_BUFFERED_BYTES,
  WORKFLOW_LIMITS,
  WORKFLOW_MESSAGE_LIMITS,
  WORKFLOW_PROGRESS_LIMITS,
  WORKFLOW_SANDBOX_PROCESS_LIMITS,
  WorkflowSandboxCleanupError,
  WorkflowScriptError,
  buildWorkflowSandboxCommand,
  createWorkflowHostMessageReceiver,
  getUnconfirmedSandboxChildPids,
  makeBoundedLineSplitter,
  parseWorkflowMeta,
  reapUnconfirmedSandboxChildren,
  resolveWorkflowSandboxEntryPath,
  runWorkflowScript,
  terminateSandboxChild,
  workflowConcurrency,
  type WorkflowAgentCall,
  type WorkflowHost,
  type WorkflowSandboxCleanupFailure,
} from "./sandbox.ts";
import {
  WORKFLOW_SANDBOX_WORKER_ARG,
  makeWorkflowProgressBudget,
  type WorkflowProgressLimits,
} from "./sandboxWorker.ts";

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
            Buffer: typeof Buffer,
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
        Buffer: "undefined",
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
      expect(WORKFLOW_SANDBOX_PROCESS_LIMITS.maxOldSpaceSizeMb).toBeLessThanOrEqual(256);
      expect(WORKFLOW_SANDBOX_PROCESS_LIMITS.maxSemiSpaceSizeMb).toBeLessThanOrEqual(16);
      expect(WORKFLOW_SANDBOX_PROCESS_LIMITS.rlimitDataKiB).toBe(1024 * 1024);
      expect(WORKFLOW_SANDBOX_PROCESS_LIMITS.nodeFlags).toEqual([
        "--max-old-space-size=256",
        "--max-semi-space-size=16",
      ]);

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
      ).rejects.toThrow(/exceeded its memory limit or crashed/i);

      await expect(
        runWorkflowScript({
          script: makeScript(`return "still-alive";`),
          args: {},
          host: makeHost(),
          concurrency: 1,
        }),
      ).resolves.toBe("still-alive");
    });

    it("resolves sandbox entry paths across dev, bundled dist, app.asar, server.asar, and SEA builds", () => {
      expect(
        resolveWorkflowSandboxEntryPath("file:///repo/apps/server/src/workflows/sandbox.ts", {
          windows: false,
        }),
      ).toBe("/repo/apps/server/src/bin.ts");
      expect(
        resolveWorkflowSandboxEntryPath(
          "file:///Applications/T3%20Code.app/Contents/Resources/app.asar/apps/server/dist/bin.mjs",
          { windows: false },
        ),
      ).toBe("/Applications/T3 Code.app/Contents/Resources/app.asar/apps/server/dist/bin.mjs");
      expect(
        resolveWorkflowSandboxEntryPath(
          "file:///C:/Program%20Files/T3%20Code/resources/server.asar/apps/server/dist/bin.mjs",
          { windows: true },
        ),
      ).toBe("C:\\Program Files\\T3 Code\\resources\\server.asar\\apps\\server\\dist\\bin.mjs");
      expect(
        resolveWorkflowSandboxEntryPath("file:///usr/lib/node_modules/t3/dist/sandbox-chunk.mjs", {
          windows: false,
        }),
      ).toBe("/usr/lib/node_modules/t3/dist/bin.mjs");

      const linuxCmd = buildWorkflowSandboxCommand({
        platform: "linux",
        execPath: "/usr/bin/node",
        isExecutable: false,
        entryPath: "/app/resources/app.asar/apps/server/dist/bin.mjs",
      });
      expect(linuxCmd).toEqual({
        command: "/bin/sh",
        args: [
          "-c",
          'ulimit -d 1048576 && exec "$0" "$@"',
          "/usr/bin/node",
          "--max-old-space-size=256",
          "--max-semi-space-size=16",
          "/app/resources/app.asar/apps/server/dist/bin.mjs",
          WORKFLOW_SANDBOX_WORKER_ARG,
        ],
        env: { ELECTRON_RUN_AS_NODE: "1" },
      });

      const darwinAsarCmd = buildWorkflowSandboxCommand({
        platform: "darwin",
        execPath: "/Applications/T3 Code.app/Contents/MacOS/T3 Code",
        isExecutable: false,
        entryPath: "/Applications/T3 Code.app/Contents/Resources/app.asar/apps/server/dist/bin.mjs",
      });
      expect(darwinAsarCmd).toEqual({
        command: "/bin/sh",
        args: [
          "-c",
          'ulimit -d 1048576 && exec "$0" "$@"',
          "/Applications/T3 Code.app/Contents/MacOS/T3 Code",
          "--max-old-space-size=256",
          "--max-semi-space-size=16",
          "/Applications/T3 Code.app/Contents/Resources/app.asar/apps/server/dist/bin.mjs",
          WORKFLOW_SANDBOX_WORKER_ARG,
        ],
        env: { ELECTRON_RUN_AS_NODE: "1" },
      });

      const winAsarCmd = buildWorkflowSandboxCommand({
        platform: "win32",
        execPath: "C:\\Program Files\\T3 Code\\T3 Code.exe",
        isExecutable: false,
        entryPath:
          "C:\\Program Files\\T3 Code\\resources\\server.asar\\apps\\server\\dist\\bin.mjs",
      });
      expect(winAsarCmd).toEqual({
        command: "C:\\Program Files\\T3 Code\\T3 Code.exe",
        args: [
          "--max-old-space-size=256",
          "--max-semi-space-size=16",
          "C:\\Program Files\\T3 Code\\resources\\server.asar\\apps\\server\\dist\\bin.mjs",
          WORKFLOW_SANDBOX_WORKER_ARG,
        ],
        env: { ELECTRON_RUN_AS_NODE: "1" },
      });

      // Packaged SEA binaries receive CLI flags in process.argv, so heap limits move to NODE_OPTIONS.
      const linuxSeaCmd = buildWorkflowSandboxCommand({
        platform: "linux",
        execPath: "/usr/local/bin/t3",
        isExecutable: true,
      });
      expect(linuxSeaCmd).toEqual({
        command: "/bin/sh",
        args: [
          "-c",
          'ulimit -d 1048576 && exec "$0" "$@"',
          "/usr/local/bin/t3",
          WORKFLOW_SANDBOX_WORKER_ARG,
        ],
        env: {
          ELECTRON_RUN_AS_NODE: "1",
          NODE_OPTIONS: "--max-old-space-size=256 --max-semi-space-size=16",
        },
      });

      const winSeaCmd = buildWorkflowSandboxCommand({
        platform: "win32",
        execPath: "C:\\t3\\t3.exe",
        isExecutable: true,
      });
      expect(winSeaCmd).toEqual({
        command: "C:\\t3\\t3.exe",
        args: [WORKFLOW_SANDBOX_WORKER_ARG],
        env: {
          ELECTRON_RUN_AS_NODE: "1",
          NODE_OPTIONS: "--max-old-space-size=256 --max-semi-space-size=16",
        },
      });
    });

    it("completes the worker handshake when V8 heap flags arrive through NODE_OPTIONS as in SEA builds", async () => {
      const entryPath = resolveWorkflowSandboxEntryPath();
      const result = await runWorkflowScript({
        script: makeScript(`return { greeting: "hello " + args.name };`),
        args: { name: "sea" },
        host: makeHost(),
        concurrency: 1,
        spawnCommand: {
          command: process.execPath,
          args: [entryPath, WORKFLOW_SANDBOX_WORKER_ARG],
          env: {
            ELECTRON_RUN_AS_NODE: "1",
            NODE_OPTIONS: WORKFLOW_SANDBOX_PROCESS_LIMITS.nodeFlags.join(" "),
          },
        },
      });
      expect(result).toEqual({ greeting: "hello sea" });
    });

    it("fails promptly when a spawned child stalls before sending ready and distinguishes kill errors from confirmed exit", async () => {
      await expect(
        runWorkflowScript({
          script: makeScript("return 1;"),
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          handshakeTimeoutMs: 120,
          spawnCommand: {
            command: process.execPath,
            args: ["-e", "setInterval(() => {}, 60_000);"],
            env: { ELECTRON_RUN_AS_NODE: "1" },
          },
        }),
      ).rejects.toThrow(/startup handshake/i);

      // A kill failure other than ESRCH must not claim the child exited and must release retry-delay exit waiters.
      let destroyed = 0;
      let confirmedExit = false;
      let killAttempts = 0;
      const activeEpermListeners = new Set<() => void>();
      let peakEpermListeners = 0;
      const epermOutcome = await terminateSandboxChild({
        child: {
          pid: 999_999,
          exitCode: null,
          signalCode: null,
          stdin: {
            destroy: () => {
              destroyed += 1;
            },
          },
          stdout: {
            destroy: () => {
              destroyed += 1;
            },
          },
          stderr: {
            destroy: () => {
              destroyed += 1;
            },
          },
          kill: () => {
            killAttempts += 1;
            const err = new Error("Operation not permitted") as Error & { code: string };
            err.code = "EPERM";
            throw err;
          },
        },
        isExited: () => confirmedExit,
        markExited: () => {
          confirmedExit = true;
        },
        onExit: (listener) => {
          activeEpermListeners.add(listener);
          if (activeEpermListeners.size > peakEpermListeners) {
            peakEpermListeners = activeEpermListeners.size;
          }
          return () => {
            activeEpermListeners.delete(listener);
          };
        },
        maxKillAttempts: 3,
        retryDelayMs: 5,
      });
      expect(destroyed).toBe(3);
      expect(killAttempts).toBe(3);
      expect(confirmedExit).toBe(false);
      expect(peakEpermListeners).toBe(1);
      expect(activeEpermListeners.size).toBe(0);
      expect(epermOutcome).toMatchObject({
        status: "kill_failed",
        pid: 999_999,
        attempts: 3,
      });

      // ESRCH means the process is already gone and confirms exit.
      let esrchConfirmed = false;
      const esrchOutcome = await terminateSandboxChild({
        child: {
          pid: 999_999,
          exitCode: null,
          signalCode: null,
          stdin: { destroy: () => {} },
          stdout: { destroy: () => {} },
          stderr: { destroy: () => {} },
          kill: () => {
            const err = new Error("No such process") as Error & { code: string };
            err.code = "ESRCH";
            throw err;
          },
        },
        isExited: () => esrchConfirmed,
        markExited: () => {
          esrchConfirmed = true;
        },
        onExit: () => () => {},
        retryDelayMs: 0,
      });
      expect(esrchConfirmed).toBe(true);
      expect(esrchOutcome).toEqual({ status: "exited" });

      // A successful kill("SIGKILL") that never emits exit/close must be bounded, return kill_failed,
      // and release every exit-wait subscription when the timer wins so listeners never accumulate.
      let hangKillAttempts = 0;
      let hangConfirmed = false;
      const activeHangListeners = new Set<() => void>();
      let peakHangListeners = 0;
      const hangOutcome = await terminateSandboxChild({
        child: {
          pid: 999_998,
          exitCode: null,
          signalCode: null,
          stdin: { destroy: () => {} },
          stdout: { destroy: () => {} },
          stderr: { destroy: () => {} },
          kill: () => {
            hangKillAttempts += 1;
            return true;
          },
        },
        isExited: () => hangConfirmed,
        markExited: () => {
          hangConfirmed = true;
        },
        onExit: (listener) => {
          activeHangListeners.add(listener);
          if (activeHangListeners.size > peakHangListeners) {
            peakHangListeners = activeHangListeners.size;
          }
          return () => {
            activeHangListeners.delete(listener);
          };
        },
        maxKillAttempts: 3,
        retryDelayMs: 0,
        exitConfirmTimeoutMs: 5,
      });
      expect(hangKillAttempts).toBe(3);
      expect(hangConfirmed).toBe(false);
      expect(peakHangListeners).toBe(1);
      expect(activeHangListeners.size).toBe(0);
      expect(hangOutcome).toMatchObject({
        status: "kill_failed",
        pid: 999_998,
        attempts: 3,
      });
    });

    it("retries transient kill failures in runWorkflowScript, keeps ownership on persistent kill failures, and distinguishes spawn errors from live-PID errors", async () => {
      // 1. Transient EPERM kill failure succeeds on bounded retry and confirms exit on real child exit/close,
      //    and worker.onExit unsubscribes cancelled listeners cleanly.
      let transientAttempts = 0;
      let cancelledListenerCalls = 0;
      let activeListenerCalls = 0;
      await expect(
        runWorkflowScript({
          script: makeScript("return 1;"),
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          handshakeTimeoutMs: 80,
          maxKillAttempts: 4,
          killRetryDelayMs: 0,
          onWorkerSpawned: (worker) => {
            const unsub = worker.onExit(() => {
              cancelledListenerCalls += 1;
            });
            unsub();
            worker.onExit(() => {
              activeListenerCalls += 1;
            });
          },
          spawnCommand: {
            command: process.execPath,
            args: ["-e", "setInterval(() => {}, 60_000);"],
            env: { ELECTRON_RUN_AS_NODE: "1" },
          },
          spawnChild: (command, args, opts) => {
            const realChild = NodeChildProcess.spawn(command, args, opts);
            const origKill = realChild.kill.bind(realChild);
            realChild.kill = ((signal?: NodeJS.Signals | number) => {
              transientAttempts += 1;
              if (transientAttempts < 3) {
                const err = new Error("Operation not permitted") as Error & { code: string };
                err.code = "EPERM";
                throw err;
              }
              return origKill(signal);
            }) as typeof realChild.kill;
            return realChild;
          },
        }),
      ).rejects.toThrow(/startup handshake/i);
      expect(transientAttempts).toBe(3);
      expect(cancelledListenerCalls).toBe(0);
      expect(activeListenerCalls).toBe(1);

      // 2. Persistent EPERM kill failure retries boundedly, keeps PID ownership, reports cleanup failure,
      //    and rejects with WorkflowSandboxCleanupError until real exit releases ownership.
      let persistentAttempts = 0;
      let leakedChild: NodeChildProcess.ChildProcessWithoutNullStreams | undefined;
      let realKill: NodeChildProcess.ChildProcessWithoutNullStreams["kill"] | undefined;
      const cleanupFailures: Array<WorkflowSandboxCleanupFailure> = [];
      try {
        let caught: unknown;
        try {
          await runWorkflowScript({
            script: makeScript("return 1;"),
            args: undefined,
            host: makeHost(),
            concurrency: 1,
            handshakeTimeoutMs: 80,
            maxKillAttempts: 3,
            killRetryDelayMs: 0,
            onCleanupFailure: (failure) => {
              cleanupFailures.push(failure);
            },
            spawnCommand: {
              command: process.execPath,
              args: ["-e", "setInterval(() => {}, 60_000);"],
              env: { ELECTRON_RUN_AS_NODE: "1" },
            },
            spawnChild: (command, args, opts) => {
              const child = NodeChildProcess.spawn(command, args, opts);
              leakedChild = child;
              realKill = child.kill.bind(child);
              child.kill = (() => {
                persistentAttempts += 1;
                const err = new Error("Operation not permitted") as Error & { code: string };
                err.code = "EPERM";
                throw err;
              }) as typeof child.kill;
              return child;
            },
          });
        } catch (err) {
          caught = err;
        }
        expect(caught).toBeInstanceOf(WorkflowSandboxCleanupError);
        expect(persistentAttempts).toBe(3);
        const leakedPid = leakedChild?.pid;
        expect(leakedPid).toBeDefined();
        expect(cleanupFailures).toHaveLength(1);
        expect(cleanupFailures[0]?.pid).toBe(leakedPid);
        expect(cleanupFailures[0]?.attempts).toBe(3);
        expect(getUnconfirmedSandboxChildPids()).toContain(leakedPid);

        // Restoring kill allows reapUnconfirmedSandboxChildren() to retry and release ownership on exit.
        leakedChild!.kill = realKill!;
        await reapUnconfirmedSandboxChildren();
        expect(getUnconfirmedSandboxChildPids()).not.toContain(leakedPid);
      } finally {
        if (leakedChild !== undefined && realKill !== undefined && leakedChild.exitCode === null) {
          realKill("SIGKILL");
        }
      }

      // 3. An "error" event on a live spawned PID does NOT mark exited: runner still SIGKILLs and waits for exit.
      let liveErrorKillCalled = false;
      let liveChildExitCode: number | null = null;
      let liveChildSignal: NodeJS.Signals | null = null;
      const liveErrRun = runWorkflowScript({
        script: makeScript("return 1;"),
        args: undefined,
        host: makeHost(),
        concurrency: 1,
        handshakeTimeoutMs: 5_000,
        spawnCommand: {
          command: process.execPath,
          args: ["-e", "setInterval(() => {}, 60_000);"],
          env: { ELECTRON_RUN_AS_NODE: "1" },
        },
        spawnChild: (command, args, opts) => {
          const child = NodeChildProcess.spawn(command, args, opts);
          const origKill = child.kill.bind(child);
          child.kill = ((signal?: NodeJS.Signals | number) => {
            liveErrorKillCalled = true;
            return origKill(signal);
          }) as typeof child.kill;
          child.once("exit", (code, signal) => {
            liveChildExitCode = code;
            liveChildSignal = signal;
          });
          child.once("spawn", () => {
            const err = new Error("simulated live process error") as Error & { code: string };
            err.code = "EPIPE";
            child.emit("error", err);
          });
          return child;
        },
      });
      await expect(liveErrRun).rejects.toThrow(
        /Workflow sandbox process error \(EPIPE\): simulated live process error/,
      );
      expect(liveErrorKillCalled).toBe(true);
      expect(liveChildExitCode !== null || liveChildSignal !== null).toBe(true);

      // 4. A real spawn failure (ENOENT, no PID) reports "failed to start" cleanly without hanging.
      await expect(
        runWorkflowScript({
          script: makeScript("return 1;"),
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          spawnCommand: {
            command: "/nonexistent-t3-workflow-sandbox-binary",
            args: [],
            env: { ELECTRON_RUN_AS_NODE: "1" },
          },
        }),
      ).rejects.toThrow(/Workflow sandbox failed to start \(ENOENT\)/);
    });

    it.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
      "runs the sandbox child under the 1 GiB data rlimit on Linux",
      async () => {
        const { command, args, env } = buildWorkflowSandboxCommand();
        const child = NodeChildProcess.spawn(command, args, {
          stdio: ["pipe", "pipe", "pipe"],
          env,
          windowsHide: true,
        });
        child.stderr.resume();

        const readyPromise = new Promise<void>((resolve, reject) => {
          let buf = "";
          child.once("error", reject);
          child.once("close", (code, signal) =>
            reject(new Error(`Worker exited before ready (${String(code)}/${String(signal)})`)),
          );
          child.stdout.on("data", (chunk: Buffer) => {
            buf += chunk.toString("utf8");
            if (buf.includes('{"type":"ready"}\n')) resolve();
          });
        });

        try {
          await readyPromise;
          expect(child.pid).toBeDefined();
          const line = NodeFS.readFileSync(`/proc/${child.pid}/limits`, "utf8")
            .split("\n")
            .find((entry) => entry.startsWith("Max data size"));
          const expectedBytes = String(WORKFLOW_SANDBOX_PROCESS_LIMITS.rlimitDataKiB * 1024);
          expect(line ?? "").toContain(expectedBytes);
        } finally {
          child.kill("SIGKILL");
        }
      },
    );

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
  describe("off-heap allocations and progress budgets", () => {
    it("blocks allocators that live outside the worker heap", async () => {
      for (const code of [
        "return new ArrayBuffer(1e9);",
        "return new Uint8Array(1e9);",
        "return new SharedArrayBuffer(8);",
        "return new WebAssembly.Memory({ initial: 1 });",
        "return new DataView(new ArrayBuffer(8));",
      ]) {
        await expect(
          runWorkflowScript({
            script: makeScript(code),
            args: undefined,
            host: makeHost(),
            concurrency: 1,
          }),
        ).rejects.toThrow(/not available in workflow scripts/);
      }
    });

    it("rejects an oversized script before scanning it, counting UTF-8 bytes", () => {
      const asciiOver = "x".repeat(WORKFLOW_MESSAGE_LIMITS.maxValueBytes + 1);
      expect(() => parseWorkflowMeta(asciiOver)).toThrow(/exceeds the 4 MiB limit/);

      const utf8Over = `export const meta = { name: "ok", description: "ok" };\n// ${"é".repeat(2 * 1024 * 1024)}`;
      expect(utf8Over.length).toBeLessThan(WORKFLOW_MESSAGE_LIMITS.maxValueBytes);
      expect(Buffer.byteLength(utf8Over, "utf8")).toBeGreaterThan(
        WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
      );
      expect(() => parseWorkflowMeta(utf8Over)).toThrow(WorkflowScriptError);
      expect(() => parseWorkflowMeta(utf8Over)).toThrow(/exceeds the 4 MiB limit/);
    });

    it("rejects meta.phases past the entry count, title byte, and 64 KiB total byte caps", async () => {
      const tooManyEntries = Array.from(
        { length: WORKFLOW_PROGRESS_LIMITS.maxPhaseTitles + 1 },
        (_, i) => `{ title: "P${i}" }`,
      ).join(",");
      expect(() =>
        parseWorkflowMeta(
          `export const meta = { name: "a", description: "d", phases: [${tooManyEntries}] };`,
        ),
      ).toThrow(/at most 256 entries/);

      const longTitle = "é".repeat(WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes / 2 + 1);
      expect(() =>
        parseWorkflowMeta(
          `export const meta = { name: "a", description: "d", phases: [{ title: "${longTitle}" }] };`,
        ),
      ).toThrow(/at most 1024 bytes/);

      const bigDetail = "d".repeat(WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleTotalBytes);
      const bigDetailScript = `export const meta = { name: "a", description: "d", phases: [{ title: "A", detail: "${bigDetail}" }] };`;
      expect(() => parseWorkflowMeta(bigDetailScript)).toThrow(/64 KiB limit/);
      await expect(
        runWorkflowScript({
          script: bigDetailScript,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(/64 KiB limit/);

      const bigModel = "m".repeat(WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleTotalBytes);
      expect(() =>
        parseWorkflowMeta(
          `export const meta = { name: "a", description: "d", phases: [{ title: "A", model: "${bigModel}" }] };`,
        ),
      ).toThrow(/64 KiB limit/);

      const manyLargeTitles = Array.from(
        { length: 65 },
        (_, i) => `{ title: "${String(i).padStart(4, "0")}${"x".repeat(1020)}" }`,
      ).join(",");
      expect(() =>
        parseWorkflowMeta(
          `export const meta = { name: "a", description: "d", phases: [${manyLargeTitles}] };`,
        ),
      ).toThrow(/64 KiB limit/);
    });

    it("fails the run when an unterminated child stdout line exceeds 4 MiB", () => {
      const lines: Array<{ readonly line: string; readonly bytes: number }> = [];
      let overflowed = 0;
      const splitter = makeBoundedLineSplitter({
        maxLineBytes: WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
        onLine: (line, bytes) => lines.push({ line, bytes }),
        onOverflow: () => {
          overflowed += 1;
        },
      });
      // Split a 2-byte UTF-8 sequence across chunks to verify byte-level buffering.
      const encoded = Buffer.from('{"a":"é"}\n', "utf8");
      const splitAt = encoded.indexOf(0xc3) + 1;
      splitter.push(encoded.subarray(0, splitAt));
      splitter.push(encoded.subarray(splitAt));
      expect(lines).toEqual([{ line: '{"a":"é"}', bytes: encoded.byteLength - 1 }]);

      const splitterHalf = Buffer.alloc(WORKFLOW_MESSAGE_LIMITS.maxValueBytes / 2, 0x61);
      splitter.push(splitterHalf);
      splitter.push(splitterHalf);
      expect(overflowed).toBe(0);
      splitter.push(Buffer.from("x"));
      expect(overflowed).toBe(1);

      const receiverHalf = Buffer.alloc(MAX_VALUE_MESSAGE_BYTES / 2, 0x61);
      let hostError: unknown;
      const receiver = createWorkflowHostMessageReceiver({
        script: makeScript("return 1;"),
        argsJson: undefined,
        host: makeHost(),
        concurrency: 1,
        syncTimeoutMs: 5_000,
        initialSpent: 0,
        postToChild: () => true,
        onIdleSettled: () => {},
        onSucceed: () => {},
        onFail: (err) => {
          hostError = err;
        },
        isSettled: () => hostError !== undefined,
      });
      receiver.push(receiverHalf);
      receiver.push(receiverHalf);
      receiver.push(Buffer.from("z"));
      expect(hostError).toBeInstanceOf(WorkflowScriptError);
      expect((hostError as Error).message).toBe("Workflow message exceeded the 4 MiB limit.");
    });

    it("accepts scripts, args, nested scripts, and phase titles within decoded limits even with heavy JSON escaping", async () => {
      // 1024-byte phase title of quotes and control chars expands past old maxPhaseTitleBytes + 256.
      const escapedPhaseTitle = `a${"\u0000".repeat(511)}${'"'.repeat(512)}`;
      expect(Buffer.byteLength(escapedPhaseTitle, "utf8")).toBe(
        WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes,
      );
      const seenPhases: Array<string> = [];
      await expect(
        runWorkflowScript({
          script: makeScript(`phase(${JSON.stringify(escapedPhaseTitle)}); return "ok";`),
          args: undefined,
          host: makeHost({ onPhase: (title) => seenPhases.push(title) }),
          concurrency: 1,
        }),
      ).resolves.toBe("ok");
      expect(seenPhases).toEqual([escapedPhaseTitle]);

      // Script (~3 MiB of quotes -> ~6 MiB JSON-escaped) + args (~3.5 MiB) exceed old 8 MiB start cap.
      const heavyScript = makeScript(`// ${'"'.repeat(3 * 1024 * 1024)}\nreturn args.data.length;`);
      expect(Buffer.byteLength(heavyScript, "utf8")).toBeLessThan(
        WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
      );
      const heavyArgs = { data: "a".repeat(3.5 * 1024 * 1024) };
      expect(Buffer.byteLength(JSON.stringify(heavyArgs), "utf8")).toBeLessThan(
        WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
      );
      await expect(
        runWorkflowScript({
          script: heavyScript,
          args: heavyArgs,
          host: makeHost(),
          concurrency: 1,
        }),
      ).resolves.toBe(3.5 * 1024 * 1024);

      // Nested script (~3 MiB of quotes -> ~6 MiB JSON-escaped) passes 4 MiB decoded cap.
      const heavyNestedScript = makeScript(
        `// ${'"'.repeat(3 * 1024 * 1024)}\nreturn "nested-ok";`,
        `{ name: "nested", description: "nested heavy" }`,
      );
      expect(Buffer.byteLength(heavyNestedScript, "utf8")).toBeLessThan(
        WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
      );
      await expect(
        runWorkflowScript({
          script: makeScript(`return await workflow({ scriptPath: "/heavy-nested.js" });`),
          args: undefined,
          host: makeHost({
            loadScript: async () => heavyNestedScript,
          }),
          concurrency: 1,
        }),
      ).resolves.toBe("nested-ok");
    });

    it("caps aggregate serialized phase output across repeated titles and caps worker stdout backlog", async () => {
      // Repeating two 1024-byte titles stays within distinct-title budget but exceeds 4 MiB emitted phase bytes.
      const repeatedFlood = makeScript(`
        const a = "a".repeat(${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes});
        const b = "b".repeat(${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes});
        for (let i = 0; i < 4500; i += 1) {
          try { phase(i % 2 === 0 ? a : b); } catch {}
        }
        return "done";
      `);
      await expect(
        runWorkflowScript({
          script: repeatedFlood,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(/phase\(\) output exceeds/i);

      // When stdout buffers past maxStdoutBufferedBytes, the worker rejects with a sticky cap failure.
      const workerModuleUrl = new URL("./sandboxWorker.ts", import.meta.url).href;
      const corkedWorkerScript = `
        const origWrite = process.stdout.write.bind(process.stdout);
        let readySent = false;
        let uncorkScheduled = false;
        process.stdout.write = (chunk, ...rest) => {
          const text = typeof chunk === "string" ? chunk : chunk.toString("utf8");
          if (readySent && text === "" && !uncorkScheduled) {
            uncorkScheduled = true;
            setImmediate(() => process.stdout.uncork());
            return origWrite(chunk, ...rest);
          }
          if (text.includes('"type":"error"') || text.includes('"type":"done"')) {
            process.stdout.uncork();
            return origWrite(chunk, ...rest);
          }
          const ok = origWrite(chunk, ...rest);
          if (readySent && process.stdout.writableLength > 1024) {
            process.exit(42);
          }
          if (!readySent && text.includes('"type":"ready"')) {
            readySent = true;
            process.stdout.cork();
          }
          return ok;
        };
        import(${JSON.stringify(workerModuleUrl)}).then((m) => m.runWorkflowSandboxWorker());
      `;
      const backlogFlood = makeScript(`
        const msg = "x".repeat(600);
        for (let i = 0; i < 10; i += 1) {
          try { log(msg); } catch {}
        }
        return "done";
      `);
      await expect(
        runWorkflowScript({
          script: backlogFlood,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          maxStdoutBufferedBytes: 1024,
          spawnCommand: {
            command: process.execPath,
            args: ["-e", corkedWorkerScript],
            env: { ELECTRON_RUN_AS_NODE: "1" },
          },
        }),
      ).rejects.toThrow(/output backlog exceeded/i);
    });

    it("flushes queued worker stdout before writing terminal done and error frames and enforces framing + newline within the backlog cap", async () => {
      const workerModuleUrl = new URL("./sandboxWorker.ts", import.meta.url).href;
      const cap = 1024;
      const guardedCorkWorkerScript = `
        const origWrite = process.stdout.write.bind(process.stdout);
        let readySent = false;
        let uncorkScheduled = false;
        process.stdout.write = (chunk, ...rest) => {
          const text = typeof chunk === "string" ? chunk : chunk.toString("utf8");
          if (readySent && text === "" && !uncorkScheduled) {
            uncorkScheduled = true;
            setImmediate(() => process.stdout.uncork());
            return origWrite(chunk, ...rest);
          }
          const ok = origWrite(chunk, ...rest);
          if (readySent && process.stdout.writableLength > ${cap}) {
            process.exit(42);
          }
          if (!readySent && text.includes('"type":"ready"')) {
            readySent = true;
            process.stdout.cork();
          } else if (text.includes('"type":"done"') || text.includes('"type":"error"')) {
            process.stdout.uncork();
          }
          return ok;
        };
        import(${JSON.stringify(workerModuleUrl)}).then((m) => m.runWorkflowSandboxWorker());
      `;
      const result = await runWorkflowScript({
        script: makeScript(`
          log("x".repeat(600));
          return "y".repeat(600);
        `),
        args: undefined,
        host: makeHost(),
        concurrency: 1,
        maxStdoutBufferedBytes: cap,
        spawnCommand: {
          command: process.execPath,
          args: ["-e", guardedCorkWorkerScript],
          env: { ELECTRON_RUN_AS_NODE: "1" },
        },
      });
      expect(result).toBe("y".repeat(600));

      // Terminal error frames must also drain queued stdout instead of overflowing the backlog cap.
      await expect(
        runWorkflowScript({
          script: makeScript(`
            log("x".repeat(600));
            throw new Error("boom-" + "e".repeat(500));
          `),
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          maxStdoutBufferedBytes: cap,
          spawnCommand: {
            command: process.execPath,
            args: ["-e", guardedCorkWorkerScript],
            env: { ELECTRON_RUN_AS_NODE: "1" },
          },
        }),
      ).rejects.toThrow(/boom-e+/);

      // Even with an empty queue, a done frame that fits before framing but exceeds the cap with framing + newline is rejected.
      await expect(
        runWorkflowScript({
          script: makeScript(`
            return "z".repeat(1010);
          `),
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          maxStdoutBufferedBytes: cap,
          spawnCommand: {
            command: process.execPath,
            args: ["-e", guardedCorkWorkerScript],
            env: { ELECTRON_RUN_AS_NODE: "1" },
          },
        }),
      ).rejects.toThrow(/exceeds.*limit/i);

      // Boundary below the minimum error-frame size: runner rejects before spawning, and the worker writer refuses oversized frames even on an empty queue.
      const minErrorFrameBytes =
        Buffer.byteLength(
          JSON.stringify({ type: "error", kind: "WorkflowScriptError", message: "" }),
          "utf8",
        ) + 1;
      expect(MIN_WORKER_STDOUT_BUFFERED_BYTES).toBe(minErrorFrameBytes);
      let spawnedBelowMin = false;
      await expect(
        runWorkflowScript({
          script: makeScript("return 1;"),
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          maxStdoutBufferedBytes: minErrorFrameBytes - 1,
          spawnChild: (...args) => {
            spawnedBelowMin = true;
            return NodeChildProcess.spawn(...args);
          },
        }),
      ).rejects.toThrow(new RegExp(`at least ${minErrorFrameBytes}`));
      expect(spawnedBelowMin).toBe(false);

      expect(() =>
        createWorkflowHostMessageReceiver({
          script: makeScript("return 1;"),
          argsJson: undefined,
          host: makeHost(),
          concurrency: 1,
          syncTimeoutMs: 5_000,
          initialSpent: 0,
          maxStdoutBufferedBytes: minErrorFrameBytes - 1,
          postToChild: () => true,
          onIdleSettled: () => {},
          onSucceed: () => {},
          onFail: () => {},
          isSettled: () => false,
        }),
      ).toThrow(new RegExp(`at least ${minErrorFrameBytes}`));

      // Exact minimum error-frame cap is accepted and keeps terminal error frames within the cap.
      const exactMinCapScript = `
        const origWrite = process.stdout.write.bind(process.stdout);
        let readySent = false;
        process.stdout.write = (chunk, ...rest) => {
          const text = typeof chunk === "string" ? chunk : chunk.toString("utf8");
          if (readySent && Buffer.byteLength(text, "utf8") > ${minErrorFrameBytes}) {
            process.exit(43);
          }
          const ok = origWrite(chunk, ...rest);
          if (!readySent && text.includes('"type":"ready"')) {
            readySent = true;
          }
          return ok;
        };
        import(${JSON.stringify(workerModuleUrl)}).then((m) => m.runWorkflowSandboxWorker());
      `;
      await expect(
        runWorkflowScript({
          script: makeScript(`return "z".repeat(200);`),
          args: undefined,
          host: makeHost(),
          concurrency: 1,
          maxStdoutBufferedBytes: minErrorFrameBytes,
          spawnCommand: {
            command: process.execPath,
            args: ["-e", exactMinCapScript],
            env: { ELECTRON_RUN_AS_NODE: "1" },
          },
        }),
      ).rejects.toBeInstanceOf(WorkflowScriptError);

      // Direct worker with a cap below the empty error envelope must not write an oversized frame even on an empty queue.
      const belowMinCap = minErrorFrameBytes - 1;
      const directWorker = NodeChildProcess.spawn(
        process.execPath,
        [
          "-e",
          `import(${JSON.stringify(workerModuleUrl)}).then((m) => m.runWorkflowSandboxWorker());`,
        ],
        { stdio: ["pipe", "pipe", "pipe"], env: { ELECTRON_RUN_AS_NODE: "1" } },
      );
      const directStdoutLines: Array<string> = [];
      let directStdoutBuf = "";
      const directReady = Promise.withResolvers<void>();
      directWorker.stdout.on("data", (chunk: Buffer) => {
        directStdoutBuf += chunk.toString("utf8");
        let idx = directStdoutBuf.indexOf("\n");
        while (idx !== -1) {
          const line = directStdoutBuf.slice(0, idx);
          directStdoutBuf = directStdoutBuf.slice(idx + 1);
          directStdoutLines.push(line);
          if (line.includes('"type":"ready"')) {
            directReady.resolve();
          }
          idx = directStdoutBuf.indexOf("\n");
        }
      });
      const directClosed = new Promise<number | null>((resolve) => {
        directWorker.once("close", (code) => resolve(code));
      });
      try {
        await directReady.promise;
        directWorker.stdin.write(
          `${JSON.stringify({
            type: "start",
            seq: 1,
            script: makeScript(`return "z".repeat(200);`),
            hasArgs: false,
            concurrency: 1,
            syncTimeoutMs: 5_000,
            budgetTotal: null,
            budgetSpent: 0,
            maxAgents: WORKFLOW_LIMITS.maxAgents,
            maxItems: WORKFLOW_LIMITS.maxItems,
            maxPromptBytes: WORKFLOW_MESSAGE_LIMITS.maxPromptBytes,
            maxValueBytes: WORKFLOW_MESSAGE_LIMITS.maxValueBytes,
            maxLogChars: WORKFLOW_MESSAGE_LIMITS.maxLogChars,
            progressLimits: WORKFLOW_PROGRESS_LIMITS,
            maxStdoutBufferedBytes: belowMinCap,
          })}\n`,
        );
        const exitCode = await directClosed;
        expect(exitCode).not.toBe(0);
        const postReadyLines = directStdoutLines.slice(1);
        for (const line of postReadyLines) {
          expect(Buffer.byteLength(`${line}\n`, "utf8")).toBeLessThanOrEqual(belowMinCap);
        }
      } finally {
        if (directWorker.exitCode === null && directWorker.signalCode === null) {
          directWorker.kill("SIGKILL");
        }
      }
    });

    it("limits in-flight nested loadScript requests on the host before opening files", async () => {
      const gate = Promise.withResolvers<void>();
      let opened = 0;
      const limit = WORKFLOW_LIMITS.maxInFlightScriptLoads;

      const runPromise = runWorkflowScript({
        script: makeScript(`
          const promises = Array.from({ length: ${limit + 4} }, (_, i) =>
            workflow({ scriptPath: "/nested-" + i + ".js" }),
          );
          // Ordered after all loadScript frames on the stdout pipe so the host sees the full batch before gate releases.
          phase("all-sent");
          const settled = await Promise.allSettled(promises);
          const after = await workflow({ scriptPath: "/nested-after.js" });
          return {
            fulfilled: settled.filter((r) => r.status === "fulfilled").map((r) => r.value),
            rejected: settled
              .filter((r) => r.status === "rejected")
              .map((r) => ({ name: r.reason?.name, message: r.reason?.message })),
            after,
          };
        `),
        args: undefined,
        host: makeHost({
          onPhase: (title) => {
            if (title === "all-sent") {
              gate.resolve();
            }
          },
          loadScript: async ({ scriptPath }) => {
            opened += 1;
            if (scriptPath === "/nested-after.js") {
              return makeScript(`return "after-ok";`);
            }
            await gate.promise;
            return makeScript(`return ${JSON.stringify(scriptPath)};`);
          },
        }),
        concurrency: 1,
      });

      const outcome = (await runPromise) as {
        readonly fulfilled: ReadonlyArray<string>;
        readonly rejected: ReadonlyArray<{ readonly name?: string; readonly message?: string }>;
        readonly after: string;
      };
      expect(opened).toBe(limit + 1);
      expect(outcome.fulfilled).toHaveLength(limit);
      expect(outcome.rejected).toHaveLength(4);
      for (const rejection of outcome.rejected) {
        expect(rejection.name).toBe("WorkflowScriptError");
        expect(rejection.message).toMatch(/too many concurrent workflow\(\) script loads/i);
      }
      expect(outcome.after).toBe("after-ok");

      // Direct forged loadScript frames on the host receiver obey maxInFlightScriptLoads before opening files.
      const forgedGate = Promise.withResolvers<void>();
      const forgedOpenedBoth = Promise.withResolvers<void>();
      let forgedOpened = 0;
      const posted: Array<Record<string, unknown>> = [];
      const receiver = createWorkflowHostMessageReceiver({
        script: makeScript("return 1;"),
        argsJson: undefined,
        host: makeHost({
          loadScript: async () => {
            forgedOpened += 1;
            if (forgedOpened === 2) {
              forgedOpenedBoth.resolve();
            }
            await forgedGate.promise;
            return makeScript("return 1;");
          },
        }),
        concurrency: 1,
        syncTimeoutMs: 5_000,
        initialSpent: 0,
        maxInFlightScriptLoads: 2,
        postToChild: (message) => {
          posted.push(JSON.parse(message) as Record<string, unknown>);
          return true;
        },
        onIdleSettled: () => {},
        onSucceed: () => {},
        onFail: () => {},
        isSettled: () => false,
      });
      receiver.push(Buffer.from('{"type":"loadScript","id":1,"scriptPath":"/a.js"}\n', "utf8"));
      receiver.push(Buffer.from('{"type":"loadScript","id":2,"scriptPath":"/b.js"}\n', "utf8"));
      receiver.push(Buffer.from('{"type":"loadScript","id":3,"scriptPath":"/c.js"}\n', "utf8"));
      expect(posted).toEqual([
        expect.objectContaining({
          type: "loadScriptResult",
          id: 3,
          ok: false,
          errorKind: "WorkflowScriptError",
        }),
      ]);
      await forgedOpenedBoth.promise;
      forgedGate.resolve();
      expect(forgedOpened).toBe(2);
    });

    it("fails the run once the log budget is spent, even when the script catches", async () => {
      const script = makeScript(`
        for (let i = 0; i <= ${WORKFLOW_PROGRESS_LIMITS.maxLogs}; i++) {
          try { log("line " + i); } catch {}
        }
        return "done";
      `);
      await expect(
        runWorkflowScript({ script, args: undefined, host: makeHost(), concurrency: 1 }),
      ).rejects.toThrow(/logs at most/);
    });

    it("fails the run past the distinct phase, title byte, and total phase byte budgets in worker", async () => {
      const overDistinct = makeScript(`
        for (let i = 0; i <= ${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitles}; i++) {
          try { phase("P" + i); } catch {}
        }
        return "done";
      `);
      await expect(
        runWorkflowScript({
          script: overDistinct,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(/distinct phases/);

      const overSingleTitle = makeScript(`
        try { phase("é".repeat(${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes / 2 + 1})); } catch {}
        return "done";
      `);
      await expect(
        runWorkflowScript({
          script: overSingleTitle,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(/at most 1024 bytes/);

      const overTotalTitleBytes = makeScript(`
        const pad = "x".repeat(1020);
        for (let i = 0; i < 65; i++) {
          try { phase(String(i).padStart(4, "0") + pad); } catch {}
        }
        return "done";
      `);
      await expect(
        runWorkflowScript({
          script: overTotalTitleBytes,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(/64 KiB limit/);

      // Repeating a valid 1024-byte title counts bytes once per distinct title.
      const repeatedTitle = makeScript(`
        const title = "x".repeat(${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes});
        for (let i = 0; i < 100; i++) {
          phase(title);
        }
        return "ok";
      `);
      await expect(
        runWorkflowScript({
          script: repeatedTitle,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).resolves.toBe("ok");

      const agentOverSingleTitle = makeScript(`
        try {
          await agent("p", { phase: "é".repeat(${WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes / 2 + 1}) });
        } catch {}
        return "done";
      `);
      await expect(
        runWorkflowScript({
          script: agentOverSingleTitle,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(/at most 1024 bytes/);

      const agentOverTotalTitleBytes = makeScript(`
        const pad = "x".repeat(1020);
        for (let i = 0; i < 65; i++) {
          try { await agent("p" + i, { phase: String(i).padStart(4, "0") + pad }); } catch {}
        }
        return "done";
      `);
      await expect(
        runWorkflowScript({
          script: agentOverTotalTitleBytes,
          args: undefined,
          host: makeHost(),
          concurrency: 1,
        }),
      ).rejects.toThrow(/64 KiB limit/);
    });

    it("enforces every progress budget in the host-side tracker and forged host messages", () => {
      const limits = {
        maxLogs: 2,
        maxLogBytes: 10,
        maxPhaseTitles: 2,
        maxPhaseTitleBytes: 8,
        maxPhaseTitleTotalBytes: 12,
        maxPhaseSwitches: 4,
        maxPhaseEventBytes: 120,
      };
      const logs = makeWorkflowProgressBudget(limits);
      expect(logs.log("a")).toBeUndefined();
      expect(logs.log("b")).toBeUndefined();
      expect(logs.log("c")).toMatch(/logs at most 2/);
      const bytes = makeWorkflowProgressBudget(limits);
      expect(bytes.log("0123456789x")).toMatch(/exceeds the 4 MiB limit/);

      const phases = makeWorkflowProgressBudget(limits);
      expect(phases.phase("A")).toBeUndefined();
      expect(phases.phase("B")).toBeUndefined();
      expect(phases.phase("A")).toBeUndefined();
      expect(phases.phase("C")).toMatch(/at most 2 distinct phases/);

      const singleBytes = makeWorkflowProgressBudget(limits);
      expect(singleBytes.phase("ééééé")).toMatch(/at most 8 bytes/);

      const totalBytes = makeWorkflowProgressBudget(limits);
      expect(totalBytes.phase("12345678")).toBeUndefined();
      expect(totalBytes.phase("12345678")).toBeUndefined();
      expect(totalBytes.phase("12345")).toMatch(/64 KiB limit/);

      const switches = makeWorkflowProgressBudget(limits);
      for (let i = 0; i < 4; i++) expect(switches.phase("A")).toBeUndefined();
      expect(switches.phase("A")).toMatch(/at most 4 times/);

      // agent() opts.phase registers titles/bytes without counting as a phase switch.
      const agentTitles = makeWorkflowProgressBudget(limits);
      for (let i = 0; i < 10; i++) expect(agentTitles.phaseTitle("12345678")).toBeUndefined();
      expect(agentTitles.phase("12345678")).toBeUndefined();
      expect(agentTitles.phaseTitle("12345")).toMatch(/64 KiB limit/);

      const makeReceiver = (customLimits?: WorkflowProgressLimits) => {
        let failed: unknown;
        const phasesSeen: Array<string> = [];
        const agentsSeen: Array<WorkflowAgentCall> = [];
        const receiver = createWorkflowHostMessageReceiver({
          script: makeScript("return 1;"),
          argsJson: undefined,
          host: makeHost({
            onPhase: (title) => phasesSeen.push(title),
            runAgent: async (call) => {
              agentsSeen.push(call);
              return "ok";
            },
          }),
          concurrency: 1,
          syncTimeoutMs: 5_000,
          initialSpent: 0,
          postToChild: () => true,
          onIdleSettled: () => {},
          onSucceed: () => {},
          onFail: (err) => {
            failed = err;
          },
          isSettled: () => failed !== undefined,
          ...(customLimits !== undefined ? { progressLimits: customLimits } : {}),
        });
        const send = (payload: unknown) => {
          receiver.push(Buffer.from(`${JSON.stringify(payload)}\n`, "utf8"));
        };
        return { send, getFailed: () => failed, phasesSeen, agentsSeen };
      };

      // Forged phase message exceeding the per-message frame ceiling.
      const overPhaseFrame = makeReceiver();
      overPhaseFrame.send({
        type: "phase",
        title: "ok",
        pad: "p".repeat(MAX_PHASE_MESSAGE_BYTES),
      });
      expect(overPhaseFrame.getFailed()).toBeInstanceOf(WorkflowScriptError);
      expect((overPhaseFrame.getFailed() as Error).message).toContain(
        `${MAX_PHASE_MESSAGE_BYTES}-byte limit`,
      );
      expect(overPhaseFrame.phasesSeen).toEqual([]);

      // Forged repeated phase messages exceeding aggregate serialized phase output budget.
      const overPhaseEventBytes = makeReceiver({
        ...WORKFLOW_PROGRESS_LIMITS,
        maxPhaseEventBytes: 60,
      });
      overPhaseEventBytes.send({ type: "phase", title: "RepeatMe" });
      overPhaseEventBytes.send({ type: "phase", title: "RepeatMe" });
      overPhaseEventBytes.send({ type: "phase", title: "RepeatMe" });
      expect(overPhaseEventBytes.getFailed()).toBeInstanceOf(WorkflowScriptError);
      expect((overPhaseEventBytes.getFailed() as Error).message).toMatch(
        /phase\(\) output exceeds/i,
      );

      // Forged phase message under the frame cap with a title over 1024 bytes.
      const overPhaseTitle = makeReceiver();
      overPhaseTitle.send({
        type: "phase",
        title: "a".repeat(WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes + 1),
      });
      expect(overPhaseTitle.getFailed()).toBeInstanceOf(WorkflowScriptError);
      expect((overPhaseTitle.getFailed() as Error).message).toMatch(/at most 1024 bytes/);
      expect(overPhaseTitle.phasesSeen).toEqual([]);

      // Forged phase messages exceeding 64 KiB total distinct title bytes.
      const overPhaseTotal = makeReceiver();
      const pad1020 = "x".repeat(1020);
      for (let i = 0; i < 65; i += 1) {
        overPhaseTotal.send({ type: "phase", title: `${String(i).padStart(4, "0")}${pad1020}` });
      }
      expect(overPhaseTotal.getFailed()).toBeInstanceOf(WorkflowScriptError);
      expect((overPhaseTotal.getFailed() as Error).message).toMatch(/64 KiB limit/);
      expect(overPhaseTotal.phasesSeen).toHaveLength(64);

      // Forged agent message carrying an overlong phase title.
      const overAgentTitle = makeReceiver();
      overAgentTitle.send({
        type: "agent",
        id: 1,
        index: 0,
        prompt: "hi",
        opts: { phase: "b".repeat(WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes + 1) },
        phase: "b".repeat(WORKFLOW_PROGRESS_LIMITS.maxPhaseTitleBytes + 1),
      });
      expect(overAgentTitle.getFailed()).toBeInstanceOf(WorkflowScriptError);
      expect((overAgentTitle.getFailed() as Error).message).toMatch(/at most 1024 bytes/);
      expect(overAgentTitle.agentsSeen).toEqual([]);

      // Forged agent messages exceeding 64 KiB total distinct phase bytes.
      const overAgentTotal = makeReceiver();
      for (let i = 0; i < 65; i += 1) {
        const title = `${String(i).padStart(4, "0")}${pad1020}`;
        overAgentTotal.send({
          type: "agent",
          id: i + 1,
          index: i,
          prompt: "hi",
          opts: { phase: title },
          phase: title,
        });
      }
      expect(overAgentTotal.getFailed()).toBeInstanceOf(WorkflowScriptError);
      expect((overAgentTotal.getFailed() as Error).message).toMatch(/64 KiB limit/);

      // Forged agent messages carrying a repeated phase do not consume phase switches.
      const repeatedAgentPhase = makeReceiver({
        ...WORKFLOW_PROGRESS_LIMITS,
        maxPhaseSwitches: 1,
      });
      repeatedAgentPhase.send({
        type: "agent",
        id: 1,
        index: 0,
        prompt: "hi",
        opts: { phase: "Reuse" },
        phase: "Reuse",
      });
      repeatedAgentPhase.send({
        type: "agent",
        id: 2,
        index: 1,
        prompt: "hi",
        opts: { phase: "Reuse" },
        phase: "Reuse",
      });
      repeatedAgentPhase.send({ type: "phase", title: "Reuse" });
      expect(repeatedAgentPhase.getFailed()).toBeUndefined();
      repeatedAgentPhase.send({ type: "phase", title: "Reuse" });
      expect(repeatedAgentPhase.getFailed()).toBeInstanceOf(WorkflowScriptError);
      expect((repeatedAgentPhase.getFailed() as Error).message).toMatch(/at most 1 times/);
    });
  });
});
