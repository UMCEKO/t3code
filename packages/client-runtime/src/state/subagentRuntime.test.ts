import { describe, expect, it } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2Subagent,
  type OrchestrationV2SubagentWorkflow,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import {
  deriveWorkflowGroups,
  isStoppableWorkflow,
  isWorkflowScriptMessage,
  projectedSubagentsToRuntime,
  resolveWorkflowOpening,
  workflowMemberModelSelection,
  workflowOpeningHeadline,
} from "./subagentRuntime.ts";
import { resolveSubagentMetadata } from "./subagentDisplay.ts";
import { formatSubagentEffort } from "./threadExecution.ts";

const at = (iso: string) => DateTime.makeUnsafe(iso);

type ProjectedSubagent = Parameters<typeof projectedSubagentsToRuntime>[0][number];

function subagent(overrides: Partial<ProjectedSubagent> = {}): ProjectedSubagent {
  return {
    id: "coordinator",
    title: "probe",
    prompt: "export const meta = …",
    model: "claude-opus-5[1m]",
    status: "running" satisfies OrchestrationV2Subagent["status"],
    result: null,
    startedAt: at("2026-08-01T10:00:00.000Z"),
    completedAt: null,
    updatedAt: at("2026-08-01T10:00:05.000Z"),
    ...overrides,
  };
}

const member = (
  overrides: Partial<OrchestrationV2SubagentWorkflow["agents"][number]> = {},
): OrchestrationV2SubagentWorkflow["agents"][number] => ({
  index: 1,
  label: "alpha:one",
  state: "running",
  phaseIndex: 1,
  phaseTitle: "Alpha",
  ...overrides,
});

const groupsOf = (subagents: ReadonlyArray<ProjectedSubagent>) =>
  deriveWorkflowGroups(projectedSubagentsToRuntime(subagents));

describe("projectedSubagentsToRuntime workflow expansion", () => {
  it("carries each member's own provider instance", () => {
    const [, first, second] = projectedSubagentsToRuntime([
      subagent({
        workflow: {
          phases: [],
          agents: [
            member({ index: 0, providerInstanceId: ProviderInstanceId.make("codex") }),
            member({ index: 1, providerInstanceId: ProviderInstanceId.make("claude") }),
          ],
        },
      }),
    ]);
    expect(first?.providerInstanceId).toBe("codex");
    expect(second?.providerInstanceId).toBe("claude");
  });

  it("uses a settled member's wall-clock completion separately from its execution duration", () => {
    const [, settled] = projectedSubagentsToRuntime([
      subagent({
        status: "completed",
        completedAt: at("2026-08-01T10:00:09.000Z"),
        workflow: {
          phases: [],
          agents: [
            member({
              state: "completed",
              model: "claude-opus-5[1m]",
              effort: "xhigh",
              speed: "standard",
              startedAt: Date.UTC(2026, 7, 1, 10, 0, 0),
              durationMs: 1500,
              completedAt: Date.UTC(2026, 7, 1, 10, 0, 4, 432),
              totalTokens: 71_141,
              toolCalls: 3,
              prompt: "Reply with exactly: A1",
              result: "A1",
            }),
          ],
        },
      }),
    ]);
    expect(settled?.status).toBe("completed");
    expect(settled?.completedAt).toBe("2026-08-01T10:00:04.432Z");
    expect(settled?.result).toBe("A1");
    expect(settled?.model).toBe("claude-opus-5[1m]");
    expect(settled?.effort).toBe("xhigh");
    expect(settled?.speed).toBe("standard");
    expect(settled?.progress).toBe("Reply with exactly: A1");
    expect(settled?.usage).toEqual({ totalTokens: 71_141, toolUses: 3, durationMs: 1500 });
    const instanceId = ProviderInstanceId.make("claudeAgent");
    expect(workflowMemberModelSelection(settled!, instanceId)).toEqual({
      instanceId,
      model: "claude-opus-5[1m]",
      options: [
        { id: "effort", value: "xhigh" },
        { id: "fastMode", value: false },
      ],
    });
    expect(workflowMemberModelSelection({ model: null, effort: "xhigh" }, instanceId)).toBe(
      undefined,
    );
    const provider = {
      driver: ProviderDriverKind.make("claudeAgent"),
      models: [
        { slug: "claude-opus-5", name: "Claude Opus 5", isCustom: false, capabilities: null },
      ],
    };
    const modelSelection = workflowMemberModelSelection(settled!, instanceId);
    const { modelSlug } = resolveSubagentMetadata({
      model: settled!.model,
      modelSelection,
      provider,
    });
    expect(formatSubagentEffort({ modelSlug, modelSelection, provider })).toBe("xhigh");
  });

  it("names a workflow by the model it runs on when the provider reports none", () => {
    const [workflow] = projectedSubagentsToRuntime([subagent({ model: null })]);
    expect(
      resolveSubagentMetadata({
        model: workflow!.model,
        modelSelection: { model: "claude-opus-5-5" },
      }).modelLabel,
    ).toBe("Claude Opus 5.5");
    expect(resolveSubagentMetadata({ model: workflow!.model }).modelLabel).toBe("Not reported");
  });

  it.each(["completed", "cancelled", "failed"] as const)(
    "settles unfinished members when the coordinator is %s",
    (status) => {
      const groups = groupsOf([
        subagent({
          status,
          completedAt: at("2026-08-01T10:00:09.000Z"),
          workflow: {
            phases: [],
            agents: [
              member(),
              member({ index: 2, state: "queued" }),
              member({
                index: 3,
                state: "completed",
                startedAt: Date.UTC(2026, 7, 1, 10, 0, 0),
                durationMs: 1500,
              }),
            ],
          },
        }),
      ]);
      expect(groups[0]?.workflow.status).toBe(status);
      const members = groups[0]?.phases.flatMap((phase) => phase.members);
      expect(members?.map((agent) => agent.status)).toEqual([status, status, "completed"]);
      expect(members?.map((agent) => agent.completedAt)).toEqual([
        "2026-08-01T10:00:09.000Z",
        "2026-08-01T10:00:09.000Z",
        "2026-08-01T10:00:01.500Z",
      ]);
      expect(members?.[0]?.model).toBeNull();
      expect(members?.[0]?.effort).toBeNull();
      expect(members?.[0]?.speed).toBeUndefined();
    },
  );

  it("titles an untitled subagent from its prompt without splitting an emoji", () => {
    const [untitled] = projectedSubagentsToRuntime([
      subagent({ title: null, prompt: `${"a".repeat(76)}😀 and more work.` }),
    ]);
    expect(untitled?.title).toBe(`${"a".repeat(76)}...`);
  });

  it("maps additive workflow fields: error over result, cached badge, logs, limits, journalPath, and stopRequestedAt", () => {
    const [wf, cachedMember, failedMember, fallbackFailedMember] = projectedSubagentsToRuntime([
      subagent({
        id: "wf-1",
        origin: "app_owned",
        childThreadId: ThreadId.make("coord-1"),
        workflow: {
          name: "audit",
          phases: [{ index: 0, title: "Review" }],
          logs: ["step 1", "step 2"],
          limits: { tokenBudget: 50_000, concurrency: 4, maxAgents: 1000 },
          stopRequestedAt: 1_700_000_000_000,
          runHandles: {
            runId: "wf_1",
            scriptPath: "/tmp/wf_1/script.js",
            journalPath: "/tmp/wf_1/journal.jsonl",
          },
          agents: [
            member({
              index: 0,
              label: "cached-worker",
              state: "completed",
              phaseIndex: 0,
              cached: true,
              result: "ok",
            }),
            member({
              index: 1,
              label: "failed-worker",
              state: "failed",
              phaseIndex: 0,
              error: "Provider is not authenticated.",
              result: "outcome: null",
            }),
            member({
              index: 2,
              label: "legacy-failed",
              state: "failed",
              phaseIndex: 0,
              result: "Legacy failure",
            }),
          ],
        },
      }),
    ]);
    expect(wf?.logs).toEqual(["step 1", "step 2"]);
    expect(wf?.limits).toEqual({ tokenBudget: 50_000, concurrency: 4, maxAgents: 1000 });
    expect(wf?.stopRequestedAt).toBe(1_700_000_000_000);
    expect(wf?.runHandles?.journalPath).toBe("/tmp/wf_1/journal.jsonl");
    expect(cachedMember?.cached).toBe(true);
    expect(cachedMember?.logs).toBeUndefined();
    expect(cachedMember?.limits).toBeUndefined();
    expect(cachedMember?.stopRequestedAt).toBeUndefined();
    expect(failedMember?.error).toBe("Provider is not authenticated.");
    expect(failedMember?.result).toBeNull();
    expect(fallbackFailedMember?.error).toBe("Legacy failure");
  });
});

describe("workflow stop predicate", () => {
  const workflow = { name: "audit", phases: [], agents: [] };
  const base = {
    status: "running" as const,
    origin: "app_owned" as const,
    driver: ProviderDriverKind.make("codex"),
    nativeTaskRef: null as OrchestrationV2Subagent["nativeTaskRef"],
    workflow,
  };

  it("gates workflow Stop across app-owned and Claude native workflows", () => {
    expect(isStoppableWorkflow(base)).toBe(true);
    expect(isStoppableWorkflow({ ...base, status: "waiting" })).toBe(false);
    expect(isStoppableWorkflow({ ...base, status: "completed" })).toBe(false);
    expect(isStoppableWorkflow({ ...base, workflow: undefined })).toBe(false);

    const claudeNative = {
      ...base,
      origin: "provider_native" as const,
      driver: ProviderDriverKind.make("claudeAgent"),
      childThreadId: ThreadId.make("coord"),
      nativeTaskRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        strength: "strong" as const,
        nativeId: "native-wf",
      },
    };
    expect(isStoppableWorkflow(claudeNative)).toBe(true);
    expect(isStoppableWorkflow({ ...claudeNative, driver: ProviderDriverKind.make("codex") })).toBe(
      false,
    );
    expect(isStoppableWorkflow({ ...claudeNative, nativeTaskRef: null })).toBe(false);
    expect(
      isStoppableWorkflow({
        ...claudeNative,
        nativeTaskRef: { ...claudeNative.nativeTaskRef, nativeId: "" },
      }),
    ).toBe(false);
  });
});

describe("deriveWorkflowGroups over a dynamic workflow", () => {
  it("groups members under their phases and counts the run", () => {
    const groups = groupsOf([
      subagent({
        workflow: {
          name: "probe-wf",
          phases: [
            { index: 1, title: "Alpha" },
            { index: 2, title: "Beta" },
          ],
          agents: [
            member({ state: "completed" }),
            member({ index: 2, label: "alpha:two", state: "failed" }),
            member({ index: 3, label: "beta:one", phaseIndex: 2, phaseTitle: "Beta" }),
            member({
              index: 4,
              label: "beta:two",
              phaseIndex: 2,
              phaseTitle: "Beta",
              state: "queued",
            }),
          ],
        },
      }),
    ]);
    const group = groups[0];
    expect(group?.phases.map((phase) => [phase.title, phase.state])).toEqual([
      ["Alpha", "done"],
      ["Beta", "running"],
    ]);
    expect(group?.phases.map((phase) => phase.settledCount)).toEqual([2, 0]);
    expect(group?.phases.flatMap((phase) => phase.members)).toHaveLength(4);
    expect(group?.agentCount).toBe(4);
    expect(group?.unphasedMembers).toEqual([]);
  });

  it("uses workflow.agentCount for totals and headline text when agents is a bounded subset", () => {
    const groups = groupsOf([
      subagent({
        workflow: {
          name: "fan-out",
          phases: [{ index: 0, title: "Scan" }],
          agents: [
            member({ index: 0, state: "running", phaseIndex: 0 }),
            member({ index: 1, state: "completed", phaseIndex: 0 }),
          ],
          agentCount: 1_000,
        },
      }),
    ]);
    expect(groups[0]?.workflow.agentCount).toBe(1_000);
    expect(groups[0]?.agentCount).toBe(1_000);
    expect(groups[0]?.phases[0]?.members[0]?.agentCount).toBeUndefined();
  });

  it.each([false, true])("keeps a reported phase title, missing title first=%s", (missingFirst) => {
    const agents = [member({ phaseTitle: "Review" }), member({ index: 2, phaseTitle: undefined })];
    const groups = groupsOf([
      subagent({ workflow: { phases: [], agents: missingFirst ? agents.toReversed() : agents } }),
    ]);
    expect(groups[0]?.phases[0]?.title).toBe("Review");
  });

  it("keeps unphased members with their workflow and excludes unrelated agents", () => {
    const runtime = projectedSubagentsToRuntime([
      subagent({ id: "ordinary" }),
      subagent({
        id: "__proto__",
        workflow: {
          phases: [],
          agents: [member({ phaseIndex: undefined, phaseTitle: undefined })],
        },
      }),
      subagent({ id: "constructor", workflow: { phases: [], agents: [] } }),
      subagent({ id: "orphan", workflow: { phases: [], agents: [member()] } }),
    ]);
    const groups = deriveWorkflowGroups(runtime.filter((agent) => agent.id !== "orphan"));
    expect(groups.map((group) => group.workflow.id)).toEqual(["__proto__", "constructor"]);
    expect(groups[0]?.phases).toEqual([]);
    expect(groups[0]?.unphasedMembers.map((agent) => agent.title)).toEqual(["alpha:one"]);
    expect(groups[1]?.phases).toEqual([]);
    expect(groups[1]?.unphasedMembers).toEqual([]);
  });
});

describe("resolveWorkflowOpening", () => {
  const SCRIPT = '// probe\nexport const meta = { name: "probe-wf" };\nconst GROUPS = [];';
  const childThread = {
    id: ThreadId.make("child"),
    lineage: {
      parentThreadId: ThreadId.make("parent"),
      relationshipToParent: "subagent" as const,
      rootThreadId: ThreadId.make("parent"),
    },
    creationSource: "provider" as const,
  };
  const coordinator = (workflow?: OrchestrationV2SubagentWorkflow) => ({
    ...subagent({
      title: "Run the alpha workflow",
      childThreadId: "child",
      ...(workflow === undefined ? {} : { workflow }),
    }),
    driver: ProviderDriverKind.make("claudeAgent"),
    providerInstanceId: ProviderInstanceId.make("claude"),
  });
  const brief = (text: string, workflowScript?: boolean) => ({
    id: "message-1",
    role: "user",
    text,
    ...(workflowScript === undefined ? {} : { workflowScript }),
  });

  it("opens a marked thread with the coordinator's plan and members", () => {
    const opening = resolveWorkflowOpening({
      thread: childThread,
      messages: [brief(SCRIPT, true)],
      parentSubagents: [
        coordinator({
          name: "probe-wf",
          phases: [{ index: 1, title: "Alpha" }],
          agents: [member({ childThreadId: ThreadId.make("member-1") })],
        }),
      ],
    });
    expect(opening).toMatchObject({
      messageId: "message-1",
      script: SCRIPT,
      name: "probe-wf",
      description: "Run the alpha workflow",
      parentThreadId: ThreadId.make("parent"),
      subagentId: "coordinator",
    });
    expect(
      opening?.group?.phases.map((phase) => ({
        title: phase.title,
        members: phase.members.map((agent) => [agent.title, agent.status, agent.childThreadId]),
      })),
    ).toEqual([{ title: "Alpha", members: [["alpha:one", "running", "member-1"]] }]);
  });

  it("opens a server coordinator thread (creationSource: server) with stoppable=true and additive fields", () => {
    const serverCoord = {
      ...childThread,
      creationSource: "server" as const,
    };
    const opening = resolveWorkflowOpening({
      thread: serverCoord,
      messages: [brief(SCRIPT, true)],
      parentSubagents: [
        {
          ...subagent({
            id: "wf-server",
            origin: "app_owned",
            title: "Server workflow description",
            childThreadId: "child",
            workflow: {
              name: "server-wf",
              phases: [{ index: 0, title: "Work" }],
              logs: ["started"],
              limits: { concurrency: 2, tokenBudget: 1000 },
              agents: [member({ index: 0, phaseIndex: 0, childThreadId: ThreadId.make("m-0") })],
            },
          }),
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
      ],
    });
    expect(opening).toMatchObject({
      name: "server-wf",
      description: "Server workflow description",
      parentThreadId: ThreadId.make("parent"),
      subagentId: "wf-server",
      origin: "app_owned",
      stoppable: true,
    });
    expect(opening?.group?.workflow.logs).toEqual(["started"]);
    expect(opening?.group?.workflow.limits).toEqual({ concurrency: 2, tokenBudget: 1000 });
  });

  it("recognizes an unmarked script before the parent loads", () => {
    const opening = resolveWorkflowOpening({
      thread: childThread,
      messages: [brief(SCRIPT)],
      parentSubagents: null,
    });
    expect(opening).toMatchObject({ script: SCRIPT, name: null, group: null });
  });

  it("leaves an ordinary subagent thread and other threads alone", () => {
    expect(
      resolveWorkflowOpening({
        thread: childThread,
        messages: [brief("Audit the last five commits.")],
        parentSubagents: [coordinator()],
      }),
    ).toBeNull();
    expect(
      resolveWorkflowOpening({
        thread: {
          ...childThread,
          lineage: { ...childThread.lineage, relationshipToParent: null, parentThreadId: null },
        },
        messages: [brief(SCRIPT)],
        parentSubagents: null,
      }),
    ).toBeNull();
    expect(isWorkflowScriptMessage({ role: "user", text: "Explain `export const meta`." })).toBe(
      false,
    );
    expect(isWorkflowScriptMessage({ role: "assistant", text: SCRIPT })).toBe(false);
    expect(
      isWorkflowScriptMessage({ role: "user", text: `// a\r// b\u2028/* c\n * d */\r\n${SCRIPT}` }),
    ).toBe(true);
    expect(isWorkflowScriptMessage({ role: "user", text: "// export const meta = {}" })).toBe(
      false,
    );
    // Divider comments ahead of ordinary code must reject without catastrophic backtracking.
    const dividers = `${"/".repeat(40)}\n`.repeat(40);
    expect(isWorkflowScriptMessage({ role: "user", text: `${dividers}const x = 1;` })).toBe(false);
    expect(
      isWorkflowScriptMessage({ role: "user", text: `${"/**/".repeat(200)}const x = 1;` }),
    ).toBe(false);
  });

  it("formats opening headline agent counts from agentCount ?? agents.length", () => {
    const full = resolveWorkflowOpening({
      thread: childThread,
      messages: [brief(SCRIPT, true)],
      parentSubagents: [
        coordinator({
          name: "probe-wf",
          phases: [{ index: 1, title: "Alpha" }],
          agents: [
            member({ index: 0, state: "running" }),
            member({ index: 1, state: "completed" }),
          ],
        }),
      ],
    });
    expect(workflowOpeningHeadline(full!).agents).toBe("2 agents · 1 working · 1 done");

    const bounded = resolveWorkflowOpening({
      thread: childThread,
      messages: [brief(SCRIPT, true)],
      parentSubagents: [
        coordinator({
          name: "probe-wf",
          phases: [{ index: 1, title: "Alpha" }],
          agents: [
            member({ index: 0, state: "running" }),
            member({ index: 1, state: "completed" }),
          ],
          agentCount: 1_000,
        }),
      ],
    });
    expect(workflowOpeningHeadline(bounded!).agents).toBe("1000 agents");

    const emptyDelta = resolveWorkflowOpening({
      thread: childThread,
      messages: [brief(SCRIPT, true)],
      parentSubagents: [
        coordinator({
          name: "probe-wf",
          phases: [],
          agents: [],
          agentCount: 1,
        }),
      ],
    });
    expect(workflowOpeningHeadline(emptyDelta!).agents).toBe("1 agent");

    const empty = resolveWorkflowOpening({
      thread: childThread,
      messages: [brief(SCRIPT, true)],
      parentSubagents: [
        coordinator({
          name: "probe-wf",
          phases: [],
          agents: [],
        }),
      ],
    });
    expect(workflowOpeningHeadline(empty!).agents).toBeNull();
  });
});
