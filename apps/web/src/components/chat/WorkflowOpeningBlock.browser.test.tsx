import * as DateTime from "effect/DateTime";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom } from "effect/reactivity";
import {
  EnvironmentId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Subagent,
  type ServerProvider,
} from "@t3tools/contracts";
import { resolveWorkflowOpening } from "@t3tools/client-runtime/state/subagentRuntime";
import { expect, it, vi } from "vite-plus/test";

import "~/index.css";

const state = vi.hoisted(() => ({
  canOperateWorkflow: true,
  commandSucceeds: true,
  commandCalls: [] as Array<unknown>,
  providers: [] as Array<ServerProvider>,
}));

vi.mock(import("../../state/use-atom-command"), () => ({
  useAtomCommand: (() => async (arg: unknown) => {
    state.commandCalls.push(arg);
    return state.commandSucceeds
      ? AsyncResult.success(undefined)
      : AsyncResult.failure(Cause.fail(new Error("Stop failed")));
  }) as never,
}));

vi.mock(import("../../state/query"), async (original) => ({
  ...(await original()),
  useEnvironmentQuery: (() => ({
    data: null,
    dataUpdatedAt: 0,
    error: null,
    failure: null,
    isPending: false,
    isSuccess: false,
    refresh: () => {},
  })) as never,
}));

import { render } from "../../../test/render";
import { orchestrationEnvironment } from "../../state/orchestration";
import { serverEnvironment } from "../../state/server";
import { ToastProvider } from "../ui/toast";
import { WorkflowOpeningBlock } from "./WorkflowOpeningBlock";

const ENVIRONMENT_ID = EnvironmentId.make("environment-local");
const PARENT_THREAD_ID = ThreadId.make("parent-thread");
const COORDINATOR_THREAD_ID = ThreadId.make("coordinator-thread");
const MEMBER_OK_ID = ThreadId.make("member-ok");
const MEMBER_FAIL_ID = ThreadId.make("member-fail");
const MEMBER_CACHED_ID = ThreadId.make("member-cached");
const NOW = DateTime.makeUnsafe("2026-11-22T12:00:00.000Z");

const CLAUDE_INSTANCE_ID = ProviderInstanceId.make("claudeAgent");
const CODEX_INSTANCE_ID = ProviderInstanceId.make("codex");

const PROVIDERS: ReadonlyArray<ServerProvider> = [
  {
    instanceId: CLAUDE_INSTANCE_ID,
    driver: ProviderDriverKind.make("claudeAgent"),
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-11-22T12:00:00.000Z",
    models: [
      { slug: "claude-sonnet-4-6", name: "Claude Sonnet", isCustom: false, capabilities: null },
    ],
    slashCommands: [],
    skills: [],
  },
  {
    instanceId: CODEX_INSTANCE_ID,
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-11-22T12:00:00.000Z",
    models: [{ slug: "gpt-5.3-codex", name: "GPT-5.3 Codex", isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
  },
];

const LONG_SCRIPT = [
  'export const meta = { name: "Audit", description: "Checks modules across providers.", concurrency: 2 };',
  ...Array.from({ length: 24 }, (_, idx) => `log("step ${idx + 1}");`),
  "return { ok: true };",
].join("\n");

function buildOpening(origin: OrchestrationV2Subagent["origin"]) {
  const isNative = origin === "provider_native";
  const coordinatorThread: Pick<OrchestrationV2AppThread, "id" | "lineage" | "creationSource"> = {
    id: COORDINATOR_THREAD_ID,
    creationSource: isNative ? "provider" : "server",
    lineage: {
      rootThreadId: PARENT_THREAD_ID,
      parentThreadId: PARENT_THREAD_ID,
      relationshipToParent: "subagent",
    },
  };
  const coordinatorSubagent: OrchestrationV2Subagent = {
    id: NodeId.make(`wf:${origin}`),
    threadId: PARENT_THREAD_ID,
    runId: null,
    parentNodeId: NodeId.make("parent-root"),
    origin,
    createdBy: "agent",
    driver: ProviderDriverKind.make(isNative ? "claudeAgent" : "codex"),
    providerInstanceId: isNative ? CLAUDE_INSTANCE_ID : CODEX_INSTANCE_ID,
    providerThreadId: null,
    childThreadId: COORDINATOR_THREAD_ID,
    nativeTaskRef: isNative
      ? {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId: "native-wf-task",
          strength: "strong",
        }
      : null,
    prompt: LONG_SCRIPT,
    title: "Checks modules across providers.",
    model: null,
    status: "running",
    result: "Audit summary: 1 issue found",
    startedAt: NOW,
    completedAt: null,
    updatedAt: NOW,
    workflow: {
      name: "Audit",
      totalTokens: 12_340,
      toolCalls: 4,
      durationMs: 3_000,
      phases: [
        { index: 0, title: "Alpha" },
        { index: 1, title: "Beta" },
      ],
      logs: ["Alpha started", "Beta finished"],
      limits: {
        tokenBudget: 50_000,
        concurrency: 2,
      },
      runHandles: {
        runId: "wf_run_1",
        scriptPath: "/tmp/workflows/wf_run_1/script.js",
        journalPath: "/tmp/workflows/wf_run_1/journal.jsonl",
      },
      agents: [
        {
          index: 0,
          label: "alpha-claude",
          childThreadId: MEMBER_OK_ID,
          providerInstanceId: CLAUDE_INSTANCE_ID,
          state: "completed",
          phaseIndex: 0,
          phaseTitle: "Alpha",
          result: "Checked A",
          startedAt: 1_000,
          completedAt: 2_000,
        },
        {
          index: 1,
          label: "alpha-codex-fail",
          childThreadId: MEMBER_FAIL_ID,
          providerInstanceId: CODEX_INSTANCE_ID,
          state: "failed",
          phaseIndex: 0,
          phaseTitle: "Alpha",
          error: "Provider is not authenticated",
          startedAt: 1_100,
          completedAt: 1_500,
        },
        {
          index: 2,
          label: "beta-cached",
          childThreadId: MEMBER_CACHED_ID,
          providerInstanceId: CLAUDE_INSTANCE_ID,
          state: "completed",
          cached: true,
          phaseIndex: 1,
          phaseTitle: "Beta",
          result: "Replayed summary",
          startedAt: 2_000,
          completedAt: 2_100,
        },
      ],
    },
  };
  const opening = resolveWorkflowOpening({
    thread: coordinatorThread,
    messages: [
      {
        id: MessageId.make("msg-script"),
        role: "user",
        text: LONG_SCRIPT,
        workflowScript: true,
      },
    ],
    parentSubagents: [coordinatorSubagent],
  });
  expect(opening).not.toBeNull();
  return { opening: opening!, subagentId: coordinatorSubagent.id };
}

it.each(["provider_native", "app_owned"] as const)(
  "renders full enriched opening for %s workflows, opens members, and stops cleanly",
  async (origin) => {
    state.canOperateWorkflow = true;
    state.commandSucceeds = true;
    state.commandCalls = [];
    state.providers = [...PROVIDERS];

    using _providersSpy = vi
      .spyOn(serverEnvironment, "providersValueAtom")
      .mockReturnValue(Atom.make(state.providers) as never);
    using _settingsSpy = vi
      .spyOn(serverEnvironment, "settingsValueAtom")
      .mockReturnValue(Atom.make(null) as never);
    using _permSpy = vi
      .spyOn(orchestrationEnvironment.stopWorkflow, "permissionAtom")
      .mockReturnValue(Atom.make(state.canOperateWorkflow) as never);

    const { opening, subagentId } = buildOpening(origin);
    const onOpenThread = vi.fn();
    const onOpenFile = vi.fn();

    await using screen = await render(
      <ToastProvider timeout={0}>
        <div style={{ width: 640 }}>
          <WorkflowOpeningBlock
            opening={opening}
            environmentId={ENVIRONMENT_ID}
            onOpenThread={onOpenThread}
            onOpenFile={onOpenFile}
          />
        </div>
      </ToastProvider>,
    );

    // Headline and budget/limits.
    await expect.element(screen.getByRole("heading", { name: "Audit", level: 3 })).toBeVisible();
    await expect.element(screen.getByText("Checks modules across providers.")).toBeVisible();
    await expect.element(screen.getByText(/12,340 \/ 50,000 tokens/)).toBeVisible();
    await expect.element(screen.getByText(/Concurrency 2/)).toBeVisible();

    // Phases and members with their own distinct provider icons.
    await expect.element(screen.getByRole("region", { name: "Alpha phase" })).toBeVisible();
    await expect.element(screen.getByRole("region", { name: "Beta phase" })).toBeVisible();

    const claudeMember = screen.getByRole("button", { name: "Open alpha-claude" });
    const codexMember = screen.getByRole("button", { name: "Open alpha-codex-fail" });
    const cachedMember = screen.getByRole("button", { name: "Open beta-cached" });
    await expect.element(claudeMember).toBeVisible();
    await expect.element(codexMember).toBeVisible();
    await expect.element(cachedMember).toBeVisible();

    const claudeSvg = claudeMember.element().querySelector("svg")?.innerHTML ?? "";
    const codexSvg = codexMember.element().querySelector("svg")?.innerHTML ?? "";
    expect(claudeSvg.length).toBeGreaterThan(0);
    expect(codexSvg.length).toBeGreaterThan(0);
    expect(claudeSvg).not.toBe(codexSvg);

    // Error reason on the failed member and in the Failures summary list.
    await expect.element(codexMember.getByText("Provider is not authenticated")).toBeVisible();
    const failures = screen.getByRole("region", { name: "Failures" });
    await expect.element(failures).toBeVisible();
    await expect.element(failures.getByText("alpha-codex-fail")).toBeVisible();
    await expect.element(failures.getByText("Provider is not authenticated")).toBeVisible();

    // Cached badge on replayed member.
    await expect.element(cachedMember.getByText("Cached")).toBeVisible();

    // Result section.
    const resultRegion = screen.getByRole("region", { name: "Result" });
    await expect.element(resultRegion).toBeVisible();
    await expect.element(resultRegion.getByText("Audit summary: 1 issue found")).toBeVisible();

    // Collapsible logs (newest last).
    const logsToggle = screen.getByRole("button", { name: /Logs \(2\)/ });
    await expect.element(logsToggle).toHaveAttribute("aria-expanded", "false");
    await logsToggle.click();
    await expect.element(logsToggle).toHaveAttribute("aria-expanded", "true");
    const logItems = [
      ...screen.getByRole("list", { name: "Workflow logs" }).element().children,
    ].map((item) => item.textContent);
    expect(logItems).toEqual(["Alpha started", "Beta finished"]);

    // Script & journal paths open in the file viewer.
    await screen.getByRole("button", { name: /Open Journal:/ }).click();
    expect(onOpenFile).toHaveBeenCalledWith("/tmp/workflows/wf_run_1/journal.jsonl");

    // Script disclosure unfolds the tall script.
    const scriptToggle = screen.getByRole("button", { name: "Show full script" });
    await expect.element(scriptToggle).toHaveAttribute("aria-expanded", "false");
    await scriptToggle.click();
    await expect
      .element(screen.getByRole("button", { name: "Show less" }))
      .toHaveAttribute("aria-expanded", "true");

    // Clicking a member opens M_i.
    await claudeMember.click();
    expect(onOpenThread).toHaveBeenCalledWith(MEMBER_OK_ID, "primary");

    // Clicking Stop calls stopWorkflow once and switches to "Stopping…".
    const stopButton = screen.getByRole("button", { name: "Stop workflow Audit" });
    await expect.element(stopButton).toBeEnabled();
    await stopButton.click();
    expect(state.commandCalls).toEqual([
      {
        environmentId: ENVIRONMENT_ID,
        input: { threadId: PARENT_THREAD_ID, subagentId },
      },
    ]);
    const stoppingButton = screen.getByRole("button", { name: "Stopping workflow Audit" });
    await expect.element(stoppingButton).toBeDisabled();
    await expect.element(stoppingButton.getByText("Stopping…")).toBeVisible();
  },
);
