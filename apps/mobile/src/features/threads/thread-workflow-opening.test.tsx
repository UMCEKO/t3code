import * as DateTime from "effect/DateTime";
import { AsyncResult } from "effect/reactivity";
import { resolveWorkflowOpening } from "@t3tools/client-runtime/state/subagentRuntime";
import {
  EnvironmentId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2Subagent,
  type ServerProvider,
} from "@t3tools/contracts";
import type { ReactNode } from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  canOperateWorkflow: true,
  commandCalls: [] as Array<unknown>,
  openedThreads: [] as Array<{ environmentId: string; threadId: string }>,
  subagentRows: [] as Array<{
    title: string | null;
    providerInstanceId: string;
    driver: string;
    result: string | null;
  }>,
  providers: [] as Array<ServerProvider>,
}));

vi.mock("react-native", () => ({
  Pressable: ({
    children,
    disabled,
    onPress,
    accessibilityLabel,
    accessibilityState,
  }: {
    children: ReactNode;
    disabled?: boolean;
    onPress?: () => void;
    accessibilityLabel?: string;
    accessibilityState?: { expanded?: boolean };
  }) => (
    <button
      type="button"
      disabled={disabled}
      aria-label={accessibilityLabel}
      aria-expanded={accessibilityState?.expanded}
      onClick={onPress}
    >
      {children}
    </button>
  ),
  View: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock("gt-react-native", () => ({
  T: ({ children }: { children: ReactNode }) => <>{children}</>,
  Var: ({ children }: { children: ReactNode }) => <>{children}</>,
  useGT: () => (template: string, vars?: Record<string, unknown>) =>
    template.replace(/\{(\w+)\}/g, (_, key: string) => String(vars?.[key] ?? "")),
  useMessages: () => (value: string) => value,
}));

vi.mock("@effect/atom-react", () => ({
  useAtomValue: () => state.canOperateWorkflow,
}));

vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children: ReactNode }) => <span>{children}</span>,
}));

vi.mock("../../components/ui/AppSymbol", () => ({
  SymbolView: () => null,
}));

vi.mock("../../components/ui/Button", () => ({
  Button: ({
    label,
    accessibilityLabel,
    disabled,
    onPress,
  }: {
    label: string;
    accessibilityLabel?: string;
    disabled?: boolean;
    onPress: () => void;
  }) => (
    <button
      type="button"
      aria-label={accessibilityLabel ?? label}
      disabled={disabled}
      onClick={onPress}
    >
      {label}
    </button>
  ),
}));

vi.mock("../../components/ui/Dialog", () => ({
  alert: vi.fn(),
  confirm: ({ onConfirm }: { onConfirm: () => void }) => onConfirm(),
}));

vi.mock("../../state/entities", () => ({
  useEnvironmentServerConfig: () => ({ providers: state.providers }),
}));

vi.mock("../../state/orchestration", () => ({
  orchestrationEnvironment: {
    stopWorkflow: {
      permissionAtom: () => ({}),
    },
  },
}));

vi.mock("../../state/threads", () => ({
  environmentThreadDetails: {
    threadAtom: () => ({}),
  },
}));

vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: () => async (arg: unknown) => {
    state.commandCalls.push(arg);
    return AsyncResult.success(undefined);
  },
}));

vi.mock("../settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({ themeAppearance: "dark" }),
}));

vi.mock("./markdownCodeHighlightState", () => ({
  useMarkdownCodeHighlight: () => null,
}));

vi.mock("./SubagentRow", () => ({
  SubagentRow: ({
    subagent,
  }: {
    subagent: {
      title: string | null;
      providerInstanceId: string;
      driver: string;
      result: string | null;
    };
  }) => {
    state.subagentRows.push(subagent);
    return (
      <div
        data-member-title={subagent.title ?? ""}
        data-member-provider={subagent.providerInstanceId}
        data-member-driver={subagent.driver}
      >
        {subagent.title} {subagent.result}
      </div>
    );
  },
}));

vi.mock("./work-log-layout", () => ({
  WorkLogBlock: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock("./useOpenLinkedThread", () => ({
  useOpenLinkedThread: () => (target: { environmentId: string; threadId: string }) => {
    state.openedThreads.push(target);
  },
}));

import { ThreadWorkflowOpening } from "./thread-workflow-opening";

const ENVIRONMENT_ID = EnvironmentId.make("env-1");
const PARENT_THREAD_ID = ThreadId.make("parent-1");
const COORDINATOR_THREAD_ID = ThreadId.make("coord-1");
const MEMBER_A = ThreadId.make("member-a");
const MEMBER_B = ThreadId.make("member-b");
const NOW = DateTime.makeUnsafe("2026-11-22T12:00:00.000Z");
const CLAUDE_ID = ProviderInstanceId.make("claudeAgent");
const CODEX_ID = ProviderInstanceId.make("codex");

const PROVIDERS: ReadonlyArray<ServerProvider> = [
  {
    instanceId: CLAUDE_ID,
    driver: ProviderDriverKind.make("claudeAgent"),
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-11-22T12:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  },
  {
    instanceId: CODEX_ID,
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-11-22T12:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  },
];

describe("ThreadWorkflowOpening", () => {
  it.each(["provider_native", "app_owned"] as const)(
    "renders enriched workflow opening for %s workflows, resolves per-member providers, and stops",
    async (origin) => {
      state.canOperateWorkflow = true;
      state.commandCalls = [];
      state.openedThreads = [];
      state.subagentRows = [];
      state.providers = [...PROVIDERS];

      const isNative = origin === "provider_native";
      const subagentId = NodeId.make(`wf:${origin}`);
      const script = 'export const meta = { name: "Audit", concurrency: 2 };\nreturn "ok";';
      const coordinatorSubagent: OrchestrationV2Subagent = {
        id: subagentId,
        threadId: PARENT_THREAD_ID,
        runId: null,
        parentNodeId: NodeId.make("root"),
        origin,
        createdBy: "agent",
        driver: ProviderDriverKind.make(isNative ? "claudeAgent" : "codex"),
        providerInstanceId: isNative ? CLAUDE_ID : CODEX_ID,
        providerThreadId: null,
        childThreadId: COORDINATOR_THREAD_ID,
        nativeTaskRef: isNative
          ? {
              driver: ProviderDriverKind.make("claudeAgent"),
              nativeId: "native-task",
              strength: "strong",
            }
          : null,
        prompt: script,
        title: "Checks all packages",
        model: null,
        status: "running",
        result: "Summary: 1 failure",
        startedAt: NOW,
        completedAt: null,
        updatedAt: NOW,
        workflow: {
          name: "Audit",
          totalTokens: 8_500,
          limits: { tokenBudget: 20_000, concurrency: 2 },
          logs: ["Phase 1 started", "Phase 1 done"],
          runHandles: {
            runId: "wf_1",
            scriptPath: "/tmp/wf_1/script.js",
            journalPath: "/tmp/wf_1/journal.jsonl",
          },
          phases: [{ index: 0, title: "Alpha" }],
          agents: [
            {
              index: 0,
              label: "worker-claude",
              childThreadId: MEMBER_A,
              providerInstanceId: CLAUDE_ID,
              state: "completed",
              cached: true,
              phaseIndex: 0,
              phaseTitle: "Alpha",
              result: "Done A",
            },
            {
              index: 1,
              label: "worker-codex",
              childThreadId: MEMBER_B,
              providerInstanceId: CODEX_ID,
              state: "failed",
              phaseIndex: 0,
              phaseTitle: "Alpha",
              error: "Provider is not authenticated",
            },
          ],
        },
      };

      const opening = resolveWorkflowOpening({
        thread: {
          id: COORDINATOR_THREAD_ID,
          creationSource: isNative ? "provider" : "server",
          lineage: {
            rootThreadId: PARENT_THREAD_ID,
            parentThreadId: PARENT_THREAD_ID,
            relationshipToParent: "subagent",
          },
        },
        messages: [{ id: MessageId.make("m1"), role: "user", text: script, workflowScript: true }],
        parentSubagents: [coordinatorSubagent],
      });
      expect(opening).not.toBeNull();

      const container = document.createElement("div");
      const root = createRoot(container);
      try {
        await act(async () => {
          root.render(
            <ThreadWorkflowOpening
              environmentId={ENVIRONMENT_ID}
              threadId={COORDINATOR_THREAD_ID}
              opening={opening!}
            />,
          );
        });

        const text = container.textContent ?? "";
        expect(text).toContain("Audit");
        expect(text).toContain("8,500 / 20,000 tokens");
        expect(text).toContain("Concurrency 2");
        expect(text).toContain("Cached");
        expect(text).toContain("Failures");
        expect(text).toContain("Provider is not authenticated");
        expect(text).toContain("Summary: 1 failure");
        expect(text).toContain("/tmp/wf_1/script.js");
        expect(text).toContain("/tmp/wf_1/journal.jsonl");

        // Per-member providers resolved independently.
        expect(
          state.subagentRows.map((row) => ({
            title: row.title,
            providerInstanceId: row.providerInstanceId,
            driver: row.driver,
          })),
        ).toEqual([
          { title: "worker-claude", providerInstanceId: CLAUDE_ID, driver: "claudeAgent" },
          { title: "worker-codex", providerInstanceId: CODEX_ID, driver: "codex" },
        ]);

        // Logs expand in chronological order (newest last).
        const buttons = [...container.querySelectorAll("button")];
        const logsButton = buttons.find((button) => button.textContent?.includes("Logs (2)"));
        expect(logsButton).toBeDefined();
        expect(container.textContent).not.toContain("Phase 1 started");
        await act(async () => {
          logsButton!.click();
        });
        const expandedText = container.textContent ?? "";
        expect(expandedText.indexOf("Phase 1 started")).toBeGreaterThan(-1);
        expect(expandedText.indexOf("Phase 1 done")).toBeGreaterThan(
          expandedText.indexOf("Phase 1 started"),
        );

        // Clicking Stop calls stopWorkflow once and disables into "Stopping…".
        const stopButton = buttons.find(
          (button) => button.getAttribute("aria-label") === "Stop workflow Audit",
        );
        expect(stopButton).toBeDefined();
        await act(async () => {
          stopButton!.click();
        });
        expect(state.commandCalls).toEqual([
          {
            environmentId: ENVIRONMENT_ID,
            input: { threadId: PARENT_THREAD_ID, subagentId },
          },
        ]);
        const updatedStopButton = [...container.querySelectorAll("button")].find(
          (button) => button.getAttribute("aria-label") === "Stopping workflow Audit",
        );
        expect(updatedStopButton?.disabled).toBe(true);
        expect(updatedStopButton?.textContent).toContain("Stopping…");
      } finally {
        await act(async () => {
          root.unmount();
        });
      }
    },
  );
});
