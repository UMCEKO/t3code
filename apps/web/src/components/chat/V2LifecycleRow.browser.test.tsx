import * as DateTime from "effect/DateTime";
import { Atom } from "effect/reactivity";
import {
  EnvironmentId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import "~/index.css";

import { render } from "../../../test/render";
import { isMacPlatform } from "../../lib/utils";
import { environmentThreadDetails } from "../../state/threads";
import { V2LifecycleRow } from "./V2LifecycleRow";

const ENVIRONMENT_ID = EnvironmentId.make("environment-local");
const PARENT_THREAD_ID = ThreadId.make("parent-thread");
const COORDINATOR_THREAD_ID = ThreadId.make("workflow-coordinator");
const NOW = DateTime.makeUnsafe("2026-11-22T12:00:00.000Z");

function makeWorkflowCard(origin: OrchestrationV2Subagent["origin"]) {
  const driver = ProviderDriverKind.make(origin === "provider_native" ? "claudeAgent" : "codex");
  const providerInstanceId = ProviderInstanceId.make(
    origin === "provider_native" ? "claudeAgent" : "codex",
  );
  const subagentId = NodeId.make(`wf:${origin}`);
  const subagent: OrchestrationV2Subagent = {
    id: subagentId,
    threadId: PARENT_THREAD_ID,
    runId: RunId.make("run-1"),
    parentNodeId: NodeId.make("root-1"),
    origin,
    createdBy: "agent",
    driver,
    providerInstanceId,
    providerThreadId: null,
    childThreadId: COORDINATOR_THREAD_ID,
    nativeTaskRef:
      origin === "provider_native"
        ? {
            driver: ProviderDriverKind.make("claudeAgent"),
            nativeId: "task-native",
            strength: "strong",
          }
        : null,
    prompt: 'export const meta = { name: "Audit" };',
    title: "Audits repository modules",
    model: null,
    status: "running",
    progress: "Running alpha-1",
    result: null,
    startedAt: NOW,
    completedAt: null,
    updatedAt: NOW,
    workflow: {
      name: "Audit",
      phases: [{ index: 0, title: "Scan" }],
      agents: [],
    },
  };
  const item: OrchestrationV2TurnItem = {
    id: TurnItemId.make(`item:${origin}`),
    threadId: PARENT_THREAD_ID,
    runId: RunId.make("run-1"),
    nodeId: NodeId.make("root-1"),
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "running",
    title: "Audits repository modules",
    startedAt: NOW,
    completedAt: null,
    updatedAt: NOW,
    type: "subagent",
    subagentId,
    origin,
    driver,
    providerInstanceId,
    childThreadId: COORDINATOR_THREAD_ID,
    prompt: subagent.prompt,
    progress: subagent.progress,
    result: null,
  };
  return { subagent, item };
}

it.each(["provider_native", "app_owned"] as const)(
  "renders %s workflow cards as buttons that open C with the same click and modifier intents",
  async (origin) => {
    const { subagent, item } = makeWorkflowCard(origin);
    using _threadAtom = vi.spyOn(environmentThreadDetails, "threadAtom").mockReturnValue(
      Atom.make({
        projection: { subagents: [subagent], turnItems: [item], visibleTurnItems: [] },
      }) as never,
    );
    const onOpenThread = vi.fn();

    await using screen = await render(
      <V2LifecycleRow
        item={item}
        environmentId={ENVIRONMENT_ID}
        runs={[]}
        onOpenThread={onOpenThread}
      />,
    );

    const button = screen.getByRole("button", { name: "Open Audits repository modules" });
    await expect.element(button).toBeVisible();
    await expect.element(button).toHaveAttribute("data-v2-item-type", "subagent");

    await button.click();
    expect(onOpenThread).toHaveBeenNthCalledWith(1, COORDINATOR_THREAD_ID, undefined, "primary");

    const modifier = isMacPlatform(navigator.platform) ? "Meta" : "Control";
    await userEvent.click(button, { modifiers: [modifier] });
    expect(onOpenThread).toHaveBeenNthCalledWith(2, COORDINATOR_THREAD_ID, undefined, "alternate");

    button
      .element()
      .dispatchEvent(new MouseEvent("auxclick", { button: 1, bubbles: true, cancelable: true }));
    expect(onOpenThread).toHaveBeenNthCalledWith(3, COORDINATOR_THREAD_ID, undefined, "alternate");
  },
);
