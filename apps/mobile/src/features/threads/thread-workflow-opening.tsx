import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  resolveWorkflowOpening,
  workflowMemberModelSelection,
  workflowOpeningHeadline,
  workflowOpeningParentThreadId,
  type RuntimeSubagent,
} from "@t3tools/client-runtime/state/subagentRuntime";
import {
  NodeId,
  type EnvironmentId,
  type ScopedThreadRef,
  type ServerProvider,
  type ThreadId,
} from "@t3tools/contracts";
import type { EnvironmentThread } from "@t3tools/client-runtime/state/shell";
import { Atom } from "effect/reactivity";
import { T, Var, useGT, useMessages } from "gt-react-native";
import { useState } from "react";
import { Pressable, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { SymbolView } from "../../components/ui/AppSymbol";
import { Button } from "../../components/ui/Button";
import * as Dialog from "../../components/ui/Dialog";
import { cn } from "../../lib/cn";
import { useEnvironmentServerConfig } from "../../state/entities";
import { orchestrationEnvironment } from "../../state/orchestration";
import { environmentThreadDetails } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { useAppearancePreferences } from "../settings/appearance/AppearancePreferencesProvider";
import { useMarkdownCodeHighlight } from "./markdownCodeHighlightState";
import { SubagentRow } from "./SubagentRow";
import { SUBAGENT_TONE_TEXT_CLASS } from "./SubagentStatusDot";
import { WorkLogBlock } from "./work-log-layout";
import { useOpenLinkedThread } from "./useOpenLinkedThread";

const FOLDED_SCRIPT_LINES = 10;

const NO_THREAD_ATOM = Atom.make<EnvironmentThread | null>(null);

export function useWorkflowOpening(ref: ScopedThreadRef | null) {
  const messages = useMessages();
  const threadAtom = ref === null ? NO_THREAD_ATOM : environmentThreadDetails.threadAtom(ref);
  // Selected as a primitive so an ordinary thread's updates never change the result.
  const parentId = useAtomValue(threadAtom, (thread) =>
    thread === null
      ? null
      : workflowOpeningParentThreadId({
          thread: thread.projection.thread,
          messages: thread.projection.messages,
        }),
  );
  const projection = useAtomValue(threadAtom, (thread) =>
    parentId === null ? null : (thread?.projection ?? null),
  );
  const parentSubagents = useAtomValue(
    ref === null || parentId === null
      ? NO_THREAD_ATOM
      : environmentThreadDetails.threadAtom(scopeThreadRef(ref.environmentId, parentId)),
    (parent) => (parentId === null ? null : (parent?.projection.subagents ?? null)),
  );
  return projection === null
    ? null
    : resolveWorkflowOpening(
        {
          thread: projection.thread,
          messages: projection.messages,
          parentSubagents,
        },
        messages,
      );
}

function memberRowSubagent(
  member: RuntimeSubagent,
  opening: WorkflowOpeningValue,
  coordinatorThreadId: ThreadId,
  providers: ReadonlyArray<ServerProvider>,
) {
  const providerInstanceId = member.providerInstanceId ?? opening.providerInstanceId!;
  const provider = providers.find((candidate) => candidate.instanceId === providerInstanceId);
  const driver = provider?.driver ?? opening.driver!;
  const failed = member.status === "failed";
  return {
    // A member's parent thread is the coordinator's thread, the one showing this opening.
    threadId: coordinatorThreadId,
    childThreadId: member.childThreadId as ThreadId | null,
    model: member.model,
    modelSelection: workflowMemberModelSelection(member, providerInstanceId),
    driver,
    providerInstanceId,
    title: member.title,
    prompt: member.progress ?? member.title,
    status: member.status,
    progress: undefined,
    result: failed ? (member.error ?? member.result) : (member.result ?? member.error),
  };
}

type WorkflowOpeningValue = NonNullable<ReturnType<typeof useWorkflowOpening>>;

export function ThreadWorkflowOpening(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly opening: WorkflowOpeningValue;
}) {
  const gt = useGT();
  const messages = useMessages();
  const openLinkedThread = useOpenLinkedThread();
  const providers = useEnvironmentServerConfig(props.environmentId)?.providers ?? [];
  const canOperateWorkflow = useAtomValue(
    orchestrationEnvironment.stopWorkflow.permissionAtom(props.environmentId),
  );
  const stopWorkflowCommand = useAtomCommand(orchestrationEnvironment.stopWorkflow);
  const [stoppingSubagentId, setStoppingSubagentId] = useState<string | null>(null);
  const { opening } = props;
  const workflow = opening.group?.workflow ?? null;
  const headline = workflowOpeningHeadline(opening, messages);
  const canListMembers = opening.driver !== null && opening.providerInstanceId !== null;
  const phases = (opening.group?.phases ?? []).filter((phase) => phase.members.length > 0);
  const unphased = opening.group?.unphasedMembers ?? [];
  const allMembers = [...phases.flatMap((phase) => phase.members), ...unphased];
  const failedMembers = allMembers.filter((member) => member.status === "failed");
  const logs = workflow?.logs ?? [];
  const limits = workflow?.limits;
  const usage = workflow?.usage ?? null;
  const resultText = workflow?.result?.trim() || null;
  const scriptPath = workflow?.runHandles?.scriptPath;
  const journalPath = workflow?.runHandles?.journalPath;

  const stopping = stoppingSubagentId !== null && stoppingSubagentId === opening.subagentId;
  const stopRequested = stopping || workflow?.stopRequestedAt !== undefined;
  const canStop =
    canOperateWorkflow &&
    opening.stoppable &&
    opening.parentThreadId !== null &&
    opening.subagentId !== null;

  const stopWorkflow = async () => {
    if (stopRequested || opening.parentThreadId === null || opening.subagentId === null) return;
    const targetId = opening.subagentId;
    setStoppingSubagentId(targetId);
    const result = await stopWorkflowCommand({
      environmentId: props.environmentId,
      input: {
        threadId: opening.parentThreadId,
        subagentId: NodeId.make(targetId),
      },
    });
    if (result._tag === "Failure") {
      setStoppingSubagentId((current) => (current === targetId ? null : current));
      Dialog.alert(gt("Could not stop workflow"));
    }
  };

  const metaItems: Array<string> = [];
  if (limits?.tokenBudget !== undefined) {
    const spent = (usage?.totalTokens ?? 0).toLocaleString();
    const budget = limits.tokenBudget.toLocaleString();
    metaItems.push(gt("{spent} / {budget} tokens", { spent, budget }));
  } else if (usage?.totalTokens !== undefined && usage.totalTokens > 0) {
    metaItems.push(gt("{count} tokens", { count: usage.totalTokens.toLocaleString() }));
  }
  if (usage?.toolUses !== undefined && usage.toolUses > 0) {
    metaItems.push(
      gt("{count, plural, one {{count} tool call} other {{count} tool calls}}", {
        count: usage.toolUses,
      }),
    );
  }
  if (limits?.concurrency !== undefined) {
    metaItems.push(gt("Concurrency {count}", { count: limits.concurrency }));
  }
  if (limits?.maxAgents !== undefined) {
    metaItems.push(gt("Max {count} agents", { count: limits.maxAgents }));
  }

  const renderMember = (member: RuntimeSubagent) => {
    const threadId = member.childThreadId;
    return (
      <Pressable
        key={member.id}
        accessibilityRole={threadId === null ? undefined : "link"}
        accessibilityHint={threadId === null ? undefined : gt("Opens this agent's thread")}
        disabled={threadId === null}
        onPress={() => {
          if (threadId !== null)
            openLinkedThread({ environmentId: String(props.environmentId), threadId });
        }}
        className="rounded-lg px-3 py-3 active:bg-subtle"
      >
        <View className="gap-1">
          <SubagentRow
            environmentId={props.environmentId}
            subagent={memberRowSubagent(member, opening, props.threadId, providers)}
            elapsed={null}
          />
          {member.cached === true ? (
            <View className="ml-5 self-start rounded bg-subtle px-1.5 py-0.5">
              <Text className="font-t3-medium text-2xs text-foreground-muted">
                <T context="workflow member replayed from journal">Cached</T>
              </Text>
            </View>
          ) : null}
        </View>
      </Pressable>
    );
  };
  return (
    <WorkLogBlock>
      <View className="mb-5 gap-3">
        <View className="flex-row items-start gap-2">
          <View className="min-w-0 flex-1 gap-0.5">
            <Text className="font-t3-medium text-2xs text-foreground-muted">
              <T>Workflow started</T>
            </Text>
            <Text numberOfLines={1} className="font-t3-medium text-sm text-foreground">
              {headline.title}
            </Text>
            {headline.description ? (
              <Text className="text-xs text-foreground-muted">{headline.description}</Text>
            ) : null}
            {metaItems.length > 0 ? (
              <Text className="text-2xs tabular-nums text-foreground-muted">
                {metaItems.join(" · ")}
              </Text>
            ) : null}
          </View>
          {headline.agents ? (
            <Text className="shrink-0 text-2xs tabular-nums text-foreground-muted">
              {headline.agents}
            </Text>
          ) : null}
        </View>
        {canStop ? (
          <Button
            tone="danger"
            size="compact"
            icon={stopRequested ? undefined : "stop.fill"}
            loading={stopRequested}
            disabled={stopRequested}
            label={stopRequested ? gt("Stopping…") : gt("Stop")}
            accessibilityLabel={
              stopRequested
                ? gt("Stopping workflow {label}", { label: headline.title })
                : gt("Stop workflow {label}", { label: headline.title })
            }
            onPress={() =>
              Dialog.confirm({
                title: gt("Stop workflow?"),
                message: gt("Every running agent in {label} stops.", { label: headline.title }),
                confirmText: gt("Stop", { $context: "verb, confirm button" }),
                destructive: true,
                onConfirm: () => void stopWorkflow(),
              })
            }
          />
        ) : null}
        {canListMembers
          ? phases.map((phase) => (
              <View key={phase.index} className="gap-px">
                <Text className="px-1 pt-1 font-t3-medium text-2xs text-foreground-muted">
                  {phase.title} · {phase.settledCount}/{phase.members.length}
                </Text>
                {phase.members.map(renderMember)}
              </View>
            ))
          : null}
        {canListMembers ? unphased.map(renderMember) : null}
        {failedMembers.length > 0 ? (
          <View className="gap-1 rounded-lg border border-border-subtle bg-subtle px-3 py-2.5">
            <Text className={cn(SUBAGENT_TONE_TEXT_CLASS.failed, "font-t3-medium text-2xs")}>
              <T>Failures</T>
            </Text>
            {failedMembers.map((member) => (
              <Text key={member.id} selectable className="text-xs text-foreground">
                <Text className="font-t3-medium text-foreground">{member.title}</Text>
                {": "}
                <Text className={SUBAGENT_TONE_TEXT_CLASS.failed}>
                  {member.error ?? member.result ?? gt("Failed")}
                </Text>
              </Text>
            ))}
          </View>
        ) : null}
        {resultText !== null ? (
          <View className="gap-1 rounded-lg bg-subtle px-3 py-2.5">
            <Text className="font-t3-medium text-2xs text-foreground-muted">
              <T>Result</T>
            </Text>
            <Text selectable className="text-xs text-foreground">
              {resultText}
            </Text>
          </View>
        ) : null}
        {logs.length > 0 ? <WorkflowLogs logs={logs} /> : null}
        {scriptPath || journalPath ? (
          <View className="gap-1 px-1">
            {scriptPath ? (
              <Text selectable className="font-mono text-2xs text-foreground-muted">
                <T>
                  Script: <Var>{scriptPath}</Var>
                </T>
              </Text>
            ) : null}
            {journalPath ? (
              <Text selectable className="font-mono text-2xs text-foreground-muted">
                <T>
                  Journal: <Var>{journalPath}</Var>
                </T>
              </Text>
            ) : null}
          </View>
        ) : null}
        <WorkflowScript script={opening.script} />
      </View>
    </WorkLogBlock>
  );
}

function WorkflowLogs(props: { readonly logs: ReadonlyArray<string> }) {
  const [expanded, setExpanded] = useState(false);
  const occurrences = new Map<string, number>();
  const entries = props.logs.map((entry) => {
    const count = (occurrences.get(entry) ?? 0) + 1;
    occurrences.set(entry, count);
    return { id: `${entry}#${count}`, entry };
  });
  return (
    <View className="gap-1.5">
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        onPress={() => setExpanded((value) => !value)}
        className="flex-row items-center gap-1.5 self-start py-1 active:opacity-70"
      >
        <SymbolView
          name={expanded ? "chevron.down" : "chevron.right"}
          size={11}
          tintColorClassName="accent-icon-subtle"
        />
        <Text className="font-t3-medium text-xs text-foreground-muted">
          <T>
            Logs (<Var>{props.logs.length}</Var>)
          </T>
        </Text>
      </Pressable>
      {expanded ? (
        <View className="gap-1 rounded-lg bg-subtle px-3 py-2.5">
          {entries.map(({ id, entry }) => (
            <Text key={id} selectable className="font-mono text-2xs leading-normal text-foreground">
              {entry}
            </Text>
          ))}
        </View>
      ) : null}
    </View>
  );
}

function WorkflowScript(props: { readonly script: string }) {
  const gt = useGT();
  const [expanded, setExpanded] = useState(false);
  const { themeAppearance } = useAppearancePreferences();
  const script = props.script.trimEnd();
  const lines = useMarkdownCodeHighlight({
    code: script,
    language: "javascript",
    theme: themeAppearance,
    enabled: true,
  });
  const lineCount = script.split("\n").length;
  const canFold = lineCount > FOLDED_SCRIPT_LINES;
  const shownLines = canFold && !expanded ? FOLDED_SCRIPT_LINES : lineCount;
  let offset = 0;
  const highlighted = lines?.slice(0, shownLines).flatMap((line, lineIndex) => {
    const separator = lineIndex > 0 ? "\n" : "";
    offset += separator.length;
    return [
      separator,
      ...line.map((token) => {
        const start = offset;
        offset += token.content.length;
        return (
          <Text key={start} style={token.color ? { color: token.color } : undefined}>
            {token.content}
          </Text>
        );
      }),
    ];
  });
  return (
    <View className="gap-1">
      <View className="rounded-lg bg-subtle px-3 py-2.5">
        <Text selectable className="font-mono text-2xs leading-normal text-foreground">
          {highlighted ?? script.split("\n").slice(0, shownLines).join("\n")}
        </Text>
      </View>
      {canFold ? (
        <Pressable
          accessibilityRole="button"
          accessibilityState={{ expanded }}
          onPress={() => setExpanded((value) => !value)}
          className="self-start py-1 active:opacity-70"
        >
          <Text className="font-t3-medium text-xs text-foreground-muted">
            {expanded
              ? gt("Show less", { $context: "collapses the workflow script" })
              : gt("Show full script")}
          </Text>
        </Pressable>
      ) : null}
    </View>
  );
}
