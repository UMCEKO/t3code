import { T, Var, useGT, useMessages } from "gt-react";
import type { SideThreadOpenIntent } from "@t3tools/client-runtime/side-thread";
import { useSideThreadTrigger } from "../../hooks/useSideThreadTrigger";
import { useAtomValue } from "@effect/atom-react";
import {
  isTerminalSubagentStatus,
  workflowMemberModelSelection,
  workflowOpeningHeadline,
  type RuntimeSubagent,
  type WorkflowOpening,
} from "@t3tools/client-runtime/state/subagentRuntime";
import {
  NodeId,
  type EnvironmentId,
  type OrchestrationV2Subagent,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ScopedThreadRef,
  type ServerProvider,
  type ThreadId,
} from "@t3tools/contracts";
import {
  ChevronDownIcon,
  ChevronRightIcon,
  FileCode2Icon,
  FileTextIcon,
  SquareIcon,
  WorkflowIcon,
} from "lucide-react";
import { useState, type ReactNode } from "react";

import { cn } from "../../lib/utils";
import { useRightPanelStore } from "../../rightPanelStore";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useEnvironmentQuery } from "../../state/query";
import { providersIgnoringCheckedAtAtom } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import ChatMarkdown from "../ChatMarkdown";
import { ThreadHoverCardPopup } from "../ThreadHoverCard";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { MiddleTruncate } from "../ui/middle-truncate";
import { Spinner } from "../ui/spinner";
import { toastManager } from "../ui/toast";
import { Tooltip, TooltipTrigger } from "../ui/tooltip";
import { AgentElapsed } from "./AgentElapsed";
import { findRevealRef } from "./markdownFindContext";
import { useTallEnoughToFold } from "./useTallEnoughToFold";
import { SubagentTooltipContent } from "./SubagentTooltipContent";
import { EnvironmentSubagentAvatar } from "./V2LifecycleRow";

const FOLDED_SCRIPT_MASK = "linear-gradient(to bottom, black calc(100% - 1.75rem), transparent)";

function scriptMarkdown(script: string) {
  const longestRun = Math.max(2, ...(script.match(/`+/g) ?? []).map((run) => run.length));
  const fence = "`".repeat(longestRun + 1);
  return `${fence}javascript\n${script.trimEnd()}\n${fence}`;
}

export function WorkflowOpeningBlock(props: {
  readonly opening: WorkflowOpening;
  readonly environmentId: EnvironmentId;
  readonly threadRef?: ScopedThreadRef | null | undefined;
  readonly canOperate?: boolean | undefined;
  readonly onOpenThread: (threadId: string, intent?: SideThreadOpenIntent) => void;
  readonly onOpenFile?: ((path: string) => void) | undefined;
}) {
  const gt = useGT();
  const { opening } = props;
  const m = useMessages();
  const providers = useAtomValue(providersIgnoringCheckedAtAtom(props.environmentId));
  const canOperateWorkflow = useAtomValue(
    orchestrationEnvironment.stopWorkflow.permissionAtom(props.environmentId),
  );
  const stopWorkflowCommand = useAtomCommand(orchestrationEnvironment.stopWorkflow);
  const [stoppingSubagentId, setStoppingSubagentId] = useState<string | null>(null);

  const workflow = opening.group?.workflow ?? null;
  const headline = workflowOpeningHeadline(opening, m);
  const phases = opening.group?.phases.filter((phase) => phase.members.length > 0) ?? [];
  const unphasedMembers = opening.group?.unphasedMembers ?? [];
  const allMembers = [...phases.flatMap((phase) => phase.members), ...unphasedMembers];
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
    (props.canOperate ?? true) &&
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
      toastManager.add({ type: "error", title: gt("Could not stop workflow") });
    }
  };

  const canOpenFile =
    props.onOpenFile !== undefined || (props.threadRef !== null && props.threadRef !== undefined);
  const openFilePath = (filePath: string) => {
    if (props.onOpenFile) {
      props.onOpenFile(filePath);
    } else if (props.threadRef) {
      useRightPanelStore.getState().openFile(props.threadRef, filePath);
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

  const memberRow = (member: RuntimeSubagent) => {
    const memberProviderInstanceId = member.providerInstanceId ?? opening.providerInstanceId;
    if (memberProviderInstanceId === null) return null;
    const memberProvider = providers.find(
      (provider) => provider.instanceId === memberProviderInstanceId,
    );
    const memberDriver =
      memberProvider?.driver ??
      (memberProviderInstanceId === opening.providerInstanceId
        ? (opening.driver ?? undefined)
        : undefined);
    return (
      <WorkflowMemberRow
        key={member.id}
        member={member}
        environmentId={props.environmentId}
        providerInstanceId={memberProviderInstanceId}
        provider={memberProvider}
        providers={providers}
        driver={memberDriver}
        origin={opening.origin ?? "provider_native"}
        onOpenThread={props.onOpenThread}
      />
    );
  };

  return (
    <section
      aria-label={gt("Workflow started: {title}", { title: headline.title })}
      className="flex min-w-0 flex-col gap-3 px-1 py-0.5"
      data-workflow-opening
    >
      <header className="flex min-w-0 items-start gap-2">
        <WorkflowIcon aria-hidden className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
        <div className="flex min-w-0 flex-1 flex-col gap-0.5">
          <p className="text-2xs font-medium text-muted-foreground">
            <T>Workflow started</T>
          </p>
          <h3 className="truncate text-sm font-medium text-foreground" data-thread-find-text="true">
            {headline.title}
          </h3>
          {headline.description ? (
            <p className="text-xs text-muted-foreground" data-thread-find-text="true">
              {headline.description}
            </p>
          ) : null}
          {metaItems.length > 0 ? (
            <p className="text-2xs tabular-nums text-muted-foreground" data-workflow-limits>
              {metaItems.join(" · ")}
            </p>
          ) : null}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {headline.agents ? (
            <span
              aria-live="polite"
              className="shrink-0 text-2xs tabular-nums text-muted-foreground"
            >
              {headline.agents}
            </span>
          ) : null}
          {canStop ? (
            <Button
              type="button"
              size="xs"
              variant="destructive-outline"
              disabled={stopRequested}
              aria-label={
                stopRequested
                  ? gt("Stopping workflow {title}", { title: headline.title })
                  : gt("Stop workflow {title}", { title: headline.title })
              }
              onClick={() => void stopWorkflow()}
            >
              {stopRequested ? (
                <>
                  <Spinner aria-hidden size="xs" />
                  <T>Stopping…</T>
                </>
              ) : (
                <>
                  <SquareIcon aria-hidden className="size-3 fill-current" />
                  <T>Stop</T>
                </>
              )}
            </Button>
          ) : null}
        </div>
      </header>
      {phases.map((phase) => (
        <section key={phase.index} aria-label={gt("{title} phase", { title: phase.title })}>
          <h4 className="flex items-baseline gap-2 px-2 pb-0.5 text-2xs font-medium text-muted-foreground">
            <span className="min-w-0 truncate" data-thread-find-text="true">
              {phase.title}
            </span>
            <span className="shrink-0 font-normal tabular-nums">
              {phase.settledCount}/{phase.members.length}
            </span>
          </h4>
          <ul className="m-0 list-none p-0">{phase.members.map(memberRow)}</ul>
        </section>
      ))}
      {unphasedMembers.length > 0 ? (
        <ul className="m-0 list-none p-0">{unphasedMembers.map(memberRow)}</ul>
      ) : null}
      {failedMembers.length > 0 ? (
        <section
          aria-label={gt("Failures")}
          className="flex min-w-0 flex-col gap-1 rounded-md border border-destructive/25 bg-destructive/5 px-2.5 py-2"
          data-workflow-failures
        >
          <h4 className="text-2xs font-medium text-destructive">
            <T>Failures</T>
          </h4>
          <ul className="m-0 flex list-none flex-col gap-1 p-0">
            {failedMembers.map((member) => (
              <li key={member.id} className="min-w-0 text-xs">
                <span className="font-medium text-foreground">{member.title}</span>
                {": "}
                <span className="text-destructive">
                  {member.error ?? member.result ?? gt("Failed")}
                </span>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      {resultText !== null ? (
        <section
          aria-label={gt("Result")}
          className="flex min-w-0 flex-col gap-1 rounded-md border border-border/60 bg-muted/30 px-2.5 py-2"
          data-workflow-result
        >
          <h4 className="text-2xs font-medium text-muted-foreground">
            <T>Result</T>
          </h4>
          <div className="min-w-0 text-xs" data-thread-find-text="true">
            <ChatMarkdown text={resultText} cwd={undefined} />
          </div>
        </section>
      ) : null}
      {logs.length > 0 ? <WorkflowLogs logs={logs} /> : null}
      {scriptPath || journalPath ? (
        <div
          className="flex flex-wrap items-center gap-x-3 gap-y-1 text-2xs text-muted-foreground"
          data-workflow-paths
        >
          {scriptPath ? (
            <WorkflowPathItem
              label={gt("Script")}
              path={scriptPath}
              icon={<FileCode2Icon aria-hidden className="size-3 shrink-0" />}
              onOpen={canOpenFile ? () => openFilePath(scriptPath) : undefined}
            />
          ) : null}
          {journalPath ? (
            <WorkflowPathItem
              label={gt("Journal")}
              path={journalPath}
              icon={<FileTextIcon aria-hidden className="size-3 shrink-0" />}
              onOpen={canOpenFile ? () => openFilePath(journalPath) : undefined}
            />
          ) : null}
        </div>
      ) : null}
      <WorkflowScript
        script={opening.script}
        scriptPath={scriptPath}
        threadId={props.threadRef?.threadId ?? null}
        environmentId={props.environmentId}
      />
    </section>
  );
}

function WorkflowPathItem(props: {
  readonly label: string;
  readonly path: string;
  readonly icon: ReactNode;
  readonly onOpen: (() => void) | undefined;
}) {
  const gt = useGT();
  const content = (
    <>
      {props.icon}
      <span className="shrink-0 font-medium">{props.label}:</span>
      <span className="min-w-0 max-w-64 font-mono">
        <MiddleTruncate value={props.path} />
      </span>
    </>
  );
  if (props.onOpen) {
    return (
      <button
        type="button"
        aria-label={gt("Open {label}: {path}", { label: props.label, path: props.path })}
        onClick={props.onOpen}
        className="inline-flex min-w-0 max-w-full cursor-pointer items-center gap-1 rounded px-1 py-0.5 transition-colors hover:bg-accent/40 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/70"
      >
        {content}
      </button>
    );
  }
  return <span className="inline-flex min-w-0 max-w-full items-center gap-1">{content}</span>;
}

function WorkflowLogs(props: { readonly logs: ReadonlyArray<string> }) {
  const gt = useGT();
  const [expanded, setExpanded] = useState(false);
  const revealRef = findRevealRef(() => setExpanded(true));
  const occurrences = new Map<string, number>();
  const entries = props.logs.map((entry) => {
    const count = (occurrences.get(entry) ?? 0) + 1;
    occurrences.set(entry, count);
    return { id: `${entry}#${count}`, entry };
  });
  return (
    <div ref={revealRef} className="flex min-w-0 flex-col gap-1" data-workflow-logs>
      <div className="-ms-1">
        <Button
          type="button"
          size="xs"
          variant="ghost-muted"
          aria-expanded={expanded}
          data-scroll-anchor-ignore
          onClick={() => setExpanded((value) => !value)}
        >
          <ChevronDownIcon
            aria-hidden
            className={cn("size-3.5 transition-transform", !expanded && "-rotate-90")}
          />
          <T>
            Logs (<Var>{props.logs.length}</Var>)
          </T>
        </Button>
      </div>
      {expanded ? (
        <ol
          aria-label={gt("Workflow logs")}
          className="m-0 max-h-56 list-none space-y-0.5 overflow-y-auto rounded-md border border-border/60 bg-muted/30 px-2.5 py-2 font-mono text-2xs leading-relaxed text-muted-foreground"
          data-thread-find-text="true"
        >
          {entries.map(({ id, entry }) => (
            <li key={id} className="whitespace-pre-wrap break-words">
              {entry}
            </li>
          ))}
        </ol>
      ) : null}
    </div>
  );
}

function WorkflowMemberRow(props: {
  readonly member: RuntimeSubagent;
  readonly environmentId: EnvironmentId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly provider: ServerProvider | undefined;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly driver: ProviderDriverKind | undefined;
  readonly origin: OrchestrationV2Subagent["origin"];
  readonly onOpenThread: (threadId: string, intent?: SideThreadOpenIntent) => void;
}) {
  const gt = useGT();
  const trigger = useSideThreadTrigger();
  const { member } = props;
  const threadId = member.childThreadId;
  const failed = member.status === "failed";
  const detail = (
    isTerminalSubagentStatus(member.status)
      ? (member.error ?? member.result ?? member.progress)
      : member.progress
  )
    ?.replace(/`/g, "")
    .replace(/\s+/g, " ")
    .trim();
  const className =
    "group/subagent flex w-full min-w-0 items-center gap-2.5 rounded-md px-2 py-1.5 text-left";
  return (
    <li>
      <Tooltip>
        <TooltipTrigger
          delay={200}
          render={
            threadId === null ? (
              <div aria-description={member.status} className={className} />
            ) : (
              <button
                type="button"
                aria-label={gt("Open {title}", { title: member.title })}
                aria-description={member.status}
                {...trigger((intent) => props.onOpenThread(threadId, intent))}
                className={cn(
                  className,
                  "cursor-pointer transition-colors hover:bg-accent/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/70",
                )}
              />
            )
          }
        >
          <EnvironmentSubagentAvatar
            environmentId={props.environmentId}
            providerInstanceId={props.providerInstanceId}
            driver={props.driver}
            status={member.status}
          />
          <span className="min-w-0 flex-1">
            <span className="flex min-w-0 items-center gap-1.5">
              <span className="min-w-0 truncate text-xs font-medium text-foreground">
                {member.title}
              </span>
              {member.cached === true ? (
                <Badge variant="secondary" size="sm">
                  <T context="workflow member replayed from journal">Cached</T>
                </Badge>
              ) : null}
            </span>
            {detail ? (
              <span
                className={cn(
                  "block truncate text-2xs leading-relaxed",
                  failed ? "text-destructive" : "text-muted-foreground",
                )}
              >
                {detail}
              </span>
            ) : null}
          </span>
          {member.startedAt ? (
            <span className="shrink-0 text-xs text-muted-foreground tabular-nums">
              <AgentElapsed agent={member} />
            </span>
          ) : null}
          {threadId !== null ? (
            <ChevronRightIcon
              aria-hidden
              className="size-3.5 shrink-0 text-muted-foreground/60 transition-colors group-hover/subagent:text-foreground"
            />
          ) : null}
        </TooltipTrigger>
        <ThreadHoverCardPopup>
          <SubagentTooltipContent
            title={member.title}
            model={member.model}
            providerInstanceId={props.providerInstanceId}
            origin={props.origin}
            modelSelection={workflowMemberModelSelection(member, props.providerInstanceId)}
            provider={props.provider}
            providers={props.providers}
            driver={props.driver}
            elapsed={<AgentElapsed agent={member} />}
            status={member.status}
            result={failed ? (member.error ?? member.result) : (member.result ?? member.error)}
            progress={member.progress}
          />
        </ThreadHoverCardPopup>
      </Tooltip>
    </li>
  );
}

function WorkflowScript(props: {
  readonly script: string;
  readonly scriptPath: string | undefined;
  readonly threadId: ThreadId | null;
  readonly environmentId: EnvironmentId;
}) {
  const gt = useGT();
  const [expanded, setExpanded] = useState(false);
  const revealRef = findRevealRef(() => setExpanded(true));
  const { ref: contentRef, tallEnough: canFold } = useTallEnoughToFold();
  const needsRemoteScript =
    props.script.trim().length === 0 && props.scriptPath !== undefined && props.threadId !== null;
  const loadedScript = useEnvironmentQuery(
    needsRemoteScript
      ? orchestrationEnvironment.workflowScript({
          environmentId: props.environmentId,
          input: { threadId: props.threadId!, scriptPath: props.scriptPath! },
        })
      : null,
  );
  const scriptText =
    props.script.trim().length > 0 ? props.script : (loadedScript.data?.contents ?? "");
  if (scriptText.length === 0) return null;
  const folded = canFold && !expanded;
  return (
    <div>
      <div
        ref={revealRef}
        className={cn("relative", folded && "max-h-44 overflow-hidden")}
        data-thread-find-text="true"
        data-thread-find-fold={folded ? "" : undefined}
        style={
          folded
            ? { WebkitMaskImage: FOLDED_SCRIPT_MASK, maskImage: FOLDED_SCRIPT_MASK }
            : undefined
        }
      >
        <div ref={contentRef}>
          <ChatMarkdown text={scriptMarkdown(scriptText)} cwd={undefined} />
        </div>
      </div>
      {canFold ? (
        <div className="-ms-1">
          <Button
            type="button"
            size="xs"
            variant="ghost-muted"
            aria-expanded={expanded}
            data-scroll-anchor-ignore
            onClick={() => setExpanded((value) => !value)}
          >
            {expanded ? gt("Show less") : gt("Show full script")}
          </Button>
        </div>
      ) : null}
    </div>
  );
}
