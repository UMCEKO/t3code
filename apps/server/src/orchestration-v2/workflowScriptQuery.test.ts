// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { it as effectIt } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { afterAll, assert, describe } from "vite-plus/test";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import { readWorkflowScript } from "./workflowScriptQuery.ts";

const root = NodePath.join(NodeOS.homedir(), ".claude", "projects", "__wf_script_test__");
NodeFS.mkdirSync(root, { recursive: true });
const scriptPath = NodePath.join(root, "run.js");
NodeFS.writeFileSync(scriptPath, "export const meta = {};\n");
const workflowsDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-wf-scripts-root-"));
const serverRunDir = NodePath.join(workflowsDir, "wf_server_run");
NodeFS.mkdirSync(serverRunDir, { recursive: true });
const serverScriptPath = NodePath.join(serverRunDir, "script.js");
NodeFS.writeFileSync(serverScriptPath, "export const meta = { name: 'server' };\n");
const emptyHome = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-wf-empty-home-"));
const outside = NodePath.join(NodeOS.tmpdir(), "wf-outside.js");
NodeFS.writeFileSync(outside, "evil\n");
const link = NodePath.join(root, "sneaky.js");
const serverLink = NodePath.join(serverRunDir, "sneaky.js");
if (symlinksSupported) {
  NodeFS.rmSync(link, { force: true });
  NodeFS.symlinkSync(outside, link);
  if (!NodeFS.lstatSync(link).isSymbolicLink()) {
    throw new Error("test setup: sneaky.js must be a symlink");
  }
  NodeFS.symlinkSync(outside, serverLink);
}

afterAll(() => {
  NodeFS.rmSync(root, { recursive: true, force: true });
  NodeFS.rmSync(workflowsDir, { recursive: true, force: true });
  NodeFS.rmSync(emptyHome, { recursive: true, force: true });
  NodeFS.rmSync(outside, { force: true });
});

describe("workflow script containment", () => {
  effectIt.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "rejects a FIFO without waiting for a writer",
    () =>
      Effect.gen(function* () {
        const fifo = NodePath.join(root, "pipe.js");
        NodeChildProcess.execFileSync("mkfifo", [fifo]);
        const error = yield* Effect.flip(readWorkflowScript({ scriptPath: fifo }));
        assert.equal(error.reason, "not-regular-file");
      }),
  );

  effectIt.effect("serves a real script under the projects root", () =>
    Effect.gen(function* () {
      const result = yield* readWorkflowScript({ scriptPath });
      assert.include(result.contents, "export const meta");
      assert.equal(result.truncated, false);
    }),
  );

  effectIt.effect(
    "serves a server workflow script under workflowsDir even when ~/.claude/projects is absent",
    () =>
      Effect.gen(function* () {
        const result = yield* readWorkflowScript({
          scriptPath: serverScriptPath,
          workflowsDir,
        }).pipe(Effect.provideService(HostProcess.HomeDirectory, emptyHome));
        assert.include(result.contents, "name: 'server'");
        assert.equal(result.truncated, false);
      }),
  );

  effectIt.effect.skipIf(!symlinksSupported)(
    "rejects paths outside workflowsDir and symlink escapes from workflowsDir",
    () =>
      Effect.gen(function* () {
        const outsideErr = yield* Effect.flip(
          readWorkflowScript({ scriptPath: outside, workflowsDir }).pipe(
            Effect.provideService(HostProcess.HomeDirectory, emptyHome),
          ),
        );
        assert.equal(outsideErr.reason, "outside-root");
        const symlinkErr = yield* Effect.flip(
          readWorkflowScript({ scriptPath: serverLink, workflowsDir }).pipe(
            Effect.provideService(HostProcess.HomeDirectory, emptyHome),
          ),
        );
        assert.equal(symlinkErr.reason, "outside-root");
      }),
  );

  effectIt.effect.skipIf(!symlinksSupported)(
    "rejects paths outside the root and symlink escapes",
    () =>
      Effect.gen(function* () {
        const escaped = yield* Effect.exit(readWorkflowScript({ scriptPath: outside }));
        assert.equal(escaped._tag, "Failure");
        const sneaky = yield* Effect.exit(
          readWorkflowScript({ scriptPath: link }).pipe(
            Effect.flip,
            Effect.map((error) => error.reason),
          ),
        );
        assert.equal(sneaky._tag, "Success");
        if (sneaky._tag === "Success") {
          assert.equal(sneaky.value, "outside-root");
        }
      }),
  );
});
