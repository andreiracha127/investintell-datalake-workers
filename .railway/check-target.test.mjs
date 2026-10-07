import assert from "node:assert/strict";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import baseline from "./baseline.json" with { type: "json" };
import { checkTarget, linkedProject, validateTarget } from "./check-target.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const link = { project: baseline.project.id, environment: baseline.environment.id };
const status = { id: baseline.project.id, name: baseline.project.name, environments: { edges: [{ node: { id: baseline.environment.id, name: baseline.environment.name } }] } };

function harness({ version = "railway 5.63.4", live = status, linked = link } = {}) {
  const calls = [];
  return {
    calls,
    options: {
      env: {},
      readConfig: () => ({ projects: { [root]: linked } }),
      run: (cli, args, options) => {
        calls.push({ cli, args, cwd: options.cwd });
        return args[0] === "--version" ? version : JSON.stringify(live);
      },
    },
  };
}

test("preflight uses only pinned version and scoped read-only status in the repository", () => {
  const { calls, options } = harness();
  assert.deepEqual(checkTarget("/tooling/railway", options), { projectId: link.project, environmentId: link.environment });
  assert.deepEqual(calls, [
    { cli: "/tooling/railway", args: ["--version"], cwd: root },
    { cli: "/tooling/railway", args: ["status", "--environment", link.environment, "--json"], cwd: root },
  ]);
});

test("rejects wrong linked IDs even when live status would show the desired target", () => {
  for (const key of ["project", "environment"]) {
    const { calls, options } = harness({ linked: { ...link, [key]: "replaced-id" } });
    assert.throws(() => checkTarget("railway", options), /Linked Railway .* ID/);
    assert.equal(calls.length, 1);
  }
});

test("rejects replaced IDs and renamed targets in live read-back", () => {
  for (const key of ["id", "name"]) {
    assert.throws(() => validateTarget(link, { ...status, [key]: "other" }), /Live Railway project/);
    assert.throws(() => validateTarget(link, { ...status, environments: { edges: [{ node: { ...status.environments.edges[0].node, [key]: "other" } }] } }), /Live Railway environment/);
  }
});

test("rejects unscoped status listing production alongside staging", () => {
  const { options } = harness({ live: { ...status, environments: { edges: [...status.environments.edges, { node: { id: "staging-id", name: "staging" } }] } } });
  assert.throws(() => checkTarget("railway", options), /exactly the linked environment/);
});

test("rejects targeting overrides and staging API before invoking CLI", () => {
  for (const name of ["RAILWAY_TOKEN", "RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_ID", "RAILWAY_ENV"]) {
    const { calls, options } = harness();
    assert.throws(() => checkTarget("railway", { ...options, env: { [name]: "other" } }));
    assert.equal(calls.length, 0);
  }
});

test("rejects an unpinned CLI before reading status", () => {
  const { calls, options } = harness({ version: "railway 5.45.0" });
  assert.throws(() => checkTarget("railway", options), /pinned Railway CLI/);
  assert.equal(calls.length, 1);
});

test("link lookup chooses nearest ancestor and preserves path case", () => {
  const parent = dirname(root);
  assert.equal(linkedProject({ [parent]: "parent", [root]: "nearest" }, root), "nearest");
  assert.equal(linkedProject({ [parent]: "parent" }, root), "parent");
  assert.throws(() => linkedProject({}, root), /No Railway project/);
  const alternate = root === root.toLowerCase() ? root.toUpperCase() : root.toLowerCase();
  assert.throws(() => linkedProject({ [alternate]: link }, root), /No Railway project/);
});

test("CLI failures and malformed JSON never pass the preflight", () => {
  const { options } = harness();
  assert.throws(() => checkTarget("railway", { ...options, run: () => { throw new Error("private CLI error output"); } }), { message: "Railway --version read failed; verify CLI access and the local link." });
  assert.throws(() => checkTarget("railway", { ...options, run: (_cli, args) => {
    if (args[0] === "--version") return "railway 5.63.4";
    throw new Error("private status output");
  } }), { message: "Railway status read failed; verify CLI access and the local link." });
  assert.throws(() => checkTarget("railway", { ...options, run: (_cli, args) => args[0] === "--version" ? "railway 5.63.4" : "invalid json" }), { message: "Malformed Railway status JSON; cannot verify the linked target." });
});

test("malformed config and status JSON errors never expose private input", () => {
  const privateInput = '{"user":{"accessToken":DUMMY_PRIVATE_INPUT}}';
  const { options } = harness();
  for (const [overrides, expected] of [
    [{ readConfig: () => JSON.parse(privateInput) }, "Cannot read Railway link configuration; verify the local config.json file."],
    [{ run: (_cli, args) => args[0] === "--version" ? "railway 5.63.4" : privateInput }, "Malformed Railway status JSON; cannot verify the linked target."],
  ]) {
    assert.throws(() => checkTarget("railway", { ...options, ...overrides }), (error) => {
      assert.equal(error.message, expected);
      assert.doesNotMatch(error.message, /DUMMY_PRIVATE_INPUT|accessToken/);
      assert.equal(error.cause, undefined);
      return true;
    });
  }
});
