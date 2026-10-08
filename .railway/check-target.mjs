import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import baseline from "./baseline.json" with { type: "json" };

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cliVersion = "5.63.4";

export function linkedProject(projects, directory) {
  // Match the pinned CLI's nearest-ancestor, exact path-key lookup. Windows
  // config can contain differently linked keys that differ only by case.
  for (let current = directory; ; current = dirname(current)) {
    if (Object.hasOwn(projects, current)) return projects[current];
    if (dirname(current) === current) throw new Error("No Railway project is linked to this repository.");
  }
}

export function validateTarget(link, status) {
  assert.equal(link.project, baseline.project.id, "Linked Railway project ID differs from the reviewed baseline.");
  assert.equal(link.environment, baseline.environment.id, "Linked Railway environment ID differs from the reviewed baseline.");
  assert.equal(status.id, baseline.project.id, "Live Railway project ID differs from the reviewed baseline.");
  assert.equal(status.name, baseline.project.name, "Live Railway project name differs from the reviewed baseline.");
  const environments = status.environments?.edges;
  assert.ok(Array.isArray(environments) && environments.length === 1, "Railway status must be scoped to exactly the linked environment.");
  assert.equal(environments[0].node?.id, baseline.environment.id, "Live Railway environment ID differs from the reviewed baseline.");
  assert.equal(environments[0].node?.name, baseline.environment.name, "Live Railway environment name differs from the reviewed baseline.");
}

/** Read-only preflight; never invokes config plan/apply, link, or any mutation. */
export function checkTarget(cli, { env = process.env, run = execFileSync, readConfig = () => JSON.parse(readFileSync(join(homedir(), ".railway/config.json"), "utf8")) } = {}) {
  assert.ok(cli, "Provide the explicit path to Railway CLI 5.63.4.");
  for (const name of ["RAILWAY_TOKEN", "RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_ID"]) {
    assert.ok(!env[name], `Unset ${name} so the preflight and operational command use the same local link.`);
  }
  assert.ok(!env.RAILWAY_ENV || env.RAILWAY_ENV === "production", "Use the production Railway API and config file (unset RAILWAY_ENV or use production).");
  const options = { cwd: repositoryRoot, env, encoding: "utf8", timeout: 60_000, stdio: ["ignore", "pipe", "pipe"] };
  const read = (args) => {
    try {
      return run(cli, args, options);
    } catch {
      throw new Error(`Railway ${args[0]} read failed; verify CLI access and the local link.`);
    }
  };
  assert.equal(read(["--version"]).trim(), `railway ${cliVersion}`, "Use the pinned Railway CLI 5.63.4.");
  let config;
  try {
    config = readConfig();
  } catch {
    throw new Error("Cannot read Railway link configuration; verify the local config.json file.");
  }
  const link = linkedProject(config.projects ?? {}, repositoryRoot);
  // Check IDs before querying: status --json without --environment lists every
  // environment and cannot establish the locally selected target.
  assert.equal(link.project, baseline.project.id, "Linked Railway project ID differs from the reviewed baseline.");
  assert.equal(link.environment, baseline.environment.id, "Linked Railway environment ID differs from the reviewed baseline.");
  const statusText = read(["status", "--environment", link.environment, "--json"]);
  let status;
  try {
    status = JSON.parse(statusText);
  } catch {
    throw new Error("Malformed Railway status JSON; cannot verify the linked target.");
  }
  validateTarget(link, status);
  return { projectId: link.project, environmentId: link.environment };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    assert.equal(process.argv.length, 3, "Usage: node .railway/check-target.mjs <path-to-pinned-railway-cli>");
    const target = checkTarget(process.argv[2]);
    console.log(`PASS: CLI ${cliVersion}, linked ${baseline.project.name}/${baseline.environment.name} (${target.projectId}/${target.environmentId}); read-only status verified.`);
  } catch (error) {
    // Do not echo CLI stdout/stderr or configuration, which may contain secrets.
    console.error(error.message);
    process.exitCode = 1;
  }
}
