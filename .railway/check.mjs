import assert from "node:assert/strict";
import baseline from "./baseline.json" with { type: "json" };
import { evaluate } from "./evaluate.mjs";
import program from "./railway.ts";
import { createRailwayContext, project as projectFactory } from "railway/iac";

const { partial, project } = await evaluate();
const expectedServices = baseline.services.filter((service) => service.owner === partial);
assert.ok(expectedServices.length > 0, `No baseline services belong to partial ${partial}`);
assert.equal(project.name, baseline.project.name);
assert.equal(baseline.environment.name, "production");
assert.deepEqual(Object.keys(project).sort(), ["name", "resources"]);

const expectedNames = expectedServices.map((service) => service.name).sort();
const actualNames = project.resources.map((service) => service.name).sort();
assert.equal(new Set(expectedNames).size, expectedNames.length, "Duplicate baseline service");
assert.equal(new Set(actualNames).size, actualNames.length, "Duplicate authored service");
assert.deepEqual(actualNames, expectedNames, "Authored service ownership differs from the baseline");

const byName = new Map(expectedServices.map((service) => [service.name, service]));
let variableCount = 0;
for (const service of project.resources) {
  const captured = byName.get(service.name);
  assert.equal(service.type, "service");
  assert.equal(service.address, `service.${service.name}`);
  assert.equal(service.kind, captured.effective.source?.type === "github" ? "github" : "empty");
  assert.deepEqual(
    Object.keys(service).sort(),
    ["address", "build", "deploy", "kind", "name", "source", "type", "variables"],
    `${service.name}: unexpected managed fields`,
  );
  for (const field of ["source", "build", "deploy"]) {
    assert.deepEqual(service[field], captured.effective[field], `${service.name}.${field} differs`);
  }

  const variableNames = [...captured.current.variables].sort();
  assert.deepEqual(Object.keys(service.variables).sort(), variableNames, `${service.name}: variable names differ`);
  for (const name of variableNames) {
    assert.deepEqual(service.variables[name], { type: "preserve" }, `${service.name}.${name} must preserve its value`);
  }
  variableCount += variableNames.length;

  for (const field of ["tracing", "groupId", "volumeAttachments"]) {
    assert.equal(captured.current[field], null, `${service.name}: ${field} requires explicit migration review`);
  }
}

// A populated context must produce the same graph as literal empty-context
// evaluation. Target validation belongs to the separate operational preflight.
for (const command of ["plan", "apply", "migrate"]) {
  const context = createRailwayContext({ command, projectName: baseline.project.name, environment: baseline.environment.name });
  assert.deepEqual(await program(context, projectFactory), project);
}

const serialized = JSON.stringify(project);
assert.doesNotMatch(serialized, /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
assert.doesNotMatch(serialized, /\.railway\.app\b/);
console.log(`Equivalent local SDK graph: ${project.resources.length} services, ${variableCount} preserved variable names; empty-context evaluation passes.`);
