import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createRailwayContext, project } from "railway/iac";
import program, { partial } from "./railway.ts";

/** Evaluate authoring code locally; no Railway API or CLI is invoked. */
export async function evaluate(overrides = {}) {
  const ctx = createRailwayContext({
    command: "plan",
    projectName: "investintell-db",
    environment: "production",
    ...overrides,
  });
  return { partial, project: await program(ctx, project) };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.stdout.write(`${JSON.stringify(await evaluate(), null, 2)}\n`);
}
