import {
  defineRailway,
  preserve,
  project,
  service,
  type BuildConfig,
  type DeployConfig,
  type SourceConfig,
} from "railway/iac";
import capturedServices from "./services.json" with { type: "json" };

// A named partial owns only this repository's services in the shared project.
export const partial = "investintell-workers";

type CapturedService = {
  name: string;
  source: SourceConfig | null;
  build: BuildConfig;
  deploy: DeployConfig;
  variables: string[];
};

const services = capturedServices as CapturedService[];

// Verify the linked target with check-target.mjs before operational commands.
// Authoring must also evaluate when a CLI supplies an empty context.
export default defineRailway(() => {
  const resources = services.map((captured) => {
    const node = service(captured.name, {
      env: Object.fromEntries(captured.variables.map((name) => [name, preserve()])),
    });

    // railway 3.13.0's factory drops null fields and normalizes source defaults.
    // Retain the captured source/build/deploy exactly, including null and []:
    // clearing a cron differs from parking it, and custom Docker paths matter.
    Object.assign(node, {
      source: structuredClone(captured.source),
      build: structuredClone(captured.build),
      deploy: structuredClone(captured.deploy),
    });
    node.kind = captured.source?.type === "github" ? "github" : "empty";

    // Networking is intentionally sparse. Activation requires CLI 5.63.4,
    // whose planner preserves domains omitted from this partial.
    return node;
  });

  return project("investintell-db", { resources });
});
