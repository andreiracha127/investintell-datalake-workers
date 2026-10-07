"""Capture Railway configuration with GraphQL queries only and no secret values.

Uses the existing account token in ~/.railway/config.json. It never refreshes a
token, links a project, runs a plan, deploys, or calls a mutation. Raw environment
configuration is filtered in memory before writing the snapshot.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import json
from pathlib import Path
import urllib.request

PROJECT = "35fa36a3-2641-42b2-b48b-540eac0597c6"
ENVIRONMENT = "5a1a1355-7b6e-49a1-970a-09e0c44fd156"
BUILD_KEYS = {"builder", "buildCommand", "dockerfilePath", "watchPatterns", "buildEnvironment", "nixpacksPlan", "nixpacksConfigPath"}
DEPLOY_KEYS = {"cronSchedule", "restartPolicyType", "restartPolicyMaxRetries", "startCommand", "preDeployCommand", "preDeployTimeoutSeconds", "healthcheckPath", "healthcheckTimeout", "numReplicas", "region", "multiRegionConfig", "sleepApplication", "overlapSeconds", "drainingSeconds", "runtime", "useLegacyStacker", "ipv6EgressEnabled", "limitOverride", "requiredMountPath"}
SOURCE_KEYS = {"repo", "image", "branch", "rootDirectory", "checkSuites", "autoUpdates"}

SNAPSHOT_QUERY = """query MigrationSnapshot($project: String!, $environment: String!) {
  project(id: $project) { id name prDeploys botPrEnvironments }
  environment(id: $environment) {
    id name projectId configEtag iacPartials config
    variables(first: 2000) {
      pageInfo { hasNextPage endCursor }
      edges { node { name serviceId isSealed } }
    }
    volumeInstances(first: 200) {
      pageInfo { hasNextPage endCursor }
      edges { node { serviceId mountPath volumeId } }
    }
    deploymentTriggers(first: 200) {
      pageInfo { hasNextPage endCursor }
      edges { node { serviceId branch repository provider checkSuites } }
    }
    serviceInstances(first: 200) {
      pageInfo { hasNextPage endCursor }
      edges { node {
        id serviceId serviceName updatedAt builder buildCommand dockerfilePath
        rootDirectory watchPatterns cronSchedule nextCronRunAt
        restartPolicyType restartPolicyMaxRetries startCommand preDeployCommand
        preDeployTimeoutSeconds healthcheckPath healthcheckTimeout numReplicas
        region sleepApplication overlapSeconds drainingSeconds railwayConfigFile
        tracingEnabled autoInstrumentationEnabled service { groupId } source { repo image }
        resolvedFileConfig {
          configFile commitHash deploymentId resolvedAt repo
          fileManifest propertyFileMapping
        }
        latestDeployment {
          id status createdAt deploymentStopped meta instances { id status }
        }
        activeDeployments {
          id status createdAt deploymentStopped instances { id status }
        }
      } }
    }
  }
}"""

EXECUTIONS_QUERY = """query MigrationExecutions($input: DeploymentInstanceExecutionListInput!) {
  deploymentInstanceExecutions(first: 100, input: $input) {
    pageInfo { hasNextPage endCursor }
    edges { node { id deploymentId status createdAt updatedAt completedAt } }
  }
}"""


def query(document: str, variables: dict) -> dict:
    if not document.lstrip().startswith("query") or "mutation" in document:
        raise ValueError("Only GraphQL query documents are permitted")
    config = json.loads((Path.home() / ".railway/config.json").read_text(encoding="utf-8"))
    request = urllib.request.Request(
        "https://backboard.railway.com/graphql/v2",
        data=json.dumps({"query": document, "variables": variables}).encode(),
        headers={
            "Authorization": "Bearer " + config["user"]["accessToken"],
            "Content-Type": "application/json",
            "User-Agent": "investintell-railway-readonly-audit/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        result = json.load(response)
    if result.get("errors"):
        raise RuntimeError(json.dumps(result["errors"]))
    return result["data"]


def filter_manifest(manifest: dict) -> dict:
    return {
        "build": {k: v for k, v in (manifest.get("build") or {}).items() if k in BUILD_KEYS},
        "deploy": {k: v for k, v in (manifest.get("deploy") or {}).items() if k in DEPLOY_KEYS},
    }


def capture(include_executions: bool = False) -> dict:
    data = query(SNAPSHOT_QUERY, {"project": PROJECT, "environment": ENVIRONMENT})
    env = data["environment"]
    if env["projectId"] != PROJECT or env["name"] != "production":
        raise RuntimeError("Unexpected project/environment identity")
    for connection_name in ("serviceInstances", "deploymentTriggers", "variables", "volumeInstances"):
        if env[connection_name]["pageInfo"]["hasNextPage"]:
            raise RuntimeError(f"Incomplete {connection_name} inventory; pagination is required")
    raw_config = env.pop("config")
    variables = env.pop("variables")["edges"]
    volumes = env.pop("volumeInstances")["edges"]
    for edge in env["serviceInstances"]["edges"]:
        service = edge["node"]
        service["hasVolumeAttachments"] = any(v["node"]["serviceId"] == service["serviceId"] for v in volumes)
        current = raw_config.get("services", {}).get(service["serviceId"], {})
        service["environmentConfig"] = {
            "source": {k: v for k, v in (current.get("source") or {}).items() if k in SOURCE_KEYS},
            **filter_manifest(current),
        }
        service["variableNames"] = sorted(
            variable["node"]["name"] for variable in variables
            if variable["node"]["serviceId"] == service["serviceId"]
        )
        latest = service.get("latestDeployment")
        if latest:
            metadata = latest.pop("meta") or {}
            latest["meta"] = {
                **{k: metadata[k] for k in ("commitHash", "repo", "branch", "rootDirectory", "configFile") if k in metadata},
                "serviceManifest": filter_manifest(metadata.get("serviceManifest", {})),
            }
        resolved = service.get("resolvedFileConfig")
        if resolved:
            resolved["fileManifest"] = filter_manifest(resolved["fileManifest"])
    if include_executions:
        baseline = json.loads((Path(__file__).parent / "baseline.json").read_text(encoding="utf-8"))
        jobs = [row for row in baseline["services"] if row["name"] not in {"api", "agent-dev-real-api", "livefeed"}]

        def get_executions(job: dict) -> tuple[str, dict]:
            result = query(EXECUTIONS_QUERY, {"input": {"environmentId": ENVIRONMENT, "serviceId": job["serviceId"]}})
            return job["name"], result["deploymentInstanceExecutions"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            env["jobExecutions"] = dict(pool.map(get_executions, jobs))
    return {"capturedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(), "data": data}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Snapshot file outside the .railway authoring directory")
    parser.add_argument("--executions", action="store_true", help="Also read recent executions for this partial's job services")
    args = parser.parse_args()
    output = args.output.resolve()
    if output.is_relative_to(Path(__file__).parent.resolve()):
        parser.error("Write transient evidence outside .railway; never overwrite the reviewed baseline")
    snapshot = capture(args.executions)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(f"Captured {len(snapshot['data']['environment']['serviceInstances']['edges'])} services at {snapshot['capturedAt']}; GraphQL queries only")


if __name__ == "__main__":
    main()
