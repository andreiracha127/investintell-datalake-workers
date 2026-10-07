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

EXECUTIONS_QUERY = """query MigrationExecutions($input: DeploymentInstanceExecutionListInput!, $after: String) {
  deploymentInstanceExecutions(first: 100, after: $after, input: $input) {
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


def source_configuration(service: dict, current: dict, trigger_edges: list[dict]) -> dict:
    """Compose persisted source settings with the live connection and trigger."""
    name = service["serviceName"]
    saved = current.get("source") or {}
    connected = service.get("source") or {}
    if not isinstance(saved, dict) or not isinstance(connected, dict):
        raise RuntimeError(f"{name}: malformed source configuration")
    source = {key: value for key, value in saved.items() if key in SOURCE_KEYS}

    def merge(fields: dict) -> None:
        for key, value in fields.items():
            if source.get(key) is not None and source[key] != value:
                raise RuntimeError(f"{name}: conflicting source.{key} evidence")
            source[key] = value

    for key in ("repo", "image"):
        value = connected.get(key)
        if value is not None:
            if not isinstance(value, str) or not value:
                raise RuntimeError(f"{name}: malformed connected source.{key}")
            merge({key: value})

    triggers = [edge["node"] for edge in trigger_edges if edge["node"]["serviceId"] == service["serviceId"]]
    trigger_sources = []
    for trigger in triggers:
        if (
            trigger.get("provider") != "github"
            or not isinstance(trigger.get("repository"), str)
            or not trigger["repository"]
            or not isinstance(trigger.get("branch"), str)
            or not trigger["branch"]
            or type(trigger.get("checkSuites")) is not bool
        ):
            raise RuntimeError(f"{name}: incomplete or unsupported deployment trigger")
        fields = {"repo": trigger["repository"], "branch": trigger["branch"], "checkSuites": trigger["checkSuites"]}
        if fields not in trigger_sources:
            trigger_sources.append(fields)
    if len(trigger_sources) > 1:
        raise RuntimeError(f"{name}: ambiguous deployment triggers")
    if trigger_sources:
        merge(trigger_sources[0])
    elif any(source.get(key) is not None for key in ("branch", "checkSuites")):
        raise RuntimeError(f"{name}: saved trigger settings lack deployment trigger evidence")
    if source.get("repo") and not connected.get("repo") and not trigger_sources:
        raise RuntimeError(f"{name}: saved repository lacks connected source evidence")
    if source.get("repo") and source.get("image"):
        raise RuntimeError(f"{name}: ambiguous repository and image sources")
    return source


def service_inventory(baseline: dict, edges: list[dict]) -> tuple[dict[str, dict], list[str]]:
    services = {}
    errors = []
    for edge in edges:
        service = edge["node"]
        name = service["serviceName"]
        if name in services:
            errors.append(f"{name}.duplicate service name; identity is ambiguous")
        services[name] = service
    expected_inventory = {row["name"] for row in baseline["services"]} | set(baseline["unmanagedServices"])
    if set(services) != expected_inventory:
        errors.append("service inventory changed; inspect ownership before cutover")
    for row in baseline["services"]:
        service = services.get(row["name"])
        if service and service["serviceId"] != row["serviceId"]:
            errors.append(f"{row['name']}.serviceId changed; service replacement requires review")
    return services, errors


def get_executions(service: dict) -> tuple[str, dict]:
    """Return a complete execution connection, or fail without partial evidence."""
    name = service["serviceName"]
    after = None
    seen_cursors = set()
    edges = []
    while True:
        result = query(EXECUTIONS_QUERY, {
            "input": {"environmentId": ENVIRONMENT, "serviceId": service["serviceId"]},
            "after": after,
        })
        connection = result.get("deploymentInstanceExecutions")
        if not isinstance(connection, dict):
            raise RuntimeError(f"{name}: malformed execution connection")
        page_info = connection.get("pageInfo")
        page_edges = connection.get("edges")
        if (
            not isinstance(page_info, dict)
            or type(page_info.get("hasNextPage")) is not bool
            or "endCursor" not in page_info
            or not isinstance(page_edges, list)
            or any(not isinstance(edge, dict) or not isinstance(edge.get("node"), dict) for edge in page_edges)
        ):
            raise RuntimeError(f"{name}: malformed execution page")
        cursor = page_info["endCursor"]
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise RuntimeError(f"{name}: malformed execution cursor")
        edges.extend(page_edges)
        if not page_info["hasNextPage"]:
            return name, {"pageInfo": page_info, "edges": edges}
        if not page_edges or cursor is None or cursor in seen_cursors:
            raise RuntimeError(f"{name}: execution pagination did not advance")
        seen_cursors.add(cursor)
        after = cursor


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
            "source": source_configuration(service, current, env["deploymentTriggers"]["edges"]),
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
        services, errors = service_inventory(baseline, env["serviceInstances"]["edges"])
        if errors:
            raise RuntimeError("Execution service inventory requires review: " + "; ".join(errors))
        jobs = [services[row["name"]] for row in baseline["services"] if row["name"] not in {"api", "agent-dev-real-api", "livefeed"}]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            env["jobExecutions"] = dict(pool.map(get_executions, jobs))
    return {"capturedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(), "data": data}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Snapshot file outside the .railway authoring directory")
    parser.add_argument("--executions", action="store_true", help="Also read every page of execution history for this partial's job services")
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
