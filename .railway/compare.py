"""Fail on reviewed configuration drift; compare query-only capture to baseline.

This checks persisted settings, resolved legacy overrides, variables by name, and
latest deployment settings separately. Deployment IDs/statuses and execution
histories are evidence for operator inspection, not equivalent configuration.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from capture import BUILD_KEYS, DEPLOY_KEYS


def sections(service: dict) -> tuple[dict, dict]:
    saved = service["environmentConfig"]
    source = saved.get("source") or None
    if source:
        source = {**source, "type": "image" if source.get("image") else "github"}
    current = {
        "source": source,
        "build": saved.get("build") or None,
        "deploy": saved.get("deploy") or None,
        "variables": service["variableNames"],
    }
    effective = json.loads(json.dumps(current))
    effective["build"] = effective["build"] or {}
    effective["deploy"] = effective["deploy"] or {}
    resolved = service.get("resolvedFileConfig") or {}
    for section in ("build", "deploy"):
        effective[section].update(resolved.get("fileManifest", {}).get(section, {}))
    for field in ("buildCommand", "dockerfilePath", "watchPatterns"):
        effective["build"].setdefault(field, service[field])
    for field in ("cronSchedule", "startCommand", "preDeployCommand", "preDeployTimeoutSeconds", "healthcheckPath", "healthcheckTimeout", "restartPolicyType", "restartPolicyMaxRetries", "sleepApplication", "overlapSeconds", "drainingSeconds"):
        effective["deploy"].setdefault(field, service[field])
    return current, effective


def diff(expected: object, actual: object, path: str = "") -> list[str]:
    if isinstance(expected, dict) and isinstance(actual, dict):
        differences = []
        for key in sorted(expected.keys() | actual.keys()):
            # An absent nullable sparse setting and JSON null mean no override.
            if expected.get(key) is None and actual.get(key) is None:
                continue
            differences.extend(diff(expected.get(key), actual.get(key), f"{path}.{key}"))
        return differences
    return [] if expected == actual else [path]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    args = parser.parse_args()
    baseline = json.loads((Path(__file__).parent / "baseline.json").read_text(encoding="utf-8"))
    fresh = json.loads(args.snapshot.read_text(encoding="utf-8-sig"))
    errors = []
    for key in ("id", "name"):
        if fresh["data"]["project"][key] != baseline["project"][key]:
            errors.append(f"project.{key}")
        if fresh["data"]["environment"][key] != baseline["environment"][key]:
            errors.append(f"environment.{key}")
    for key in ("prDeploys", "botPrEnvironments"):
        if fresh["data"]["project"][key] != baseline["project"][key]:
            errors.append(f"project.{key}")
    expected_inventory = {row["name"] for row in baseline["services"]} | set(baseline["unmanagedServices"])
    services = {edge["node"]["serviceName"]: edge["node"] for edge in fresh["data"]["environment"]["serviceInstances"]["edges"]}
    if set(services) != expected_inventory:
        errors.append("service inventory changed; inspect ownership before cutover")
    for row in baseline["services"]:
        if row["name"] not in services:
            continue
        service = services[row["name"]]
        name = row["name"]
        address = f"service.{name}"
        errors.extend(diff(
            baseline["environment"]["iacPartials"].get(address),
            (fresh["data"]["environment"].get("iacPartials") or {}).get(address),
            f"{name}.partial ownership",
        ))
        current, effective = sections(service)
        for section, actual in (("current", current), ("effective", effective)):
            expected = {k: row[section][k] for k in ("source", "build", "deploy", "variables")}
            errors.extend(diff(expected, actual, f"{name}.{section}"))
        errors.extend(diff(row["configuredConfigFile"], service["railwayConfigFile"], f"{name}.configuredConfigFile"))
        resolved = service.get("resolvedFileConfig") or {}
        errors.extend(diff(row["resolvedConfigFile"], resolved.get("configFile"), f"{name}.resolvedConfigFile"))
        runtime = ((service.get("latestDeployment") or {}).get("meta") or {}).get("serviceManifest") or {}
        for section, keys in (("build", BUILD_KEYS), ("deploy", DEPLOY_KEYS)):
            actual = {k: v for k, v in runtime.get(section, {}).items() if k in keys}
            errors.extend(diff(row["runtime"][section], actual, f"{name}.runtime.{section}"))
        if service.get("tracingEnabled") or service.get("autoInstrumentationEnabled"):
            errors.append(f"{name}.tracing enabled; explicit preservation is required")
        if (service.get("service") or {}).get("groupId") or service.get("hasVolumeAttachments"):
            errors.append(f"{name}.group/volume attachment added; explicit preservation is required")
    if errors:
        print("Snapshot drift requires review (values are intentionally omitted):")
        for error in errors:
            print("-", error)
        sys.exit(1)
    print(f"PASS: {len(baseline['services'])} owned services match the reviewed configuration and variable-name inventory; {len(baseline['unmanagedServices'])} services remain outside this partial")
    print("Inspect job execution/instance statuses separately; this comparison does not certify an idle merge window.")


if __name__ == "__main__":
    main()
