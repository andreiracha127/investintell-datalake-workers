"""Regression tests for complete, identity-validated read-only IaC evidence."""
from __future__ import annotations

import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import capture
import compare


def connection(edges: list[dict], has_next: bool = False, cursor: str | None = None) -> dict:
    return {"edges": edges, "pageInfo": {"hasNextPage": has_next, "endCursor": cursor}}


def execution_page(start: int, count: int, has_next: bool, cursor: str | None) -> dict:
    return {"deploymentInstanceExecutions": connection(
        [{"node": {"id": f"execution-{index}"}} for index in range(start, start + count)],
        has_next,
        cursor,
    )}


def inventory_fixture() -> tuple[dict, dict]:
    """An independent small inventory with one job and excluded continuous services."""
    build = {"buildCommand": None, "dockerfilePath": None, "watchPatterns": []}
    deploy = {field: None for field in (
        "cronSchedule", "startCommand", "preDeployCommand", "preDeployTimeoutSeconds",
        "healthcheckPath", "healthcheckTimeout", "restartPolicyType", "restartPolicyMaxRetries",
        "sleepApplication", "overlapSeconds", "drainingSeconds",
    )}
    rows = []
    nodes = []
    for name in ("job", "api", "agent-dev-real-api"):
        current = {"source": None, "build": None, "deploy": None, "variables": []}
        rows.append({
            "name": name,
            "serviceId": f"live-{name}",
            "current": current,
            "effective": {**current, "build": build, "deploy": deploy},
            "runtime": {"build": {}, "deploy": {}},
            "configuredConfigFile": None,
            "resolvedConfigFile": None,
        })
        nodes.append({"node": {
            "serviceName": name,
            "serviceId": f"live-{name}",
            "environmentConfig": {"source": {}, "build": {}, "deploy": {}},
            "variableNames": [],
            "railwayConfigFile": None,
            **build,
            **deploy,
        }})
    nodes.append({"node": {"serviceName": "livefeed", "serviceId": "live-livefeed"}})
    project = {"id": capture.PROJECT, "name": "investintell-db", "prDeploys": False, "botPrEnvironments": False}
    environment = {"id": capture.ENVIRONMENT, "name": "production", "projectId": capture.PROJECT, "iacPartials": {}}
    baseline = {"project": project, "environment": environment, "services": rows, "unmanagedServices": ["livefeed"]}
    data = {"project": copy.deepcopy(project), "environment": {
        **copy.deepcopy(environment),
        "serviceInstances": connection(nodes),
        "deploymentTriggers": connection([]),
        "variables": connection([]),
        "volumeInstances": connection([]),
        "config": {},
    }}
    return baseline, data


def github_trigger(service_id: str = "live-job", **changes) -> dict:
    return {"node": {
        "serviceId": service_id,
        "provider": "github",
        "repository": "owner/workers",
        "branch": "main",
        "checkSuites": False,
        **changes,
    }}


class SourceConfigurationTests(unittest.TestCase):
    def service(self, source: dict | None) -> dict:
        return {"serviceName": "job", "serviceId": "live-job", "source": source}

    def test_reconstructs_repo_branch_and_false_wait_for_ci_from_live_fields(self) -> None:
        source = capture.source_configuration(
            self.service({"repo": "owner/workers", "image": None}), {}, [github_trigger()],
        )
        self.assertEqual(source, {"repo": "owner/workers", "branch": "main", "checkSuites": False})

    def test_repo_only_source_does_not_invent_trigger_defaults(self) -> None:
        source = capture.source_configuration(self.service({"repo": "owner/workers"}), {}, [])
        self.assertEqual(source, {"repo": "owner/workers"})

    def test_retains_root_directory_and_image_update_settings(self) -> None:
        saved = {"source": {"rootDirectory": "/worker", "autoUpdates": {"type": "disabled"}, "unknown": "omit"}}
        source = capture.source_configuration(self.service({"image": "registry/worker:1"}), saved, [])
        self.assertEqual(source, {
            "rootDirectory": "/worker", "autoUpdates": {"type": "disabled"}, "image": "registry/worker:1",
        })

    def test_trigger_can_supply_repository_without_service_source(self) -> None:
        source = capture.source_configuration(self.service(None), {}, [github_trigger()])
        self.assertEqual(source, {"repo": "owner/workers", "branch": "main", "checkSuites": False})

    def test_manual_upload_does_not_infer_source_from_deployment_metadata(self) -> None:
        service = {**self.service(None), "latestDeployment": {"meta": {"repo": "owner/workers", "branch": "main"}}}
        self.assertEqual(capture.source_configuration(service, {}, []), {})

    def test_identical_triggers_are_unambiguous(self) -> None:
        source = capture.source_configuration(self.service(None), {}, [github_trigger(), github_trigger()])
        self.assertEqual(source["branch"], "main")
        self.assertFalse(source["checkSuites"])

    def test_rejects_incomplete_or_unsupported_triggers(self) -> None:
        for changes in ({"provider": "gitlab"}, {"repository": None}, {"branch": None}, {"checkSuites": None}, {"checkSuites": "false"}):
            with self.subTest(changes=changes), self.assertRaisesRegex(RuntimeError, "incomplete or unsupported"):
                capture.source_configuration(self.service(None), {}, [github_trigger(**changes)])

    def test_rejects_conflicting_trigger_settings(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "ambiguous deployment triggers"):
            capture.source_configuration(self.service(None), {}, [github_trigger(), github_trigger(branch="release")])

    def test_rejects_conflicting_repository_evidence(self) -> None:
        cases = [({"source": {"repo": "owner/saved"}}, []), ({}, [github_trigger(repository="owner/trigger")])]
        for saved, triggers in cases:
            with self.subTest(saved=saved, triggers=triggers), self.assertRaisesRegex(RuntimeError, "conflicting source.repo"):
                capture.source_configuration(self.service({"repo": "owner/live"}), saved, triggers)

    def test_rejects_simultaneous_repo_and_image(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "ambiguous repository and image"):
            capture.source_configuration(self.service({"repo": "owner/workers", "image": "registry/worker:1"}), {}, [])

    def test_rejects_stale_saved_repository_after_disconnection(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "lacks connected source evidence"):
            capture.source_configuration(self.service(None), {"source": {"repo": "owner/workers"}}, [])

    def test_rejects_stale_saved_trigger_settings_after_trigger_removal(self) -> None:
        saved = {"source": {"repo": "owner/workers", "branch": "main", "checkSuites": False}}
        with self.assertRaisesRegex(RuntimeError, "lack deployment trigger evidence"):
            capture.source_configuration(self.service({"repo": "owner/workers"}), saved, [])

    def test_capture_rejects_incomplete_trigger_inventory(self) -> None:
        _, data = inventory_fixture()
        data["environment"]["deploymentTriggers"]["pageInfo"]["hasNextPage"] = True
        with patch.object(capture, "query", return_value=data), self.assertRaisesRegex(RuntimeError, "Incomplete deploymentTriggers"):
            capture.capture()


class ExecutionPaginationTests(unittest.TestCase):
    service = {"serviceName": "job", "serviceId": "fresh-job-id"}

    def test_collects_more_than_100_executions_and_passes_cursor(self) -> None:
        last_page = execution_page(100, 7, False, "page-2")
        last_page["deploymentInstanceExecutions"]["edges"][-1]["node"]["status"] = "RUNNING"
        with patch.object(capture, "query", side_effect=[
            execution_page(0, 100, True, "page-1"),
            last_page,
        ]) as query:
            name, result = capture.get_executions(self.service)
        self.assertEqual(name, "job")
        self.assertEqual([edge["node"]["id"] for edge in result["edges"]], [f"execution-{index}" for index in range(107)])
        self.assertEqual(result["pageInfo"], {"hasNextPage": False, "endCursor": "page-2"})
        self.assertEqual(result["edges"][-1]["node"]["status"], "RUNNING")
        self.assertEqual([call.args[1]["after"] for call in query.call_args_list], [None, "page-1"])
        for call in query.call_args_list:
            self.assertEqual(call.args[1]["input"], {"environmentId": capture.ENVIRONMENT, "serviceId": "fresh-job-id"})
            self.assertIn("after: $after", call.args[0])

    def test_accepts_empty_final_page(self) -> None:
        with patch.object(capture, "query", side_effect=[
            execution_page(0, 1, True, "page-1"),
            execution_page(1, 0, False, None),
        ]):
            _, result = capture.get_executions(self.service)
        self.assertEqual(len(result["edges"]), 1)
        self.assertFalse(result["pageInfo"]["hasNextPage"])

    def test_rejects_malformed_pagination_without_returning_partial_results(self) -> None:
        pages = [
            {"edges": [{"node": {"id": "first"}}], "pageInfo": {"hasNextPage": True}},
            connection([{"node": {"id": "first"}}], True, None),
            connection([{"node": {"id": "first"}}], True, ""),
            connection([], True, "page-1"),
            {"edges": [], "pageInfo": {"hasNextPage": "false", "endCursor": None}},
            {"edges": None, "pageInfo": {"hasNextPage": False, "endCursor": None}},
        ]
        for page in pages:
            with self.subTest(page=page), patch.object(capture, "query", return_value={"deploymentInstanceExecutions": page}):
                with self.assertRaisesRegex(RuntimeError, "malformed|did not advance"):
                    capture.get_executions(self.service)

    def test_rejects_repeated_and_cyclic_cursors(self) -> None:
        for cursors in (("A", "A"), ("A", "B", "A")):
            with self.subTest(cursors=cursors), patch.object(capture, "query", side_effect=[
                execution_page(index, 1, True, cursor) for index, cursor in enumerate(cursors)
            ]) as query:
                with self.assertRaisesRegex(RuntimeError, "did not advance"):
                    capture.get_executions(self.service)
                self.assertEqual(query.call_count, len(cursors))

    def test_capture_failure_does_not_create_output(self) -> None:
        baseline, data = inventory_fixture()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "snapshot.json"
            with (
                patch.object(capture, "query", side_effect=[data, execution_page(0, 100, True, "page-1"), RuntimeError("second page unavailable")]),
                patch.object(Path, "read_text", return_value=json.dumps(baseline)),
                patch("sys.argv", ["capture.py", "--executions", "--output", str(output)]),
            ):
                with self.assertRaisesRegex(RuntimeError, "second page unavailable"):
                    capture.main()
            self.assertFalse(output.exists())


class ServiceIdentityTests(unittest.TestCase):
    def compare_snapshot(self, baseline: dict, data: dict) -> tuple[int, str]:
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "snapshot.json"
            snapshot.write_text(json.dumps({"data": data}), encoding="utf-8")
            read_text = Path.read_text

            def read(path: Path, *args, **kwargs) -> str:
                if path.name == "baseline.json":
                    return json.dumps(baseline)
                return read_text(path, *args, **kwargs)

            with patch.object(Path, "read_text", read), patch("sys.argv", ["compare.py", str(snapshot)]), contextlib.redirect_stdout(output):
                try:
                    compare.main()
                except SystemExit as exc:
                    return exc.code, output.getvalue()
        return 0, output.getvalue()

    def test_comparison_accepts_original_inventory(self) -> None:
        baseline, data = inventory_fixture()
        code, output = self.compare_snapshot(baseline, data)
        self.assertEqual(code, 0, output)
        self.assertIn("PASS: 3 owned services", output)

    def test_comparison_rejects_same_name_replacement(self) -> None:
        baseline, data = inventory_fixture()
        data["environment"]["serviceInstances"]["edges"][0]["node"]["serviceId"] = "replacement-id"
        code, output = self.compare_snapshot(baseline, data)
        self.assertEqual(code, 1)
        self.assertIn("job.serviceId changed; service replacement requires review", output)

    def test_execution_capture_rejects_replacement_before_querying_jobs(self) -> None:
        baseline, data = inventory_fixture()
        data["environment"]["serviceInstances"]["edges"][0]["node"]["serviceId"] = "replacement-id"
        with patch.object(capture, "query", return_value=data) as query, patch.object(Path, "read_text", return_value=json.dumps(baseline)):
            with self.assertRaisesRegex(RuntimeError, "job.serviceId changed"):
                capture.capture(include_executions=True)
        self.assertEqual(query.call_count, 1)
        self.assertEqual(query.call_args.args[0], capture.SNAPSHOT_QUERY)

    def test_execution_capture_uses_fresh_validated_nodes(self) -> None:
        baseline, data = inventory_fixture()
        fresh_job = data["environment"]["serviceInstances"]["edges"][0]["node"]
        with (
            patch.object(capture, "query", return_value=data) as query,
            patch.object(Path, "read_text", return_value=json.dumps(baseline)),
            patch.object(capture, "get_executions", return_value=("job", connection([]))) as executions,
        ):
            snapshot = capture.capture(include_executions=True)
        self.assertEqual(query.call_count, 1)
        self.assertEqual(executions.call_count, 1)
        self.assertIs(executions.call_args.args[0], fresh_job)
        self.assertEqual(set(snapshot["data"]["environment"]["jobExecutions"]), {"job"})

    def test_duplicate_names_fail_inventory_validation(self) -> None:
        baseline, data = inventory_fixture()
        edges = data["environment"]["serviceInstances"]["edges"]
        edges.append(copy.deepcopy(edges[0]))
        _, errors = capture.service_inventory(baseline, edges)
        self.assertIn("job.duplicate service name; identity is ambiguous", errors)

    def test_comparison_detects_connected_repo_branch_and_ci_drift(self) -> None:
        cases = (("repo", "owner/changed"), ("branch", "release"), ("checkSuites", True))
        for field, changed in cases:
            with self.subTest(field=field):
                baseline, data = inventory_fixture()
                expected = {"type": "github", "repo": "owner/workers", "branch": "main", "checkSuites": False}
                baseline["services"][0]["current"]["source"] = expected
                baseline["services"][0]["effective"]["source"] = expected
                job = data["environment"]["serviceInstances"]["edges"][0]["node"]
                job["source"] = {"repo": changed if field == "repo" else "owner/workers", "image": None}
                trigger_changes = {"repository" if field == "repo" else field: changed}
                data["environment"]["deploymentTriggers"] = connection([github_trigger(**trigger_changes)])
                with patch.object(capture, "query", return_value=data):
                    fresh = capture.capture()["data"]
                code, output = self.compare_snapshot(baseline, fresh)
                self.assertEqual(code, 1)
                self.assertIn(f"job.current.source.{field}", output)
                self.assertIn(f"job.effective.source.{field}", output)

    def test_comparison_accepts_reconstructed_connected_source(self) -> None:
        baseline, data = inventory_fixture()
        source = {"type": "github", "repo": "owner/workers", "branch": "main", "checkSuites": False}
        baseline["services"][0]["current"]["source"] = source
        baseline["services"][0]["effective"]["source"] = source
        data["environment"]["serviceInstances"]["edges"][0]["node"]["source"] = {"repo": "owner/workers"}
        data["environment"]["deploymentTriggers"] = connection([github_trigger()])
        with patch.object(capture, "query", return_value=data):
            fresh = capture.capture()["data"]
        code, output = self.compare_snapshot(baseline, fresh)
        self.assertEqual(code, 0, output)

    def test_rejects_unexpected_owned_addresses_of_every_resource_kind(self) -> None:
        for address in ("service.livefeed", "volume.unexpected", "bucket.unexpected"):
            with self.subTest(address=address):
                baseline, data = inventory_fixture()
                data["environment"]["iacPartials"][address] = "investintell-workers"
                code, output = self.compare_snapshot(baseline, data)
                self.assertEqual(code, 1)
                self.assertIn(f"partial ownership.{address}", output)

    def test_rejects_removal_of_reviewed_owned_nonservice_address(self) -> None:
        baseline, data = inventory_fixture()
        baseline["environment"]["iacPartials"]["volume.reviewed"] = "investintell-workers"
        code, output = self.compare_snapshot(baseline, data)
        self.assertEqual(code, 1)
        self.assertIn("partial ownership.volume.reviewed", output)

    def test_rejects_unexpected_owned_address_even_if_baseline_already_contains_it(self) -> None:
        baseline, data = inventory_fixture()
        baseline["environment"]["iacPartials"]["volume.unexpected"] = "investintell-workers"
        data["environment"]["iacPartials"]["volume.unexpected"] = "investintell-workers"
        code, output = self.compare_snapshot(baseline, data)
        self.assertEqual(code, 1)
        self.assertIn("partial ownership.volume.unexpected; unexpected address", output)

    def test_ignores_ownership_changes_confined_to_other_partials(self) -> None:
        baseline, data = inventory_fixture()
        baseline["environment"]["iacPartials"]["bucket.other"] = "customer-services"
        data["environment"]["iacPartials"]["bucket.other"] = "unrelated-partial"
        data["environment"]["iacPartials"]["volume.new-unrelated"] = "customer-services"
        code, output = self.compare_snapshot(baseline, data)
        self.assertEqual(code, 0, output)


if __name__ == "__main__":
    unittest.main()
