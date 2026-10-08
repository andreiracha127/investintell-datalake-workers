"""Capture production inputs once, then compare two offline chain revisions.

DATABASE_URL is the only DSN input. A write-capable role is refused before data
reads. JSON is emitted to stdout/--output; the PR Markdown block goes to stderr.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
    )
    return result.stdout.strip()


@contextmanager
def detached_worktree(repo: Path, destination: Path, revision: str):
    """Only remove the unique worktree we created, even after replay failure."""
    git(repo, "worktree", "add", "--detach", str(destination), revision)
    try:
        yield destination
    finally:
        # Windows fixture CRLF noise occurs even in freshly checked-out trees.
        # --force is restricted to this freshly created, temporary worktree.
        git(repo, "worktree", "remove", "--force", str(destination))


def replay_environment() -> dict[str, str]:
    return {
        key: value for key, value in os.environ.items()
        if not key.upper().startswith(("PG", "DB_", "PYTHON"))
        and not any(word in key.upper() for word in
                    ("DATABASE", "DSN", "SECRET", "TOKEN", "PASSWORD"))
    }


def differing_rows(before: list[dict], after: list[dict], limit: int = 5) -> list[dict]:
    differences = []
    for index in range(max(len(before), len(after))):
        left = before[index] if index < len(before) else None
        right = after[index] if index < len(after) else None
        # Compare the exact JSON representation, including int/float and -0.0.
        if json.dumps(left, sort_keys=True) == json.dumps(right, sort_keys=True):
            continue
        fields = {}
        for field in sorted(set(left or {}) | set(right or {})):
            a, b = (left or {}).get(field), (right or {}).get(field)
            if left is None or right is None or json.dumps(a) != json.dumps(b):
                fields[field] = {"before": a, "after": b}
        differences.append({"index": index, "before": left, "after": right, "fields": fields})
        if len(differences) == limit:
            break
    return differences


def compare_revisions(repo: Path, before: str, after: str, snapshot_path: Path) -> dict:
    snapshot_path = snapshot_path.resolve()
    snapshot_sha = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
    revisions = {
        label: git(repo, "rev-parse", "--verify", f"{rev}^{{commit}}")
        for label, rev in (("before", before), ("after", after))
    }
    results = {}
    with tempfile.TemporaryDirectory(prefix="chain-receipt-") as temporary:
        base = Path(temporary).resolve()
        for label, revision in revisions.items():
            with detached_worktree(repo, base / label, revision) as checkout:
                output = base / f"{label}.json"
                subprocess.run(
                    [sys.executable, "-B", str(ROOT / "scripts/receipt_chain_replay.py"),
                     "--snapshot", str(snapshot_path), "--snapshot-sha256", snapshot_sha,
                     "--revision", revision, "--output", str(output)],
                    cwd=checkout, env=replay_environment(), check=True,
                    # A bounded child failure is not a successful receipt.
                    timeout=3600, capture_output=True, text=True,
                )
                result = json.loads(output.read_text(encoding="utf-8"))
                if result["snapshot_sha256"] != snapshot_sha or result["commit"] != revision:
                    raise RuntimeError("replay identity does not match requested snapshot/commit")
                # Independently check child output with this receipt's canonical hash.
                from src.input_packs.hashing import canonical_json_sha256

                if result["all_months_sha256"] != canonical_json_sha256(result["rows"]):
                    raise RuntimeError("replay row digest did not match returned rows")
                if result["latest_month_sha256"] != canonical_json_sha256(result["rows"][-1:]):
                    raise RuntimeError("replay latest-row digest did not match returned row")
                results[label] = result
    if hashlib.sha256(snapshot_path.read_bytes()).hexdigest() != snapshot_sha:
        raise RuntimeError("snapshot bytes changed during comparison")
    left, right = results["before"], results["after"]
    diffs = differing_rows(left.pop("rows"), right.pop("rows"))
    projection = left.pop("projection")
    definition = left.pop("hash_definition")
    if right.pop("projection") != projection or right.pop("hash_definition") != definition:
        raise RuntimeError("revisions disagree on receipt projection/hash definition")
    return {
        **results,
        "projection": projection,
        "hash_definition": definition,
        "identical": left["all_months_sha256"] == right["all_months_sha256"]
        and left["latest_month_sha256"] == right["latest_month_sha256"],
        "first_differences": diffs,
    }


def markdown_receipt(receipt: dict) -> str:
    snapshot = receipt["snapshot"]
    lines = ["```markdown", "### open-macro-v03-chain production equivalence receipt", "",
             f"- Reference date (UTC): `{snapshot['reference_date']}`",
             f"- Candidate target: `{snapshot['target_date']}`",
             f"- Cron status at capture: `{snapshot.get('cron_status', 'unknown')}`",
             f"- Snapshot SHA-256: `{snapshot['file_sha256']}`",
             f"- Inputs SHA-256: `{snapshot['inputs_sha256']}`"]
    for label in ("before", "after"):
        result = receipt[label]
        lines.extend([f"- {label.title()} commit: `{result['commit']}`",
                      f"- {label.title()} rows: `{result['row_count']}`",
                      f"- {label.title()} all-months digest: `{result['all_months_sha256']}`",
                      f"- {label.title()} latest-month digest: `{result['latest_month_sha256']}`"])
    lines += [f"- Identical: **{str(receipt['identical']).lower()}**",
              "- Scope: pure replay through candidate target; publication gates remain separate."]
    if receipt["first_differences"]:
        lines += ["", "First differing rows (field: before -> after):"]
        for diff in receipt["first_differences"]:
            for field, values in diff["fields"].items():
                lines.append(f"- Row {diff['index']} `{field}`: "
                             f"`{json.dumps(values['before'])}` -> `{json.dumps(values['after'])}`")
    return "\n".join([*lines, "```", ""])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default="origin/main")
    parser.add_argument("--after", default="HEAD")
    parser.add_argument("--reference-date", type=dt.date.fromisoformat,
                        default=dt.datetime.now(dt.timezone.utc).date())
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--statement-timeout-ms", type=int, default=120000)
    args = parser.parse_args(argv)
    snapshot_path, output_path = args.snapshot.resolve(), args.output.resolve()
    if snapshot_path == output_path:
        parser.error("--snapshot and --output must be different files")
    if snapshot_path.exists() or output_path.exists():
        parser.error("receipt files must be new paths; preserve prior evidence")
    if args.statement_timeout_ms <= 0:
        parser.error("--statement-timeout-ms must be positive")
    # Resolve identities BEFORE opening a DB session; moving refs cannot switch
    # commits between capture and replay. Nothing reads production a second time.
    repo = Path(git(Path.cwd(), "rev-parse", "--show-toplevel"))
    before = git(repo, "rev-parse", "--verify", f"{args.before}^{{commit}}")
    after = git(repo, "rev-parse", "--verify", f"{args.after}^{{commit}}")
    from scripts.chain_production_snapshot import capture_snapshot
    from src.input_packs.hashing import canonical_json_bytes

    snapshot = capture_snapshot(args.reference_date, statement_timeout_ms=args.statement_timeout_ms)
    raw = canonical_json_bytes(snapshot)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_path.open("xb") as stream:
        stream.write(raw)
    receipt = compare_revisions(repo, before, after, snapshot_path)
    receipt["snapshot"] = {
        key: value for key, value in snapshot.items() if key != "inputs"
    } | {"file_sha256": hashlib.sha256(raw).hexdigest(), "path": str(snapshot_path)}
    rendered = json.dumps(receipt, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(rendered)
    sys.stdout.write(rendered)
    sys.stderr.write(markdown_receipt(receipt))
    return 0 if receipt["identical"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 -- redact credentials from every CLI failure
        # libpq error strings can include DSN/credential material. Do not echo them.
        sys.stderr.write(f"Chain receipt failed ({type(exc).__name__}); no equivalence approval.\n")
        from scripts.chain_production_snapshot import SnapshotRefused

        if isinstance(exc, SnapshotRefused):
            sys.stderr.write(f"{exc}\n")
        elif isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            # Child replays have no credentials and are offline. Their failure
            # explains contract drift or computation errors without exposing a DSN.
            sys.stderr.write(exc.stderr)
        raise SystemExit(2) from None
