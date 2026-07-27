"""Build, verify and publish ``investintell-quant-core`` from ``main``.

The published wheel is the app's only source of quant-core code, so the wheel
that reaches the private index must be reproducible from ``main``. This script
is the gate:

``verify``
    Build the wheel and prove it carries exactly the tracked source of the
    current checkout: same file set, same bytes, and a version that agrees
    across ``pyproject.toml``, ``version.py`` and the ``quant_engine`` pin.
    No network, no credentials — safe to run on every pull request.

``preflight``
    ``verify`` plus a look at the registry. If the declared version is already
    published, the published wheel is downloaded and compared content-wise
    against the freshly built one: a mismatch means someone published a build
    that ``main`` cannot reproduce, and the run fails. If the version is not
    published yet, ``publish_needed=true`` is emitted so the workflow can
    upload the wheel this script just verified.

Wheels are zip archives that embed file mtimes, so two builds of identical
source are never byte-identical. Every comparison here is therefore over the
*content* of each archive member, which is what actually gets imported.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html.parser
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Iterable, NamedTuple


ROOT = Path(__file__).resolve().parents[2]

PACKAGE_NAME = "investintell-quant-core"
IMPORT_NAME = "investintell_quant_core"
PACKAGE_DIR = ROOT / "packages" / IMPORT_NAME
PACKAGE_SRC = PACKAGE_DIR / "src"
CORE_PYPROJECT = PACKAGE_DIR / "pyproject.toml"
VERSION_PY = PACKAGE_SRC / IMPORT_NAME / "version.py"
ENGINE_PYPROJECT = ROOT / "services" / "quant_engine" / "pyproject.toml"

REGISTRY_PROJECT = "investintell-research-analisys"
REGISTRY_LOCATION = "southamerica-east1"
REGISTRY_REPOSITORY = "python"
SIMPLE_INDEX = (
    f"https://{REGISTRY_LOCATION}-python.pkg.dev"
    f"/{REGISTRY_PROJECT}/{REGISTRY_REPOSITORY}/simple/{PACKAGE_NAME}/"
)

PUBLISH_REF = "refs/heads/main"

_VERSION_PY_PATTERN = re.compile(r"^__version__\s*=\s*[\"']([^\"']+)[\"']\s*$", re.MULTILINE)


class VerificationError(Exception):
    """Raised when the build about to be published does not match the source."""

    def __init__(self, problems: Iterable[str]) -> None:
        self.problems = list(problems)
        super().__init__("\n".join(f"- {problem}" for problem in self.problems))


class DeclaredVersions(NamedTuple):
    """The version as stated by every file that has to agree on it."""

    core_pyproject: str
    version_module: str
    engine_pin: str

    @property
    def agreed(self) -> str | None:
        if self.core_pyproject == self.version_module == self.engine_pin:
            return self.core_pyproject
        return None


# --------------------------------------------------------------------------- #
# Declared versions
# --------------------------------------------------------------------------- #


def _project_version(pyproject: Path) -> str:
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    try:
        return str(data["project"]["version"])
    except KeyError as exc:  # pragma: no cover - malformed manifest
        raise VerificationError([f"{pyproject}: missing [project] version"]) from exc


def _module_version(version_py: Path) -> str:
    match = _VERSION_PY_PATTERN.search(version_py.read_text(encoding="utf-8"))
    if match is None:
        raise VerificationError([f"{version_py}: no __version__ assignment found"])
    return match.group(1)


def _dependency_pin(pyproject: Path, distribution: str) -> str:
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    pattern = re.compile(rf"^{re.escape(distribution)}\s*==\s*([^\s;,]+)$")
    for raw in data.get("project", {}).get("dependencies", []):
        match = pattern.match(str(raw).strip())
        if match is not None:
            return match.group(1)
    raise VerificationError([f"{pyproject}: no exact `{distribution}==` pin found"])


def declared_versions(root: Path = ROOT) -> DeclaredVersions:
    """Read the version from every file that must state it."""
    package_dir = root / "packages" / IMPORT_NAME
    return DeclaredVersions(
        core_pyproject=_project_version(package_dir / "pyproject.toml"),
        version_module=_module_version(package_dir / "src" / IMPORT_NAME / "version.py"),
        engine_pin=_dependency_pin(
            root / "services" / "quant_engine" / "pyproject.toml", PACKAGE_NAME
        ),
    )


def resolve_version(root: Path = ROOT) -> str:
    """Return the single declared version, or fail listing the disagreement."""
    versions = declared_versions(root)
    agreed = versions.agreed
    if agreed is None:
        raise VerificationError(
            [
                "the declared version must match everywhere; a bump has to touch all three: "
                f"packages/{IMPORT_NAME}/pyproject.toml={versions.core_pyproject!r}, "
                f"packages/{IMPORT_NAME}/src/{IMPORT_NAME}/version.py={versions.version_module!r}, "
                f"services/quant_engine/pyproject.toml pin={versions.engine_pin!r}"
            ]
        )
    return agreed


# --------------------------------------------------------------------------- #
# Git state
# --------------------------------------------------------------------------- #


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def tracked_sources(root: Path = ROOT) -> dict[str, bytes]:
    """Map wheel-relative path -> bytes for every tracked file of the package.

    ``git ls-files`` is the source of truth so that stray build leftovers or
    ``__pycache__`` in the checkout can never leak into the comparison.
    """
    rel_src = f"packages/{IMPORT_NAME}/src"
    listing = _git(root, "ls-files", "-z", "--", f"{rel_src}/{IMPORT_NAME}")
    payload: dict[str, bytes] = {}
    for entry in listing.split("\0"):
        if not entry:
            continue
        path = root / entry
        payload[entry[len(rel_src) + 1 :]] = path.read_bytes()
    if not payload:
        raise VerificationError([f"no tracked files under {rel_src}/{IMPORT_NAME}"])
    return payload


def dirty_paths(root: Path = ROOT) -> list[str]:
    """Return uncommitted paths under the package, engine manifest included."""
    listing = _git(
        root,
        "status",
        "--porcelain",
        "--",
        f"packages/{IMPORT_NAME}",
        "services/quant_engine/pyproject.toml",
    )
    return [line.strip() for line in listing.splitlines() if line.strip()]


# --------------------------------------------------------------------------- #
# Wheel building and comparison
# --------------------------------------------------------------------------- #


def build_wheel(outdir: Path, root: Path = ROOT) -> Path:
    """Build the wheel with the project's declared build backend."""
    outdir.mkdir(parents=True, exist_ok=True)
    package_dir = root / "packages" / IMPORT_NAME
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(outdir), str(package_dir)],
        check=True,
    )
    wheels = sorted(outdir.glob("*.whl"))
    if len(wheels) != 1:
        raise VerificationError(
            [f"expected exactly one wheel in {outdir}, found {[w.name for w in wheels]}"]
        )
    return wheels[0]


def wheel_payload(wheel: Path) -> dict[str, bytes]:
    """Map archive path -> bytes for the importable members of a wheel."""
    with zipfile.ZipFile(wheel) as archive:
        return {
            name: archive.read(name)
            for name in archive.namelist()
            if ".dist-info/" not in name
        }


def wheel_metadata_version(wheel: Path) -> str:
    with zipfile.ZipFile(wheel) as archive:
        names = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
        if len(names) != 1:
            raise VerificationError([f"{wheel.name}: expected one dist-info/METADATA, got {names}"])
        for line in archive.read(names[0]).decode("utf-8").splitlines():
            if line.startswith("Version:"):
                return line.split(":", 1)[1].strip()
    raise VerificationError([f"{wheel.name}: METADATA has no Version field"])


def compare_payloads(left: dict[str, bytes], right: dict[str, bytes], *, left_label: str,
                     right_label: str) -> list[str]:
    """Describe every content difference between two payload maps."""
    problems: list[str] = []
    for name in sorted(set(left) - set(right)):
        problems.append(f"{name}: present in {left_label}, absent from {right_label}")
    for name in sorted(set(right) - set(left)):
        problems.append(f"{name}: present in {right_label}, absent from {left_label}")
    for name in sorted(set(left) & set(right)):
        if left[name] != right[name]:
            problems.append(
                f"{name}: content differs "
                f"({left_label} sha256={hashlib.sha256(left[name]).hexdigest()[:16]}, "
                f"{right_label} sha256={hashlib.sha256(right[name]).hexdigest()[:16]})"
            )
    return problems


def verify_wheel_against_sources(wheel: Path, version: str, root: Path = ROOT) -> None:
    """Fail unless the wheel carries exactly the tracked source at this version."""
    problems: list[str] = []

    expected_name = f"{IMPORT_NAME}-{version}-py3-none-any.whl"
    if wheel.name != expected_name:
        problems.append(f"wheel filename is {wheel.name!r}, expected {expected_name!r}")

    metadata_version = wheel_metadata_version(wheel)
    if metadata_version != version:
        problems.append(
            f"wheel METADATA declares version {metadata_version!r}, sources declare {version!r}"
        )

    problems.extend(
        compare_payloads(
            wheel_payload(wheel),
            tracked_sources(root),
            left_label="wheel",
            right_label="git-tracked source",
        )
    )

    if problems:
        raise VerificationError(problems)


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #


class _SimpleIndexParser(html.parser.HTMLParser):
    """Collect ``filename -> url`` from a PEP 503 simple index page."""

    def __init__(self) -> None:
        super().__init__()
        self.links: dict[str, str] = {}
        self._href: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self._href = dict(attrs).get("href")

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            name = data.strip()
            if name:
                self.links[name] = self._href

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self._href = None


def _authorized_get(url: str, token: str) -> bytes:
    credential = base64.b64encode(f"oauth2accesstoken:{token}".encode()).decode()
    request = urllib.request.Request(url, headers={"Authorization": f"Basic {credential}"})
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - fixed https host
        return response.read()


def published_wheel_url(token: str, version: str, index_url: str = SIMPLE_INDEX) -> str | None:
    """Return the download URL of the published wheel, or ``None`` if absent.

    The simple index is queried on purpose: it is the exact view ``uv``/``pip``
    use when resolving the app's ``investintell-quant-core==...`` pin.
    """
    try:
        page = _authorized_get(index_url, token)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    parser = _SimpleIndexParser()
    parser.feed(page.decode("utf-8"))
    return parser.links.get(f"{IMPORT_NAME}-{version}-py3-none-any.whl")


def download(url: str, token: str, destination: Path) -> Path:
    destination.write_bytes(_authorized_get(url, token))
    return destination


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


def _emit(github_output: Path | None, **values: str) -> None:
    for key, value in values.items():
        print(f"{key}={value}")
    if github_output is not None:
        with github_output.open("a", encoding="utf-8") as handle:
            for key, value in values.items():
                handle.write(f"{key}={value}\n")


def run_verify(outdir: Path | None, allow_dirty: bool, root: Path = ROOT) -> Path:
    version = resolve_version(root)
    if not allow_dirty:
        dirty = dirty_paths(root)
        if dirty:
            raise VerificationError(
                ["refusing to verify a dirty checkout; commit or stash first:", *dirty]
            )
    target = outdir if outdir is not None else Path(tempfile.mkdtemp(prefix="quant-core-"))
    wheel = build_wheel(target, root)
    verify_wheel_against_sources(wheel, version, root)
    print(f"verified {wheel.name}: content matches the tracked source at version {version}")
    return wheel


def run_preflight(outdir: Path, github_output: Path | None, allow_dirty: bool,
                  require_published: bool = False, root: Path = ROOT) -> int:
    # GitHub always sets GITHUB_REF, so on CI this is a hard gate. Locally it is
    # unset, which leaves the read-only audit path usable from any checkout —
    # uploading still needs the token only the main-gated job can mint.
    ref = os.environ.get("GITHUB_REF")
    if ref is not None and ref != PUBLISH_REF:
        raise VerificationError(
            [f"publishing is only allowed from {PUBLISH_REF}, this run is on {ref}"]
        )

    token = os.environ.get("GAR_ACCESS_TOKEN", "").strip()
    if not token:
        raise VerificationError(["GAR_ACCESS_TOKEN is empty; the registry cannot be checked"])

    wheel = run_verify(outdir, allow_dirty, root)
    version = resolve_version(root)

    url = published_wheel_url(token, version)
    if url is None:
        if require_published:
            raise VerificationError(
                [
                    f"{PACKAGE_NAME}=={version} is absent from the index the app resolves "
                    f"({SIMPLE_INDEX}); the upload did not land"
                ]
            )
        _emit(github_output, publish_needed="true", version=version, wheel=str(wheel))
        print(f"{PACKAGE_NAME}=={version} is not published yet; the verified wheel will be uploaded")
        return 0

    with tempfile.TemporaryDirectory(prefix="quant-core-published-") as scratch:
        published = download(url, token, Path(scratch) / "published.whl")
        problems = compare_payloads(
            wheel_payload(published),
            wheel_payload(wheel),
            left_label="published wheel",
            right_label="wheel built from this ref",
        )
    if problems:
        raise VerificationError(
            [
                f"{PACKAGE_NAME}=={version} is published but this ref does not reproduce it; "
                "the published artifact came from somewhere other than this source. "
                "Bump the version instead of overwriting.",
                *problems,
            ]
        )

    _emit(github_output, publish_needed="false", version=version, wheel=str(wheel))
    print(f"{PACKAGE_NAME}=={version} is already published and reproduces from this ref; nothing to do")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    verify = subparsers.add_parser(
        "verify", help="build the wheel and prove it matches the tracked source"
    )
    verify.add_argument("--outdir", type=Path, default=None)
    verify.add_argument("--allow-dirty", action="store_true")

    preflight = subparsers.add_parser(
        "preflight", help="verify, then decide whether the registry needs this version"
    )
    preflight.add_argument("--outdir", type=Path, default=ROOT / "dist" / "quant-core")
    preflight.add_argument("--github-output", type=Path, default=None)
    preflight.add_argument("--allow-dirty", action="store_true")
    preflight.add_argument(
        "--require-published",
        action="store_true",
        help="fail if the version is missing from the index (post-upload confirmation)",
    )

    args = parser.parse_args(argv)
    try:
        if args.command == "verify":
            run_verify(args.outdir, args.allow_dirty)
            return 0
        if args.outdir.exists():
            shutil.rmtree(args.outdir)
        return run_preflight(
            args.outdir,
            args.github_output,
            args.allow_dirty,
            args.require_published,
        )
    except VerificationError as exc:
        print(f"{PACKAGE_NAME} publish gate failed:\n{exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(f"command failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
