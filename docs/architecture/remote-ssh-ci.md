# Remote SSH CI

This repository can run the Railway quant-engine gate on a private compute node
over SSH. The runner ships the current committed tree as a `git archive`, so it
can validate a commit before that commit is pushed to GitHub.

## Default Node

- Host: `andrei@100.96.0.3`
- Machine: `LEGION7IPRO`
- Remote root: `C:\Users\Andrei\ci-runners`
- Gate: `docker/railway-ci/Dockerfile`

## Manual Run

```powershell
powershell -NoLogo -NoProfile -NonInteractive -OutputFormat Text -ExecutionPolicy Bypass -File scripts\ci\run_remote_railway_ci.ps1
```

Useful options:

```powershell
scripts\ci\run_remote_railway_ci.ps1 -BuilderMode Legacy
scripts\ci\run_remote_railway_ci.ps1 -RemoteHost andrei@100.96.0.3
scripts\ci\run_remote_railway_ci.ps1 -RemoteRoot C:\Users\Andrei\ci-runners
```

`-BuilderMode Auto` is the default. It tries Docker BuildKit first and falls
back to the legacy builder only when Docker Desktop's credential helper is not
usable from the SSH session.

## Pre-Push Hook

Install the versioned hook:

```powershell
git config core.hooksPath .githooks
```

Bypass for an emergency push:

```powershell
$env:INVESTINTELL_SKIP_REMOTE_CI = "1"
git push
Remove-Item Env:\INVESTINTELL_SKIP_REMOTE_CI
```

## Evidence

A passing remote run prints:

```text
REMOTE_CI_SHA=<commit>
REMOTE_CI_BUILDER=<BuildKit|Legacy>
REMOTE_CI_IMAGE=investintell-railway-ci:<short-sha>
REMOTE_CI_EXIT=0
REMOTE_CI_STATUS=PASS
```

The build log remains on the remote under the snapshot worktree's `ci-logs`
directory.
