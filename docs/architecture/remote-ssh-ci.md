# Retired Remote SSH CI

Remote SSH/Docker CI is retired for PR gating. The repository now relies on
GitHub Actions as the remote status check source for pull requests and branch
pushes.

The versioned pre-push hook no longer starts a private remote Docker build. It
only records that required remote checks run after push through GitHub Actions.

Current required workflow:

```text
.github/workflows/ci.yml
```

The workflow runs the quant-engine governance gate directly on GitHub-hosted
runners without building `docker/railway-ci/Dockerfile`.
