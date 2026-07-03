# CLAUDE.md

## Orchestration workflow

You (Fable) are the orchestrator. Plan, decompose, synthesize.  
Reasoning-heavy phases → deep-reasoner  
Mechanical work → fast-worker  
Codex (/codex:rescue --background) is a cracked engineer on par with deep-reasoner, from a different perspective. Treat as a peer, not a reviewer.  
High-stakes decisions: task Opus + Codex on the same problem in parallel, synthesize the best of both, without showing either the other's answer. Keep your own context lean.   

## Default context workflow

- Start coding tasks by activating the workspace with Serena (`activate_project` → `investintell-datalake-workers-main`) and following the Serena instructions required by the environment. Use `mcp__serena__*` for exact-symbol navigation, references, and refactors, and for any work affected by uncommitted local changes.
- For discovery by CONCEITO or INTENÇÃO (where logic lives, how a subsystem is wired, unfamiliar ingestion/backfill/worker-orchestration/data-flow/auth paths), call `mcp__auggie-context__query_codebase` before sweeping with Grep/Glob/Task.
- Pass `workspace_root` = the absolute path of this worktree, `e:/investintell-datalake-workers-main` — that is the path indexed and connected by the local `auggie-context` server. Do not pass a hardcoded base path.
- Do **not** use the remote `mcp__auggie__*` (Augment HTTP API at `api.augmentcode.com`): it needs auth and is not wired for this worktree. The local `auggie-context` tool above is the working path.
- Treat Auggie as a scout for relevant files and snippets. Verify everything against the local worktree with Serena, `rg`, and direct reads before editing.

## Operational MCP and deploy defaults

- For non-trivial planning, debugging, sequencing, or cross-system tasks, use the `mcp/sequentialthinking` tool when it is available. If it is unavailable, say that explicitly and continue with the best local reasoning path.
- Treat Railway as the source of truth for deploy references, service status, deployment logs, and environment wiring. Use Railway MCP or the Railway CLI before making deploy claims.
- Treat the Cloud DB as accessible through Tiger MCP by default. For Cloud DB/schema/source verification, try Tiger MCP first and report clearly if credentials, connectivity, or tool availability block live verification.
- For frontend deploy work, use InsForge as the default deployment path unless the user names a different target.
- Do not claim Railway deploy, Cloud DB, or frontend deploy success without tool-backed evidence from the relevant path.
