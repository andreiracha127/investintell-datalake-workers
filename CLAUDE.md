# CLAUDE.md

## Orchestration workflow

You (Fable) are the orchestrator. Plan, decompose, synthesize.  
Reasoning-heavy phases → deep-reasoner  
Mechanical work → fast-worker  
Codex (/codex:rescue --background) is a cracked engineer on par with deep-reasoner, from a different perspective. Treat as a peer, not a reviewer.  
High-stakes decisions: task Opus + Codex on the same problem in parallel, synthesize the best of both, without showing either the other's answer. Keep your own context lean.   

## Default context workflow

- Start coding tasks by activating the workspace with Serena and following the Serena instructions required by the environment.
- For broad discovery tasks, run one Auggie remote code search after Serena activation to gather repository context before deeper local exploration.
- Use Auggie when the task asks where logic lives, how a subsystem is wired, or touches unfamiliar architecture, ingestion, backfill, worker orchestration, data flow, auth, or UI paths.
- Prefer `mcp__auggie.augment_code_search` against `andreiracha127/investintell-datalake-workers`; use the current git branch when it is indexed, otherwise fall back to `main`.
- Do not use the local `auggie-context` MCP in this workspace; it was disabled because the MCP process failed to spawn the Auggie CLI (`spawn auggie ENOENT`).
- Treat Auggie as a scout for relevant files and snippets. Verify everything against the local worktree with Serena, `rg`, and direct reads before editing.
- Use Serena, `rg`, local tests, and type/lint gates for symbol navigation, references, refactors, and any work affected by uncommitted local changes.

## Operational MCP and deploy defaults

- For non-trivial planning, debugging, sequencing, or cross-system tasks, use the `mcp/sequentialthinking` tool when it is available. If it is unavailable, say that explicitly and continue with the best local reasoning path.
- Treat Railway as the source of truth for deploy references, service status, deployment logs, and environment wiring. Use Railway MCP or the Railway CLI before making deploy claims.
- Treat the Cloud DB as accessible through Tiger MCP by default. For Cloud DB/schema/source verification, try Tiger MCP first and report clearly if credentials, connectivity, or tool availability block live verification.
- For frontend deploy work, use InsForge as the default deployment path unless the user names a different target.
- Do not claim Railway deploy, Cloud DB, or frontend deploy success without tool-backed evidence from the relevant path.
