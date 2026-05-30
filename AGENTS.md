# AGENTS.md

Guidance for AI coding agents working in this repository. This file is read by tools that follow the `AGENTS.md` convention (Claude Code via `CLAUDE.md`, OpenAI Codex, and others). If you use a tool that reads a different file (Cursor: `.cursor/rules`, GitHub Copilot: `.github/copilot-instructions.md`, Aider: `CONVENTIONS.md`), point it at this file or mirror the relevant parts.

**This is a public open-source repository.** Treat every commit, comment, PR description, and code suggestion as world-readable. The Open-source guardrails section below is the most important part of this file — read it before making any change.

---

## Project overview

`roark-analytics-python-livekit` is a [LiveKit Agents](https://docs.livekit.io/agents/) integration that ships call lifecycle, transcripts, tool calls, metrics, and stereo recordings to the [Roark](https://roark.ai) analytics platform. It is provider-agnostic — the same helper works against any STT / LLM / TTS stack, self-hosted `livekit-server`, and local `console` mode.

- Python 3.10+
- Async-native (built on `httpx` + LiveKit Agents' asyncio runtime)
- Single public surface: `observe_session` (the entry point) and `RoarkClient`
- Tested against `livekit-agents >= 1.0, < 2`

## Project structure

```
.
├── src/roark_analytics_python_livekit/   # The library — keep the public surface narrow
│   ├── __init__.py            # Re-exports (observe_session, RoarkClient)
│   ├── session.py             # observe_session — the main entry point
│   ├── client.py              # Roark webhook + presigned-upload HTTP client
│   ├── audio.py               # Stereo capture (StereoMixer, AudioCapture)
│   ├── _types.py              # Private type aliases (underscore = internal)
│   └── py.typed               # PEP 561 marker — ship type hints to users
├── tests/                     # Pytest, colocated test files
├── examples/                  # Runnable self-hosted uv project (not packaged)
├── pyproject.toml             # Hatchling build, ruff + pytest config
├── uv.lock                    # Lockfile — commit changes alongside deps
└── .github/workflows/         # CI, Release, Claude review
```

## Commands

Use [`uv`](https://github.com/astral-sh/uv) for everything.

```bash
uv sync --all-extras            # Install runtime + dev + examples deps
uv run pytest                   # Run tests
uv run pytest tests/test_session.py::test_x   # Run a single test
uv run ruff check .             # Lint
uv run ruff format .            # Format
uv run ruff format --check .    # Check formatting (CI mode)
uv run mypy src/roark_analytics_python_livekit   # Type-check the library
```

**Always run `uv run ruff check .` and `uv run pytest` before pushing.** CI will reject otherwise.

## Code conventions

- **Public vs private**: anything under a module starting with `_` (e.g. `_types.py`) is internal and may change without notice. The public surface is whatever is re-exported from `src/roark_analytics_python_livekit/__init__.py` — be deliberate about what lands there.
- **Type hints required** on all public functions and methods. `py.typed` ships with the package, so users rely on these.
- **Async everywhere**: this is an async library. New I/O must be async. Do not introduce blocking calls inside the session hot path (audio taps, event handlers).
- **Imports**: ruff handles sorting (`select = ["I"]` is on). Don't fight it.
- **Line length**: 100 (set in `pyproject.toml`).
- **Ruff rules in play**: `E, F, I, B, UP, N`. If you disagree with a rule, fix the code — don't add `# noqa`.
- **Comments**: write them only when the *why* is non-obvious (a LiveKit API quirk, a workaround for a known bug, a non-obvious invariant). Don't narrate the code.
- **Logging**: use the existing logger pattern in the module. Never log API keys, request bodies that contain transcripts, or PII.

## Testing

- Tests live in `tests/` and use pytest with `asyncio_mode = "auto"` (no `@pytest.mark.asyncio` needed).
- Add a test for any behavior change. Public surface changes without a corresponding test are not acceptable.
- **Never make real network calls in tests.** Mock `httpx.AsyncClient` (or the `RoarkClient`) — tests must run offline and deterministically.
- Tests that import `livekit-agents` are guarded with `pytest.importorskip`, so the suite runs on the dev extras alone.
- **Never commit a real API key**, recording URL, or transcript fixture that contains real user content. Use synthetic fixtures.

## Pull requests & commits

- Branch off `main`. Keep PRs focused on one logical change.
- PRs are **squash-merged**, so the PR title becomes the commit message on `main`. Make it conventional and descriptive: `fix: stop dropping the agent audio channel`, `feat: support disabling audio capture`, `docs: clarify console-mode setup`.
- Fill in the PR template. Tick the "no secrets / no customer data" checkbox honestly.
- CI must be green. Don't merge with red checks.
- Update `CHANGELOG.md` under `[Unreleased]` for any user-visible change (added / changed / fixed / removed).

## Releases

- Cut by maintainers via the `Release` workflow, triggered on merge to `main` once the `pyproject.toml` version is bumped.
- Version is bumped in `pyproject.toml` and `CHANGELOG.md` in the same commit that lands on `main`.
- Contributors **do not** bump versions in their PRs.

---

## Open-source guardrails

This repository is public on GitHub and published to PyPI as `roark-analytics-python-livekit`. Anyone — including competitors, customers, prospective customers, security researchers, and random internet users — can read everything you write here. The rules below exist so that "fine in a private repo" mistakes don't become "indexed by Google forever" mistakes.

### Never include in code, comments, commits, PRs, or issue replies

- **Internal infrastructure details.** No references to internal AWS accounts, account IDs, stage names, internal hostnames, Temporal workflows, GraphQL schemas, database tables, internal microservices, or any other component of the Roark backend that customers don't see.
- **Internal tooling.** No links to Linear, Notion, Slack, internal dashboards, internal Grafana boards, internal runbooks, Google Docs, Figma files, or any other internal system. If a piece of context lives behind a Roark SSO login, it doesn't belong in this repo.
- **Internal identifiers.** No internal ticket IDs (e.g. `PROJ-1234` from a private tracker), internal PR numbers from other repos, internal Slack thread links, employee handles, or codenames for unreleased features.
- **Customer information.** Never name a Roark customer, paste their data, reference their use case, or mention contract terms. If a customer's bug report drove a fix, describe the bug behavior — not the customer.
- **Unreleased product details.** Don't mention features that aren't shipped on roark.ai or in public docs. No internal roadmap, no "we're planning to add X", no references to private design discussions.
- **Credentials and secrets.** No API keys, OAuth tokens, presigned URLs, webhook secrets, or `.env` values — even fake-looking ones. Use `rk_live_replace_me` style placeholders only.
- **Personally identifiable information.** No real names, emails, phone numbers, or recordings in fixtures, examples, or test data. Synthetic only.
- **Other private Roark repos.** Don't reference internal package names, internal repo names, or anything else that 404s for an external reader. If you're unsure whether a repo is public, check whether it appears at <https://github.com/orgs/roarkhq/repositories?type=public>.

### PR descriptions and commit messages

Write them so an external contributor (or a future maintainer with no Roark context) can fully understand them.

- **Self-contained.** No "see internal ticket PROJ-1234", no "as discussed in #eng-channel", no "per the design doc". State the user-facing problem and how this PR addresses it.
- **Describe behavior, not internal reasoning.** "Fix the agent audio channel being silent in console mode" — yes. "Fix the bug Acme Corp reported in last Tuesday's call" — no.
- **No internal jargon.** Roark has internal terms for things; use the public name (the one in the README and docs.roark.ai). If a concept doesn't have a public name yet, describe it generically.
- **Reviewer mentions.** Tag GitHub usernames only. Don't paste Slack handles or internal team aliases.
- **Reference issues by GitHub number** in *this* repo (`#42`). Never reference issues in private repos.

### Code comments

- Treat every comment as part of the public API documentation — it ships in the source distribution on PyPI.
- Don't reference internal incidents, internal team decisions, or "the way we do it on the platform side". If a comment needs internal context to make sense, rewrite it so it stands alone.
- No TODOs with internal owners (`# TODO(daniel): ...`). Either fix it, file a GitHub issue, or write the TODO generically.

### Examples and fixtures

- Use generic personas (`Acme`, `the user`, `the agent`). Don't use real company names — including non-customers.
- Recording URLs in examples should point to public domains (`https://example.com/sample.mp3`) or be obvious placeholders.
- `examples/` is shipped expectation: copy-pasteable code that works against a fresh Roark API key. If it depends on internal-only behavior, it doesn't belong here.

### README and docs

- Link to `docs.roark.ai` for product concepts, not internal docs.
- Keep the dependency-version messaging conservative — pin ranges, document the tested version.
- Don't promise SLAs, uptime numbers, or roadmap dates.

### When in doubt

If you're not sure whether something is OK to commit, **don't**. Ask in the PR description ("@maintainer is this OK to expose?") or open a draft PR and tag a maintainer for review before pushing further. The cost of pausing is low; the cost of indexing internal info on a public repo is high (Git history is forever, even after a force-push, because of forks and caches).

---

## Where to find more

- [README.md](./README.md) — user-facing docs and quick start
- [CONTRIBUTING.md](./CONTRIBUTING.md) — contribution flow and dev setup
- [SECURITY.md](./SECURITY.md) — how to report vulnerabilities (privately)
- [CHANGELOG.md](./CHANGELOG.md) — release history
- [docs.roark.ai/integrations/livekit](https://docs.roark.ai/integrations/livekit) — product documentation
