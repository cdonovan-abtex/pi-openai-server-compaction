# TESTPLAN

## Goals

1. Verify supported OpenAI-compatible Responses sessions use the expected continuity path:
   - `store: true`
   - `context_management`
   - `previous_response_id` when safe
   - `/v1/responses` with a trailing `compaction_trigger` during Pi compaction
2. Verify Pi remains usable:
   - `/model`
   - `/tree`
   - session resume/reload
   - cost totals on WS path are non-zero and plausible

## Suggested manual tests

### 1. Baseline supported turn
- Start Pi with this extension enabled.
- Use either:
  - a direct `openai/*` Responses model, or
  - an `openai-codex/*` model
- Confirm normal response succeeds.

### 2. Live continuation path
- Run a multi-turn session with tool calls.
- For direct `openai/*`, confirm later requests use `previous_response_id` or WS continuation.
- For `openai-codex/*`, confirm normal Codex transport behavior remains intact.
- Confirm no obvious continuity drop across normal turns.

### 3. Remote compaction path
- Force `/compact` in a supported session.
- Confirm extension returns a Pi compaction entry.
- Inspect the session JSONL and confirm `details.remoteCompaction.replacementHistory` exists.
- Inject a transient HTTP or streamed `server_error`; confirm bounded exponential-backoff retries and immediate abort behavior.
- Exhaust the retry budget; confirm Pi warns that it saved only the text fallback and does not claim `details.remoteCompaction`.
- Confirm that warning renders in the TUI *after* the compaction is committed and survives the post-compaction re-render (see the widget-key comment in `src/index.ts` for why it is a durable widget rather than a notification).
- Confirm a successful remote compaction produces no downgrade warning, and that the warning is never shown twice or replayed against a later compaction.
- Continue the session and confirm later compatible turns still behave coherently.
- Confirm `details.remoteCompaction.implementation` is `responses_compaction_v2`.
- Confirm replacement history ends with an opaque `compaction` item and retains only the recent user-message budget outside that item.

### 4. `/model` safety
- After remote compaction, switch to another model with `/model`.
- Confirm the session continues normally.
- Switch back to the original direct OpenAI model.
- Confirm the session still works, does not crash, and does not reuse polluted remote history.
- Restart or reload after that round-trip and confirm reconstructed remote replay still excludes the intervening other-model turns.

### 5. Tree/fork safety
- Compact, then use `/tree` or fork navigation.
- Confirm session remains usable.
- Confirm stale WS / previous-response state is not reused incorrectly.

### 6. Resume/reload safety
- Compact remotely.
- Restart Pi or reload extensions.
- Resume the same session.
- Confirm remote compaction state is reconstructed from compaction details.

### 7. Cost accounting
- Use the supported provider path for several turns.
- Confirm footer/session stats show non-zero token/cost totals.
- Compare rough totals against dashboard/provider logs when possible.

## Offline smoke test

The [smoke command](README.md#testing) requires the installed Pi peers to match
the development versions in [package.json](package.json). Alongside import and
compaction checks, `scripts/smoke-provider-api.mjs` uses Pi's actual transcript
normalization API and a local WebSocket server to check that instructions and
tool declarations reach `response.create`, including their absence on a later
request. It also checks case-insensitive header overrides and null suppression
of default headers, and preserves null overrides through both portable-summary
generation and Pi's fallback compactor. The fixture rejects HTTP fallback and
bounds its wait, so these protocol checks do not require provider credentials.

## Automated live test

```bash
cd /home/algal/gits/pi-openai-server-compaction
node --experimental-strip-types ./tests/live/openai-compaction-rpc-live.ts
PI_OPENAI_SERVER_COMPACTION_TEST_MODEL=openai/gpt-5.6-luna node --experimental-strip-types ./tests/live/openai-compaction-rpc-live.ts
PI_OPENAI_SERVER_COMPACTION_TEST_MODEL=openai-codex/gpt-5.6-sol node --experimental-strip-types ./tests/live/openai-compaction-rpc-live.ts
```

The automated live harness lives in `tests/live/openai-compaction-rpc-live.ts`.

### Live retry fault injection

`tests/live/openai-compaction-retry-fault-injection.ts` drives a real `pi` session
against the real provider through a local pass-through proxy that injects the exact
streamed nested `server_error` observed in the field on compaction requests only.

```bash
node --experimental-strip-types ./tests/live/openai-compaction-retry-fault-injection.ts
PI_OPENAI_SERVER_COMPACTION_SCENARIOS=transient,exhaustion,non-retryable \
  node --experimental-strip-types ./tests/live/openai-compaction-retry-fault-injection.ts
```

Scenarios: `transient` (retry preserves the opaque `compaction` artifact and a fact
deliberately omitted from the text summary is still recoverable after compaction and
after resume), `exhaustion` (bounded attempts, then a text-only fallback that does not
claim `details.remoteCompaction`), and `non-retryable` (HTTP 400 fails on attempt 1).
Set `PI_OPENAI_SERVER_COMPACTION_BASELINE=1` with
`PI_OPENAI_SERVER_COMPACTION_EXTENSION=<path to a pre-fix src/index.ts>` to record the
pre-fix behaviour for an A/B comparison instead of asserting the retry contract.

Current automated coverage includes:
- nested streamed provider-error parsing
- transient retry success, bounded exhaustion, non-retryable 4xx behavior, `Retry-After`, and abort during backoff
- retry-exhaustion warning: withheld during `session_before_compact`, then written once from `session_compact` to a durable extension widget rather than `ui.notify` (rationale in the widget-key comment in `src/index.ts`). Also covered: suppressed when opaque continuity survived, retracted by the next compaction and by a session change, and dropped when the compaction is abandoned (`npm run smoke`)
- compaction continuity in the same session
- `/model`-style switch away and back again
- fork after compaction
- resume/reload after compaction
- resume/reload after switching away from and back to the compacted model

Recommended follow-up live regression:
- explicit tree navigation after an intervening other-model turn, followed by restart

## Controlled compaction benchmark

The native-vs-text benchmark, reproduction instructions, retained evidence, and report live under:
- `benchmarks/native-vs-text/`
