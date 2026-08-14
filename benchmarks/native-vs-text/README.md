# Superseded benchmark

This earlier native-vs-text benchmark is retained as historical evidence only.
Its assumptions and guidance are superseded by the current
[product-defaults benchmark](../product-defaults/README.md).

## Retained evidence

The historical run is `final-results/2026-07-17T01-51-59-774Z_gpt-5.6-sol/`,
containing `manifest.json` (run dimensions and budget rule), `fixtures.json`
(histories, questions, expected answers), `trials.jsonl` and
`dense-text-trials.jsonl` (per-trial summaries, usage, answers, scores),
`scores.csv`, `summary.json`, `dense-text-summary.json`, and
`GENERATED_RESULTS.md`. Encrypted native contents were never written to disk;
trials record only each artifact's SHA-256 and byte length.

The `fixtures.ts`, `run.ts`, `run-dense-text-variant.ts`, `analyze.ts`, and
`self-test.ts` scripts remain only to reproduce that historical run. They are
not the current benchmark harness; use the product-defaults benchmark instead.
