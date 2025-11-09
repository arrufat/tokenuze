# Tokenuze

Tokenuze is a Zig CLI that summarizes OpenAI Codex, Gemini, and Claude session usage. It scans `~/.codex/sessions`, `~/.gemini/tmp`, and `~/.claude/projects`, aggregates token counts per day and per model, and reports pricing using either the live LiteLLM pricing manifest or local fallbacks. Output is emitted as compact JSON, making it easy to feed into dashboards or further scripts.

## Requirements
- Zig 0.16.0-dev.1225+bf9082518 (for building from source)
- Access to Codex session logs at `~/.codex/sessions`
- Optional: access to Gemini session logs at `~/.gemini/tmp`
- Optional: access to Claude session logs at `~/.claude/projects`
- Optional: network access to fetch remote pricing

## Quick Start
```bash
zig build --release=fast  # release mode in zig-out/bin/tokenuze
tokenuze --upload  # upload usage across all supported models
tokenuze --upload --agent codex --agent gemini --agent claude  # request specific agents
tokenuze --since 20250101
tokenuze --since 20250101 --until 20250107
tokenuze --help
```

## Command-Line Options
- `--since YYYYMMDD` limits processing to events on/after the specified local date.
- `--until YYYYMMDD` caps the range; must be >= `--since` when both are present.
- `--tz <offset>` buckets events using a fixed offset like `+09`, `-05:30`, or `UTC` (default: system timezone).
- `--pretty` enables indented JSON output (handy when reading the payload manually).
- `--table` renders the daily summaries as a ccusage-style table instead of JSON (overrides `--pretty`).
- `--log-level <error|warn|info|debug>` controls how chatty Tokenuze's logs are (defaults to `info`).
- `--agent <codex|gemini|claude>` restricts processing to the specified provider; repeat the flag to include multiple (defaults to all providers).
- `--machine-id` prints the cached/generated machine identifier and exits (no summaries).
- `--upload` captures Tokenuze's JSON summary for the selected providers and POSTs it to `/api/usage/report` using `DASHBOARD_API_URL`/`DASHBOARD_API_KEY`.

## What It Produces
Tokenuze prints a JSON payload shaped like:
```json
{
  "days": [
    {
      "date": "2025-11-01",
      "models": [
        {
          "name": "gpt-5-codex",
          "usage": { "input_tokens": 123, "output_tokens": 45, "total_tokens": 168 },
          "cost_usd": 0.10
        }
      ],
      "totals": { "total_tokens": 168, "cost_usd": 0.10 }
    }
  ],
  "total": { "total_tokens": 168, "cost_usd": 0.10 }
}
```
Missing pricing entries are listed under `missing_pricing`.

## Extending
Provider integrations live in `src/providers/`. To add a new LLM vendor:
1. Start with `provider.makeProvider` (see `claude.zig`/`gemini.zig`) to leverage shared utilities and reduce boilerplate.
2. Implement a parser that emits `Model.TokenUsageEvent` rows; use the shared helpers where possible:
   - `ParseContext.captureModel/requireModel` keep per-session model state consistent.
   - `UsageFieldDescriptor` + `parseUsageObject` let you describe raw token counters declaratively instead of hand-writing math.
   - `readJsonValue`, `timestampFromValue`, and `overrideSessionLabelFromValue` cover the common JSON load + timestamp/session-id plumbing so providers focus on their unique bits.
3. Expose any provider-specific pricing loader and register the provider in `src/root.zig` so CLI option parsing recognizes it.
4. Add fixtures under `fixtures/<provider>/` plus unit tests near the parser to lock in expected token deltas.
