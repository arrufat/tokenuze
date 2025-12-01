# Tokenuze

Tokenuze is a Zig CLI that summarizes OpenAI Codex, Gemini, Claude, and Opencode session usage. It scans `~/.codex/sessions`, `~/.gemini/tmp`, `~/.claude/projects`, and `~/.local/share/opencode/storage/session`, aggregates token counts per day and per model, and reports pricing using either the live LiteLLM pricing manifest or local fallbacks. Output is rendered as a ccusage-style table by default (or compact JSON with `--json`), making it easy to feed into dashboards or further scripts.

## Requirements
- Zig 0.16.0-dev.1456+16fc083f2 (for building from source)
- Optional: Access to Codex session logs at `~/.codex/sessions`
- Optional: access to Gemini session logs at `~/.gemini/tmp`
- Optional: access to Claude session logs at `~/.claude/projects`
- Optional: access to Opencode session logs at `~/.local/share/opencode/storage/session` (messages live under `storage/message/<sessionID>`)
- Optional: network access to fetch remote pricing / uploading stats

## Quick Start
```bash
zig build --release=fast  # release mode in zig-out/bin/tokenuze
tokenuze --upload  # upload usage across all supported models
tokenuze --upload --agent codex --agent gemini --agent claude --agent opencode  # request specific agents
tokenuze --since 20250101
tokenuze --since 20250101 --until 20250107
tokenuze --sessions --since 20250101              # print per-session table (default)
tokenuze --sessions --since 20250101 --json --pretty  # print per-session JSON
tokenuze --help
```

## Command-Line Options
- `--since YYYYMMDD` limits processing to events on/after the specified local date.
- `--until YYYYMMDD` caps the range; must be >= `--since` when both are present.
- `--tz <offset>` buckets events using a fixed offset like `+09`, `-05:30`, or `UTC` (default: system timezone).
- `--table` renders the daily summaries as a ccusage-style table (default behavior; if you also pass `--json`, whichever flag appears last on the command line decides the format).
- `--json` renders daily summaries as JSON instead of the table (respects `--pretty`; last `--table`/`--json` flag wins when both are present).
- `--sessions` renders session-level output (table by default; use `--json`/`--pretty` for JSON) and includes per-session token/cost breakdown.
- `--pretty` enables indented JSON output (handy when reading the payload manually).
- `--log-level <error|warn|info|debug>` controls how chatty Tokenuze's logs are (defaults to `info`).
- `--agent <codex|gemini|claude|opencode>` restricts processing to the specified provider; repeat the flag to include multiple (defaults to all providers).
- `--machine-id` prints the cached/generated machine identifier and exits (no summaries).
- `--upload` captures Tokenuze's JSON summary for the selected providers and POSTs it to `/api/usage/report` using `DASHBOARD_API_URL`/`DASHBOARD_API_KEY`. Pass `--table` or `--json` alongside `--upload` to display a local report after the upload completes.

## What It Produces
Tokenuze prints a JSON payload shaped like:
```json
{
  "daily": [
    {
      "date": "Nov 25, 2025",
      "isoDate": "2025-11-25",
      "inputTokens": 248291670,
      "cachedInputTokens": 236782489,
      "outputTokens": 1188464,
      "reasoningOutputTokens": 749903,
      "totalTokens": 249489543,
      "costUSD": 55.88007429999999,
      "models": {
        "gemini-2.5-flash": {
          "inputTokens": 10992,
          "cachedInputTokens": 0,
          "outputTokens": 10,
          "reasoningOutputTokens": 79,
          "totalTokens": 11032,
          "costUSD": 0.0033225999999999998,
          "pricingAvailable": true,
          "isFallback": false
        },
        "gemini-3-pro-preview": {
          "inputTokens": 5664,
          "cachedInputTokens": 10521,
          "outputTokens": 1738,
          "reasoningOutputTokens": 2048,
          "totalTokens": 16781,
          "costUSD": 0.0342882,
          "pricingAvailable": true,
          "isFallback": false
        },
        "gpt-5.1-codex": {
          "inputTokens": 248275014,
          "cachedInputTokens": 236771968,
          "outputTokens": 1186716,
          "reasoningOutputTokens": 747776,
          "totalTokens": 249461730,
          "costUSD": 55.842463499999994,
          "pricingAvailable": true,
          "isFallback": false
        }
      },
      "missingPricing": []
    }
  ],
  "totals": {
    "inputTokens": 248291670,
    "cachedInputTokens": 236782489,
    "outputTokens": 1188464,
    "reasoningOutputTokens": 749903,
    "totalTokens": 249489543,
    "costUSD": 55.88007429999999,
    "missingPricing": []
  }
}
```

Missing pricing entries are listed under `missing_pricing`.

## Extending
Provider integrations live in `src/providers/`. To add a new LLM vendor:
1. Start with `provider.makeProvider` (see `claude.zig`/`gemini.zig`) to leverage shared utilities and reduce boilerplate.
2. Implement a parser that emits `Model.TokenUsageEvent` rows; use the shared helpers where possible:
   - `ParseContext.captureModel/requireModel` keep per-session model state consistent.
   - `UsageFieldDescriptor` + `jsonParseUsageObjectWithDescriptors` let you describe raw token counters declaratively instead of hand-writing math.
   - `streamJsonLines`/`parseJsonLine`, `timestampFromSlice`, and `overrideSessionLabelFromSlice` cover the common JSON streaming + timestamp/session-id plumbing so providers focus on their unique bits.
3. Expose any provider-specific pricing loader and register the provider in `src/root.zig` so CLI option parsing recognizes it.
4. Add fixtures under `fixtures/<provider>/` plus unit tests near the parser to lock in expected token deltas.
