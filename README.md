# Tokenuze

Tokenuze is a Zig CLI that summarizes OpenAI Codex, Gemini, and Claude session usage. It scans `~/.codex/sessions`, `~/.gemini/tmp`, and `~/.claude/projects`, aggregates token counts per day and per model, and reports pricing using either the live LiteLLM pricing manifest or local fallbacks. Output is emitted as compact JSON, making it easy to feed into dashboards or further scripts.

## Requirements
- Zig 0.12.0 or newer (stdlib is expected at `/usr/lib/zig/lib`)
- Access to Codex session logs at `~/.codex/sessions`
- Optional: access to Gemini session logs at `~/.gemini/tmp`
- Optional: access to Claude session logs at `~/.claude/projects`
- Optional: network access to fetch remote pricing

## Quick Start
```bash
zig build                     # compile the debug binary
zig build run -- --since 20250101
zig build run -- --since 20250101 --until 20250107
zig build --release=fast run -- --since 20250101  # faster benchmarking runs
```

## Command-Line Options
- `--since YYYYMMDD` limits processing to events on/after the specified local date.
- `--until YYYYMMDD` caps the range; must be >= `--since` when both are present.
- `--pretty` enables indented JSON output (handy when reading the payload manually).
- `--model <codex|gemini|claude>` restricts processing to the specified provider; repeat the flag to include multiple (defaults to all providers).
- `--machine-id` prints the cached/generated machine identifier and exits (no summaries).

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
1. Copy the `codex.zig` pattern into `src/providers/<vendor>.zig`.
2. Implement a `collect` function that appends `Model.TokenUsageEvent` records.
3. Expose any provider-specific pricing loader and register the provider in `src/root.zig`.
