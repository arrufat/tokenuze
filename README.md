# Tokenuze

Tokenuze is a CLI tool that summarizes token and cost usage from various LLM providers.
It scans session logs, aggregates token counts, and reports pricing.
The output is a table or JSON, suitable for dashboards.

## Requirements
- Zig 0.16.0-dev.2535+b5bd49460 or newer (if building from source)
- `sqlite3` in your `PATH` (for Zed, Crush, and Opencode providers)
- Network access to fetch remote pricing / uploading stats (optional)

## Supported Providers
- [Amp](https://ampcode.com/)
- [Claude Code](https://www.claude.com/product/claude-code)
- [Codex CLI](https://developers.openai.com/codex/cli/)
- [Crush](https://github.com/charmbracelet/crush): project-based (supports recursive crawling with `-r`)
- [Gemini CLI](https://geminicli.com/)
- [Opencode](https://opencode.ai/)
- [Zed](https://zed.dev/)

## Installation

- **Prebuilt binaries:** grab the latest release from the [Releases page](https://github.com/bfactory-ai/tokenuze/releases) and put `tokenuze` on your `PATH`.
- **Build from source:** requires Zig 0.16.0-dev.2535+b5bd49460 or newer

```bash
git clone https://github.com/bfactory-ai/tokenuze.git
cd tokenuze
zig build -Doptimize=ReleaseFast  # binary will be in zig-out/bin/
```

## Quick Start

By default it will display daily usage across all providers since the beginning of time:
```bash
tokenuze
```

### Options

You can restrict what `tokenuze` displays by filtering by date
```bash
tokenuze --since 20250101 --until 20250107
```
Or requesting specific providers
```bash
tokenuze --agent claude --agent codex
```
And combine everything
```
tokenuze --agent claude --agent codex --since 20250101 --until 20250107
```

Use the `-r` or `--recursive` flag to enable recursive searching (supported by providers like Crush):
```bash
tokenuze --agent crush --recursive
```

By appending the `--sessions` flag you can display session usage instead of daily.

Using the `--json` flag will display the result as a JSON file, which can be pretty printed with `--pretty`.

If you are tracking the stats in a separate server you can append the `--upload` flag to send them there.

Finally, you can see all this information typing `tokenuze --help`.

### JSON Output Sample

<details>

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

Missing pricing entries are listed under `missingPricing`.

</details>


## Extending
To add a new provider:
1. Create a new file in `src/providers/` and use the `provider.makeProvider` factory.
2. Implement a parser for the provider's log format that emits `model.TokenUsageEvent` objects.
3. Use helpers in `src/providers/provider.zig` for common tasks like JSON parsing and timestamp handling.
4. Register the provider in `src/root.zig` and expose a `loadPricingData` function for fallback pricing.
5. Add test fixtures in a new `fixtures/<provider>` directory and write unit tests in your provider's file.
