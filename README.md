# Bulletin Scraper

```text
app.py                      Thin CLI entrypoint
bulletin_scraper/
  cli.py                    Interactive and scripted CLI parsing
  runner.py                 Family-level orchestration, selection, apply, artifacts
  graphs.py                 LangGraph strategy execution
  adapters.py               Target-specific prompts, validation, postprocessing, apply
  llm_client.py             OpenRouter LangChain client using LCEL
  logging_config.py         Structured console logging
  models.py                 Shared application models
  events.py                 Event merge and metadata helpers
  intentions.py             Intention merge and metadata helpers
  pdf_to_images.py          PDF rendering helpers
  scraping.py               Bulletin discovery and download helpers
  schemas.py                Pydantic schemas for LLM output and bulletin cache metadata
  sources.py                Family grouping, bulletin cache/index, artifact preparation
data/
  churches.json
  events.json
  intentions.json
tests/
  test_pipeline.py          Offline workflow, selection, CLI, and cache tests
```

Each run starts by grouping churches with the same `bulletin_website` into one family. For each family, the runner can evaluate multiple combinations of:

- targets: `schedule`, `events`, `intentions`, `combined`
- strategies: `direct`, `extract-merge`, `reviewed`
- input modes: `images`, `text`, `text-images`, `pdf`

The selection layer chooses one result per family and target. Selection is target-specific instead of using one fixed global ladder: schedule prefers confirmed changed fields, events prefer dated non-liturgical coverage with duplicate penalties, and intentions prefer stable `for` / `by` pairings over raw line inflation. `pdf` inputs are still available for manual inspection, but they are excluded from selection whenever any non-`pdf` candidate exists.

For the current recommended combinations and known weak spots from the latest live sweeps, see `CURRENT_BEST_APPROACHES.md`.

## Structured Output

The LLM boundary is intentionally narrow:

- `llm_client.py` builds an LCEL pipeline from prompt input to model invocation to JSON parsing.
- `adapters.py` validates and normalizes payloads with Pydantic schemas from `schemas.py`.
- schedule outputs are postprocessed to drop unchanged fields before scoring or apply.
- event outputs are deduplicated and filtered against their source bulletin recency window before apply.
- intentions are treated as bulletin-week data and replace the current family's stored intention slice on apply.

This means malformed or weakly structured model responses fail fast at the owning target adapter instead of leaking through the rest of the pipeline.

## Installation

Use `uv` for environment setup, dependency resolution, and command execution.

```bash
uv sync --all-groups
```

Set the OpenRouter key in `.env` or the process environment:

```bash
OPENROUTER_API_KEY=your_key_here
```

Default model:

```text
google/gemini-3.1-flash-lite-preview
```

Override it with `--model` when needed.

The application also supports env-backed defaults for common runtime options. Examples:

```bash
BULLETIN_SCRAPER_TARGETS=schedule,events
BULLETIN_SCRAPER_STRATEGIES=direct,reviewed
BULLETIN_SCRAPER_INPUT_MODES=images,text,text-images
BULLETIN_SCRAPER_WORKERS=4
BULLETIN_SCRAPER_LOG_LEVEL=INFO
BULLETIN_SCRAPER_BULLETIN_CACHE_PATH=bulletins/cache_index.json
BULLETIN_SCRAPER_ENABLE_PROMPT_CACHING=true
BULLETIN_SCRAPER_PROMPT_CACHE_TTL=5m
OPENROUTER_MODEL=google/gemini-3.1-flash-lite-preview
```

CLI flags take precedence over env defaults.

## CLI

The CLI is usable both interactively and non-interactively.

Interactive mode:

```bash
uv run python app.py --interactive
```

Interactive mode only asks for settings that were not already supplied by CLI flags or env-backed defaults.

Scripted mode:

```bash
uv run python app.py \
  --targets schedule,events,intentions \
  --strategies direct,extract-merge,reviewed \
  --input-modes images,text,text-images \
  --family-limit 1
```

Apply the selected results:

```bash
uv run python app.py \
  --targets schedule,events,intentions \
  --strategies direct,extract-merge,reviewed \
  --input-modes images,text,text-images \
  --family-limit 1 \
  --apply
```

`--apply` writes directly to the configured JSON files at `--churches-path`, `--events-path`, and `--intentions-path`. For safe manual verification, point those flags at copied files in a temp directory and inspect the diffs after the run.

Refresh bulletin discovery even if the cache already has a same-day entry:

```bash
uv run python app.py \
  --targets schedule \
  --strategies direct,reviewed \
  --input-modes text,text-images \
  --family-filter amherstburg \
  --refresh-bulletins
```

The CLI still accepts the legacy compatibility flags `--mode`, `--modify-json`, and `--no-images`, but the primary interface is `--targets`, `--strategies`, and `--input-modes`.

Useful operational flags:

- `--family-filter` narrows execution to a matching family id or name substring
- `--workers` controls family-level parallelism
- `--use-existing-bulletins` forces reuse of local PDFs
- `--no-use-existing-bulletins` disables an env default for forced reuse
- `--refresh-bulletins` bypasses same-day cache reuse for the current run
- `--no-refresh-bulletins` disables an env default for refresh behavior
- `--log-level` sets the console logging verbosity

Prompt caching is enabled by default for large text blocks sent through multi-step workflows. The relevant env settings are:

- `BULLETIN_SCRAPER_ENABLE_PROMPT_CACHING=true|false`
- `BULLETIN_SCRAPER_PROMPT_CACHE_TTL=5m|1h`

These cache hints are added only where they are useful, and unsupported providers can ignore them without breaking the workflow.

Target-specific apply behavior:

- schedule updates only overwrite confirmed changed fields for the affected churches
- events merge against stored events by id and by content similarity, can merge across families, and discard events that are stale relative to their source bulletin date
- intentions do not merge across weeks; the selected result replaces the current family's stored intentions for that bulletin window

## Bulletin Cache

Bulletin discovery and download state is recorded in `bulletins/cache_index.json` by default.

Each family entry stores:

- the family id and family name
- the bulletin website and primary parish website
- the bulletin PDF URL used for download
- the local PDF path
- the resolved bulletin date when it can be derived from the URL or filename
- status values such as `downloaded`, `cached`, `scrape_failed`, or `download_failed`
- timestamps for last attempt, scrape, download, and reuse
- the most recent error message, when one exists

Default behavior is conservative and efficient:

- same-day cached bulletin PDFs are reused automatically
- missing or failed families are retried
- `--use-existing-bulletins` forces reuse of any local PDF
- `--refresh-bulletins` bypasses same-day cache reuse for the current run

This keeps the workflow scriptable while preserving enough metadata to retry failures or manually inspect family-to-file mappings.

## Run Artifacts

Each run writes a timestamped artifact directory under `runs/`:

```text
runs/20260430T123456/
  manifest.json
  summary.md
  families/
    family-id/
      schedule-direct-images.json
      schedule-reviewed-text.json
      schedule-reviewed-text-images.json
      events-extract-merge-text-images.json
      intentions-direct-text.json
```

Important files:

- `manifest.json`: structured record of the run configuration, families, candidate results, and selected results
- `summary.md`: concise human-readable summary for issues, PR descriptions, or quick inspection
- `families/*/*.json`: per-case artifacts with parsed payloads, raw model outputs, scores, warnings, and apply details

## GitHub Actions

Two workflows are included:

- `.github/workflows/scraper.yml` runs analysis-only workflows, uploads the latest run artifacts, and can open an issue from `summary.md`
- `.github/workflows/scraper-auto-update.yml` runs with `--apply`, uploads artifacts, and can open a PR when data files changed

Both workflows require `OPENROUTER_API_KEY`.

Both workflows expose inputs for targets, strategies, input modes, model, family filters, worker count, log level, and bulletin cache controls so you can compare workflow combinations without editing YAML.

## Technical Details

The implementation is deliberately split across three boundaries:

- configuration: `config.py` provides env-backed defaults and validated runtime models
- orchestration: `runner.py`, `graphs.py`, and `sources.py` control family grouping, artifact preparation, strategy execution, selection, and apply
- target logic: `adapters.py` owns prompts, Pydantic payload validation, conservative postprocessing, and data-file mutation rules for each target

LangChain and LangGraph are used in a narrow, explicit way:

- `llm_client.py` builds an LCEL pipeline that transforms prompt plus input artifact into a `HumanMessage`, invokes the model, flattens the response, and parses JSON
- `graphs.py` uses `StateGraph` to encode `direct`, `extract-merge`, and `reviewed` flows as small deterministic state machines
- `schemas.py` enforces structured output with Pydantic after each model call so malformed payloads fail at the target boundary instead of during apply

The LLM client also memoizes artifact content blocks locally within a run and adds OpenRouter-compatible cache breakpoints to large repeated text payloads. That mainly benefits the multi-step `reviewed` and `extract-merge` flows, which resend the same bulletin input across stages.

The source layer keeps bulletin fetch state outside the LLM loop:

- `sources.py` writes `bulletins/cache_index.json` with per-family scrape/download status and timestamps
- same-day successful bulletins are reused automatically unless refresh is requested
- failed scrapes and failed downloads remain visible in the cache index for retry or manual follow-up

For contributors, the intended extension path is:

1. add or refine a schema in `schemas.py`
2. update or add a target adapter in `adapters.py`
3. plug it into the relevant strategy prompts or graph path
4. cover the behavior in `tests/test_pipeline.py`

## Testing

Run the offline test suite with:

```bash
uv run pytest tests/test_pipeline.py -q
```

The tests cover:

- family grouping by shared bulletin website
- reviewed schedule workflow execution and apply behavior
- extract-merge events workflow execution and merge behavior
- deterministic selection behavior
- validated CLI parsing
- same-day bulletin cache reuse

Live OpenRouter runs are still the right way to evaluate extraction quality across strategies and input modes.

The next level of accuracy work is empirical: run a small set of representative families, inspect the artifacts in `runs/`, and tune prompts and ranking heuristics from observed failures rather than from markdown summaries.