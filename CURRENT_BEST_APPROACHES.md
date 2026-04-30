# Current Best Approaches

This file is the quick reference for future sessions. It captures the leading strategies, modalities, and known caveats from the latest validated live sweeps.

Last updated: 2026-04-30

Primary evidence (.gitignored):

- `strategy-sweep-findings.md`
- `strategy-sweep-findings-round-2.md`
- `strategy-sweep-findings-round-3.md`

## Current Leaders

### Schedule

- Leading candidate: `reviewed-text`
- Keep available: `reviewed-text-images`
- Useful fallback: `reviewed-images`

Why:

- `reviewed-text` produced the most credible schedule deltas in the latest rerun, especially for East Windsor.
- `reviewed-text-images` is still worth keeping because it can help on image-heavy layouts, but it should not automatically outrank everything.
- `reviewed-images` remains useful when text extraction is weak.

Caveats:

- The review stage can still introduce a change that was not present in the proposal.
- Schedule changes should still be inspected before `--apply`, especially when the proposal was empty.

### Events

- Leading candidate: `extract-merge-text-images`
- Keep available: `reviewed-text-images`
- Cheap baseline: `direct-text`

Why:

- `extract-merge-text-images` currently has the best balance of completeness and precision after the event cleanup work.
- It benefits most from prompt caching because it resends the same bulletin content across multiple stages.
- `reviewed-text-images` is still useful as a comparison point when extract-merge collapses too aggressively.

Caveats:

- Raw extraction and review traces still surface liturgical notices that must be filtered out downstream.
- Event quality should be judged by community-event precision, not raw event count.

### Intentions

- Leading candidate set: `reviewed-text` and `reviewed-images`
- Keep available: `direct-images` only as a comparison case for tricky layouts
- De-emphasize: `text-images` for intention-heavy column layouts

Why:

- `reviewed-text` currently looks strongest on cleaner text bulletins such as East Windsor.
- `reviewed-images` remains important for layouts where OCR loses structure.
- `direct-images` can preserve more detail, but it also increases the risk of donor names being shifted onto adjacent lines.

Caveats:

- Amherstburg-Harrow is still an unresolved failure mode. Different modalities misread the donor column in different ways.
- Non-null `by` fields are not automatically trustworthy; some outputs attach them to the wrong intention line.

## Inputs To Exclude Or Downrank

- `pdf` should stay out of winner selection whenever any non-`pdf` candidate exists.
- Intention `text-images` should not be treated as a universal best mode.
- Event outputs that gain recall by including Mass notices, Holy Hours, or nursing-home liturgies are regressions, not improvements.

## Prompt Caching Guidance

- Keep prompt caching enabled by default.
- It is most relevant for `reviewed` and `extract-merge` flows, where the same bulletin content is sent multiple times.
- Current settings:
  - `BULLETIN_SCRAPER_ENABLE_PROMPT_CACHING=true`
  - `BULLETIN_SCRAPER_PROMPT_CACHE_TTL=5m`
- Unsupported models or providers should continue working because cache hints are optional.

## Best Next Improvements

1. Prevent schedule review from inventing changes when the proposal is empty.
2. Add a more layout-aware intention parser or reviewer for two-column donor/name bulletins like Amherstburg-Harrow.
3. Capture cache usage metadata in run artifacts so future sweeps can compare quality, latency, and cached-token behavior together.