# Bulletin Analysis Application

Analyzes church bulletins with LLM to identify discrepancies with churches.json database.

## Quick Start (GitHub Actions)

The easiest way to run the scraper is via GitHub Actions. Two workflows are available:

### 1. Analysis & Issue Reporting
🚀 **[Bulletin Scraper & Analysis](https://github.com/jere-mie/massfinder-we/actions/workflows/scraper.yml)**

Creates a GitHub issue with the bulletin analysis report.

1. Click the link above
2. Click **"Run workflow"** button
3. The action runs and creates an issue titled `Sync churches.json {YYYY-MM-DD}` with the analysis results
4. Review the issue to see differences between bulletins and database

### 2. Auto-Update with Pull Request
🚀 **[Bulletin Scraper Auto-Update](https://github.com/jere-mie/massfinder-we/actions/workflows/scraper-auto-update.yml)**

Automatically updates `churches.json` and creates a pull request for review.

1. Click the link above
2. Click **"Run workflow"** button
3. The action runs the analysis with `--modify-json` flag
4. If differences are found:
   - Updates `churches.json` automatically
   - Creates a PR with the analysis as the description
   - Tags with `bulletin-sync` and `automated` labels
5. Review and merge the PR to apply changes

**Prerequisites:**
- `OPENROUTER_API_KEY` must be configured as a [GitHub secret](https://github.com/jere-mie/massfinder-we/settings/secrets/actions)

## Local Setup (Manual)

To run the scraper locally:

```bash
# Install dependencies
pip install -r requirements.txt

# Create .env file with API key
echo "OPENROUTER_API_KEY=your_key_here" > .env

# Run analysis
python app.py
```

## Usage

The scraper supports three modes: **mass** (default) for analyzing Mass times, **events** for extracting parish events, and **intentions** for extracting Mass intentions listed in bulletins.

### Mass Analysis Mode

```bash
# Basic analysis (generates report only, uses image mode)
python app.py

# Analysis with automatic JSON updates
python app.py --modify-json

# Use PDF mode instead of images (faster but less accurate)
python app.py --no-images

# With options
python app.py --log-level DEBUG --workers 8 --output results.md --modify-json
```

### Events Extraction Mode

```bash
# Extract events from bulletins
python app.py --mode events

# Extract events and save to events.json
python app.py --mode events --modify-json

# Specify custom events.json path
python app.py --mode events --events-path ../public/events.json --modify-json
```

### Intentions Extraction Mode

```bash
# Extract Mass intentions from bulletins (generates a markdown report)
python app.py --mode intentions

# Extract intentions and save to `public/intentions.json`
python app.py --mode intentions --modify-json

# Specify custom intentions.json path
python app.py --mode intentions --intentions-path ../public/intentions.json --modify-json
```

**Options:**
- `--mode` - `mass` (default), `events`, or `intentions`
- `--log-level` - DEBUG, INFO (default), WARNING, ERROR
- `--workers` - Parallel workers (default: 10)
- `--output` - Output file (default: bulletins_analysis.md for mass, events_analysis.md for events)
- `--churches-path` - Path to churches.json (default: ../public/churches.json)
- `--events-path` - Path to events.json (default: ../public/events.json)
- `--modify-json` - Apply LLM suggestions to automatically update churches.json or events.json
- `--no-images` - Disable image-based analysis and use PDF mode instead (less accurate but faster)
- `--model` - Override default LLM model

## Image-Based Analysis (Recommended)

By default, the scraper uses **image-based analysis** for better accuracy:

### Why Images Are Better

- **More accurate** - Vision models excel at reading tables and formatted schedules
- **Handles complex layouts** - Preserves visual context (bold text, columns, boxes)
- **Works with scanned PDFs** - Many bulletins are image-only PDFs where text extraction fails
- **Better format detection** - Understands visual cues that help identify Mass times vs. other content

### How It Works

1. Each PDF is converted to PNG images (one per page) using **PyMuPDF**
2. Images are sent to the LLM with `detail: high` for best quality
3. Falls back to PDF mode if image conversion fails
4. Limited to first 6 pages for Mass analysis, 8 pages for events

### Disabling Image Mode

If you need faster processing and are willing to sacrifice some accuracy:

```bash
python app.py --no-images
```

This uses the legacy PDF-based analysis which is faster but less reliable.

## GitHub Action Workflows

### Bulletin Scraper & Analysis

**Workflow:** `.github/workflows/scraper.yml`

Analyzes bulletins and creates a GitHub issue with findings:
1. Runs the scraper (`python app.py`)
2. Generates analysis report (`bulletins_analysis.md`)
3. Creates a GitHub issue with:
   - Title: `Sync churches.json {YYYY-MM-DD}`
   - Body: Full analysis markdown
   - Labels: `bulletin-sync`, `automated`

**When to use:**
- Review-only reports without automatic changes
- Archive analysis results as GitHub issues
- Discussion and collaborative review

### Bulletin Scraper Auto-Update

**Workflow:** `.github/workflows/scraper-auto-update.yml`

Analyzes bulletins, updates churches.json, and creates a pull request:
1. Runs the scraper with `--modify-json` flag
2. LLM automatically updates `churches.json` based on analysis
3. Checks if changes were made
4. If changes exist:
   - Creates a PR with title: `chore: Sync churches.json from bulletin analysis`
   - Uses analysis report as PR description
   - Assigns labels: `bulletin-sync`, `automated`
   - Auto-deletes branch after merge
5. If no changes: Skips PR creation

**When to use:**
- Automated, hands-off updates for trusted sources
- Batch process multiple church updates
- Minimize manual data entry

**Review checklist before merging:**
- Verify times are in correct 24-hour format
- Check that only documented differences are changed
- Ensure no data was invented or assumed
- Validate church details match source bulletins

## Automatic JSON Updates

The `--modify-json` flag enables intelligent updating of `churches.json` based on bulletin analysis:

### How It Works

1. **Analysis Phase**
   - Scraper compares each bulletin against current database
   - Generates markdown report with all differences found

2. **Update Phase** (when `--modify-json` is used)
   - Sends both `churches.json` and analysis report to LLM
   - LLM reviews documented differences
   - Updates church data accordingly

3. **Safety Measures**
   - LLM only modifies fields with confirmed differences
   - Does not invent or assume data not in the report
   - Returns complete updated JSON with all fields preserved
   - No changes applied if analysis is uncertain

### Usage Examples

```bash
# Analysis only (generates report, no modifications)
python app.py

# Analysis + automatic updates
python app.py --modify-json

# Combined with other options
python app.py --log-level DEBUG --workers 8 --modify-json
```

### LLM Update Instructions

The LLM is given specific rules:
- Only modify fields with CONFIRMED differences in the analysis report
- Use exact times from bulletins (24-hour HHMM format)
- Maintain all existing fields and structure
- Do NOT invent or assume data
- Return valid JSON only

## How It Works

1. **Load churches.json** - Master database with `bulletin_website` field
2. **Scrape bulletin links** - Extracts PDFs from websites (cached, Cloudflare-safe)
3. **Download PDFs** - Saves to `bulletins/` directory
4. **Convert to Images** - Transforms PDF pages to high-quality PNG images (PyMuPDF)
5. **Analyze with LLM** - Google Gemini 2.5 Flash Lite analyzes images vs. database (parallel)
6. **Generate report** - Markdown output with differences grouped by bulletin

## Output

Creates `bulletins_analysis.md` with:
- Summary of differences found
- Results grouped by bulletin website
- Side-by-side comparison tables (Bulletin | Database)
- Page references and notes

Example:
```markdown
## [Bulletin](https://example.com/bulletin.pdf)

### St. John
| Field | Bulletin | Database | Page |
|---|---|---|---|
| Tuesday Mass | 1800 | 1700 | 1 |
```

## Data Format

churches.json required fields:
```json
{
  "name": "Church Name",
  "bulletin_website": "https://...",
  "masses": [{"day": "Sunday", "time": "0900"}],
  "confession": [{"day": "Saturday", "start": "0945", "end": "1030"}],
  "adoration": [{"day": "Wednesday", "start": "0930", "end": "2130"}]
}
```

Times: 24-hour format `"HHMM"` (e.g., `"1830"` = 6:30 PM)

## Configuration

**Environment Variable:**
```
OPENROUTER_API_KEY=your_api_key_here
```

**LLM Models** (in `utils/llm.py`):
```python
PREFERRED_MODEL = 'google/gemini-3.1-flash-lite-preview'
FALLBACK_MODEL = 'google/gemini-2.5-flash-lite'
```

## Features

- **Image-Based Analysis** - Converts PDFs to images for superior accuracy with vision models
- **Parallel LLM Analysis** - ThreadPoolExecutor with configurable workers
- **Intelligent Caching** - Avoids re-scraping bulletin websites
- **Cloudflare Bypass** - Automatic bot detection handling
- **Retry Logic** - Exponential backoff (1, 2, 4, 8, 16 sec, max 10 attempts)
- **Colored Logging** - Emoji indicators (🔍 DEBUG, ✓ INFO, ⚠ WARNING, ✗ ERROR)
- **Dual Modes** - Mass time analysis and event extraction
- **Auto-Update** - LLM-powered automatic JSON updates with `--modify-json`

## Performance

Typical runtime for 34+ churches (with image mode):
- Scraping: ~14 seconds (heavy caching)
- Downloading: ~10 seconds
- PDF to Images: ~8 seconds (parallel conversion)
- LLM Analysis: ~45 seconds (10 workers, image mode)
- Report Generation: <1 second
- **Total: ~1.5 minutes**

With `--no-images` (PDF mode):
- LLM Analysis: ~38 seconds (10 workers)
- **Total: ~1 minute**

Analysis is highly parallelizable - more workers significantly reduce LLM analysis time. Image mode takes slightly longer but provides significantly better accuracy.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| 403 Forbidden | Automatic with cloudscraper, check internet |
| PDF link not found | Verify `bulletin_website` URL manually |
| LLM errors | Check API key in `.env` and quota at openrouter.ai |
| Image conversion fails | Install PyMuPDF: `pip install PyMuPDF`, or use `--no-images` |
| Poor accuracy | Use default image mode (remove `--no-images` flag) |
| Slow | Increase `--workers` value or use `--no-images` for faster (but less accurate) processing |
| Debug | Use `--log-level DEBUG` |

## Architecture

```
app.py                    - Main orchestrator
├── utils/scraping.py         - Scraping, downloading, caching
├── utils/llm.py              - LLM API interaction (image + PDF modes)
├── utils/pdf_to_images.py    - PDF to image conversion (PyMuPDF)
├── utils/events.py           - Event extraction and management
└── utils/logging_config.py   - Colored logging
```

## Dependencies

- `requests` - HTTP requests
- `beautifulsoup4` - HTML parsing
- `cloudscraper` - Cloudflare bypass
- `python-dotenv` - Environment variables
- `PyMuPDF` - PDF to image conversion (fast, no external deps)
- `Pillow` - Image processing

See `requirements.txt` for versions.

## API

Uses [OpenRouter.ai](https://openrouter.ai) - free account available.

**Cost:** ~$0.01-0.02 USD per run (34 churches)
