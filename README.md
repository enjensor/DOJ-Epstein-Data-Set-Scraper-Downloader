# DOJ Epstein Data Set Scraper & Downloader

A resilient, pagination-aware Playwright-based downloader for the U.S.
Department of Justice Epstein disclosure PDFs.

This tool:

-   Scrapes all paginated dataset index pages
-   Collects every listed PDF link
-   Downloads files using real browser navigation
-   Handles DOJ age-verification flow
-   Persists browser session state
-   Skips already-downloaded files
-   Resumes safely across runs

------------------------------------------------------------------------

## Why This Approach

The DOJ disclosure site:

-   Paginates index listings (≈50 PDFs per page)
-   Redirects direct PDF access through an age-verification layer
-   May return HTML or HTTP 401 responses to programmatic access

Rather than sequentially probing millions of URLs, this project:

1.  Scrapes the official dataset index pages  
2.  Follows pagination (`?page=1`, `?page=2`, etc.)  
3.  Downloads PDFs using browser navigation with correct session state
    and referer

This is:

-   Faster
-   More stable
-   Less likely to trigger access controls
-   More respectful of the host site

------------------------------------------------------------------------

## Requirements

-   Python 3.9+
-   macOS, Linux, or Windows
-   Playwright

------------------------------------------------------------------------

## Installation

``` bash
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
python -m playwright install chromium
```

`requirements.txt`:

``` txt
playwright>=1.58.0
```

------------------------------------------------------------------------

## First Run (Recommended: Headed Mode)

On first execution, run in headed mode to ensure the DOJ
age-verification page is properly cleared.

``` bash
python epstein_scrape_and_download.py   --out "./_doj_epstein_pdfs"   --dataset-start 1   --dataset-end 12   --headed   --use-chrome-channel
```

If prompted, click **“Yes”** to confirm age verification.

This creates:

    storage_state.json
    download.log
    DataSet_01/
    DataSet_02/
    ...
    DataSet_12/

The `storage_state.json` file stores verified browser session state and
prevents repeated age prompts.

------------------------------------------------------------------------

## Subsequent Runs (Headless Mode)

Once `storage_state.json` exists, you may run headless:

``` bash
python epstein_scrape_and_download.py   --out "./_doj_epstein_pdfs"   --dataset-start 1   --dataset-end 12   --headless
```

Headless mode is suitable for:

-   Long-running jobs
-   Background execution
-   Server environments

------------------------------------------------------------------------

## Resume Behaviour

The downloader is safe to interrupt and restart.

It:

-   Skips files that already exist and have non-zero size
-   Writes files atomically (`.part` → rename)
-   Reuses saved browser session state
-   Continues where it left off

You may re-run the script repeatedly until completion.

------------------------------------------------------------------------

## Pagination Handling

Each dataset index page is scraped iteratively:

    data-set-1-files
    data-set-1-files?page=1
    data-set-1-files?page=2
    ...

The scraper:

-   Collects PDF links from each page
-   Deduplicates across pages
-   Stops automatically when no new links are found

This avoids the 50-file limitation per page.

------------------------------------------------------------------------

## Output Structure

    doj_epstein_pdfs/
    ├── DataSet_01/
    │   ├── EFTA00000001.pdf
    │   ├── EFTA00000002.pdf
    │   └── ...
    ├── DataSet_02/
    ├── ...
    ├── storage_state.json
    └── download.log

Each dataset is stored in its own directory.

------------------------------------------------------------------------

## Troubleshooting

### HTTP 401 Errors

If you see repeated 401 responses:

1.  Run once with `--headed`
2.  Add `--use-chrome-channel`
3.  Ensure `storage_state.json` is preserved

Example:

``` bash
python epstein_scrape_and_download.py   --out "./_doj_epstein_pdfs"   --headed   --use-chrome-channel
```

------------------------------------------------------------------------

### HTML Instead of PDF

If a file downloads as HTML:

-   Re-run once in headed mode
-   Confirm age verification
-   Do not delete `storage_state.json`

------------------------------------------------------------------------

## Expected Runtime

Approximate performance:

-   \~2–10 PDFs per second depending on network
-   \~2,700 total files across datasets
-   Typical runtime: 1–4 hours

You may re-run safely until all files are downloaded.

------------------------------------------------------------------------

## Responsible Use

This tool accesses publicly available DOJ disclosure pages.

It:

-   Does not bypass authentication
-   Does not modify content
-   Does not attempt to circumvent protections
-   Uses randomized delays to reduce load

Please use responsibly.

------------------------------------------------------------------------

## Optional Extensions

Possible enhancements:

-   SHA256 integrity manifest generation
-   CSV index export
-   Parallel browser contexts
-   Automated verification of existing files

------------------------------------------------------------------------

## Disclaimer

Users are responsible for compliance with applicable laws and site terms
of use.

------------------------------------------------------------------------

## License

GNU General Public License v3.0 (GPL-3.0)