#!/usr/bin/env python3
"""
DOJ Epstein Data Set scraper + downloader (pagination-aware, resilient navigation).

Key capabilities:
- Scrapes Data Set 1..12 index pages and all pagination (?page=1, ?page=2, ...)
- Collects all listed PDF links, deduplicated
- Downloads PDFs using browser navigation with Referer (more stable than direct fetch)
- Handles DOJ age verification flow and persists session state (storage_state.json)
- Resume-safe: skips already-downloaded files, atomic writes via .part files
- Robust navigation: retries and uses wait_until="domcontentloaded" to avoid net::ERR_ABORTED issues.

Usage example (recommended first run headed):
  python epstein_scrape_and_download.py --out "./doj_epstein_pdfs" --headed --use-chrome-channel

Then headless:
  python epstein_scrape_and_download.py --out "./doj_epstein_pdfs" --headless
"""

from __future__ import annotations

import argparse
import os
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import (
    sync_playwright,
    Browser,
    BrowserContext,
    Page,
    Response,
    TimeoutError as PlaywrightTimeoutError,
)

ROOT = "https://www.justice.gov"
EPSTEIN_HOME = f"{ROOT}/epstein"
DATASET_LISTING_BASE = f"{ROOT}/epstein/doj-disclosures/data-set-{{n}}-files"

# Match: https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf
PDF_RE = re.compile(r"/epstein/files/DataSet%20(\d+)/EFTA(\d{8})\.pdf$", re.IGNORECASE)


def log_factory(log_path: Path):
    def log(msg: str) -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    return log


def is_age_verify_url(url: str) -> bool:
    u = (url or "").lower()
    return "/age-verify" in u or "age-verify" in u


def try_click_yes(page: Page) -> bool:
    selectors = [
        'text="Yes"',
        'role=button[name="Yes"]',
        'button:has-text("Yes")',
        'a:has-text("Yes")',
        'input[type="submit"][value="Yes"]',
    ]
    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=5000)
            return True
        except Exception:
            continue
    return False


def looks_like_pdf(content_type: str, body_prefix: bytes) -> bool:
    ct = (content_type or "").lower()
    if "pdf" in ct or "octet-stream" in ct:
        return True
    return body_prefix.startswith(b"%PDF")


def atomic_write(dest: Path, data: bytes) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, dest)


def safe_goto(
    page: Page,
    url: str,
    log,
    *,
    referer: Optional[str] = None,
    timeout_ms: int = 60000,
    retries: int = 6,
) -> None:
    """
    Robust navigation helper.
    - Uses domcontentloaded instead of networkidle (avoids net::ERR_ABORTED on noisy pages)
    - Retries transient failures with backoff/jitter
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms, referer=referer)
            return
        except (PlaywrightTimeoutError, Exception) as e:
            last_err = e
            log(f"WARNING: goto failed (attempt {attempt}/{retries}) url={url} err={e}")
            # Exponential-ish backoff with jitter (caps to avoid excessive delay)
            time.sleep(min(10.0, (1.3**attempt)) + random.random())
            try:
                page.wait_for_timeout(200)
            except Exception:
                pass
    raise last_err if last_err else RuntimeError(f"safe_goto failed for {url}")


def ensure_age_verified(page: Page, context: BrowserContext, storage_state_path: Path, log) -> None:
    """
    Visit Epstein home page and pass age verification if encountered.
    Persist storage state after the check so subsequent runs reuse it.
    """
    safe_goto(page, EPSTEIN_HOME, log)

    if is_age_verify_url(page.url):
        log(f"Age verification encountered at {page.url}. Attempting to click 'Yes'.")
        clicked = try_click_yes(page)
        if not clicked:
            log("WARNING: Could not auto-click 'Yes'. Run with --headed once and click it manually.")
        try:
            page.wait_for_load_state("domcontentloaded", timeout=30000)
        except Exception:
            pass

    storage_state_path.parent.mkdir(parents=True, exist_ok=True)
    context.storage_state(path=str(storage_state_path))
    log(f"Saved storage state to: {storage_state_path}")


def extract_pdf_links_from_current_page(page: Page, dataset_n: int) -> List[Tuple[str, str]]:
    """
    Extract all matching PDF links from the current page.
    Returns list of (filename, absolute_url).
    """
    hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
    out: List[Tuple[str, str]] = []

    for h in hrefs:
        if not h:
            continue
        # Restrict to Epstein PDF links
        if h.lower().endswith(".pdf") and "/epstein/files/dataset%20" in h.lower():
            abs_url = h if h.startswith("http") else (ROOT + h)
            m = PDF_RE.search(abs_url)
            if not m:
                continue
            ds_from_url = int(m.group(1))
            if ds_from_url != dataset_n:
                continue
            num = m.group(2)
            filename = f"EFTA{num}.pdf"
            out.append((filename, abs_url))

    return out


def collect_pdf_links_for_dataset_paginated(
    page: Page,
    dataset_n: int,
    log,
    max_pages: int = 5000,
    polite_sleep: float = 0.12,
) -> Dict[str, Tuple[str, str]]:
    """
    Scrape dataset listing page and its pagination:
      base, ?page=1, ?page=2, ... until no NEW links found on a page (after page 0).

    Returns:
      filename -> (pdf_url, referer_url_where_found)
    """
    base = DATASET_LISTING_BASE.format(n=dataset_n)
    collected: Dict[str, Tuple[str, str]] = {}

    for page_num in range(max_pages):
        if page_num == 0:
            idx_url = base
        else:
            idx_url = f"{base}?page={page_num}"

        safe_goto(page, idx_url, log)

        # If gated, verify and reload the index page
        if is_age_verify_url(page.url):
            log(f"Dataset {dataset_n} index redirected to age-verify at page={page_num}; verifying then retrying.")
            try_click_yes(page)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=30000)
            except Exception:
                pass
            safe_goto(page, idx_url, log)

        links = extract_pdf_links_from_current_page(page, dataset_n)

        new_count = 0
        for fn, pdf_url in links:
            if fn not in collected:
                collected[fn] = (pdf_url, idx_url)  # keep referer as the index page URL
                new_count += 1

        log(f"Dataset {dataset_n}: index page={page_num} links={len(links)} new={new_count} total={len(collected)}")

        # Stop when a paginated page yields no new links (after first page)
        if page_num > 0 and new_count == 0:
            break

        time.sleep(polite_sleep + random.random() * polite_sleep)

    return collected


def nav_get_bytes(page: Page, url: str, referer: str, log) -> Tuple[int, str, str, bytes]:
    """
    Navigate to URL with Referer and return:
      (status, final_url, content_type, body_bytes)

    Uses domcontentloaded + retry wrapper (safe_goto) to avoid ERR_ABORTED.
    """
    # We want the Response object for headers/body; safe_goto doesn't return it.
    # We'll do a direct goto here with retries similar to safe_goto and capture Response.
    last_err: Optional[Exception] = None
    for attempt in range(1, 6 + 1):
        try:
            resp: Optional[Response] = page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=60000,
                referer=referer,
            )
            final_url = page.url
            if resp is None:
                return (0, final_url, "", b"")
            status = resp.status
            ctype = resp.headers.get("content-type", "")
            body = b""
            if status < 400 and status != 204:
                try:
                    body = resp.body()
                except Exception:
                    body = b""
            return (status, final_url, ctype, body)
        except (PlaywrightTimeoutError, Exception) as e:
            last_err = e
            log(f"WARNING: PDF goto failed (attempt {attempt}/6) url={url} err={e}")
            time.sleep(min(10.0, (1.3**attempt)) + random.random())
            try:
                page.wait_for_timeout(200)
            except Exception:
                pass
    raise last_err if last_err else RuntimeError(f"nav_get_bytes failed for {url}")


def already_downloaded(out_dir: Path, dataset_n: int, filename: str) -> bool:
    """
    Skip rule:
    - only skip if the destination file exists and is non-empty
    """
    dest = out_dir / f"DataSet_{dataset_n:02d}" / filename
    return dest.exists() and dest.stat().st_size > 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--dataset-start", type=int, default=1)
    ap.add_argument("--dataset-end", type=int, default=12)
    ap.add_argument("--sleep", type=float, default=0.6, help="Base sleep between downloads")
    ap.add_argument("--jitter", type=float, default=0.4, help="Random jitter added to sleep")
    ap.add_argument("--headless", action="store_true", help="Run headless")
    ap.add_argument("--headed", action="store_true", help="Run headed (overrides --headless)")
    ap.add_argument("--use-chrome-channel", action="store_true", help='Use Playwright channel="chrome" if available')
    ap.add_argument("--max-index-pages", type=int, default=5000, help="Safety cap on pagination pages per dataset")
    args = ap.parse_args()

    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    log_path = out_dir / "download.log"
    log = log_factory(log_path)
    storage_state_path = out_dir / "storage_state.json"

    headless = args.headless and not args.headed

    log(f"Starting scrape+download: out={out_dir} datasets={args.dataset_start}..{args.dataset_end} headless={headless}")

    with sync_playwright() as p:
        launch_kwargs = {"headless": headless}
        if args.use_chrome_channel:
            launch_kwargs["channel"] = "chrome"

        browser: Browser = p.chromium.launch(**launch_kwargs)

        context_kwargs = {}
        if storage_state_path.exists():
            context_kwargs["storage_state"] = str(storage_state_path)

        context: BrowserContext = browser.new_context(
            **context_kwargs,
            locale="en-AU",
            timezone_id="Australia/Sydney",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )

        page: Page = context.new_page()

        # Reduce flakiness by blocking heavy resources
        def block_heavy(route):
            rt = route.request.resource_type
            if rt in ("image", "font", "media"):
                route.abort()
            else:
                route.continue_()

        page.route("**/*", block_heavy)

        ensure_age_verified(page, context, storage_state_path, log)

        total_links = 0
        downloaded = 0
        blocked_or_nonpdf = 0

        for ds in range(args.dataset_start, args.dataset_end + 1):
            ds_dir = out_dir / f"DataSet_{ds:02d}"
            ds_dir.mkdir(parents=True, exist_ok=True)

            mapping = collect_pdf_links_for_dataset_paginated(
                page=page,
                dataset_n=ds,
                log=log,
                max_pages=args.max_index_pages,
            )

            # Sort by EFTA numeric id for readable progress
            items = sorted(mapping.items(), key=lambda kv: int(kv[0][4:12]))
            total_links += len(items)

            log(f"Dataset {ds}: total collected across pages = {len(items)}")

            for filename, (pdf_url, referer_url) in items:
                dest = ds_dir / filename
                if dest.exists() and dest.stat().st_size > 0:
                    continue

                status, final_url, ctype, body = nav_get_bytes(page, pdf_url, referer=referer_url, log=log)

                # Handle age-verify bounce (HTML), then retry once
                if is_age_verify_url(final_url) or (
                    status == 200
                    and "text/html" in (ctype or "").lower()
                    and not looks_like_pdf(ctype, body[:8] if body else b"")
                ):
                    log(f"{filename}: got HTML/age-verify (status={status}, ctype={ctype}); re-verifying then retrying once.")
                    ensure_age_verified(page, context, storage_state_path, log)
                    status, final_url, ctype, body = nav_get_bytes(page, pdf_url, referer=referer_url, log=log)

                if status == 401:
                    blocked_or_nonpdf += 1
                    log(f"{filename}: 401 at {pdf_url}. Suggest run --headed and/or --use-chrome-channel.")
                    time.sleep(2.0 + random.random() * 2.0)
                    continue

                if status >= 400 or status == 0:
                    log(f"{filename}: error status={status} final_url={final_url}; skipping.")
                    time.sleep(1.0 + random.random() * 1.5)
                    continue

                if not looks_like_pdf(ctype, body[:8] if body else b""):
                    blocked_or_nonpdf += 1
                    log(f"{filename}: status=200 but not PDF (ctype={ctype}, final_url={final_url}); skipping.")
                    time.sleep(1.0 + random.random() * 1.5)
                    continue

                atomic_write(dest, body)
                downloaded += 1
                log(f"{filename}: downloaded (dataset {ds}) -> {dest}")

                time.sleep(max(0.0, args.sleep + random.random() * args.jitter))

        context.storage_state(path=str(storage_state_path))
        browser.close()

    log(f"Done. total_links={total_links} downloaded={downloaded} blocked_or_nonpdf={blocked_or_nonpdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
