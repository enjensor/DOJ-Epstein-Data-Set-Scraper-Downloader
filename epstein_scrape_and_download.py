#!/usr/bin/env python3
"""
DOJ Epstein Data Set scraper + downloader (pagination-aware, robust, downloads REAL PDF bytes).

Fix for "536-byte corrupted PDFs":
- Do NOT save page.goto() body for PDF URLs (Chromium may return PDF-viewer HTML wrapper).
- Instead download via Playwright APIRequestContext: context.request.get(pdf_url)
  which returns the real response bytes from the server.

Features:
- Scrapes Data Set index pages with pagination (?page=N) until no new links appear
- Deduplicates links
- Downloads PDFs to DataSet_XX/
- Handles DOJ age-verify (clicks Yes when encountered) and persists storage_state.json
- Resume-safe:
    - skips valid PDFs already downloaded
    - re-downloads invalid "PDFs" (too small or missing %PDF header)
- Atomic writes (.part -> final)
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
            page.locator(sel).first.click(timeout=8000)
            return True
        except Exception:
            continue
    return False


def atomic_write(dest: Path, data: bytes) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, dest)


def file_is_valid_pdf(path: Path, min_bytes: int = 1024) -> bool:
    """
    Treat tiny files or non-%PDF headers as invalid.
    This catches the 536-byte Chromium viewer HTML shells.
    """
    try:
        if not path.exists():
            return False
        if path.stat().st_size < min_bytes:
            return False
        with open(path, "rb") as f:
            return f.read(4) == b"%PDF"
    except Exception:
        return False


def safe_goto(
    page: Page,
    url: str,
    log,
    *,
    referer: Optional[str] = None,
    timeout_ms: int = 60000,
    retries: int = 6,
) -> Optional[Response]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms, referer=referer)
            return resp
        except (PlaywrightTimeoutError, Exception) as e:
            last_err = e
            log(f"WARNING: goto failed (attempt {attempt}/{retries}) url={url} err={e}")
            time.sleep(min(10.0, (1.3 ** attempt)) + random.random())
            try:
                page.wait_for_timeout(200)
            except Exception:
                pass
    if last_err:
        raise last_err
    return None


def ensure_age_verified_home(page: Page, context: BrowserContext, storage_state_path: Path, log) -> None:
    """
    Verify age via Epstein home page (good baseline).
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


def satisfy_age_verify_if_present(page: Page, context: BrowserContext, storage_state_path: Path, log) -> bool:
    """
    If currently on age-verify page, click Yes and persist storage state.
    """
    if not is_age_verify_url(page.url):
        return False
    log(f"Age-verify page detected: {page.url} — clicking 'Yes'.")
    clicked = try_click_yes(page)
    if not clicked:
        log("WARNING: Could not auto-click 'Yes' on age-verify. Run with --headed once.")
        return False
    try:
        page.wait_for_load_state("domcontentloaded", timeout=30000)
    except Exception:
        pass
    storage_state_path.parent.mkdir(parents=True, exist_ok=True)
    context.storage_state(path=str(storage_state_path))
    log(f"Saved storage state to: {storage_state_path}")
    return True


def extract_pdf_links_from_current_page(page: Page, dataset_n: int) -> List[Tuple[str, str]]:
    hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
    out: List[Tuple[str, str]] = []
    for h in hrefs:
        if not h:
            continue
        if h.lower().endswith(".pdf") and "/epstein/files/dataset%20" in h.lower():
            abs_url = h if h.startswith("http") else (ROOT + h)
            m = PDF_RE.search(abs_url)
            if not m:
                continue
            ds_from_url = int(m.group(1))
            if ds_from_url != dataset_n:
                continue
            num = m.group(2)
            out.append((f"EFTA{num}.pdf", abs_url))
    return out


def collect_pdf_links_for_dataset_paginated(
    page: Page,
    dataset_n: int,
    context: BrowserContext,
    storage_state_path: Path,
    log,
    max_pages: int = 5000,
    polite_sleep: float = 0.12,
) -> Dict[str, Tuple[str, str]]:
    """
    Scrape dataset listing pages: base, ?page=1, ?page=2... until a page yields no new links.
    Returns filename -> (pdf_url, referer_url)
    """
    base = DATASET_LISTING_BASE.format(n=dataset_n)
    collected: Dict[str, Tuple[str, str]] = {}

    for page_num in range(max_pages):
        idx_url = base if page_num == 0 else f"{base}?page={page_num}"
        safe_goto(page, idx_url, log)

        if is_age_verify_url(page.url):
            log(f"Dataset {dataset_n} index redirected to age-verify at page={page_num}; verifying then retrying.")
            satisfy_age_verify_if_present(page, context, storage_state_path, log)
            safe_goto(page, idx_url, log)

        links = extract_pdf_links_from_current_page(page, dataset_n)

        new_count = 0
        for fn, pdf_url in links:
            if fn not in collected:
                collected[fn] = (pdf_url, idx_url)
                new_count += 1

        log(f"Dataset {dataset_n}: index page={page_num} links={len(links)} new={new_count} total={len(collected)}")

        if page_num > 0 and new_count == 0:
            break

        time.sleep(polite_sleep + random.random() * polite_sleep)

    return collected


def download_pdf_via_request(
    context: BrowserContext,
    page: Page,
    storage_state_path: Path,
    pdf_url: str,
    referer_url: str,
    log,
) -> Tuple[bool, int, str, bytes]:
    """
    Download PDF bytes using context.request.get() (real server response, not Chromium PDF viewer HTML).
    If it returns age-verify HTML, we navigate to the URL, click Yes, then retry.
    Returns: (ok, status, content_type, body)
    """
    def do_get() -> Tuple[int, str, bytes]:
        r = context.request.get(
            pdf_url,
            headers={
                "Referer": referer_url,
                "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
            },
            timeout=60000,
        )
        ctype = r.headers.get("content-type", "")
        body = r.body()
        return (r.status, ctype, body)

    status, ctype, body = do_get()

    # If redirected/gated, satisfy age verify in the browser context and retry once.
    is_html = "text/html" in (ctype or "").lower()
    looks_pdf = body[:4] == b"%PDF" if body else False

    if status == 200 and is_html and not looks_pdf:
        log(f"PDF request returned HTML (likely age-verify). Attempting to satisfy age-verify then retry: {pdf_url}")
        safe_goto(page, pdf_url, log, referer=referer_url)
        satisfied = satisfy_age_verify_if_present(page, context, storage_state_path, log)
        if satisfied:
            status, ctype, body = do_get()

    ok = (status == 200) and (body[:4] == b"%PDF")
    return (ok, status, ctype, body)


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

        # Reduce flakiness by blocking heavy resources (does not affect API request downloads)
        def block_heavy(route):
            rt = route.request.resource_type
            if rt in ("image", "font", "media"):
                route.abort()
            else:
                route.continue_()

        page.route("**/*", block_heavy)

        ensure_age_verified_home(page, context, storage_state_path, log)

        total_links = 0
        downloaded = 0
        skipped_existing = 0
        redownloaded_invalid = 0
        failed = 0

        for ds in range(args.dataset_start, args.dataset_end + 1):
            ds_dir = out_dir / f"DataSet_{ds:02d}"
            ds_dir.mkdir(parents=True, exist_ok=True)

            mapping = collect_pdf_links_for_dataset_paginated(
                page=page,
                dataset_n=ds,
                context=context,
                storage_state_path=storage_state_path,
                log=log,
                max_pages=args.max_index_pages,
            )

            items = sorted(mapping.items(), key=lambda kv: int(kv[0][4:12]))
            total_links += len(items)
            log(f"Dataset {ds}: total collected across pages = {len(items)}")

            for filename, (pdf_url, referer_url) in items:
                dest = ds_dir / filename

                if file_is_valid_pdf(dest):
                    skipped_existing += 1
                    continue

                if dest.exists() and dest.stat().st_size > 0:
                    # Exists but invalid -> we will overwrite
                    redownloaded_invalid += 1

                ok, status, ctype, body = download_pdf_via_request(
                    context=context,
                    page=page,
                    storage_state_path=storage_state_path,
                    pdf_url=pdf_url,
                    referer_url=referer_url,
                    log=log,
                )

                if not ok:
                    failed += 1
                    log(f"{filename}: FAILED status={status} ctype={ctype} url={pdf_url}")
                    time.sleep(1.0 + random.random() * 1.5)
                    continue

                atomic_write(dest, body)
                downloaded += 1
                log(f"{filename}: downloaded (dataset {ds}) -> {dest}")

                time.sleep(max(0.0, args.sleep + random.random() * args.jitter))

        context.storage_state(path=str(storage_state_path))
        browser.close()

    log(
        "Done. "
        f"total_links={total_links} downloaded={downloaded} "
        f"skipped_existing={skipped_existing} redownloaded_invalid={redownloaded_invalid} failed={failed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
