#!/usr/bin/env python3
"""
DOJ Epstein Data Set scraper + downloader (pagination-aware, hardened against age-verify re-prompts).

Enhancements over earlier versions:
- Pagination across dataset index pages (?page=N)
- Robust navigation with retries (domcontentloaded, not networkidle)
- If a PDF request is redirected to /age-verify?destination=..., the script clicks "Yes" on THAT page
  and retries the same PDF URL immediately (most reliable pattern).
- Resource blocking to reduce flakiness.
- Resume-safe: skips existing non-empty files; atomic .part writes.
- Persists browser storage state to storage_state.json.
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
DATASET_LISTING_BASE = f"{ROOT}/epstein/_doj-disclosures/data-set-{{n}}-files"

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
) -> Optional[Response]:
    """
    Robust navigation helper that returns the Response (if available).
    Uses domcontentloaded (less brittle than networkidle).
    Retries transient failures (ERR_ABORTED, timeouts, etc).
    """
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
    Verify age via the Epstein home page (useful up-front).
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
    If the current page is an age-verify page, click Yes and save storage state.
    Returns True if it performed a click, else False.
    """
    if not is_age_verify_url(page.url):
        return False

    log(f"Age-verify page detected during PDF fetch: {page.url} — clicking 'Yes' and continuing.")
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


def fetch_pdf_with_age_verify_handling(
    page: Page,
    context: BrowserContext,
    storage_state_path: Path,
    pdf_url: str,
    referer_url: str,
    log,
) -> Tuple[int, str, str, bytes]:
    """
    Fetch a PDF by navigation. If redirected to age-verify, click Yes on that page and retry once.
    Returns (status, final_url, content_type, body).
    """
    resp = safe_goto(page, pdf_url, log, referer=referer_url)
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

    # If we landed on age-verify (HTML), satisfy it right there and retry the same PDF once.
    if is_age_verify_url(final_url) or (status == 200 and "text/html" in (ctype or "").lower() and not looks_like_pdf(ctype, body[:8])):
        satisfied = satisfy_age_verify_if_present(page, context, storage_state_path, log)
        if satisfied:
            resp2 = safe_goto(page, pdf_url, log, referer=referer_url)
            final_url = page.url
            if resp2 is None:
                return (0, final_url, "", b"")
            status = resp2.status
            ctype = resp2.headers.get("content-type", "")
            body = b""
            if status < 400 and status != 204:
                try:
                    body = resp2.body()
                except Exception:
                    body = b""

    return (status, final_url, ctype, body)


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

        # Verify once up-front
        ensure_age_verified_home(page, context, storage_state_path, log)

        total_links = 0
        downloaded = 0
        skipped_existing = 0
        blocked_or_nonpdf = 0

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
                if dest.exists() and dest.stat().st_size > 0:
                    skipped_existing += 1
                    continue

                status, final_url, ctype, body = fetch_pdf_with_age_verify_handling(
                    page=page,
                    context=context,
                    storage_state_path=storage_state_path,
                    pdf_url=pdf_url,
                    referer_url=referer_url,
                    log=log,
                )

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

    log(f"Done. total_links={total_links} downloaded={downloaded} skipped_existing={skipped_existing} blocked_or_nonpdf={blocked_or_nonpdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
