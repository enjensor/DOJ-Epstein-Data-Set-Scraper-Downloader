#!/usr/bin/env python3
"""
Download DOJ Epstein PDFs by scraping the official Data Set listing pages,
including pagination via ?page=1, ?page=2, ...

Features:
- Scrapes Data Set 1..12 listing pages AND their paginated pages
- Collects all PDF links (deduplicated) per dataset
- Downloads PDFs using browser navigation with Referer
- Resume-safe: skips already-downloaded files
- Persists browser storage state to storage_state.json (cookies/localStorage)
"""

from __future__ import annotations

import argparse
import os
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Response

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
            page.locator(sel).first.click(timeout=5000)
            return True
        except Exception:
            continue
    return False


def ensure_age_verified(page: Page, context: BrowserContext, storage_state_path: Path, log) -> None:
    """
    Visit Epstein home and, if we encounter age verification, click Yes.
    Persist storage state so subsequent runs reuse it.
    """
    page.goto(EPSTEIN_HOME, wait_until="domcontentloaded")
    if is_age_verify_url(page.url):
        log(f"Age verification encountered at {page.url}. Attempting to click 'Yes'.")
        clicked = try_click_yes(page)
        if not clicked:
            log("WARNING: Could not auto-click 'Yes'. Run with --headed once and click it manually.")
        page.wait_for_load_state("networkidle")

    storage_state_path.parent.mkdir(parents=True, exist_ok=True)
    context.storage_state(path=str(storage_state_path))
    log(f"Saved storage state to: {storage_state_path}")


def looks_like_pdf(content_type: str, body_prefix: bytes) -> bool:
    ct = (content_type or "").lower()
    if "pdf" in ct or "octet-stream" in ct:
        return True
    return body_prefix.startswith(b"%PDF")


def nav_get_bytes(page: Page, url: str, referer: str) -> Tuple[int, str, str, bytes]:
    """
    Navigate to URL with Referer and return (status, final_url, content_type, body_bytes).
    """
    resp: Optional[Response] = page.goto(url, wait_until="domcontentloaded", referer=referer)
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


def atomic_write(dest: Path, data: bytes) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, dest)


def already_downloaded_anywhere(out_dir: Path, filename: str) -> bool:
    return bool(list(out_dir.glob(f"DataSet_*/{filename}")))


def extract_pdf_links_from_current_page(page: Page, dataset_n: int) -> List[Tuple[str, str]]:
    """
    Parse all <a href> on the currently loaded page and return (filename, absolute_url)
    for PDFs that match the dataset.
    """
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
            filename = f"EFTA{num}.pdf"
            out.append((filename, abs_url))

    return out


def collect_pdf_links_for_dataset_paginated(
    page: Page,
    dataset_n: int,
    log,
    max_pages: int = 10000,
    polite_sleep: float = 0.15,
) -> Dict[str, Tuple[str, str]]:
    """
    Scrape the dataset listing page *and* its pagination (?page=1, ?page=2, ...).

    Returns a mapping:
      filename -> (pdf_url, referer_url_where_found)

    Stop condition:
    - when a page yields zero *new* PDF links (deduplicated), after at least the first page.
    """
    base = DATASET_LISTING_BASE.format(n=dataset_n)
    collected: Dict[str, Tuple[str, str]] = {}

    for page_num in range(max_pages):
        if page_num == 0:
            url = base
        else:
            url = f"{base}?page={page_num}"

        page.goto(url, wait_until="networkidle")

        # If page is gated, attempt verification then reload
        if is_age_verify_url(page.url):
            log(f"Dataset {dataset_n} index redirected to age-verify at page={page_num}; verifying then retrying.")
            try_click_yes(page)
            page.wait_for_load_state("networkidle")
            page.goto(url, wait_until="networkidle")

        links = extract_pdf_links_from_current_page(page, dataset_n)

        new_count = 0
        for fn, pdf_url in links:
            if fn not in collected:
                collected[fn] = (pdf_url, url)  # store referer as the index page URL
                new_count += 1

        log(f"Dataset {dataset_n}: index page={page_num} links={len(links)} new={new_count} total={len(collected)}")

        # Stop if this page contributes no new links (after first page)
        if page_num > 0 and new_count == 0:
            break

        time.sleep(polite_sleep + random.random() * polite_sleep)

    return collected


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

            # Sort by EFTA numeric id for nicer progress
            items = sorted(mapping.items(), key=lambda kv: int(kv[0][4:12]))
            total_links += len(items)

            log(f"Dataset {ds}: total collected across pages = {len(items)}")

            for filename, (pdf_url, referer_url) in items:
                dest = ds_dir / filename
                if dest.exists() and dest.stat().st_size > 0:
                    continue

                status, final_url, ctype, body = nav_get_bytes(page, pdf_url, referer=referer_url)

                # Handle age-verify bounce
                if is_age_verify_url(final_url) or (status == 200 and "text/html" in (ctype or "").lower() and not looks_like_pdf(ctype, body[:8])):
                    log(f"{filename}: got HTML/age-verify (status={status}, ctype={ctype}); re-verifying then retrying once.")
                    ensure_age_verified(page, context, storage_state_path, log)
                    status, final_url, ctype, body = nav_get_bytes(page, pdf_url, referer=referer_url)

                if status == 401:
                    blocked_or_nonpdf += 1
                    log(f"{filename}: 401 at {pdf_url}. Try --headed and/or --use-chrome-channel.")
                    time.sleep(2.0 + random.random() * 2.0)
                    continue

                if status >= 400 or status == 0:
                    log(f"{filename}: error status={status} final_url={final_url}; skipping.")
                    time.sleep(1.0 + random.random() * 1.5)
                    continue

                if not looks_like_pdf(ctype, body[:8]):
                    blocked_or_nonpdf += 1
                    log(f"{filename}: status=200 but not PDF (ctype={ctype}, final_url={final_url}); skipping.")
                    time.sleep(1.0 + random.random() * 1.5)
                    continue

                atomic_write(dest, body)
                downloaded += 1
                log(f"{filename}: downloaded (dataset {ds}) -> {dest}")

                time.sleep(max(0.0, args.sleep + random.random() * args.jitter))

        browser.close()

    log(f"Done. total_links={total_links} downloaded={downloaded} blocked_or_nonpdf={blocked_or_nonpdf}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())