#!/usr/bin/env python3
"""
Shelby County Clerk – New Business List Scraper
================================================
Target  : https://secure.tncountyclerk.com/businesslist/index.php?countylist=79
Method  : Playwright (headless Chromium) – the page uses a JS-rendered form
          that POSTs to a results endpoint; requests+BS4 alone misses it.
Date    : 01 Apr 2026 → 14 Apr 2026
Output  : data/records.json  +  data/leads_ghl.csv

Root-cause of the original zero-record run
-------------------------------------------
1. The script fetched  https://www.assessor.shelby.tn.us/downloads/parcels.zip
   (Assessor parcel data) – that is a *property* dataset, NOT the Clerk's
   new-business list.  The ZIP also returned an HTML 404 page ("<!DO…") so
   the bulk-file branch was skipped, leaving 0 records.
2. Even if the correct URL were used, the business-list form requires:
   a) a Start Date + End Date to be filled in, then
   b) a "Search" / submit button to be clicked.
   Static requests never trigger that; the response is always empty.
3. After submitting, results are rendered by JavaScript into a <table> that
   doesn't exist in the initial HTML, so BeautifulSoup on the raw page body
   finds nothing.

Fix strategy
------------
• Use Playwright to open the real business-list page (countylist=79 = Shelby).
• Fill the date fields with the requested range.
• Click Search and wait for the results table to appear.
• Parse the table rows into structured records.
• Save JSON + GHL-compatible CSV.
"""

import asyncio
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ── dependencies ──────────────────────────────────────────────────────────────
# pip install playwright && playwright install chromium
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    sys.exit(
        "Playwright not installed.\n"
        "Run:  pip install playwright && playwright install chromium"
    )

# ── configuration ─────────────────────────────────────────────────────────────
COUNTY_ID   = "79"          # Shelby County
START_DATE  = "04/01/2026"  # MM/DD/YYYY  (matches the site's date picker format)
END_DATE    = "04/14/2026"
BASE_URL    = f"https://secure.tncountyclerk.com/businesslist/index.php?countylist={COUNTY_ID}"
OUTPUT_DIR  = Path("data")
JSON_FILE   = OUTPUT_DIR / "records.json"
CSV_FILE    = OUTPUT_DIR / "leads_ghl.csv"
HEADLESS    = True          # set False locally to watch the browser

# GHL CSV column mapping
GHL_FIELDS = [
    "First Name", "Last Name", "Business Name",
    "Address", "City", "State", "Zip",
    "Phone", "Email", "Tags",
    "License Date", "Owner Name", "Record Source",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def parse_name(full_name: str) -> tuple[str, str]:
    """Split 'LAST, FIRST M' or 'FIRST LAST' into (first, last)."""
    if not full_name:
        return "", ""
    if "," in full_name:
        parts = full_name.split(",", 1)
        last  = parts[0].strip().title()
        first = parts[1].strip().title()
    else:
        tokens = full_name.strip().split()
        first  = tokens[0].title() if tokens else ""
        last   = " ".join(tokens[1:]).title() if len(tokens) > 1 else ""
    return first, last


def parse_address(raw: str) -> dict:
    """Best-effort split of 'STREET, CITY, TN ZIP' into components."""
    result = {"address": raw, "city": "", "state": "TN", "zip": ""}
    if not raw:
        return result
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 1:
        result["address"] = parts[0]
    if len(parts) >= 2:
        result["city"] = parts[1].title()
    if len(parts) >= 3:
        state_zip = parts[2].split()
        if state_zip:
            result["state"] = state_zip[0].upper()
        if len(state_zip) > 1:
            result["zip"] = state_zip[1]
    return result


def to_ghl_row(record: dict) -> dict:
    """Convert a raw scraped record to a GHL-compatible flat row."""
    first, last = parse_name(record.get("owner_name", ""))
    addr = parse_address(record.get("address", ""))
    return {
        "First Name":     first,
        "Last Name":      last,
        "Business Name":  record.get("business_name", ""),
        "Address":        addr["address"],
        "City":           addr["city"],
        "State":          addr["state"],
        "Zip":            addr["zip"],
        "Phone":          record.get("phone", ""),
        "Email":          record.get("email", ""),
        "Tags":           "Shelby-NewBiz",
        "License Date":   record.get("date", ""),
        "Owner Name":     record.get("owner_name", ""),
        "Record Source":  "ShelbyCountyClerk",
    }


# ── main scraper ──────────────────────────────────────────────────────────────

async def scrape() -> list[dict]:
    records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],   # needed in CI
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        # ── 1. Open the business-list search page ─────────────────────────
        print(f"[nav] Opening {BASE_URL}")
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(1500)  # let JS settle

        # ── DEBUG: page length + title ────────────────────────────────────
        body_len = len(await page.content())
        title    = await page.title()
        print(f"[debug] Page title : {title!r}")
        print(f"[debug] Page length: {body_len} chars")

        # ── 2. Locate and fill the date inputs ────────────────────────────
        # The form has two <input type="text"> date fields (Start / End).
        # Their IDs vary slightly by county; we use visible label proximity.
        start_sel = "input[name='startDate'], input[id*='start'], input[placeholder*='Start']"
        end_sel   = "input[name='endDate'],   input[id*='end'],   input[placeholder*='End']"

        # Fallback: grab the first two date-like inputs on the page
        date_inputs = await page.query_selector_all("input[type='text']")
        print(f"[debug] Text inputs found: {len(date_inputs)}")

        if len(date_inputs) >= 2:
            await date_inputs[0].triple_click()
            await date_inputs[0].type(START_DATE, delay=60)
            await date_inputs[1].triple_click()
            await date_inputs[1].type(END_DATE, delay=60)
            print(f"[form] Filled dates: {START_DATE} → {END_DATE}")
        else:
            # Try named selectors
            for sel, val, label in [
                (start_sel, START_DATE, "start"),
                (end_sel,   END_DATE,   "end"),
            ]:
                el = await page.query_selector(sel)
                if el:
                    await el.triple_click()
                    await el.type(val, delay=60)
                    print(f"[form] Filled {label} date: {val}")
                else:
                    print(f"[warn] Could not find {label} date input with selector: {sel}")

        # ── 3. Click the Search / Submit button ───────────────────────────
        submit_sel = (
            "input[type='submit'], "
            "button[type='submit'], "
            "input[value*='Search'], "
            "button:has-text('Search')"
        )
        submit_btn = await page.query_selector(submit_sel)
        if submit_btn:
            print("[form] Clicking Search button …")
            await submit_btn.click()
        else:
            # Some deployments use an <a> or JS onclick – try pressing Enter
            print("[form] Submit button not found; pressing Enter on last input")
            if date_inputs:
                await date_inputs[-1].press("Enter")

        # ── 4. Wait for results ───────────────────────────────────────────
        result_sel = "table.results, table#businessList, table[id*='result'], table"
        try:
            await page.wait_for_selector(result_sel, timeout=20_000)
            print("[wait] Results table appeared")
        except PWTimeout:
            print("[warn] Timed out waiting for results table; dumping page snippet …")
            snippet = (await page.content())[:2000]
            print(snippet)

        await page.wait_for_timeout(1000)  # brief extra settle

        # ── DEBUG: count tables ───────────────────────────────────────────
        tables = await page.query_selector_all("table")
        print(f"[debug] Tables on page after submit: {len(tables)}")

        # ── 5. Parse result rows ──────────────────────────────────────────
        # Strategy: iterate every <table>, find the one with the most rows
        # and the expected headers (Business Name / Owner / Address / Date).
        best_table = None
        best_rows  = 0
        for tbl in tables:
            rows = await tbl.query_selector_all("tr")
            if len(rows) > best_rows:
                best_rows  = len(rows)
                best_table = tbl

        if best_table is None or best_rows < 2:
            print("[warn] No data table found – 0 records returned")
            await browser.close()
            return []

        print(f"[parse] Parsing table with {best_rows} rows (including header)")

        # Grab header cells to map column positions
        header_row  = await best_table.query_selector("tr:first-child")
        header_cells = await header_row.query_selector_all("th, td")
        headers = [
            (await c.inner_text()).strip().lower().replace(" ", "_")
            for c in header_cells
        ]
        print(f"[debug] Columns detected: {headers}")

        # Column-index mapping (flexible)
        col = {
            "business_name": next((i for i, h in enumerate(headers) if "business" in h), 0),
            "owner_name":    next((i for i, h in enumerate(headers) if "owner" in h or "name" in h), 1),
            "address":       next((i for i, h in enumerate(headers) if "address" in h or "location" in h), 2),
            "date":          next((i for i, h in enumerate(headers) if "date" in h), 3),
            "phone":         next((i for i, h in enumerate(headers) if "phone" in h), -1),
        }

        data_rows = await best_table.query_selector_all("tr:not(:first-child)")
        for row in data_rows:
            cells = await row.query_selector_all("td")
            if not cells:
                continue

            def cell_text(idx: int) -> str:
                if idx < 0 or idx >= len(cells):
                    return ""
                return ""  # evaluated below via asyncio

            texts = [
                (await c.inner_text()).strip()
                for c in cells
            ]
            if not any(texts):
                continue

            record = {
                "business_name": texts[col["business_name"]] if col["business_name"] < len(texts) else "",
                "owner_name":    texts[col["owner_name"]]    if col["owner_name"]    < len(texts) else "",
                "address":       texts[col["address"]]       if col["address"]       < len(texts) else "",
                "date":          texts[col["date"]]          if col["date"]          < len(texts) else "",
                "phone":         texts[col["phone"]]         if 0 <= col["phone"]    < len(texts) else "",
                "email":         "",
                "raw":           texts,
            }
            records.append(record)

        print(f"[parse] Extracted {len(records)} records")

        # ── SAMPLE OUTPUT ─────────────────────────────────────────────────
        if records:
            print("\n── Sample Records (first 3) ─────────────────────────")
            for r in records[:3]:
                print(
                    f"  Business : {r['business_name']}\n"
                    f"  Owner    : {r['owner_name']}\n"
                    f"  Address  : {r['address']}\n"
                    f"  Date     : {r['date']}\n"
                    f"  Phone    : {r['phone']}\n"
                )

        await browser.close()
    return records


# ── save outputs ──────────────────────────────────────────────────────────────

def save_json(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "scraped_at":   datetime.utcnow().isoformat() + "Z",
        "county":       "Shelby",
        "start_date":   START_DATE,
        "end_date":     END_DATE,
        "total_records": len(records),
        "records":      records,
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[save] JSON → {path}  ({len(records)} records)")


def save_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [to_ghl_row(r) for r in records]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[export] GHL CSV → {path}  ({len(rows)} rows)")


# ── entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    print("=" * 60)
    print("  Shelby County Clerk – New Business List Scraper")
    print(f"  Range : {START_DATE} → {END_DATE}")
    print("=" * 60)

    records = await scrape()

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  ✅  Done – {len(records)} total records, "
          f"{sum(1 for r in records if r.get('address')) } with address")
    print("=" * 60)

    save_json(records, JSON_FILE)
    save_csv(records, CSV_FILE)


if __name__ == "__main__":
    asyncio.run(main())
