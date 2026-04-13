"""
Shelby County, TN — Motivated Seller Lead Scraper
Corrected to use: https://search.register.shelby.tn.us/search/index.php
"""

import asyncio
import csv
import io
import json
import os
import re
import sys
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False
    print("[WARN] dbfread not installed – parcel enrichment disabled")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))

# ✅ CORRECTED URLs
CLERK_SEARCH   = "https://search.register.shelby.tn.us/search/index.php"
CLERK_BASE     = "https://search.register.shelby.tn.us"
APPRAISER_URL  = "https://www.assessor.shelby.tn.us"   # ✅ actual assessor site

OUTPUT_PATHS   = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 3

DOC_TYPES = {
    "LP":       ("LP",     "Lis Pendens"),
    "NOFC":     ("NOFC",   "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED","Tax Deed"),
    "JUD":      ("JUD",    "Judgment"),
    "CCJ":      ("JUD",    "Certified Judgment"),
    "DRJUD":    ("JUD",    "Domestic Judgment"),
    "LNCORPTX": ("LNTAX",  "Corp Tax Lien"),
    "LNIRS":    ("LNTAX",  "IRS Lien"),
    "LNFED":    ("LNTAX",  "Federal Lien"),
    "LN":       ("LN",     "Lien"),
    "LNMECH":   ("LN",     "Mechanic Lien"),
    "LNHOA":    ("LN",     "HOA Lien"),
    "MEDLN":    ("LN",     "Medicaid Lien"),
    "PRO":      ("PRO",    "Probate"),
    "NOC":      ("NOC",    "Notice of Commencement"),
    "RELLP":    ("RELLP",  "Release Lis Pendens"),
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def safe_float(val) -> Optional[float]:
    if not val:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(val)))
    except Exception:
        return None


def parse_date(s: str) -> Optional[str]:
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return s.strip() or None


def date_range():
    end   = datetime.utcnow()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return start, end


def name_variants(full_name: str):
    n = full_name.upper().strip()
    variants = [n]
    if "," in n:
        parts = [p.strip() for p in n.split(",", 1)]
        variants.append(f"{parts[1]} {parts[0]}")
        variants.append(n.replace(",", ""))
    else:
        tokens = n.split()
        if len(tokens) >= 2:
            variants.append(f"{tokens[-1]} {' '.join(tokens[:-1])}")
            variants.append(f"{tokens[-1]}, {' '.join(tokens[:-1])}")
    return list(dict.fromkeys(variants))

# ─────────────────────────────────────────────────────────────────────────────
# PARCEL ENRICHMENT  — Shelby County Assessor
# ─────────────────────────────────────────────────────────────────────────────

def download_parcel_dbf() -> dict:
    if not HAS_DBF:
        return {}

    headers = {"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)"}

    # ✅ CORRECTED: Shelby County Assessor of Property bulk data URLs
    candidate_urls = [
        "https://www.assessor.shelby.tn.us/downloads/parcel_data.zip",
        "https://www.assessor.shelby.tn.us/downloads/Parcel.zip",
        "https://www.assessor.shelby.tn.us/downloads/parcels.zip",
        "https://www.assessor.shelby.tn.us/GIS/GIS_Data.zip",
        # Shelby County GIS open data portal fallback
        "https://opendata.shelbycountytn.gov/datasets/shelby-county-parcel-data.zip",
    ]

    # Try to discover actual download link from the assessor's site
    for discovery_url in [
        "https://www.assessor.shelby.tn.us/downloads",
        "https://www.assessor.shelby.tn.us/GIS",
    ]:
        try:
            r = requests.get(discovery_url, headers=headers, timeout=30)
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"parcel|data|dbf|zip|gis", href, re.I):
                    full = href if href.startswith("http") else "https://www.assessor.shelby.tn.us/" + href.lstrip("/")
                    if full not in candidate_urls:
                        candidate_urls.insert(0, full)
                        print(f"[parcel] discovered: {full}")
        except Exception as exc:
            print(f"[parcel] discovery at {discovery_url} failed: {exc}")

    raw_bytes = None
    for url in candidate_urls:
        try:
            print(f"[parcel] trying {url}")
            resp = requests.get(url, headers=headers, timeout=90, stream=True)
            if resp.status_code == 200 and len(resp.content) > 1000:
                raw_bytes = resp.content
                print(f"[parcel] downloaded {len(raw_bytes):,} bytes from {url}")
                break
        except Exception as exc:
            print(f"[parcel] {url} → {exc}")

    if not raw_bytes:
        print("[parcel] no bulk file found – skipping enrichment")
        return {}

    owner_map: dict = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            dbf_names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
            if not dbf_names:
                print("[parcel] no DBF inside zip")
                return {}
            dbf_bytes = zf.read(dbf_names[0])

        tmp = Path("/tmp/parcels.dbf")
        tmp.write_bytes(dbf_bytes)

        col_map = {
            "owner":     ["OWNER", "OWN1", "OWNERNAME", "OWNER_NAME"],
            "site_addr": ["SITE_ADDR", "SITEADDR", "ADDRESS", "PROP_ADDR"],
            "site_city": ["SITE_CITY", "SITECITY", "CITY"],
            "site_zip":  ["SITE_ZIP", "SITEZIP", "ZIP"],
            "mail_addr": ["ADDR_1", "MAILADR1", "MAILADDR1", "MAIL_ADDR"],
            "mail_city": ["MAILCITY", "MAIL_CITY"],
            "mail_state":["STATE", "MAILSTATE", "MAIL_STATE"],
            "mail_zip":  ["ZIP", "MAILZIP", "MAIL_ZIP"],
        }

        def get_col(row, keys):
            for k in keys:
                if k in row and row[k]:
                    return str(row[k]).strip()
            return ""

        for rec in DBF(str(tmp), load=True, ignore_missing_memofile=True):
            try:
                owner = get_col(rec, col_map["owner"])
                if not owner:
                    continue
                parcel = {
                    "prop_address": get_col(rec, col_map["site_addr"]),
                    "prop_city":    get_col(rec, col_map["site_city"]) or "Memphis",
                    "prop_state":   "TN",
                    "prop_zip":     get_col(rec, col_map["site_zip"]),
                    "mail_address": get_col(rec, col_map["mail_addr"]),
                    "mail_city":    get_col(rec, col_map["mail_city"]),
                    "mail_state":   get_col(rec, col_map["mail_state"]) or "TN",
                    "mail_zip":     get_col(rec, col_map["mail_zip"]),
                }
                for variant in name_variants(owner):
                    if variant not in owner_map:
                        owner_map[variant] = parcel
            except Exception:
                pass

        print(f"[parcel] built lookup with {len(owner_map):,} entries")
    except Exception as exc:
        print(f"[parcel] parse error: {exc}")
        traceback.print_exc()

    return owner_map


def enrich_from_parcel(record: dict, owner_map: dict) -> dict:
    if not owner_map:
        return record
    for variant in name_variants(record.get("owner", "")):
        if variant in owner_map:
            p = owner_map[variant]
            record.update({k: v for k, v in p.items() if not record.get(k)})
            break
    return record

# ─────────────────────────────────────────────────────────────────────────────
# CLERK SCRAPER  — Playwright targeting the CORRECT portal
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_clerk(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """
    Scrape https://search.register.shelby.tn.us/search/index.php
    Uses the Instrument Type Search with date range.
    """
    records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122 Safari/537.36"
        )
        page = await ctx.new_page()

        print(f"[clerk] loading {CLERK_SEARCH}")
        try:
            await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=60000)
        except PWTimeout:
            await page.goto(CLERK_SEARCH, timeout=60000)
        await page.wait_for_timeout(3000)

        # ── The real portal has a left-side menu. Click "Instrument Type Search"
        # which is the "Instrument Type - All" option showing all doc types
        # Look for it in the menu links or tabs
        try:
            # Try clicking the instrument type search menu item
            inst_link = await page.query_selector(
                "text=Instrument Type Search, "
                "a:has-text('Instrument'), "
                "[onclick*='inst'], "
                "a:has-text('Type Search')"
            )
            if inst_link:
                await inst_link.click()
                await page.wait_for_timeout(2000)
                print("[clerk] clicked Instrument Type Search")
        except Exception as e:
            print(f"[clerk] menu click failed: {e}")

        # ── Get all instrument type options from the dropdown ─────────────────
        # The form has: Inst Type #1, Start Date, End Date
        # First discover what instrument codes are available
        inst_options = []
        try:
            select_el = await page.query_selector("select[name*='inst'], select[name*='Inst'], select[id*='inst']")
            if select_el:
                opts = await select_el.query_selector_all("option")
                for opt in opts:
                    val = await opt.get_attribute("value") or ""
                    txt = (await opt.inner_text()).strip()
                    if val and val.upper() not in ("", "ALL TYPES", "ALL"):
                        inst_options.append((val, txt))
                print(f"[clerk] found {len(inst_options)} instrument types in dropdown")
        except Exception as e:
            print(f"[clerk] dropdown read failed: {e}")

        # ── Map our target codes to whatever the portal calls them ────────────
        # Portal may use full names like "LIS PENDENS", "NOTICE OF FORECLOSURE"
        target_map = {
            "LIS PENDENS":             ("LP",      "LP",     "Lis Pendens"),
            "LIS P":                   ("LP",      "LP",     "Lis Pendens"),
            "LP":                      ("LP",      "LP",     "Lis Pendens"),
            "NOTICE OF FORECLOSURE":   ("NOFC",    "NOFC",   "Notice of Foreclosure"),
            "NOFC":                    ("NOFC",    "NOFC",   "Notice of Foreclosure"),
            "TAX DEED":                ("TAXDEED", "TAXDEED","Tax Deed"),
            "TAXDEED":                 ("TAXDEED", "TAXDEED","Tax Deed"),
            "JUDGMENT":                ("JUD",     "JUD",    "Judgment"),
            "JUD":                     ("JUD",     "JUD",    "Judgment"),
            "FEDERAL TAX LIEN":        ("LNTAX",   "LNFED",  "Federal Lien"),
            "IRS LIEN":                ("LNTAX",   "LNIRS",  "IRS Lien"),
            "STATE TAX LIEN":          ("LNTAX",   "LNCORPTX","Corp Tax Lien"),
            "LIEN":                    ("LN",      "LN",     "Lien"),
            "LN":                      ("LN",      "LN",     "Lien"),
            "MECHANIC":                ("LN",      "LNMECH", "Mechanic Lien"),
            "HOA":                     ("LN",      "LNHOA",  "HOA Lien"),
            "PROBATE":                 ("PRO",     "PRO",    "Probate"),
            "PRO":                     ("PRO",     "PRO",    "Probate"),
            "NOTICE OF COMMENCEMENT":  ("NOC",     "NOC",    "Notice of Commencement"),
            "NOC":                     ("NOC",     "NOC",    "Notice of Commencement"),
            "RELEASE LIS PENDENS":     ("RELLP",   "RELLP",  "Release Lis Pendens"),
            "RELLP":                   ("RELLP",   "RELLP",  "Release Lis Pendens"),
            # Foreclosure-related
            "SUBSTITUTE TRUSTEE":      ("NOFC",    "NOFC",   "Notice of Foreclosure"),
            "APPOINTMENT OF SUBSTITUTE":("NOFC",   "NOFC",   "Notice of Foreclosure"),
            "FORECLOSURE":             ("NOFC",    "NOFC",   "Notice of Foreclosure"),
        }

        # Match available options to our targets
        matched = []
        if inst_options:
            for val, txt in inst_options:
                txt_upper = txt.upper()
                for keyword, mapping in target_map.items():
                    if keyword in txt_upper or keyword in val.upper():
                        matched.append((val, txt, mapping[0], mapping[1], mapping[2]))
                        break
            print(f"[clerk] matched {len(matched)} target instrument types")
        
        # If no dropdown found, we'll use the instrument type search with known codes
        if not matched:
            print("[clerk] no dropdown matched – will try direct instrument type search")
            matched = [
                # (portal_value, display_name, cat, doc_type, cat_label)
                ("LP",       "Lis Pendens",             "LP",     "LP",      "Lis Pendens"),
                ("NOFC",     "Notice of Foreclosure",   "NOFC",   "NOFC",    "Notice of Foreclosure"),
                ("JUD",      "Judgment",                "JUD",    "JUD",     "Judgment"),
                ("LN",       "Lien",                    "LN",     "LN",      "Lien"),
                ("PRO",      "Probate",                 "PRO",    "PRO",     "Probate"),
                ("NOC",      "Notice of Commencement",  "NOC",    "NOC",     "Notice of Commencement"),
                ("TAXDEED",  "Tax Deed",                "TAXDEED","TAXDEED", "Tax Deed"),
                ("RELLP",    "Release Lis Pendens",     "RELLP",  "RELLP",   "Release Lis Pendens"),
            ]

        # ── Search each instrument type ───────────────────────────────────────
        for portal_val, display_name, cat, doc_type, cat_label in matched:
            try:
                batch = await _instrument_type_search(
                    page, portal_val, display_name,
                    cat, doc_type, cat_label,
                    start_str, end_str
                )
                records.extend(batch)
                print(f"[clerk] {doc_type} ({display_name}): {len(batch)} records")
            except Exception as exc:
                print(f"[clerk] {doc_type} error: {exc}")
                traceback.print_exc()

        await browser.close()

    return records


async def _instrument_type_search(
    page, portal_val: str, display_name: str,
    cat: str, doc_type: str, cat_label: str,
    start_str: str, end_str: str
) -> list[dict]:
    """
    Fill the Instrument Type Search form and parse results.
    The portal at search.register.shelby.tn.us uses a specific form structure.
    """
    records = []

    # ── Navigate/reload to ensure clean form state ────────────────────────────
    try:
        await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=45000)
    except PWTimeout:
        pass
    await page.wait_for_timeout(2000)

    # ── Click the "Instrument Type Search" or "Instrument Type - All" menu item
    for selector in [
        "text=Instrument Type Search",
        "text=Instrument Type - All",
        "a:has-text('Instrument Type')",
        "[onclick*='instrType']",
        "[onclick*='inst_type']",
        "[onclick*='InstrType']",
    ]:
        try:
            el = await page.query_selector(selector)
            if el:
                await el.click()
                await page.wait_for_timeout(1500)
                print(f"[clerk] menu: clicked '{selector}'")
                break
        except Exception:
            pass

    # ── Fill the form fields ──────────────────────────────────────────────────
    filled = False

    # Try: select the instrument type from dropdown
    for sel_selector in [
        "select[name*='inst']", "select[name*='Inst']",
        "select[id*='inst']",   "select[id*='Inst']",
        "select"
    ]:
        try:
            sel_el = await page.query_selector(sel_selector)
            if sel_el:
                # Try to select by value or label
                try:
                    await sel_el.select_option(value=portal_val)
                    filled = True
                    break
                except Exception:
                    pass
                # Try selecting by label text
                opts = await sel_el.query_selector_all("option")
                for opt in opts:
                    txt = (await opt.inner_text()).strip().upper()
                    if portal_val.upper() in txt or display_name.upper() in txt:
                        val = await opt.get_attribute("value")
                        await sel_el.select_option(value=val)
                        filled = True
                        break
                if filled:
                    break
        except Exception:
            pass

    if not filled:
        print(f"[clerk] WARNING: could not select instrument type {portal_val}")

    # Fill start date
    for date_sel in [
        "input[name*='start']", "input[name*='Start']",
        "input[name*='beg']",   "input[name*='Beg']",
        "input[name*='from']",  "input[id*='start']",
        "input[id*='beg']",
    ]:
        try:
            el = await page.query_selector(date_sel)
            if el:
                await el.triple_click()
                await el.fill(start_str)
                break
        except Exception:
            pass

    # Fill end date
    for date_sel in [
        "input[name*='end']",   "input[name*='End']",
        "input[name*='to']",    "input[id*='end']",
    ]:
        try:
            el = await page.query_selector(date_sel)
            if el:
                await el.triple_click()
                await el.fill(end_str)
                break
        except Exception:
            pass

    # ── Submit ────────────────────────────────────────────────────────────────
    submitted = False
    for btn_selector in [
        "input[type='submit']",
        "button[type='submit']",
        "button:has-text('Search')",
        "input[value='Search']",
        "input[value='Go']",
        "button:has-text('Go')",
    ]:
        try:
            btn = await page.query_selector(btn_selector)
            if btn:
                await btn.click()
                submitted = True
                break
        except Exception:
            pass

    if not submitted:
        print(f"[clerk] WARNING: could not submit form for {portal_val}")
        return records

    # ── Wait for results ──────────────────────────────────────────────────────
    try:
        await page.wait_for_selector("table, .results, #results, .no-results", timeout=15000)
    except PWTimeout:
        pass
    await page.wait_for_timeout(2000)

    # ── Parse results ─────────────────────────────────────────────────────────
    html  = await page.content()
    soup  = BeautifulSoup(html, "lxml")

    # Handle pagination — collect all pages
    all_rows = []
    page_num = 0
    while True:
        page_num += 1
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
            if not any(k in " ".join(headers) for k in ["doc", "date", "grantor", "name", "instr"]):
                continue
            for tr in rows[1:]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if not cells or all(c == "" for c in cells):
                    continue
                row = dict(zip(headers, cells))

                link_tag = tr.find("a", href=True)
                clerk_url = ""
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = href if href.startswith("http") else CLERK_BASE + "/" + href.lstrip("/")

                doc_num = _pick(row, ["instrument", "doc", "instr #", "inst #", "number"])
                filed   = _pick(row, ["date", "filed", "record date", "file date"])
                owner   = _pick(row, ["grantor", "owner", "party 1", "name"])
                grantee = _pick(row, ["grantee", "party 2"])
                amount  = _pick(row, ["amount", "consideration"])
                legal   = _pick(row, ["legal", "description"])

                if not doc_num and not owner:
                    continue

                all_rows.append({
                    "doc_num":   doc_num or "",
                    "doc_type":  doc_type,
                    "filed":     parse_date(filed) or "",
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     owner or "",
                    "grantee":   grantee or "",
                    "amount":    safe_float(amount),
                    "legal":     legal or "",
                    "prop_address": "", "prop_city": "Memphis",
                    "prop_state": "TN", "prop_zip": "",
                    "mail_address": "", "mail_city": "",
                    "mail_state": "", "mail_zip": "",
                    "clerk_url": clerk_url,
                    "flags": [], "score": 0,
                })

        # Check for next page
        next_btn = soup.find("a", string=re.compile(r"next|>", re.I))
        if not next_btn or page_num >= 20:
            break
        try:
            href = next_btn.get("href", "")
            if href:
                next_url = href if href.startswith("http") else CLERK_BASE + "/" + href.lstrip("/")
                await page.goto(next_url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(1500)
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
            else:
                await next_btn.click()  # won't work via BS4, but try
                break
        except Exception:
            break

    return all_rows


def _pick(row: dict, keys: list) -> str:
    for k in keys:
        for rk, rv in row.items():
            if k in rk and rv:
                return str(rv).strip()
    return ""

# ─────────────────────────────────────────────────────────────────────────────
# FALLBACK — Direct HTTP to the search portal
# ─────────────────────────────────────────────────────────────────────────────

def scrape_clerk_requests(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """
    Fallback: POST directly to the search.register.shelby.tn.us search endpoint.
    """
    records = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)",
        "Referer": CLERK_SEARCH,
    })

    # The portal accepts GET with query params for instrument type search
    # Endpoint discovered from the form: search/index.php with these params
    search_endpoints = [
        "https://search.register.shelby.tn.us/search/index.php",
        "https://search.register.shelby.tn.us/search/",
    ]

    for code, (cat, cat_label) in DOC_TYPES.items():
        for attempt in range(RETRY_ATTEMPTS):
            try:
                params = {
                    "inst_type":  code,
                    "beg_date":   start_dt.strftime("%m/%d/%Y"),
                    "end_date":   end_dt.strftime("%m/%d/%Y"),
                    "searchType": "instrType",
                }
                r = session.get(search_endpoints[0], params=params, timeout=30)
                soup = BeautifulSoup(r.text, "lxml")

                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    if len(rows) < 2:
                        continue
                    hdrs = [th.get_text(strip=True).lower()
                            for th in rows[0].find_all(["th", "td"])]
                    if not any(k in " ".join(hdrs) for k in ["doc","date","grantor","name"]):
                        continue

                    for tr in rows[1:]:
                        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                        if not cells:
                            continue
                        row = dict(zip(hdrs, cells)) if hdrs else {}

                        link_tag = tr.find("a", href=True)
                        clerk_url = ""
                        if link_tag:
                            href = link_tag["href"]
                            clerk_url = href if href.startswith("http") else CLERK_BASE + "/" + href.lstrip("/")

                        doc_num = _pick(row, ["instrument","doc","instr"]) or (cells[0] if cells else "")
                        owner   = _pick(row, ["grantor","owner","party","name"]) or ""

                        if not doc_num and not owner:
                            continue

                        records.append({
                            "doc_num":   doc_num,
                            "doc_type":  code,
                            "filed":     parse_date(_pick(row, ["date","filed"])) or "",
                            "cat":       cat,
                            "cat_label": cat_label,
                            "owner":     owner,
                            "grantee":   _pick(row, ["grantee"]) or "",
                            "amount":    safe_float(_pick(row, ["amount"])),
                            "legal":     _pick(row, ["legal"]) or "",
                            "prop_address": "", "prop_city": "Memphis",
                            "prop_state": "TN", "prop_zip": "",
                            "mail_address": "", "mail_city": "",
                            "mail_state": "", "mail_zip": "",
                            "clerk_url": clerk_url,
                            "flags": [], "score": 0,
                        })
                break
            except Exception as exc:
                if attempt == RETRY_ATTEMPTS - 1:
                    print(f"[fallback] {code} failed: {exc}")
                time.sleep(RETRY_DELAY)

    return records

# ─────────────────────────────────────────────────────────────────────────────
# SCORING  (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def score_record(rec: dict, week_ago: datetime) -> dict:
    flags = []
    score = 30
    cat   = rec.get("cat", "")
    code  = rec.get("doc_type", "")
    amount = rec.get("amount") or 0
    filed  = rec.get("filed", "")
    owner  = rec.get("owner", "").upper()

    if cat == "LP":
        flags.append("Lis pendens");  score += 10
    if cat == "NOFC":
        flags.append("Pre-foreclosure"); score += 10
    if cat == "JUD":
        flags.append("Judgment lien");   score += 10
    if cat == "LNTAX":
        flags.append("Tax lien");        score += 10
    if code == "LNMECH":
        flags.append("Mechanic lien");   score += 10
    if code == "LNHOA":
        score += 5
    if cat == "PRO":
        flags.append("Probate / estate"); score += 10
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bLLP\b", owner):
        flags.append("LLC / corp owner"); score += 10
    if amount and amount > 100_000:
        score += 15; flags.append("High-value debt")
    elif amount and amount > 50_000:
        score += 10
    if filed:
        try:
            if datetime.strptime(filed, "%Y-%m-%d") >= week_ago:
                score += 5; flags.append("New this week")
        except Exception:
            pass
    if rec.get("prop_address"):
        score += 5; flags.append("Has address")

    rec["flags"] = list(dict.fromkeys(flags))
    rec["score"] = min(score, 100)
    return rec


def apply_lp_fc_combo(records):
    owner_cats = {}
    for r in records:
        o = r.get("owner", "").upper()
        if o:
            owner_cats.setdefault(o, set()).add(r.get("cat"))
    for r in records:
        o = r.get("owner", "").upper()
        if o and {"LP", "NOFC"}.issubset(owner_cats.get(o, set())):
            r["score"] = min(r["score"] + 20, 100)
            for flag in ("Lis pendens", "Pre-foreclosure"):
                if flag not in r["flags"]:
                    r["flags"].append(flag)
    return records

# ─────────────────────────────────────────────────────────────────────────────
# GHL CSV  (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def export_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "First Name","Last Name","Mailing Address","Mailing City",
        "Mailing State","Mailing Zip","Property Address","Property City",
        "Property State","Property Zip","Lead Type","Document Type",
        "Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            owner = r.get("owner", "")
            parts = owner.split(",", 1) if "," in owner else owner.rsplit(" ", 1)
            first = parts[1].strip() if len(parts) > 1 else ""
            last  = parts[0].strip()
            w.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       r.get("mail_address",""),
                "Mailing City":          r.get("mail_city",""),
                "Mailing State":         r.get("mail_state",""),
                "Mailing Zip":           r.get("mail_zip",""),
                "Property Address":      r.get("prop_address",""),
                "Property City":         r.get("prop_city",""),
                "Property State":        r.get("prop_state",""),
                "Property Zip":          r.get("prop_zip",""),
                "Lead Type":             r.get("cat_label",""),
                "Document Type":         r.get("doc_type",""),
                "Date Filed":            r.get("filed",""),
                "Document Number":       r.get("doc_num",""),
                "Amount/Debt Owed":      r.get("amount",""),
                "Seller Score":          r.get("score",0),
                "Motivated Seller Flags":"; ".join(r.get("flags",[])),
                "Source":               "Shelby County Register of Deeds",
                "Public Records URL":    r.get("clerk_url",""),
            })
    print(f"[export] GHL CSV → {path}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    start_dt, end_dt = date_range()
    week_ago = datetime.utcnow() - timedelta(days=7)
    print(f"[run] date range {start_dt.date()} → {end_dt.date()}")

    records = []

    # 1. Playwright scrape (correct portal)
    try:
        records = await scrape_clerk(start_dt, end_dt)
        print(f"[clerk] playwright total: {len(records)}")
    except Exception as exc:
        print(f"[clerk] playwright failed: {exc}")
        traceback.print_exc()

    # 2. Fallback HTTP scrape
    try:
        fallback = scrape_clerk_requests(start_dt, end_dt)
        existing = {r["doc_num"] for r in records if r["doc_num"]}
        added = [r for r in fallback if r["doc_num"] not in existing]
        records.extend(added)
        print(f"[fallback] added {len(added)} new records")
    except Exception as exc:
        print(f"[fallback] error: {exc}")

    # 3. Parcel enrichment
    owner_map = {}
    try:
        owner_map = download_parcel_dbf()
    except Exception as exc:
        print(f"[parcel] download error: {exc}")
    for r in records:
        try:
            enrich_from_parcel(r, owner_map)
        except Exception:
            pass

    # 4. Score & sort
    for r in records:
        try:
            score_record(r, week_ago)
        except Exception:
            pass
    records = apply_lp_fc_combo(records)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 5. Save
    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source":     "Shelby County Register of Deeds",
        "date_range": {"start": start_dt.strftime("%Y-%m-%d"), "end": end_dt.strftime("%Y-%m-%d")},
        "total":       len(records),
        "with_address": with_address,
        "records":     records,
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"[save] {path} ({len(records)} records)")

    export_ghl_csv(records, Path("data/leads_ghl.csv"))
    print(f"\n✅ Done — {len(records)} records, {with_address} with address")


if __name__ == "__main__":
    asyncio.run(main())
