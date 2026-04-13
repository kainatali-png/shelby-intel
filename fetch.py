"""
Shelby County, TN — Motivated Seller Lead Scraper
Collects: LP, NOFC, TAXDEED, JUD/CCJ/DRJUD, LNCORPTX/LNIRS/LNFED,
          LN/LNMECH/LNHOA, MEDLN, PRO, NOC, RELLP
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
import urllib.parse
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── optional dbfread ──────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False
    print("[WARN] dbfread not installed – parcel enrichment disabled")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_URL       = "https://register.shelby.tn.us"          # actual register of deeds
CLERK_SEARCH    = f"{CLERK_URL}/ords/f?p=150:1"             # public search portal
APPRAISER_URL   = "https://shelbycountypropertyappraiser.org"
OUTPUT_PATHS    = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
RETRY_ATTEMPTS  = 3
RETRY_DELAY     = 3   # seconds

# doc type → category mapping
DOC_TYPES = {
    "LP":       ("LP",       "Lis Pendens"),
    "NOFC":     ("NOFC",     "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED",  "Tax Deed"),
    "JUD":      ("JUD",      "Judgment"),
    "CCJ":      ("JUD",      "Certified Judgment"),
    "DRJUD":    ("JUD",      "Domestic Judgment"),
    "LNCORPTX": ("LNTAX",    "Corp Tax Lien"),
    "LNIRS":    ("LNTAX",    "IRS Lien"),
    "LNFED":    ("LNTAX",    "Federal Lien"),
    "LN":       ("LN",       "Lien"),
    "LNMECH":   ("LN",       "Mechanic Lien"),
    "LNHOA":    ("LN",       "HOA Lien"),
    "MEDLN":    ("LN",       "Medicaid Lien"),
    "PRO":      ("PRO",      "Probate"),
    "NOC":      ("NOC",      "Notice of Commencement"),
    "RELLP":    ("RELLP",    "Release Lis Pendens"),
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def retry(fn, *args, attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY, **kwargs):
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if i == attempts - 1:
                raise
            print(f"  [retry {i+1}/{attempts}] {exc}")
            time.sleep(delay)


def safe_float(val) -> Optional[float]:
    if not val:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(val)))
    except Exception:
        return None


def parse_date(s: str) -> Optional[str]:
    """Return ISO date string or None."""
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
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
    """Return list of lookup keys from a name string."""
    n = full_name.upper().strip()
    variants = [n]
    # "LAST, FIRST" → "FIRST LAST"
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
# PARCEL DATA  (Shelby County Assessor bulk DBF)
# ─────────────────────────────────────────────────────────────────────────────

def download_parcel_dbf() -> dict:
    """
    Attempt to download & parse the Shelby County assessor bulk parcel file.
    Returns owner→parcel dict.
    """
    if not HAS_DBF:
        return {}

    # Known candidate URLs for the bulk parcel export
    candidate_urls = [
        "https://www.shelbycountypropertyappraiser.org/downloads/parcel_data.zip",
        "https://shelbycountypropertyappraiser.org/Downloads/ParcelData.zip",
        "https://shelbycountypropertyappraiser.org/downloads/Parcel.zip",
        "https://shelbycountypropertyappraiser.org/downloads/parcels.zip",
    ]

    headers = {"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)"}

    # Try to discover actual URL from the downloads page
    try:
        r = requests.get(
            f"{APPRAISER_URL}/Downloads",
            headers=headers, timeout=30
        )
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"parcel|data|dbf|zip", href, re.I):
                full = href if href.startswith("http") else APPRAISER_URL + "/" + href.lstrip("/")
                if full not in candidate_urls:
                    candidate_urls.insert(0, full)
    except Exception as exc:
        print(f"[parcel] discovery failed: {exc}")

    raw_bytes = None
    for url in candidate_urls:
        try:
            print(f"[parcel] trying {url}")
            resp = requests.get(url, headers=headers, timeout=60, stream=True)
            if resp.status_code == 200 and len(resp.content) > 1000:
                raw_bytes = resp.content
                print(f"[parcel] downloaded {len(raw_bytes):,} bytes from {url}")
                break
        except Exception as exc:
            print(f"[parcel] {url} → {exc}")

    if not raw_bytes:
        print("[parcel] no bulk file found – skipping enrichment")
        return {}

    # Extract DBF from zip
    owner_map: dict = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            dbf_names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
            if not dbf_names:
                print("[parcel] no DBF inside zip")
                return {}
            dbf_name = dbf_names[0]
            print(f"[parcel] reading {dbf_name}")
            dbf_bytes = zf.read(dbf_name)

        # write temp file (dbfread needs file path)
        tmp = Path("/tmp/parcels.dbf")
        tmp.write_bytes(dbf_bytes)

        col_map = {
            "owner":       ["OWNER", "OWN1", "OWNERNAME"],
            "site_addr":   ["SITE_ADDR", "SITEADDR", "SITEADDRESS"],
            "site_city":   ["SITE_CITY", "SITECITY"],
            "site_zip":    ["SITE_ZIP", "SITEZIP"],
            "mail_addr":   ["ADDR_1", "MAILADR1", "MAILADDR1", "MAILADDRESS"],
            "mail_city":   ["CITY", "MAILCITY"],
            "mail_state":  ["STATE", "MAILSTATE"],
            "mail_zip":    ["ZIP", "MAILZIP"],
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
# CLERK PORTAL  (Shelby County Register of Deeds — Playwright)
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_clerk(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """
    Scrape the Shelby County Register of Deeds public search portal.
    URL: https://register.shelby.tn.us
    Uses Playwright for JS-heavy APEX / ORDS portal.
    """
    records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    target_codes = list(DOC_TYPES.keys())

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx     = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122 Safari/537.36"
        )
        page = await ctx.new_page()

        # ── navigate to search page ──────────────────────────────────────────
        print(f"[clerk] navigating to {CLERK_SEARCH}")
        try:
            await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=60000)
        except PWTimeout:
            await page.goto(CLERK_SEARCH, timeout=60000)

        await page.wait_for_timeout(2000)

        # ── try to find search form inputs ───────────────────────────────────
        # The APEX portal typically has date range + doc type fields
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Look for any iframe that might host the real search
        iframes = soup.find_all("iframe")
        if iframes:
            src = iframes[0].get("src", "")
            if src:
                iframe_url = src if src.startswith("http") else CLERK_URL + src
                print(f"[clerk] following iframe → {iframe_url}")
                try:
                    await page.goto(iframe_url, wait_until="networkidle", timeout=60000)
                    await page.wait_for_timeout(2000)
                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                except Exception as e:
                    print(f"[clerk] iframe nav failed: {e}")

        # ── fill date range ──────────────────────────────────────────────────
        date_inputs = await page.query_selector_all("input[type='text']")
        filled = 0
        for inp in date_inputs:
            ph = (await inp.get_attribute("placeholder") or "").lower()
            nm = (await inp.get_attribute("name") or "").lower()
            label_text = ph + nm
            if "start" in label_text or "from" in label_text or "begin" in label_text or filled == 0:
                await inp.fill(start_str)
                filled += 1
            elif "end" in label_text or "to" in label_text or filled == 1:
                await inp.fill(end_str)
                filled += 1
            if filled >= 2:
                break

        # ── iterate each doc type ────────────────────────────────────────────
        for code in target_codes:
            try:
                batch = await _search_doc_type(page, code, start_str, end_str)
                records.extend(batch)
                print(f"[clerk] {code}: {len(batch)} records")
            except Exception as exc:
                print(f"[clerk] {code} error: {exc}")
                traceback.print_exc()

        await browser.close()

    return records


async def _search_doc_type(page, code: str, start_str: str, end_str: str) -> list[dict]:
    """Search for a single document type and return raw records."""
    records = []
    cat, cat_label = DOC_TYPES[code]

    # Try filling a doc-type selector
    selects = await page.query_selector_all("select")
    for sel in selects:
        opts = await sel.query_selector_all("option")
        for opt in opts:
            val = (await opt.get_attribute("value") or "").upper()
            txt = (await opt.inner_text() or "").upper()
            if code in val or code in txt:
                await sel.select_option(value=await opt.get_attribute("value"))
                break

    # Submit search
    for btn_text in ["Search", "Find", "Submit", "Go"]:
        btn = await page.query_selector(f"button:has-text('{btn_text}'), input[value='{btn_text}']")
        if btn:
            await btn.click()
            await page.wait_for_timeout(3000)
            break

    # Parse results page
    html  = await page.content()
    soup  = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return records

    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    for tr in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cells:
            continue
        row = dict(zip(headers, cells))

        # Find direct URL from link in row
        link_tag = tr.find("a", href=True)
        clerk_url = ""
        if link_tag:
            href = link_tag["href"]
            clerk_url = href if href.startswith("http") else CLERK_URL + "/" + href.lstrip("/")

        doc_num = _pick(row, ["doc #", "doc no", "document number", "instrument", "book/page"])
        filed   = _pick(row, ["filed", "date filed", "record date", "file date"])
        owner   = _pick(row, ["grantor", "owner", "party 1"])
        grantee = _pick(row, ["grantee", "party 2"])
        amount  = _pick(row, ["amount", "consideration"])
        legal   = _pick(row, ["legal", "legal description", "desc"])

        records.append({
            "doc_num":    doc_num or "",
            "doc_type":   code,
            "filed":      parse_date(filed) or "",
            "cat":        cat,
            "cat_label":  cat_label,
            "owner":      owner or "",
            "grantee":    grantee or "",
            "amount":     safe_float(amount),
            "legal":      legal or "",
            "prop_address":  "",
            "prop_city":     "Memphis",
            "prop_state":    "TN",
            "prop_zip":      "",
            "mail_address":  "",
            "mail_city":     "",
            "mail_state":    "",
            "mail_zip":      "",
            "clerk_url":     clerk_url,
            "flags":         [],
            "score":         0,
        })

    return records


def _pick(row: dict, keys: list) -> str:
    for k in keys:
        for rk, rv in row.items():
            if k in rk and rv:
                return str(rv).strip()
    return ""

# ─────────────────────────────────────────────────────────────────────────────
# FALLBACK  — requests+BeautifulSoup direct search
# (used when Playwright finds nothing, or as supplemental source)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_clerk_requests(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """
    Fallback HTTP scraper that hits the public record search endpoint directly.
    """
    records = []
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)"})

    # Shelby County Register of Deeds ORDS/APEX search endpoint
    search_url = f"{CLERK_URL}/ords/f"

    for code, (cat, cat_label) in DOC_TYPES.items():
        for attempt in range(RETRY_ATTEMPTS):
            try:
                params = {
                    "p": "150:5",
                    "P5_DOC_TYPE": code,
                    "P5_BEG_DATE": start_dt.strftime("%m/%d/%Y"),
                    "P5_END_DATE": end_dt.strftime("%m/%d/%Y"),
                }
                r = session.get(search_url, params=params, timeout=30)
                soup = BeautifulSoup(r.text, "lxml")

                table = soup.find("table", {"class": re.compile(r"t-Report|apexir", re.I)}) \
                     or soup.find("table")
                if not table:
                    break

                rows = table.find_all("tr")
                hdrs = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])] if rows else []

                for tr in rows[1:]:
                    cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                    if not cells:
                        continue
                    row = dict(zip(hdrs, cells)) if hdrs else {}

                    link_tag = tr.find("a", href=True)
                    clerk_url = ""
                    if link_tag:
                        href = link_tag["href"]
                        clerk_url = href if href.startswith("http") else CLERK_URL + "/" + href.lstrip("/")

                    records.append({
                        "doc_num":   _pick(row, ["doc", "instrument"]) or (cells[0] if cells else ""),
                        "doc_type":  code,
                        "filed":     parse_date(_pick(row, ["date", "filed"])) or "",
                        "cat":       cat,
                        "cat_label": cat_label,
                        "owner":     _pick(row, ["grantor", "owner", "party"]) or "",
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
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def score_record(rec: dict, week_ago: datetime) -> dict:
    flags  = []
    score  = 30  # base

    cat    = rec.get("cat", "")
    code   = rec.get("doc_type", "")
    amount = rec.get("amount") or 0
    filed  = rec.get("filed", "")
    owner  = rec.get("owner", "").upper()

    # Flag assignment
    if cat == "LP":
        flags.append("Lis pendens")
        score += 10
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
        score += 10
    if cat in ("LP", "NOFC") and any(r.get("owner") == rec.get("owner")
                                     and r.get("cat") in ("LP","NOFC")
                                     for r in [rec]):  # combo handled below
        pass
    if cat == "JUD":
        flags.append("Judgment lien")
        score += 10
    if cat == "LNTAX":
        flags.append("Tax lien")
        score += 10
    if code == "LNMECH":
        flags.append("Mechanic lien")
        score += 10
    if code == "LNHOA":
        score += 5
    if cat == "PRO":
        flags.append("Probate / estate")
        score += 10
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bLLP\b", owner):
        flags.append("LLC / corp owner")
        score += 10

    # Amount bonuses
    if amount and amount > 100_000:
        score += 15
        flags.append("High-value debt")
    elif amount and amount > 50_000:
        score += 10

    # New this week
    if filed:
        try:
            filed_dt = datetime.strptime(filed, "%Y-%m-%d")
            if filed_dt >= week_ago:
                score += 5
                flags.append("New this week")
        except Exception:
            pass

    # Has address
    if rec.get("prop_address"):
        score += 5
        flags.append("Has address")

    rec["flags"] = list(dict.fromkeys(flags))
    rec["score"] = min(score, 100)
    return rec


def apply_lp_fc_combo(records: list[dict]) -> list[dict]:
    """Grant +20 bonus to owners who have both LP and NOFC."""
    owner_cats: dict = {}
    for r in records:
        o = r.get("owner", "").upper()
        if o:
            owner_cats.setdefault(o, set()).add(r.get("cat"))
    for r in records:
        o = r.get("owner", "").upper()
        if o and {"LP", "NOFC"}.issubset(owner_cats.get(o, set())):
            r["score"] = min(r["score"] + 20, 100)
            if "Lis pendens" not in r["flags"]:
                r["flags"].append("Lis pendens")
            if "Pre-foreclosure" not in r["flags"]:
                r["flags"].append("Pre-foreclosure")
    return records

# ─────────────────────────────────────────────────────────────────────────────
# GHL CSV EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def export_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
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
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", ""),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", ""),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         r.get("doc_type", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                "Shelby County Register of Deeds",
                "Public Records URL":    r.get("clerk_url", ""),
            })
    print(f"[export] GHL CSV → {path}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    start_dt, end_dt = date_range()
    week_ago = datetime.utcnow() - timedelta(days=7)
    print(f"[run] date range {start_dt.date()} → {end_dt.date()}")

    # 1. Playwright scrape
    records = []
    try:
        records = await scrape_clerk(start_dt, end_dt)
        print(f"[clerk] playwright total: {len(records)}")
    except Exception as exc:
        print(f"[clerk] playwright failed: {exc}")
        traceback.print_exc()

    # 2. Fallback requests scrape (merge, deduplicate by doc_num)
    try:
        fallback = scrape_clerk_requests(start_dt, end_dt)
        existing_nums = {r["doc_num"] for r in records if r["doc_num"]}
        added = [r for r in fallback if r["doc_num"] not in existing_nums]
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

    # 4. Score
    for r in records:
        try:
            score_record(r, week_ago)
        except Exception:
            pass
    records = apply_lp_fc_combo(records)

    # 5. Sort by score desc
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 6. Build output payload
    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Shelby County Register of Deeds",
        "date_range":   {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end":   end_dt.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }

    # 7. Save JSON
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"[save] {path} ({len(records)} records)")

    # 8. GHL CSV
    export_ghl_csv(records, Path("data/leads_ghl.csv"))

    print(f"\n✅ Done — {len(records)} records, {with_address} with address")


if __name__ == "__main__":
    asyncio.run(main())
