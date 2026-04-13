"""
Shelby County, TN — Motivated Seller Lead Scraper
v4 — Playwright with correct form fields (checkboxes + start_date/end_date)
     Downloads CSV directly from the site for reliability
"""

import asyncio, csv, io, json, os, re, traceback, zipfile
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ── CONFIG ────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE    = "https://search.register.shelby.tn.us"
CLERK_SEARCH  = f"{CLERK_BASE}/search/index.php"
OUTPUT_PATHS  = [Path("dashboard/records.json"), Path("data/records.json")]
DEBUG_DIR     = Path("data/debug")

# Map our code → the exact checkbox label text on the website
DOC_TYPE_LABELS = {
    "LP":      ("LP",     "Lis Pendens",           "LIS PENDENS"),
    "NOFC":    ("NOFC",   "Notice of Foreclosure", "NOTICE OF FORECLOSURE"),
    "TAXDEED": ("TAXDEED","Tax Deed",               "TAX DEED"),
    "JUD":     ("JUD",    "Judgment",               "JUDGMENT"),
    "CCJ":     ("JUD",    "Certified Judgment",     "CERTIFIED JUDGMENT"),
    "LNFED":   ("LNTAX",  "Federal Tax Lien",       "FEDERAL TAX LIEN"),
    "LNIRS":   ("LNTAX",  "IRS Lien",               "IRS LIEN"),
    "LN":      ("LN",     "Lien",                   "LIEN"),
    "LNMECH":  ("LN",     "Mechanic Lien",          "MECHANIC'S LIEN"),
    "PRO":     ("PRO",    "Probate",                "PROBATE"),
    "NOC":     ("NOC",    "Notice of Commencement", "NOTICE OF COMMENCEMENT"),
    "RELLP":   ("RELLP",  "Release Lis Pendens",    "RELEASE OF LIS PENDENS"),
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(re.sub(r"[^\d.]", "", str(v))) if v else None
    except: return None

def parse_date(s):
    if not s: return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
        try: return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except: pass
    return s.strip() or None

def date_range():
    end = datetime.utcnow()
    return end - timedelta(days=LOOKBACK_DAYS), end

def name_variants(n):
    n = n.upper().strip()
    v = [n]
    if "," in n:
        p = [x.strip() for x in n.split(",", 1)]
        v += [f"{p[1]} {p[0]}", n.replace(",", "")]
    else:
        t = n.split()
        if len(t) >= 2:
            v += [f"{t[-1]} {' '.join(t[:-1])}", f"{t[-1]}, {' '.join(t[:-1])}"]
    return list(dict.fromkeys(v))

def _pick(row, keys):
    for k in keys:
        for rk, rv in row.items():
            if k in rk and rv: return str(rv).strip()
    return ""

def save_debug(name, content, ext="html"):
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    (DEBUG_DIR / f"{name}.{ext}").write_text(
        str(content), encoding="utf-8", errors="replace"
    )
    print(f"[debug] saved {name}.{ext}")

# ── MAIN SCRAPER ──────────────────────────────────────────────────────────────
async def scrape_clerk(start_dt, end_dt):
    all_records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = await ctx.new_page()

        # ── Load the search page and wait for JS to finish ────────────────────
        print(f"[clerk] loading search page...")
        try:
            await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=60000)
        except PWTimeout:
            await page.goto(CLERK_SEARCH, timeout=60000)
        await page.wait_for_timeout(3000)

        # Save debug screenshot
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        await page.screenshot(path=str(DEBUG_DIR / "page_loaded.png"), full_page=True)
        page_html = await page.content()
        save_debug("page_loaded", page_html)

        # Log all checkboxes visible on page
        checkboxes = await page.query_selector_all("input[type='checkbox']")
        print(f"[debug] found {len(checkboxes)} checkboxes on page")

        # Log all input fields
        inputs = await page.query_selector_all("input[type='text'], input[type='date']")
        for inp in inputs:
            name = await inp.get_attribute("name") or ""
            id_  = await inp.get_attribute("id") or ""
            print(f"[debug] input: name={name} id={id_}")

        # ── Search each document type one at a time ───────────────────────────
        for code, (cat, cat_label, checkbox_label) in DOC_TYPE_LABELS.items():
            try:
                print(f"[clerk] searching {code} ({checkbox_label})...")
                records = await search_one_type(
                    page, ctx, code, cat, cat_label,
                    checkbox_label, start_str, end_str
                )
                all_records.extend(records)
                print(f"[clerk] {code}: {len(records)} records {'✓' if records else ''}")
            except Exception as e:
                print(f"[clerk] {code} error: {e}")
                traceback.print_exc()

        await browser.close()

    return all_records


async def search_one_type(page, ctx, code, cat, cat_label,
                          checkbox_label, start_str, end_str):
    """
    For each doc type:
    1. Press F9 (New Search) to reset the form
    2. Uncheck SELECT ALL
    3. Check only the matching instrument checkbox
    4. Fill in date range
    5. Press F2 (Search)
    6. Wait for results, then download CSV or parse HTML table
    """

    # ── Step 1: New Search (reset form) ──────────────────────────────────────
    await page.keyboard.press("F9")
    await page.wait_for_timeout(2000)

    # ── Step 2: Uncheck SELECT ALL ────────────────────────────────────────────
    try:
        select_all = await page.query_selector("input[type='checkbox']")
        if select_all:
            is_checked = await select_all.is_checked()
            if is_checked:
                await select_all.click()
                await page.wait_for_timeout(500)
    except Exception as e:
        print(f"[clerk] could not uncheck SELECT ALL: {e}")

    # Alternatively try by label text
    try:
        await page.click("text=SELECT ALL")
        await page.wait_for_timeout(500)
    except:
        pass

    # ── Step 3: Check our target instrument type ───────────────────────────────
    checked = False
    # Try by label text (case-insensitive partial match)
    try:
        label = await page.query_selector(f"text={checkbox_label}")
        if label:
            await label.click()
            await page.wait_for_timeout(300)
            checked = True
            print(f"[clerk] ✓ checked '{checkbox_label}' by text")
    except:
        pass

    if not checked:
        # Try finding the checkbox near that label text
        try:
            labels = await page.query_selector_all("label, td")
            for lbl in labels:
                txt = (await lbl.inner_text()).strip().upper()
                if checkbox_label in txt:
                    await lbl.click()
                    await page.wait_for_timeout(300)
                    checked = True
                    print(f"[clerk] ✓ checked via label: {txt}")
                    break
        except:
            pass

    if not checked:
        print(f"[clerk] ⚠ could not find checkbox for '{checkbox_label}' — skipping")
        return []

    # ── Step 4: Fill in date range ────────────────────────────────────────────
    date_filled = False
    # Try known field names/ids
    for field_name in ["start_date", "beg_date", "begin_date", "startDate"]:
        try:
            field = await page.query_selector(f"input[name='{field_name}'], input[id='{field_name}']")
            if field:
                await field.triple_click()
                await field.type(start_str)
                date_filled = True
                print(f"[clerk] ✓ filled begin date field: {field_name}")
                break
        except:
            pass

    for field_name in ["end_date", "endDate", "stop_date"]:
        try:
            field = await page.query_selector(f"input[name='{field_name}'], input[id='{field_name}']")
            if field:
                await field.triple_click()
                await field.type(end_str)
                print(f"[clerk] ✓ filled end date field: {field_name}")
                break
        except:
            pass

    if not date_filled:
        # Fallback: find all text inputs and fill by position/placeholder
        try:
            all_inputs = await page.query_selector_all("input[type='text']")
            for inp in all_inputs:
                ph = (await inp.get_attribute("placeholder") or "").upper()
                nm = (await inp.get_attribute("name") or "").upper()
                if "BEGIN" in ph or "BEGIN" in nm or "START" in nm or "START" in ph:
                    await inp.triple_click()
                    await inp.type(start_str)
                if "END" in ph or "END" in nm:
                    await inp.triple_click()
                    await inp.type(end_str)
        except Exception as e:
            print(f"[clerk] date fill fallback error: {e}")

    await page.wait_for_timeout(500)

    # ── Step 5: Submit search ─────────────────────────────────────────────────
    await page.keyboard.press("F2")
    await page.wait_for_timeout(5000)

    # Also try clicking Search button if F2 didn't work
    try:
        html_check = await page.content()
        if "Record Count" not in html_check and "entries" not in html_check:
            for btn_text in ["Search", "Search (F2)"]:
                try:
                    await page.click(f"text={btn_text}", timeout=3000)
                    await page.wait_for_timeout(4000)
                    break
                except:
                    pass
    except:
        pass

    # Wait for results to load
    try:
        await page.wait_for_selector("text=Record Count", timeout=15000)
    except:
        pass
    await page.wait_for_timeout(2000)

    html = await page.content()

    # ── Step 6: Try to download CSV first (most reliable) ─────────────────────
    csv_records = await try_download_csv(page, ctx, code, cat, cat_label)
    if csv_records:
        return csv_records

    # ── Step 7: Fall back to parsing HTML table ───────────────────────────────
    if code == "LP":
        save_debug(f"results_{code}", html)

    soup = BeautifulSoup(html, "lxml")
    return parse_results_table(soup, code, cat, cat_label)


async def try_download_csv(page, ctx, code, cat, cat_label):
    """Click 'Download results into CSV file' and parse the downloaded CSV."""
    try:
        # Check if the download link exists
        dl_link = await page.query_selector("text=Download results into CSV file")
        if not dl_link:
            return []

        # Capture the download
        async with ctx.expect_download(timeout=30000) as dl_info:
            await dl_link.click()
        download = await dl_info.value

        # Save to temp file
        tmp_path = f"/tmp/shelby_{code}.csv"
        await download.save_as(tmp_path)
        print(f"[clerk] ✓ downloaded CSV for {code}")

        # Parse CSV
        records = []
        with open(tmp_path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Normalize keys to lowercase
                row = {k.lower().strip(): v.strip() if v else ""
                       for k, v in row.items()}

                owner = (row.get("grantor") or row.get("owner") or
                         row.get("grantor name") or "")
                doc_num = (row.get("record info") or row.get("instrument #") or
                           row.get("instrument number") or row.get("doc #") or "")
                filed = parse_date(row.get("rec. date") or row.get("date") or
                                   row.get("recorded date") or "")
                prop_desc = (row.get("prop. description") or
                             row.get("property description") or "")
                amount = safe_float(row.get("consideration") or
                                    row.get("amount") or "")
                instr_type = (row.get("instrument type") or code)

                if not owner and not doc_num:
                    continue

                # Parse address from prop description (often "123 MAIN ST MEMPHIS TN 38116")
                prop_address, prop_city, prop_state, prop_zip = parse_address(prop_desc)

                records.append({
                    "doc_num":      doc_num,
                    "doc_type":     code,
                    "filed":        filed or "",
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "owner":        owner,
                    "grantee":      row.get("grantee") or row.get("grantee name") or "",
                    "amount":       amount,
                    "legal":        prop_desc,
                    "prop_address": prop_address,
                    "prop_city":    prop_city or "Memphis",
                    "prop_state":   prop_state or "TN",
                    "prop_zip":     prop_zip,
                    "mail_address": "", "mail_city": "",
                    "mail_state":   "", "mail_zip": "",
                    "clerk_url":    "",
                    "flags": [], "score": 0,
                })
        print(f"[clerk] parsed {len(records)} rows from CSV for {code}")
        return records

    except Exception as e:
        print(f"[clerk] CSV download {code} → {e}")
        return []


def parse_address(desc):
    """Try to extract address components from a property description string."""
    if not desc:
        return "", "", "", ""
    desc = desc.strip().upper()
    # Pattern: number + street + city + state + zip
    m = re.search(
        r"(\d+\s+[\w\s]+?)\s+(MEMPHIS|BARTLETT|GERMANTOWN|COLLIERVILLE|ARLINGTON|MILLINGTON|CORDOVA)"
        r"[\s,]*(TN)?[\s,]*(\d{5})?", desc
    )
    if m:
        return m.group(1).strip(), m.group(2).strip(), "TN", m.group(4) or ""
    # Return full desc as address if it looks like one
    if re.match(r"^\d+\s", desc):
        return desc, "Memphis", "TN", ""
    return "", "", "", ""


def parse_results_table(soup, code, cat, cat_label):
    """Parse the HTML results table as fallback."""
    records = []
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th", "td"])]
        if not any(k in " ".join(hdrs)
                   for k in ["record", "grantor", "instrument", "rec. date", "date"]):
            continue
        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells or all(c == "" for c in cells):
                continue
            row  = dict(zip(hdrs, cells))
            link = tr.find("a", href=True)
            href = link["href"] if link else ""
            clerk_url = (
                href if href.startswith("http")
                else (CLERK_BASE + "/" + href.lstrip("/")) if href else ""
            )
            doc_num = (_pick(row, ["record info","instrument","doc","instr","number"])
                       or cells[0])
            owner   = _pick(row, ["grantor","owner","party 1","name"])
            filed   = parse_date(_pick(row, ["rec. date","date","filed"]))
            prop_desc = _pick(row, ["prop. description","property","description","legal"])
            prop_address, prop_city, prop_state, prop_zip = parse_address(prop_desc)

            if not doc_num and not owner:
                continue
            records.append({
                "doc_num":      doc_num or "",
                "doc_type":     code,
                "filed":        filed or "",
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner or "",
                "grantee":      _pick(row, ["grantee","party 2"]) or "",
                "amount":       safe_float(_pick(row, ["consideration","amount"])),
                "legal":        prop_desc,
                "prop_address": prop_address,
                "prop_city":    prop_city or "Memphis",
                "prop_state":   prop_state or "TN",
                "prop_zip":     prop_zip,
                "mail_address": "", "mail_city": "",
                "mail_state":   "", "mail_zip": "",
                "clerk_url":    clerk_url,
                "flags": [], "score": 0,
            })
    return records

# ── PARCEL ENRICHMENT ─────────────────────────────────────────────────────────
def download_parcel_dbf():
    if not HAS_DBF:
        return {}
    hdrs = {"User-Agent": "Mozilla/5.0"}
    urls = [
        "https://www.assessor.shelby.tn.us/downloads/parcel_data.zip",
        "https://www.assessor.shelby.tn.us/downloads/Parcel.zip",
        "https://www.assessor.shelby.tn.us/downloads/parcels.zip",
    ]
    raw = None
    for url in urls:
        try:
            print(f"[parcel] trying {url}")
            r = requests.get(url, headers=hdrs, timeout=90, stream=True)
            if r.status_code == 200 and len(r.content) > 1000:
                raw = r.content
                break
        except Exception as e:
            print(f"[parcel] {url} → {e}")
    if not raw:
        print("[parcel] no bulk file — skipping")
        return {}

    owner_map = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            dbf_names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
            if not dbf_names:
                return {}
            tmp = Path("/tmp/parcels.dbf")
            tmp.write_bytes(zf.read(dbf_names[0]))
        col_map = {
            "owner":     ["OWNER","OWN1","OWNERNAME"],
            "site_addr": ["SITE_ADDR","SITEADDR","ADDRESS"],
            "site_city": ["SITE_CITY","SITECITY","CITY"],
            "site_zip":  ["SITE_ZIP","SITEZIP","ZIP"],
            "mail_addr": ["ADDR_1","MAILADR1","MAILADDR1"],
            "mail_city": ["MAILCITY","MAIL_CITY"],
            "mail_state":["STATE","MAILSTATE"],
            "mail_zip":  ["MAILZIP","MAIL_ZIP"],
        }
        def gc(row, keys):
            for k in keys:
                if k in row and row[k]: return str(row[k]).strip()
            return ""
        for rec in DBF(str(tmp), load=True, ignore_missing_memofile=True):
            try:
                owner = gc(rec, col_map["owner"])
                if not owner:
                    continue
                p = {
                    "prop_address": gc(rec, col_map["site_addr"]),
                    "prop_city":    gc(rec, col_map["site_city"]) or "Memphis",
                    "prop_state":   "TN",
                    "prop_zip":     gc(rec, col_map["site_zip"]),
                    "mail_address": gc(rec, col_map["mail_addr"]),
                    "mail_city":    gc(rec, col_map["mail_city"]),
                    "mail_state":   gc(rec, col_map["mail_state"]) or "TN",
                    "mail_zip":     gc(rec, col_map["mail_zip"]),
                }
                for v in name_variants(owner):
                    if v not in owner_map:
                        owner_map[v] = p
            except:
                pass
        print(f"[parcel] {len(owner_map):,} entries")
    except Exception as e:
        print(f"[parcel] error: {e}")
    return owner_map

def enrich(rec, owner_map):
    for v in name_variants(rec.get("owner", "")):
        if v in owner_map:
            p = owner_map[v]
            rec.update({k: val for k, val in p.items() if not rec.get(k)})
            break
    return rec

# ── SCORING ───────────────────────────────────────────────────────────────────
def score_record(rec, week_ago):
    flags, score = [], 30
    cat   = rec.get("cat", "")
    code  = rec.get("doc_type", "")
    amt   = rec.get("amount") or 0
    filed = rec.get("filed", "")
    owner = rec.get("owner", "").upper()
    if cat == "LP":      flags.append("Lis pendens");      score += 10
    if cat == "NOFC":    flags.append("Pre-foreclosure");  score += 10
    if cat == "JUD":     flags.append("Judgment lien");    score += 10
    if cat == "LNTAX":   flags.append("Tax lien");         score += 10
    if code == "LNMECH": flags.append("Mechanic lien");    score += 10
    if cat == "PRO":     flags.append("Probate / estate"); score += 10
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b", owner):
        flags.append("LLC/corp owner"); score += 10
    if amt > 100000:  score += 15; flags.append("High-value debt")
    elif amt > 50000: score += 10
    if filed:
        try:
            if datetime.strptime(filed, "%Y-%m-%d") >= week_ago:
                score += 5; flags.append("New this week")
        except:
            pass
    if rec.get("prop_address"):
        score += 5; flags.append("Has address")
    rec["flags"] = list(dict.fromkeys(flags))
    rec["score"] = min(score, 100)
    return rec

def apply_combo(records):
    oc = {}
    for r in records:
        o = r.get("owner", "").upper()
        if o: oc.setdefault(o, set()).add(r.get("cat"))
    for r in records:
        o = r.get("owner", "").upper()
        if o and {"LP", "NOFC"}.issubset(oc.get(o, set())):
            r["score"] = min(r["score"] + 20, 100)
    return records

# ── GHL CSV ───────────────────────────────────────────────────────────────────
def export_ghl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State",
        "Mailing Zip","Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            own = r.get("owner", "")
            pts = own.split(",", 1) if "," in own else own.rsplit(" ", 1)
            w.writerow({
                "First Name":             pts[1].strip() if len(pts) > 1 else "",
                "Last Name":              pts[0].strip(),
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", ""),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", ""),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", ""),
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Shelby County Register of Deeds",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    print(f"[export] GHL CSV → {path}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    start_dt, end_dt = date_range()
    week_ago  = datetime.utcnow() - timedelta(days=7)
    print(f"[run] {start_dt.date()} → {end_dt.date()}")

    records = []
    try:
        records = await scrape_clerk(start_dt, end_dt)
        print(f"[clerk] total raw: {len(records)}")
    except Exception as e:
        print(f"[clerk] failed: {e}")
        traceback.print_exc()

    owner_map = {}
    try:
        owner_map = download_parcel_dbf()
    except Exception as e:
        print(f"[parcel] error: {e}")

    for r in records:
        try: enrich(r, owner_map)
        except: pass
    for r in records:
        try: score_record(r, week_ago)
        except: pass
    records = apply_combo(records)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    with_addr = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Shelby County Register of Deeds",
        "date_range":   {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end":   end_dt.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": with_addr,
        "records":      records,
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"[save] {path} ({len(records)} records)")

    export_ghl(records, Path("data/leads_ghl.csv"))
    print(f"\n✅ Done — {len(records)} records, {with_addr} with address")

if __name__ == "__main__":
    asyncio.run(main())
