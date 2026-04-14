"""
Shelby County, TN — Motivated Seller Lead Scraper
v6 — Robust: auto-detects form fields, tries HTTP POST fallback,
     handles new /search/ URL, and dumps full debug on any failure.
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
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE     = "https://search.register.shelby.tn.us"
# Try both known search URLs — we'll probe which one works
CLERK_SEARCH_URLS = [
    f"{CLERK_BASE}/search/index.php",
    f"{CLERK_BASE}/search/",
    f"{CLERK_BASE}/search/search.php",
]
OUTPUT_PATHS   = [Path("dashboard/records.json"), Path("data/records.json")]
DEBUG_DIR      = Path("data/debug")

# Instrument type codes used by the site
DOC_TYPES = {
    "LP":      ("LP",     "Lis Pendens"),
    "NOFC":    ("NOFC",   "Notice of Foreclosure"),
    "TAXDEED": ("TAXDEED","Tax Deed"),
    "JUD":     ("JUD",    "Judgment"),
    "CCJ":     ("JUD",    "Certified Judgment"),
    "LNFED":   ("LNTAX",  "Federal Tax Lien"),
    "LNIRS":   ("LNTAX",  "IRS Lien"),
    "LN":      ("LN",     "Lien"),
    "LNMECH":  ("LN",     "Mechanic Lien"),
    "PRO":     ("PRO",    "Probate"),
    "NOC":     ("NOC",    "Notice of Commencement"),
    "RELLP":   ("RELLP",  "Release Lis Pendens"),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
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

def parse_address(desc):
    if not desc: return "", "", "", ""
    desc = desc.strip().upper()
    m = re.search(
        r"(\d+\s+[\w\s]+?)\s+"
        r"(MEMPHIS|BARTLETT|GERMANTOWN|COLLIERVILLE|ARLINGTON|MILLINGTON|CORDOVA)"
        r"[\s,]*(TN)?[\s,]*(\d{5})?", desc
    )
    if m:
        return m.group(1).strip(), m.group(2).strip(), "TN", m.group(4) or ""
    if re.match(r"^\d+\s", desc):
        return desc, "Memphis", "TN", ""
    return "", "", "", ""

# ── PAGE PROBING ──────────────────────────────────────────────────────────────
async def find_working_url(page):
    """Try each candidate URL and return the one that loads a search form."""
    for url in CLERK_SEARCH_URLS:
        try:
            print(f"[probe] trying {url}")
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            if resp and resp.status < 400:
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                # Check for any form or date inputs
                has_form  = bool(soup.find("form"))
                has_input = bool(soup.find("input"))
                print(f"[probe] {url} → status={resp.status} form={has_form} inputs={has_input}")
                if has_form or has_input:
                    save_debug("initial_page", html)
                    return url, html
        except Exception as e:
            print(f"[probe] {url} error: {e}")
    return None, None


def audit_form(html):
    """
    Parse the HTML and return a dict describing every input/select found,
    so we can map our fields to the real field names.
    """
    soup = BeautifulSoup(html, "lxml")
    fields = {}
    for tag in soup.find_all(["input", "select", "textarea"]):
        name = tag.get("name") or tag.get("id") or ""
        typ  = tag.get("type", tag.name).lower()
        val  = tag.get("value", "")
        opts = [o.get("value","") for o in tag.find_all("option")] if tag.name=="select" else []
        if name:
            fields[name] = {"type": typ, "value": val, "options": opts}
    return fields


def detect_field(fields, *keywords):
    """Return the first field name whose key contains any of the keywords (case-insensitive)."""
    for kw in keywords:
        for name in fields:
            if kw.lower() in name.lower():
                return name
    return None


# ── HTTP FALLBACK ─────────────────────────────────────────────────────────────
def http_search(search_url, form_fields, code, cat, cat_label, start_str, end_str):
    """
    Direct HTTP POST to the search endpoint. Works when Playwright can't
    interact with the form due to JS-rendering issues.
    """
    sess = requests.Session()
    sess.headers.update(HEADERS)

    # First GET to get cookies/session
    try:
        sess.get(search_url, timeout=30)
    except Exception as e:
        print(f"[http] session GET failed: {e}")

    # Build payload using detected field names, with fallbacks
    start_field = detect_field(form_fields, "start", "begin", "beg") or "start_date"
    end_field   = detect_field(form_fields, "end", "stop")            or "end_date"
    inst_field  = detect_field(form_fields, "inst", "type", "doc")    or "instrument_type"

    payload = {
        start_field: start_str,
        end_field:   end_str,
        inst_field:  code,
        "submit":    "Search",
    }

    # Also add any hidden fields from the form
    for name, info in form_fields.items():
        if info["type"] == "hidden" and name not in payload:
            payload[name] = info["value"]

    print(f"[http] POST {search_url} with {payload}")
    try:
        r = sess.post(search_url, data=payload, timeout=60)
        save_debug(f"http_{code}", r.text)
        soup = BeautifulSoup(r.text, "lxml")
        records = parse_html_table(soup, code, cat, cat_label)
        if records:
            print(f"[http] ✓ {code}: {len(records)} records via HTTP POST")
        return records
    except Exception as e:
        print(f"[http] {code} failed: {e}")
        return []


# ── SCRAPER ───────────────────────────────────────────────────────────────────
async def scrape_clerk(start_dt, end_dt):
    all_records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        ctx = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 900},
            accept_downloads=True,
        )
        page = await ctx.new_page()

        # ── Step 1: Find the working search URL ───────────────────────────────
        working_url, initial_html = await find_working_url(page)
        if not working_url:
            print("[clerk] ❌ Could not load any search page. Dumping screenshot.")
            await page.screenshot(path=str(DEBUG_DIR / "failed_load.png"), full_page=True)
            await browser.close()
            return []

        print(f"[clerk] ✓ Using search URL: {working_url}")

        # ── Step 2: Audit the form to discover real field names ───────────────
        # Wait extra time for JS to fully render
        await page.wait_for_timeout(5000)
        html_after_js = await page.content()
        save_debug("page_after_js", html_after_js)
        await page.screenshot(path=str(DEBUG_DIR / "page_after_js.png"), full_page=True)

        form_fields = audit_form(html_after_js)
        print(f"[clerk] Form fields detected: {list(form_fields.keys())}")

        # Log all checkboxes and their values
        checkboxes = await page.query_selector_all("input[type='checkbox']")
        text_inputs = await page.query_selector_all("input[type='text']")
        selects = await page.query_selector_all("select")
        print(f"[debug] checkboxes={len(checkboxes)}, text_inputs={len(text_inputs)}, selects={len(selects)}")

        for sel in selects:
            sel_name = await sel.get_attribute("name") or ""
            sel_id = await sel.get_attribute("id") or ""
            opts = await sel.evaluate("el => Array.from(el.options).map(o => o.value + '=' + o.text)")
            print(f"[debug] <select name='{sel_name}' id='{sel_id}'> options: {opts[:10]}")

        for cb in checkboxes:
            cb_name = await cb.get_attribute("name") or ""
            cb_val  = await cb.get_attribute("value") or ""
            try:
                label = await cb.evaluate("el => { let l = document.querySelector('label[for=\"' + el.id + '\"]'); return l ? l.innerText : (el.parentElement ? el.parentElement.innerText : ''); }")
            except:
                label = ""
            print(f"[debug] checkbox name='{cb_name}' value='{cb_val}' label='{label.strip()[:60]}'")

        # ── Step 3: Try Playwright-based search for each doc type ─────────────
        for code, (cat, cat_label) in DOC_TYPES.items():
            try:
                records = await search_one_type(
                    page, ctx, working_url, form_fields,
                    code, cat, cat_label, start_str, end_str
                )
                if not records:
                    # Fallback: try direct HTTP POST
                    records = http_search(
                        working_url, form_fields,
                        code, cat, cat_label, start_str, end_str
                    )
                all_records.extend(records)
                status = "✓" if records else "⚠ 0"
                print(f"[clerk] {code}: {len(records)} records {status}")
            except Exception as e:
                print(f"[clerk] {code} error: {e}")
                traceback.print_exc()

        await browser.close()
    return all_records


async def wait_for_form(page):
    """Wait for the search form to be ready after each New Search."""
    for selector in [
        "input[name*='date']",
        "input[id*='date']",
        "input.hasDatepicker",
        "text=SELECT ALL",
        "text=Begin Date",
        "input[name*='start']",
        "form",
    ]:
        try:
            await page.wait_for_selector(selector, timeout=8000)
            return True
        except:
            pass
    await page.wait_for_timeout(3000)
    return False


async def search_one_type(page, ctx, working_url, form_fields,
                           code, cat, cat_label, start_str, end_str):
    # ── Reset form ────────────────────────────────────────────────────────────
    reset_done = False
    for sel in ["text=New Search", "a:has-text('New Search')", "text=New Search (F9)"]:
        try:
            await page.click(sel, timeout=4000)
            await page.wait_for_timeout(1500)
            reset_done = True
            break
        except:
            pass
    if not reset_done:
        try:
            await page.goto(working_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
        except:
            await page.keyboard.press("F9")
            await page.wait_for_timeout(2000)

    await wait_for_form(page)
    await page.wait_for_timeout(1000)

    # ── Handle instrument type: checkbox OR select ────────────────────────────
    checked = False

    # A) Try <select> for instrument type
    selects = await page.query_selector_all("select")
    for sel_el in selects:
        sel_name = (await sel_el.get_attribute("name") or "").lower()
        sel_id   = (await sel_el.get_attribute("id") or "").lower()
        if any(k in sel_name + sel_id for k in ["inst", "type", "doc", "kind"]):
            # Try to select our code as an option
            try:
                await sel_el.select_option(value=code)
                checked = True
                print(f"[clerk] ✓ selected {code} in <select name='{sel_name}'>")
                break
            except:
                # Try label match
                opts = await sel_el.evaluate(
                    "el => Array.from(el.options).map(o => ({v: o.value, t: o.text}))"
                )
                for opt in opts:
                    if code.upper() in opt["t"].upper() or code.upper() in opt["v"].upper():
                        try:
                            await sel_el.select_option(value=opt["v"])
                            checked = True
                            print(f"[clerk] ✓ selected by label match: {opt}")
                            break
                        except:
                            pass
                if checked:
                    break

    # B) Try checkboxes
    if not checked:
        # Uncheck all first
        all_cbs = await page.query_selector_all("input[type='checkbox']")
        for cb in all_cbs:
            try:
                if await cb.is_checked():
                    await cb.click()
                    await page.wait_for_timeout(50)
            except:
                pass

        # Find our specific checkbox
        for cb in all_cbs:
            try:
                val    = (await cb.get_attribute("value") or "").upper()
                cb_id  = (await cb.get_attribute("id") or "").upper()
                cb_name = (await cb.get_attribute("name") or "").upper()
                label  = await cb.evaluate(
                    "el => { "
                    "  let l = document.querySelector('label[for=\"' + el.id + '\"]'); "
                    "  return l ? l.innerText : (el.parentElement ? el.parentElement.innerText : ''); "
                    "}"
                )
                nearby = (val + cb_id + cb_name + label).upper()
                if code.upper() in nearby:
                    if not await cb.is_checked():
                        await cb.click()
                    checked = True
                    print(f"[clerk] ✓ checked checkbox for {code}: val={val}")
                    break
            except:
                pass

    if not checked:
        print(f"[clerk] ⚠ could not set instrument type for {code}")

    # ── Fill in date range — detect field names dynamically ───────────────────
    start_field = detect_field(form_fields, "start", "begin", "beg")
    end_field   = detect_field(form_fields, "end", "stop")

    # Also try live selectors (JS may have added fields not in initial HTML)
    if not start_field:
        for nm in ["start_date", "beg_date", "begin_date", "startdate", "from_date"]:
            f = await page.query_selector(f"input[name='{nm}'], input[id='{nm}']")
            if f:
                start_field = nm
                break

    if not end_field:
        for nm in ["end_date", "stop_date", "enddate", "to_date", "endDate"]:
            f = await page.query_selector(f"input[name='{nm}'], input[id='{nm}']")
            if f:
                end_field = nm
                break

    date_filled = False
    if start_field:
        f = await page.query_selector(
            f"input[name='{start_field}'], input[id='{start_field}']"
        )
        if f:
            await f.triple_click()
            await f.fill(start_str)
            date_filled = True

    if end_field:
        f = await page.query_selector(
            f"input[name='{end_field}'], input[id='{end_field}']"
        )
        if f:
            await f.triple_click()
            await f.fill(end_str)

    # Last resort: fill all date-looking inputs
    if not date_filled:
        all_text = await page.query_selector_all("input[type='text'], input[type='date']")
        filled_count = 0
        for inp in all_text:
            try:
                ph = (await inp.get_attribute("placeholder") or "").upper()
                nm = (await inp.get_attribute("name") or "").upper()
                cl = (await inp.get_attribute("class") or "").upper()
                combined = ph + nm + cl
                if any(k in combined for k in ["DATE", "BEGIN", "START", "FROM"]):
                    await inp.triple_click()
                    await inp.fill(start_str)
                    filled_count += 1
                    date_filled = True
                elif any(k in combined for k in ["END", "STOP", "TO"]):
                    await inp.triple_click()
                    await inp.fill(end_str)
                    filled_count += 1
            except:
                pass
        if filled_count:
            print(f"[clerk] last-resort filled {filled_count} date inputs")

    if not date_filled:
        print(f"[clerk] ⚠ could not fill date fields for {code} — dumping page HTML")
        html = await page.content()
        save_debug(f"no_date_fields_{code}", html)

    await page.wait_for_timeout(500)

    # ── Submit search ─────────────────────────────────────────────────────────
    submitted = False
    for sel in [
        "button[type='submit']",
        "input[type='submit']",
        "text=Search (F2)",
        "a:has-text('Search')",
        "text=Search",
        "button:has-text('Search')",
    ]:
        try:
            await page.click(sel, timeout=4000)
            submitted = True
            break
        except:
            pass
    if not submitted:
        await page.keyboard.press("F2")

    # Wait for results
    try:
        await page.wait_for_selector(
            "text=Record Count, text=entries, text=No records, table",
            timeout=25000
        )
    except:
        await page.wait_for_timeout(8000)

    await page.wait_for_timeout(2000)
    html = await page.content()

    # Always save debug for first doc type, and any that return results
    save_debug(f"results_{code}", html)
    if code == "LP":
        await page.screenshot(
            path=str(DEBUG_DIR / f"results_{code}.png"), full_page=True
        )

    # ── Try CSV download first ────────────────────────────────────────────────
    csv_records = await try_csv_download(page, ctx, code, cat, cat_label)
    if csv_records:
        return csv_records

    # ── Fall back to HTML table parse ─────────────────────────────────────────
    soup = BeautifulSoup(html, "lxml")
    return parse_html_table(soup, code, cat, cat_label)


async def try_csv_download(page, ctx, code, cat, cat_label):
    try:
        dl_link = await page.query_selector(
            "text=Download results into CSV file, "
            "a:has-text('CSV'), a:has-text('Download'), "
            "text=Export"
        )
        if not dl_link:
            return []
        async with ctx.expect_download(timeout=30000) as dl_info:
            await dl_link.click()
        download = await dl_info.value
        tmp_path = f"/tmp/shelby_{code}.csv"
        await download.save_as(tmp_path)
        print(f"[clerk] ✓ CSV downloaded for {code}")
        records = []
        with open(tmp_path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row = {k.lower().strip(): (v or "").strip() for k, v in row.items()}
                owner   = (row.get("grantor") or row.get("grantor name") or
                           row.get("owner") or "")
                doc_num = (row.get("record info") or row.get("instrument #") or
                           row.get("instrument number") or "")
                filed   = parse_date(row.get("rec. date") or row.get("date") or "")
                prop_d  = (row.get("prop. description") or
                           row.get("property description") or "")
                amount  = safe_float(row.get("consideration") or row.get("amount") or "")
                if not owner and not doc_num:
                    continue
                pa, pc, ps, pz = parse_address(prop_d)
                records.append({
                    "doc_num": doc_num, "doc_type": code,
                    "filed": filed or "", "cat": cat, "cat_label": cat_label,
                    "owner": owner,
                    "grantee": row.get("grantee") or row.get("grantee name") or "",
                    "amount": amount, "legal": prop_d,
                    "prop_address": pa, "prop_city": pc or "Memphis",
                    "prop_state": ps or "TN", "prop_zip": pz,
                    "mail_address": "", "mail_city": "",
                    "mail_state": "", "mail_zip": "",
                    "clerk_url": "", "flags": [], "score": 0,
                })
        print(f"[clerk] parsed {len(records)} rows from CSV")
        return records
    except Exception as e:
        print(f"[clerk] CSV download {code} → {e}")
        return []


def parse_html_table(soup, code, cat, cat_label):
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th", "td"])]
        if not any(k in " ".join(hdrs)
                   for k in ["record", "grantor", "instrument", "rec. date", "date"]):
            continue
        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells or all(c == "" for c in cells): continue
            row  = dict(zip(hdrs, cells))
            link = tr.find("a", href=True)
            href = link["href"] if link else ""
            clerk_url = (href if href.startswith("http")
                         else CLERK_BASE + "/" + href.lstrip("/")) if href else ""
            doc_num  = _pick(row, ["record info","instrument","doc"]) or cells[0]
            owner    = _pick(row, ["grantor","owner","party 1","name"])
            filed    = parse_date(_pick(row, ["rec. date","date","filed"]))
            prop_d   = _pick(row, ["prop. description","property","description"])
            pa, pc, ps, pz = parse_address(prop_d)
            if not doc_num and not owner: continue
            records.append({
                "doc_num": doc_num, "doc_type": code,
                "filed": filed or "", "cat": cat, "cat_label": cat_label,
                "owner": owner or "", "grantee": _pick(row, ["grantee","party 2"]) or "",
                "amount": safe_float(_pick(row, ["consideration","amount"])),
                "legal": prop_d,
                "prop_address": pa, "prop_city": pc or "Memphis",
                "prop_state": ps or "TN", "prop_zip": pz,
                "mail_address": "", "mail_city": "",
                "mail_state": "", "mail_zip": "",
                "clerk_url": clerk_url, "flags": [], "score": 0,
            })
    return records

# ── PARCEL ENRICHMENT ─────────────────────────────────────────────────────────
def download_parcel_dbf():
    if not HAS_DBF: return {}
    hdrs = {"User-Agent": "Mozilla/5.0"}
    raw = None
    for url in [
        "https://www.assessor.shelby.tn.us/downloads/parcel_data.zip",
        "https://www.assessor.shelby.tn.us/downloads/Parcel.zip",
        "https://www.assessor.shelby.tn.us/downloads/parcels.zip",
    ]:
        try:
            print(f"[parcel] trying {url}")
            r = requests.get(url, headers=hdrs, timeout=90, stream=True)
            if r.status_code == 200 and len(r.content) > 1000:
                raw = r.content
                break
        except Exception as e:
            print(f"[parcel] {url} → {e}")
    if not raw:
        print("[parcel] no bulk file — skipping"); return {}
    owner_map = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            dbf_names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
            if not dbf_names: return {}
            tmp = Path("/tmp/parcels.dbf")
            tmp.write_bytes(zf.read(dbf_names[0]))
        col_map = {
            "owner":      ["OWNER","OWN1","OWNERNAME"],
            "site_addr":  ["SITE_ADDR","SITEADDR","ADDRESS"],
            "site_city":  ["SITE_CITY","SITECITY","CITY"],
            "site_zip":   ["SITE_ZIP","SITEZIP","ZIP"],
            "mail_addr":  ["ADDR_1","MAILADR1","MAILADDR1"],
            "mail_city":  ["MAILCITY","MAIL_CITY"],
            "mail_state": ["STATE","MAILSTATE"],
            "mail_zip":   ["MAILZIP","MAIL_ZIP"],
        }
        def gc(row, keys):
            for k in keys:
                if k in row and row[k]: return str(row[k]).strip()
            return ""
        for rec in DBF(str(tmp), load=True, ignore_missing_memofile=True):
            try:
                owner = gc(rec, col_map["owner"])
                if not owner: continue
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
                    if v not in owner_map: owner_map[v] = p
            except: pass
        print(f"[parcel] {len(owner_map):,} entries")
    except Exception as e:
        print(f"[parcel] error: {e}")
    return owner_map

def enrich(rec, owner_map):
    for v in name_variants(rec.get("owner", "")):
        if v in owner_map:
            rec.update({k: val for k, val in owner_map[v].items() if not rec.get(k)})
            break
    return rec

# ── SCORING ───────────────────────────────────────────────────────────────────
def score_record(rec, week_ago):
    flags, score = [], 30
    cat = rec.get("cat",""); code = rec.get("doc_type","")
    amt = rec.get("amount") or 0; filed = rec.get("filed","")
    owner = rec.get("owner","").upper()
    if cat=="LP":      flags.append("Lis pendens");      score+=10
    if cat=="NOFC":    flags.append("Pre-foreclosure");  score+=10
    if cat=="JUD":     flags.append("Judgment lien");    score+=10
    if cat=="LNTAX":   flags.append("Tax lien");         score+=10
    if code=="LNMECH": flags.append("Mechanic lien");    score+=10
    if cat=="PRO":     flags.append("Probate / estate"); score+=10
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b", owner):
        flags.append("LLC/corp owner"); score+=10
    if amt>100000: score+=15; flags.append("High-value debt")
    elif amt>50000: score+=10
    if filed:
        try:
            if datetime.strptime(filed,"%Y-%m-%d")>=week_ago:
                score+=5; flags.append("New this week")
        except: pass
    if rec.get("prop_address"): score+=5; flags.append("Has address")
    rec["flags"] = list(dict.fromkeys(flags))
    rec["score"] = min(score,100)
    return rec

def apply_combo(records):
    oc={}
    for r in records:
        o=r.get("owner","").upper()
        if o: oc.setdefault(o,set()).add(r.get("cat"))
    for r in records:
        o=r.get("owner","").upper()
        if o and {"LP","NOFC"}.issubset(oc.get(o,set())):
            r["score"]=min(r["score"]+20,100)
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
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader()
        for r in records:
            own=r.get("owner","")
            pts=own.split(",",1) if "," in own else own.rsplit(" ",1)
            w.writerow({
                "First Name":             pts[1].strip() if len(pts)>1 else "",
                "Last Name":              pts[0].strip(),
                "Mailing Address":        r.get("mail_address",""),
                "Mailing City":           r.get("mail_city",""),
                "Mailing State":          r.get("mail_state",""),
                "Mailing Zip":            r.get("mail_zip",""),
                "Property Address":       r.get("prop_address",""),
                "Property City":          r.get("prop_city",""),
                "Property State":         r.get("prop_state",""),
                "Property Zip":           r.get("prop_zip",""),
                "Lead Type":              r.get("cat_label",""),
                "Document Type":          r.get("doc_type",""),
                "Date Filed":             r.get("filed",""),
                "Document Number":        r.get("doc_num",""),
                "Amount/Debt Owed":       r.get("amount",""),
                "Seller Score":           r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":                 "Shelby County Register of Deeds",
                "Public Records URL":     r.get("clerk_url",""),
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
        print(f"[clerk] failed: {e}"); traceback.print_exc()
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
    records.sort(key=lambda r: r.get("score",0), reverse=True)
    with_addr = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat()+"Z",
        "source":       "Shelby County Register of Deeds",
        "date_range":   {"start": start_dt.strftime("%Y-%m-%d"),
                         "end":   end_dt.strftime("%Y-%m-%d")},
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
