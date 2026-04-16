"""
Shelby County, TN — Motivated Seller Lead Scraper
v11 — Fixes: TargetClosedError, date fill failure, 0-record saves, timeout tuning
"""

import asyncio, csv, json, os, re, traceback, zipfile, io
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
LOOKBACK_DAYS    = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE       = "https://search.register.shelby.tn.us"
CLERK_SEARCH     = f"{CLERK_BASE}/search/index.php"
OUTPUT_PATHS     = [Path("dashboard/records.json"), Path("data/records.json")]
DEBUG_DIR        = Path("data/debug")

# Per-type timeout raised — site is slow
SEARCH_TIMEOUT_S = 120

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Each entry: code → (cat, cat_label, checkbox_value)
# checkbox_value = exact "value" attribute on the site's <input type=checkbox>
DOC_TYPES = {
    "JDG":       ("JUD",    "Judgment",              "JDG"),
    "LIEN":      ("LNTAX",  "Tax Lien",              "LIEN"),
    "NOFC":      ("NOFC",   "Notice of Foreclosure", "NOFC"),
    "PRO":       ("PRO",    "Probate",               "PRO"),
    "TRUSTDEED": ("TD",     "Trust Deed",            "TRUSTDEED"),
    "DISC":      ("DISC",   "Discharge",             "DISCHARGE"),
    "REL":       ("RELLP",  "Release",               "REL"),
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

# ── BROWSER FACTORY ───────────────────────────────────────────────────────────
async def make_browser(pw):
    return await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ]
    )

async def make_context(browser):
    return await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1280, "height": 900},
        accept_downloads=True,
    )

# ── FRAME DETECTION ───────────────────────────────────────────────────────────
async def get_search_frame(page):
    """Return the frame that contains the search form (may be main page or iframe)."""
    await page.wait_for_timeout(2000)
    for frame in page.frames:
        try:
            html = await frame.content()
            soup = BeautifulSoup(html, "lxml")
            cbs   = soup.find_all("input", {"type": "checkbox"})
            form  = soup.find("form")
            if cbs and form:
                save_debug("search_frame", html)
                print(f"  [frame] ✓ {frame.url[:80]}")
                return frame
        except Exception:
            pass
    return page  # fall back to main page

async def wait_for_form(frame, timeout=8000):
    for sel in ["input[name='start_date']", "input[name='end_date']",
                "input[type='checkbox']"]:
        try:
            await frame.wait_for_selector(sel, timeout=timeout)
            return True
        except Exception:
            pass
    return False

# ── DATE FILL (robust) ────────────────────────────────────────────────────────
async def fill_date_fields(frame, start_str, end_str, code):
    """
    Try multiple strategies to fill date inputs:
    1. By name attribute (start_date / end_date)
    2. By id attribute
    3. By iterating all text/date inputs in order
    Returns number of fields filled.
    """
    filled = 0

    # Strategy 1: by name
    for name_attr, value in [("start_date", start_str), ("end_date", end_str)]:
        try:
            el = await frame.query_selector(f"input[name='{name_attr}']")
            if el:
                await el.click(click_count=3)
                await el.fill("")
                await el.type(value, delay=30)
                actual = await el.input_value()
                print(f"  [{code}] name={name_attr} → '{actual}'")
                filled += 1
        except Exception as e:
            print(f"  [{code}] name fill error ({name_attr}): {e}")

    if filled == 2:
        return filled

    # Strategy 2: by positional text inputs
    filled = 0
    try:
        inputs = await frame.query_selector_all(
            "input[type='text'], input[type='date'], input:not([type])"
        )
        date_inputs = []
        for inp in inputs:
            try:
                nm = (await inp.get_attribute("name") or "").lower()
                id_ = (await inp.get_attribute("id") or "").lower()
                ph  = (await inp.get_attribute("placeholder") or "").lower()
                tag = nm + id_ + ph
                if any(k in tag for k in ["date", "from", "to", "start", "end",
                                           "begin", "thru"]):
                    date_inputs.append((tag, inp))
                elif not any(k in tag for k in ["search","name","party","inst",
                                                  "book","page","parcel"]):
                    date_inputs.append((tag, inp))
            except Exception:
                pass

        values_to_fill = [start_str, end_str]
        for i, (tag, inp) in enumerate(date_inputs[:2]):
            try:
                await inp.click(click_count=3)
                await inp.fill("")
                await inp.type(values_to_fill[i], delay=30)
                actual = await inp.input_value()
                print(f"  [{code}] input[{i}] tag='{tag}' → '{actual}'")
                filled += 1
            except Exception as e:
                print(f"  [{code}] input[{i}] fill error: {e}")
    except Exception as e:
        print(f"  [{code}] positional fill error: {e}")

    return filled

# ── SUBMIT ────────────────────────────────────────────────────────────────────
async def submit_search(frame, code):
    selectors = [
        "text=Search (F2)",
        "a:has-text('Search')",
        "button:has-text('Search')",
        "input[value='Search']",
        "input[type='submit']",
        "button[type='submit']",
    ]
    for sel in selectors:
        try:
            await frame.click(sel, timeout=3000)
            print(f"  [{code}] submitted via '{sel}'")
            return True
        except Exception:
            pass
    # Last resort: F2
    try:
        await frame.keyboard.press("F2")
        print(f"  [{code}] submitted via F2")
        return True
    except Exception:
        pass
    return False

# ── RESULTS FRAME ─────────────────────────────────────────────────────────────
async def find_results_frame(page):
    await page.wait_for_timeout(3000)
    keywords = ["Grantor", "Instrument #", "Record Count", "No records found",
                "grantor", "instrument", "record count"]
    for f in page.frames:
        try:
            html = await f.content()
            if any(k in html for k in keywords):
                return f, html
        except Exception:
            pass
    # fall back to main
    html = await page.content()
    return page, html

# ── CSV DOWNLOAD ──────────────────────────────────────────────────────────────
async def try_csv_download(frame, ctx, code, cat, cat_label):
    try:
        dl_link = None
        for sel in ["text=Download results into CSV file",
                    "a:has-text('CSV')", "a:has-text('Download')",
                    "a[href*='csv']", "a[href*='download']"]:
            try:
                dl_link = await frame.query_selector(sel)
                if dl_link:
                    break
            except Exception:
                pass
        if not dl_link:
            return []

        async with ctx.expect_download(timeout=20000) as dl_info:
            await dl_link.click()
        download = await dl_info.value
        tmp_path = f"/tmp/shelby_{code}.csv"
        await download.save_as(tmp_path)

        records = []
        with open(tmp_path, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                row = {k.lower().strip(): (v or "").strip() for k, v in row.items()}
                owner   = row.get("grantor") or row.get("owner") or row.get("party 1") or ""
                doc_num = (row.get("record info") or row.get("instrument #")
                           or row.get("instrument") or "")
                if not owner and not doc_num:
                    continue
                filed  = parse_date(row.get("rec. date") or row.get("date") or
                                    row.get("record date") or "")
                prop_d = (row.get("prop. description") or
                          row.get("property description") or
                          row.get("legal description") or "")
                pa, pc, ps, pz = parse_address(prop_d)
                records.append({
                    "doc_num":      doc_num,
                    "doc_type":     code,
                    "filed":        filed or "",
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "owner":        owner,
                    "grantee":      row.get("grantee") or row.get("party 2") or "",
                    "amount":       safe_float(row.get("consideration") or
                                               row.get("amount") or ""),
                    "legal":        prop_d,
                    "prop_address": pa,
                    "prop_city":    pc or "Memphis",
                    "prop_state":   "TN",
                    "prop_zip":     pz,
                    "mail_address": "", "mail_city": "",
                    "mail_state":   "", "mail_zip": "",
                    "clerk_url":    "",
                    "flags":        [],
                    "score":        0,
                })
        print(f"  [{code}] ✓ CSV: {len(records)} rows")
        return records
    except Exception as e:
        print(f"  [{code}] CSV download error: {e}")
        return []

# ── HTML TABLE PARSE ──────────────────────────────────────────────────────────
def parse_html_table(soup, code, cat, cat_label):
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th", "td"])]
        if not any(k in " ".join(hdrs)
                   for k in ["record", "grantor", "instrument", "date", "owner"]):
            continue
        print(f"  [{code}] HTML table: {len(rows)-1} data rows, headers={hdrs[:6]}")
        for tr in rows[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not cells or all(c == "" for c in cells):
                continue
            row  = dict(zip(hdrs, cells))
            link = tr.find("a", href=True)
            href = link["href"] if link else ""
            clerk_url = ""
            if href:
                clerk_url = (href if href.startswith("http")
                             else CLERK_BASE + "/" + href.lstrip("/"))
            doc_num = _pick(row, ["record info", "instrument", "doc #", "doc"]) or cells[0]
            owner   = _pick(row, ["grantor", "owner", "party 1", "name"])
            filed   = parse_date(_pick(row, ["rec. date", "record date", "date", "filed"]))
            prop_d  = _pick(row, ["prop. description", "property", "description", "legal"])
            pa, pc, ps, pz = parse_address(prop_d)
            if not doc_num and not owner:
                continue
            records.append({
                "doc_num":      doc_num,
                "doc_type":     code,
                "filed":        filed or "",
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner or "",
                "grantee":      _pick(row, ["grantee", "party 2"]) or "",
                "amount":       safe_float(_pick(row, ["consideration", "amount"])),
                "legal":        prop_d,
                "prop_address": pa,
                "prop_city":    pc or "Memphis",
                "prop_state":   "TN",
                "prop_zip":     pz,
                "mail_address": "", "mail_city": "",
                "mail_state":   "", "mail_zip": "",
                "clerk_url":    clerk_url,
                "flags":        [],
                "score":        0,
            })
    return records

# ── SINGLE DOC TYPE SEARCH ────────────────────────────────────────────────────
async def search_one_type(pw, code, cat, cat_label, cb_value, start_str, end_str):
    """
    Runs a completely fresh browser+context+page per doc type.
    This eliminates TargetClosedError cascades entirely.
    """
    print(f"\n[{code}] === starting fresh browser ===")
    browser = await make_browser(pw)
    ctx     = await make_context(browser)
    page    = await ctx.new_page()

    try:
        # ── Load search page ──
        print(f"  [{code}] loading {CLERK_SEARCH}")
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        frame = await get_search_frame(page)
        found = await wait_for_form(frame, timeout=10000)
        if not found:
            print(f"  [{code}] ⚠ form not found — skipping")
            return []

        # Dump all checkboxes so we can see what's available
        cbs_info = []
        for cb in await frame.query_selector_all("input[type='checkbox']"):
            try:
                val  = await cb.get_attribute("value") or ""
                name = await cb.get_attribute("name") or ""
                id_  = await cb.get_attribute("id") or ""
                cbs_info.append(f"val={val!r} name={name!r} id={id_!r}")
            except Exception:
                pass
        print(f"  [{code}] checkboxes on page: {cbs_info}")
        save_debug(f"form_{code}", "\n".join(cbs_info), ext="txt")

        # ── Uncheck all ──
        for cb in await frame.query_selector_all("input[type='checkbox']"):
            try:
                if await cb.is_checked():
                    await cb.click()
                    await frame.wait_for_timeout(50)
            except Exception:
                pass

        # ── Check our target checkbox ──
        checked = False
        for cb in await frame.query_selector_all("input[type='checkbox']"):
            try:
                val = (await cb.get_attribute("value") or "").strip().upper()
                if val == cb_value.upper():
                    if not await cb.is_checked():
                        await cb.click()
                        await frame.wait_for_timeout(100)
                    checked = True
                    print(f"  [{code}] ✓ checked checkbox value='{val}'")
                    break
            except Exception:
                pass

        if not checked:
            # Try partial match (some sites have prefix variants)
            for cb in await frame.query_selector_all("input[type='checkbox']"):
                try:
                    val = (await cb.get_attribute("value") or "").strip().upper()
                    if cb_value.upper() in val or val in cb_value.upper():
                        if not await cb.is_checked():
                            await cb.click()
                            await frame.wait_for_timeout(100)
                        checked = True
                        print(f"  [{code}] ✓ partial match checkbox value='{val}'")
                        break
                except Exception:
                    pass

        if not checked:
            print(f"  [{code}] ⚠ checkbox '{cb_value}' not found — skipping")
            return []

        # ── Fill dates ──
        await frame.wait_for_timeout(300)
        filled = await fill_date_fields(frame, start_str, end_str, code)
        print(f"  [{code}] dates filled: {filled}/2")

        if filled < 2:
            # Try JS injection as last resort
            try:
                await frame.evaluate(f"""
                    () => {{
                        const inputs = document.querySelectorAll(
                            "input[name*='start'], input[name*='begin'],
                             input[name*='from'], input[name*='date']"
                        );
                        if (inputs[0]) inputs[0].value = '{start_str}';
                        if (inputs[1]) inputs[1].value = '{end_str}';
                    }}
                """)
                print(f"  [{code}] JS-injected dates as fallback")
            except Exception as e:
                print(f"  [{code}] JS inject failed: {e}")

        # ── Submit ──
        await frame.wait_for_timeout(300)
        await submit_search(frame, code)

        # ── Wait for results ──
        await page.wait_for_timeout(4000)

        results_frame, results_html = await find_results_frame(page)
        save_debug(f"results_{code}", results_html)

        # Log snippet to help debug
        snippet = results_html[:500].replace("\n", " ")
        print(f"  [{code}] results snippet: {snippet[:200]}")

        # ── CSV download ──
        csv_records = await try_csv_download(results_frame, ctx, code, cat, cat_label)
        if csv_records:
            return csv_records

        # ── HTML parse ──
        html_records = parse_html_table(
            BeautifulSoup(results_html, "lxml"), code, cat, cat_label
        )
        print(f"  [{code}] HTML parse: {len(html_records)} records")
        return html_records

    except Exception as e:
        print(f"  [{code}] ERROR: {e}")
        traceback.print_exc()
        return []
    finally:
        try:
            await browser.close()
        except Exception:
            pass

# ── MAIN SCRAPER ──────────────────────────────────────────────────────────────
async def scrape_clerk(start_dt, end_dt):
    all_records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as pw:
        for code, (cat, cat_label, cb_value) in DOC_TYPES.items():
            print(f"\n{'='*60}")
            print(f"[clerk] Searching: {code} — {cat_label}")
            print(f"{'='*60}")
            try:
                records = await asyncio.wait_for(
                    search_one_type(pw, code, cat, cat_label,
                                    cb_value, start_str, end_str),
                    timeout=SEARCH_TIMEOUT_S
                )
                all_records.extend(records)
                print(f"[clerk] {code}: {len(records)} records ✓  (running total: {len(all_records)})")
            except asyncio.TimeoutError:
                print(f"[clerk] {code}: TIMED OUT after {SEARCH_TIMEOUT_S}s — skipping")
            except Exception as e:
                print(f"[clerk] {code}: FAILED — {e}")
                traceback.print_exc()

            # Brief pause between types
            await asyncio.sleep(2)

    return all_records

# ── PARCEL ENRICHMENT ─────────────────────────────────────────────────────────
def download_parcel_dbf():
    if not HAS_DBF:
        return {}
    raw = None
    urls = [
        "https://www.assessor.shelby.tn.us/downloads/parcel_data.zip",
        "https://www.assessor.shelby.tn.us/downloads/Parcel.zip",
        "https://www.assessor.shelby.tn.us/downloads/parcels.zip",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
            if r.status_code == 200 and len(r.content) > 1000:
                # Quick check it's actually a ZIP
                if r.content[:2] == b"PK":
                    raw = r.content
                    print(f"[parcel] got {len(raw):,} bytes from {url}")
                    break
                else:
                    print(f"[parcel] {url} → not a ZIP (got {r.content[:4]})")
        except Exception as e:
            print(f"[parcel] {url} → {e}")

    if not raw:
        print("[parcel] no bulk file — skipping")
        return {}

    owner_map = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            dbf_names = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
            print(f"[parcel] DBF files in zip: {dbf_names}")
            if not dbf_names:
                return {}
            tmp = Path("/tmp/parcels.dbf")
            tmp.write_bytes(zf.read(dbf_names[0]))

        col_map = {
            "owner":      ["OWNER", "OWN1", "OWNERNAME", "OWNER_NAME"],
            "site_addr":  ["SITE_ADDR", "SITEADDR", "ADDRESS", "PROP_ADDR"],
            "site_city":  ["SITE_CITY", "SITECITY", "CITY", "PROP_CITY"],
            "site_zip":   ["SITE_ZIP", "SITEZIP", "ZIP", "PROP_ZIP"],
            "mail_addr":  ["ADDR_1", "MAILADR1", "MAILADDR1", "MAIL_ADDR"],
            "mail_city":  ["MAILCITY", "MAIL_CITY"],
            "mail_state": ["STATE", "MAILSTATE", "MAIL_STATE"],
            "mail_zip":   ["MAILZIP", "MAIL_ZIP"],
        }

        def gc(row, keys):
            for k in keys:
                if k in row and row[k]:
                    return str(row[k]).strip()
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
            except Exception:
                pass
        print(f"[parcel] loaded {len(owner_map):,} owner entries")
    except Exception as e:
        print(f"[parcel] error: {e}")
        traceback.print_exc()
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
    cat   = rec.get("cat", "")
    amt   = rec.get("amount") or 0
    filed = rec.get("filed", "")
    owner = rec.get("owner", "").upper()

    if cat == "JUD":   flags.append("Judgment lien");    score += 10
    if cat == "LNTAX": flags.append("Tax lien");         score += 10
    if cat == "NOFC":  flags.append("Pre-foreclosure");  score += 10
    if cat == "PRO":   flags.append("Probate / estate"); score += 10
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b", owner):
        flags.append("LLC/corp owner"); score += 10
    if amt and amt > 100000:
        score += 15; flags.append("High-value debt")
    elif amt and amt > 50000:
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

def apply_combo(records):
    oc = {}
    for r in records:
        o = r.get("owner", "").upper()
        if o:
            oc.setdefault(o, set()).add(r.get("cat"))
    for r in records:
        o = r.get("owner", "").upper()
        if o and {"NOFC", "JUD"}.issubset(oc.get(o, set())):
            r["score"] = min(r["score"] + 20, 100)
    return records

# ── GHL CSV EXPORT ────────────────────────────────────────────────────────────
def export_ghl(records, path):
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
            own = r.get("owner", "")
            if "," in own:
                pts = [x.strip() for x in own.split(",", 1)]
                first = pts[1] if len(pts) > 1 else ""
                last  = pts[0]
            else:
                parts = own.rsplit(" ", 1)
                first = parts[0] if len(parts) > 1 else ""
                last  = parts[-1]
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
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
                "Amount/Debt Owed":       r.get("amount", "") or "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Shelby County Register of Deeds",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    print(f"[export] GHL CSV → {path}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    start_dt, end_dt = date_range()
    week_ago = datetime.utcnow() - timedelta(days=7)
    print(f"[run] date range: {start_dt.date()} → {end_dt.date()}")
    print(f"[run] doc types: {list(DOC_TYPES.keys())}")

    # ── Scrape (no overall cap — per-type cap is 120s, 7 types = ~14min max) ──
    records = []
    try:
        records = await scrape_clerk(start_dt, end_dt)
        print(f"\n[clerk] total raw records: {len(records)}")
    except Exception as e:
        print(f"[clerk] fatal: {e}")
        traceback.print_exc()

    # ── Parcel enrichment ──
    owner_map = {}
    try:
        owner_map = download_parcel_dbf()
    except Exception as e:
        print(f"[parcel] skipped: {e}")

    # ── Score + enrich ──
    for r in records:
        try:
            enrich(r, owner_map)
        except Exception:
            pass
    for r in records:
        try:
            score_record(r, week_ago)
        except Exception:
            pass

    records = apply_combo(records)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    with_addr = sum(1 for r in records if r.get("prop_address"))

    # ── Save JSON ──
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

    # ── GHL export ──
    export_ghl(records, Path("data/leads_ghl.csv"))

    print(f"\n{'='*60}")
    print(f"✅ Done — {len(records)} total records, {with_addr} with address")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
