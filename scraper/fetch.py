"""
Shelby County, TN — Motivated Seller Lead Scraper
v7 — Matches actual page structure:
     • Form lives inside an iframe  ← key fix
     • Instrument types are full-text checkboxes (e.g. "LIS PENDENS")
     • Date fields labeled "Begin Date" / "End Date" with placeholder MM/DD/YYYY
     • Submit = Search (F2) link
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
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE    = "https://search.register.shelby.tn.us"
CLERK_SEARCH  = f"{CLERK_BASE}/search/index.php"
OUTPUT_PATHS  = [Path("dashboard/records.json"), Path("data/records.json")]
DEBUG_DIR     = Path("data/debug")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Map: our code -> (category, label, substrings to match in checkbox text on the site)
DOC_TYPES = {
    "LP":      ("LP",     "Lis Pendens",            ["LIS PENDENS"]),
    "NOFC":    ("NOFC",   "Notice of Foreclosure",  ["NOTICE OF FORECLOSURE", "FORECLOSURE NOTICE"]),
    "TAXDEED": ("TAXDEED","Tax Deed",               ["TAX DEED"]),
    "JUD":     ("JUD",    "Judgment",               ["JUDGMENT", "JUDGEMENT"]),
    "CCJ":     ("JUD",    "Certified Judgment",     ["CERTIFIED JUDG"]),
    "LNFED":   ("LNTAX",  "Federal Tax Lien",       ["FEDERAL TAX LIEN", "FED TAX LIEN"]),
    "LNIRS":   ("LNTAX",  "IRS Lien",               ["IRS LIEN", "IRS TAX LIEN"]),
    "LN":      ("LN",     "Lien",                   ["LIEN"]),
    "LNMECH":  ("LN",     "Mechanic Lien",          ["MECHANIC", "MECHANICS LIEN"]),
    "PRO":     ("PRO",    "Probate",                ["PROBATE"]),
    "NOC":     ("NOC",    "Notice of Commencement", ["NOTICE OF COMMENCEMENT"]),
    "RELLP":   ("RELLP",  "Release Lis Pendens",    ["RELEASE LIS PENDENS", "REL LIS PENDENS"]),
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

# ── FRAME DETECTION ───────────────────────────────────────────────────────────
async def get_search_frame(page):
    """
    The site renders its search form inside an iframe.
    Find and return that frame; fall back to the main page if none found.
    """
    await page.wait_for_timeout(5000)  # let all iframes load

    best = None
    for frame in page.frames:
        try:
            url  = frame.url
            html = await frame.content()
            soup = BeautifulSoup(html, "lxml")
            has_begin = bool(soup.find(string=re.compile(r"Begin Date", re.I)))
            has_cbs   = len(soup.find_all("input", {"type": "checkbox"}))
            has_form  = bool(soup.find("form"))
            print(f"[frame] {url[:80]} | begin={has_begin} cbs={has_cbs} form={has_form}")
            if has_begin or (has_cbs > 0 and has_form):
                save_debug("search_frame", html)
                print(f"[frame] ✓ selected this frame")
                best = frame
                break
        except Exception as e:
            print(f"[frame] error: {e}")

    if best:
        return best

    print("[frame] no iframe with form found — using main page")
    save_debug("search_frame_fallback", await page.content())
    return page


async def wait_for_search_form(frame):
    for selector in [
        "input[placeholder='MM/DD/YYYY']",
        "input[name*='begin' i]",
        "input[name*='start' i]",
        "text=Begin Date",
        "text=Recorded Between",
        "input[type='checkbox']",
    ]:
        try:
            await frame.wait_for_selector(selector, timeout=8000)
            print(f"[form] ready — {selector}")
            return True
        except:
            pass
    print("[form] ⚠ form not detected")
    return False


# ── MAIN SCRAPER ──────────────────────────────────────────────────────────────
async def scrape_clerk(start_dt, end_dt):
    all_records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
            accept_downloads=True,
        )
        page = await ctx.new_page()

        print(f"[clerk] loading {CLERK_SEARCH}")
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)

        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        await page.screenshot(path=str(DEBUG_DIR / "outer_page.png"), full_page=True)
        save_debug("outer_page", await page.content())

        print(f"[clerk] frames on page: {len(page.frames)}")
        for f in page.frames:
            print(f"  frame: {f.url}")

        frame = await get_search_frame(page)
        await wait_for_search_form(frame)

        # ── Audit form elements ───────────────────────────────────────────────
        checkboxes  = await frame.query_selector_all("input[type='checkbox']")
        text_inputs = await frame.query_selector_all("input[type='text'], input:not([type])")
        selects     = await frame.query_selector_all("select")
        print(f"[audit] checkboxes={len(checkboxes)} text_inputs={len(text_inputs)} selects={len(selects)}")

        for inp in text_inputs:
            print(f"  text: name='{await inp.get_attribute('name') or ''}' "
                  f"id='{await inp.get_attribute('id') or ''}' "
                  f"placeholder='{await inp.get_attribute('placeholder') or ''}'")

        for cb in checkboxes[:30]:
            cb_id  = await cb.get_attribute("id") or ""
            cb_val = await cb.get_attribute("value") or ""
            lbl_el = await frame.query_selector(f"label[for='{cb_id}']") if cb_id else None
            if lbl_el:
                lbl = (await lbl_el.inner_text()).strip()
            else:
                lbl = await cb.evaluate(
                    "el => el.parentElement ? el.parentElement.innerText.trim() : ''"
                )
            print(f"  cb: val='{cb_val}' label='{lbl[:60]}'")

        # ── Search each doc type ──────────────────────────────────────────────
        for code, (cat, cat_label, match_strings) in DOC_TYPES.items():
            try:
                records = await search_one_type(
                    frame, ctx, page, code, cat, cat_label,
                    match_strings, start_str, end_str
                )
                all_records.extend(records)
                print(f"[clerk] {code}: {len(records)} {'✓' if records else ''}")
            except Exception as e:
                print(f"[clerk] {code} error: {e}")
                traceback.print_exc()
            # Reset form for next iteration
            frame = await reset_form(page)

        await browser.close()
    return all_records


async def reset_form(page):
    """Navigate back to search page and return the correct frame."""
    try:
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(4000)
    except Exception as e:
        print(f"[reset] reload failed: {e}")
    frame = await get_search_frame(page)
    await wait_for_search_form(frame)
    return frame


async def search_one_type(frame, ctx, page, code, cat, cat_label,
                            match_strings, start_str, end_str):
    # ── 1. Clear all instrument checkboxes ───────────────────────────────────
    cleared = False
    for sel in ["text=Clear All Instruments", "button:has-text('Clear All')"]:
        try:
            await frame.click(sel, timeout=3000)
            await frame.wait_for_timeout(500)
            cleared = True
            break
        except:
            pass

    if not cleared:
        all_cbs = await frame.query_selector_all("input[type='checkbox']")
        for cb in all_cbs:
            try:
                if await cb.is_checked():
                    await cb.click()
                    await frame.wait_for_timeout(20)
            except:
                pass

    # ── 2. Check only our target instrument ──────────────────────────────────
    checked = False
    all_cbs = await frame.query_selector_all("input[type='checkbox']")

    for cb in all_cbs:
        try:
            cb_id  = await cb.get_attribute("id") or ""
            cb_val = (await cb.get_attribute("value") or "").upper()
            lbl_el = await frame.query_selector(f"label[for='{cb_id}']") if cb_id else None
            if lbl_el:
                label_text = (await lbl_el.inner_text()).strip().upper()
            else:
                label_text = (await cb.evaluate(
                    "el => el.parentElement ? el.parentElement.innerText.trim() : ''"
                )).upper()

            combined = label_text + " " + cb_val
            for ms in match_strings:
                if ms.upper() in combined:
                    if not await cb.is_checked():
                        await cb.click()
                        await frame.wait_for_timeout(100)
                    checked = True
                    print(f"[{code}] ✓ checked: '{label_text[:50]}'")
                    break
            if checked:
                break
        except Exception as e:
            print(f"[{code}] cb error: {e}")

    if not checked:
        # Log available checkboxes for debugging
        print(f"[{code}] ⚠ no matching checkbox. Available:")
        for cb in all_cbs[:20]:
            try:
                val = await cb.get_attribute("value") or ""
                txt = await cb.evaluate(
                    "el => el.parentElement ? el.parentElement.innerText.trim() : ''"
                )
                print(f"    '{val}' / '{txt[:60]}'")
            except: pass
        return []

    # ── 3. Fill Begin Date / End Date ────────────────────────────────────────
    # Primary: find by placeholder="MM/DD/YYYY"
    date_inputs = await frame.query_selector_all("input[placeholder='MM/DD/YYYY']")
    if len(date_inputs) >= 2:
        await date_inputs[0].triple_click()
        await date_inputs[0].type(start_str)
        await date_inputs[1].triple_click()
        await date_inputs[1].type(end_str)
        print(f"[{code}] ✓ dates filled: {start_str} → {end_str}")
    else:
        # Fallback: keyword search on name/id
        filled = 0
        for inp in await frame.query_selector_all("input[type='text'], input:not([type])"):
            try:
                nm  = (await inp.get_attribute("name") or "").lower()
                id_ = (await inp.get_attribute("id") or "").lower()
                ph  = (await inp.get_attribute("placeholder") or "").lower()
                combined = nm + id_ + ph
                if any(k in combined for k in ["begin", "start", "beg", "from"]) and filled == 0:
                    await inp.triple_click()
                    await inp.type(start_str)
                    filled += 1
                elif any(k in combined for k in ["end", "stop", "to"]) and filled <= 1:
                    await inp.triple_click()
                    await inp.type(end_str)
                    filled += 1
            except:
                pass
        if filled:
            print(f"[{code}] ✓ dates filled via fallback ({filled} fields)")
        else:
            print(f"[{code}] ⚠ could not fill dates")
            save_debug(f"no_dates_{code}", await frame.content())

    await frame.wait_for_timeout(500)

    # ── 4. Submit ─────────────────────────────────────────────────────────────
    submitted = False
    for sel in [
        "text=Search (F2)",
        "a:has-text('Search')",
        "button:has-text('Search')",
        "input[value*='Search']",
    ]:
        try:
            await frame.click(sel, timeout=5000)
            submitted = True
            print(f"[{code}] submitted via '{sel}'")
            break
        except:
            pass
    if not submitted:
        await frame.keyboard.press("F2")
        print(f"[{code}] submitted via F2 key")

    # ── 5. Wait for results ───────────────────────────────────────────────────
    await page.wait_for_timeout(4000)

    # Find the frame containing results
    results_frame = frame
    for f in page.frames:
        try:
            html = await f.content()
            if any(k in html for k in ["Grantor", "Instrument #", "Record Count",
                                        "No records found", "grantor"]):
                results_frame = f
                break
        except:
            pass

    await page.wait_for_timeout(2000)
    results_html = await results_frame.content()
    save_debug(f"results_{code}", results_html)

    if code == "LP":
        await page.screenshot(
            path=str(DEBUG_DIR / f"results_{code}.png"), full_page=True
        )

    # ── 6. Try CSV download ───────────────────────────────────────────────────
    csv_records = await try_csv_download(results_frame, ctx, code, cat, cat_label)
    if csv_records:
        return csv_records

    # ── 7. Parse HTML table ───────────────────────────────────────────────────
    soup = BeautifulSoup(results_html, "lxml")
    return parse_html_table(soup, code, cat, cat_label)


async def try_csv_download(frame, ctx, code, cat, cat_label):
    try:
        dl_link = None
        for sel in [
            "text=Download results into CSV file",
            "a:has-text('CSV')",
            "a:has-text('Download')",
            "text=Export",
        ]:
            try:
                dl_link = await frame.query_selector(sel)
                if dl_link:
                    break
            except:
                pass
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
                owner   = (row.get("grantor") or row.get("grantor name") or row.get("owner") or "")
                doc_num = (row.get("record info") or row.get("instrument #") or row.get("instrument number") or "")
                filed   = parse_date(row.get("rec. date") or row.get("date") or "")
                prop_d  = (row.get("prop. description") or row.get("property description") or "")
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
                    "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                    "clerk_url": "", "flags": [], "score": 0,
                })
        print(f"[clerk] parsed {len(records)} rows from CSV")
        return records
    except Exception as e:
        print(f"[clerk] CSV {code} → {e}")
        return []


def parse_html_table(soup, code, cat, cat_label):
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        hdrs = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if not any(k in " ".join(hdrs) for k in ["record", "grantor", "instrument", "date"]):
            continue
        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells or all(c == "" for c in cells):
                continue
            row  = dict(zip(hdrs, cells))
            link = tr.find("a", href=True)
            href = link["href"] if link else ""
            clerk_url = (
                href if href.startswith("http") else CLERK_BASE + "/" + href.lstrip("/")
            ) if href else ""
            doc_num = _pick(row, ["record info", "instrument", "doc"]) or cells[0]
            owner   = _pick(row, ["grantor", "owner", "party 1", "name"])
            filed   = parse_date(_pick(row, ["rec. date", "date", "filed"]))
            prop_d  = _pick(row, ["prop. description", "property", "description"])
            pa, pc, ps, pz = parse_address(prop_d)
            if not doc_num and not owner:
                continue
            records.append({
                "doc_num": doc_num, "doc_type": code,
                "filed": filed or "", "cat": cat, "cat_label": cat_label,
                "owner": owner or "", "grantee": _pick(row, ["grantee", "party 2"]) or "",
                "amount": safe_float(_pick(row, ["consideration", "amount"])),
                "legal": prop_d,
                "prop_address": pa, "prop_city": pc or "Memphis",
                "prop_state": ps or "TN", "prop_zip": pz,
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                "clerk_url": clerk_url, "flags": [], "score": 0,
            })
    return records

# ── PARCEL ENRICHMENT ─────────────────────────────────────────────────────────
def download_parcel_dbf():
    if not HAS_DBF:
        return {}
    raw = None
    for url in [
        "https://www.assessor.shelby.tn.us/downloads/parcel_data.zip",
        "https://www.assessor.shelby.tn.us/downloads/Parcel.zip",
        "https://www.assessor.shelby.tn.us/downloads/parcels.zip",
    ]:
        try:
            print(f"[parcel] trying {url}")
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=90)
            if r.status_code == 200 and len(r.content) > 1000:
                raw = r.content; break
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
    cat   = rec.get("cat", ""); code = rec.get("doc_type", "")
    amt   = rec.get("amount") or 0; filed = rec.get("filed", "")
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
        except: pass
    if rec.get("prop_address"): score += 5; flags.append("Has address")
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

# ── GHL CSV EXPORT ────────────────────────────────────────────────────────────
def export_ghl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State",
        "Mailing Zip","Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
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
    week_ago = datetime.utcnow() - timedelta(days=7)
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
    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    with_addr = sum(1 for r in records if r.get("prop_address"))

    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
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
