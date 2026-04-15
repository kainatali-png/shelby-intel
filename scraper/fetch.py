"""
Shelby County, TN — Motivated Seller Lead Scraper
v10 — Hard per-search timeout, faster waits, no hanging
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
SEARCH_TIMEOUT_S = 90   # max seconds per doc-type search before we skip it

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Exact checkbox values confirmed from live site audit
DOC_TYPES = {
    "JDG":       ("JUD",    "Judgment",              ["JDG"]),
    "LIEN":      ("LNTAX",  "Lien",                  ["LIEN"]),
    "NOFC":      ("NOFC",   "Notice of Foreclosure", ["NOFC"]),
    "PRO":       ("PRO",    "Probate",               ["PRO"]),
    "REL":       ("RELLP",  "Release",               ["REL"]),
    "TRUSTDEED": ("TD",     "Trust Deed",            ["TRUSTDEED"]),
    "DISC":      ("DISC",   "Discharge",             ["DISCHARGE"]),
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

# ── FRAME DETECTION ───────────────────────────────────────────────────────────
async def get_search_frame(page):
    await page.wait_for_timeout(2000)
    for frame in page.frames:
        try:
            html = await frame.content()
            soup = BeautifulSoup(html, "lxml")
            has_cbs  = len(soup.find_all("input", {"type": "checkbox"}))
            has_form = bool(soup.find("form"))
            if has_cbs > 0 and has_form:
                save_debug("search_frame", html)
                print(f"[frame] ✓ {frame.url[:80]}")
                return frame
        except:
            pass
    return page

async def wait_for_search_form(frame):
    for selector in ["input[name='start_date']", "input[type='checkbox']"]:
        try:
            await frame.wait_for_selector(selector, timeout=5000)
            return True
        except:
            pass
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
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        save_debug("outer_page", await page.content())

        frame = await get_search_frame(page)
        await wait_for_search_form(frame)

        for code, (cat, cat_label, match_vals) in DOC_TYPES.items():
            print(f"[clerk] → searching {code} ...")
            try:
                # Hard timeout per search so one bad type can't hang the whole job
                records = await asyncio.wait_for(
                    search_one_type(frame, ctx, page, code, cat,
                                    cat_label, match_vals, start_str, end_str),
                    timeout=SEARCH_TIMEOUT_S
                )
                all_records.extend(records)
                print(f"[clerk] {code}: {len(records)} records ✓")
            except asyncio.TimeoutError:
                print(f"[clerk] {code}: TIMED OUT after {SEARCH_TIMEOUT_S}s — skipping")
            except Exception as e:
                print(f"[clerk] {code} error: {e}")
                traceback.print_exc()

            try:
                frame = await reset_form(page)
            except Exception as e:
                print(f"[reset] {e}")

        await browser.close()
    return all_records


async def reset_form(page):
    await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=20000)
    await page.wait_for_timeout(1500)
    frame = await get_search_frame(page)
    await wait_for_search_form(frame)
    return frame


async def search_one_type(frame, ctx, page, code, cat, cat_label,
                           match_vals, start_str, end_str):

    # 1. Uncheck all
    for cb in await frame.query_selector_all("input[type='checkbox']"):
        try:
            if await cb.is_checked():
                await cb.click()
                await frame.wait_for_timeout(10)
        except:
            pass

    # 2. Check target by exact value
    checked = False
    for cb in await frame.query_selector_all("input[type='checkbox']"):
        try:
            val = (await cb.get_attribute("value") or "").strip().upper()
            if val in [m.upper() for m in match_vals]:
                if not await cb.is_checked():
                    await cb.click()
                    await frame.wait_for_timeout(60)
                checked = True
                print(f"[{code}] ✓ checked: '{val}'")
                break
        except:
            pass

    if not checked:
        print(f"[{code}] ⚠ no checkbox match for {match_vals} — skipping")
        return []

    # 3. Fill dates
    filled = 0
    for inp in await frame.query_selector_all("input"):
        try:
            nm  = (await inp.get_attribute("name") or "").lower()
            id_ = (await inp.get_attribute("id")   or "").lower()
            combined = nm + " " + id_
            if "start_date" in combined and filled == 0:
                await inp.triple_click()
                await inp.fill(start_str)
                filled += 1
            elif "end_date" in combined and filled == 1:
                await inp.triple_click()
                await inp.fill(end_str)
                filled += 1
        except:
            pass

    print(f"[{code}] dates filled: {filled}/2  ({start_str} → {end_str})")
    await frame.wait_for_timeout(200)

    # 4. Submit
    submitted = False
    for sel in ["text=Search (F2)", "a:has-text('Search')",
                "button:has-text('Search')", "input[value*='Search']"]:
        try:
            await frame.click(sel, timeout=3000)
            submitted = True
            print(f"[{code}] submitted via '{sel}'")
            break
        except:
            pass
    if not submitted:
        await frame.keyboard.press("F2")
        print(f"[{code}] submitted via F2")

    # 5. Wait for results
    await page.wait_for_timeout(2500)

    results_frame = frame
    for f in page.frames:
        try:
            html = await f.content()
            if any(k in html for k in ["Grantor", "Instrument #",
                                        "Record Count", "No records found"]):
                results_frame = f
                break
        except:
            pass

    await page.wait_for_timeout(1000)
    results_html = await results_frame.content()
    save_debug(f"results_{code}", results_html)

    # 6. Try CSV download first
    csv_records = await try_csv_download(results_frame, ctx, code, cat, cat_label)
    if csv_records:
        return csv_records

    # 7. Fall back to HTML parse
    return parse_html_table(BeautifulSoup(results_html, "lxml"), code, cat, cat_label)


async def try_csv_download(frame, ctx, code, cat, cat_label):
    try:
        dl_link = None
        for sel in ["text=Download results into CSV file",
                    "a:has-text('CSV')", "a:has-text('Download')"]:
            try:
                dl_link = await frame.query_selector(sel)
                if dl_link: break
            except:
                pass
        if not dl_link:
            return []

        async with ctx.expect_download(timeout=15000) as dl_info:
            await dl_link.click()
        download = await dl_info.value
        tmp_path = f"/tmp/shelby_{code}.csv"
        await download.save_as(tmp_path)

        records = []
        with open(tmp_path, encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                row = {k.lower().strip(): (v or "").strip() for k, v in row.items()}
                owner   = row.get("grantor") or row.get("owner") or ""
                doc_num = row.get("record info") or row.get("instrument #") or ""
                if not owner and not doc_num: continue
                filed  = parse_date(row.get("rec. date") or row.get("date") or "")
                prop_d = row.get("prop. description") or row.get("property description") or ""
                pa, pc, ps, pz = parse_address(prop_d)
                records.append({
                    "doc_num": doc_num, "doc_type": code,
                    "filed": filed or "", "cat": cat, "cat_label": cat_label,
                    "owner": owner,
                    "grantee": row.get("grantee") or "",
                    "amount": safe_float(row.get("consideration") or row.get("amount") or ""),
                    "legal": prop_d,
                    "prop_address": pa, "prop_city": pc or "Memphis",
                    "prop_state": "TN", "prop_zip": pz,
                    "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                    "clerk_url": "", "flags": [], "score": 0,
                })
        print(f"[{code}] ✓ CSV: {len(records)} rows")
        return records
    except Exception as e:
        print(f"[{code}] CSV error: {e}")
        return []


def parse_html_table(soup, code, cat, cat_label):
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th", "td"])]
        if not any(k in " ".join(hdrs)
                   for k in ["record", "grantor", "instrument", "date"]):
            continue
        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells or all(c == "" for c in cells): continue
            row  = dict(zip(hdrs, cells))
            link = tr.find("a", href=True)
            href = link["href"] if link else ""
            clerk_url = (href if href.startswith("http")
                         else CLERK_BASE + "/" + href.lstrip("/")) if href else ""
            doc_num = _pick(row, ["record info", "instrument", "doc"]) or cells[0]
            owner   = _pick(row, ["grantor", "owner", "party 1", "name"])
            filed   = parse_date(_pick(row, ["rec. date", "date", "filed"]))
            prop_d  = _pick(row, ["prop. description", "property", "description"])
            pa, pc, ps, pz = parse_address(prop_d)
            if not doc_num and not owner: continue
            records.append({
                "doc_num": doc_num, "doc_type": code,
                "filed": filed or "", "cat": cat, "cat_label": cat_label,
                "owner": owner or "",
                "grantee": _pick(row, ["grantee", "party 2"]) or "",
                "amount": safe_float(_pick(row, ["consideration", "amount"])),
                "legal": prop_d,
                "prop_address": pa, "prop_city": pc or "Memphis",
                "prop_state": "TN", "prop_zip": pz,
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                "clerk_url": clerk_url, "flags": [], "score": 0,
            })
    return records


# ── PARCEL ENRICHMENT ─────────────────────────────────────────────────────────
def download_parcel_dbf():
    if not HAS_DBF: return {}
    raw = None
    for url in [
        "https://www.assessor.shelby.tn.us/downloads/parcel_data.zip",
        "https://www.assessor.shelby.tn.us/downloads/Parcel.zip",
        "https://www.assessor.shelby.tn.us/downloads/parcels.zip",
    ]:
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
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
        if o and {"NOFC", "JUD"}.issubset(oc.get(o, set())):
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
        records = await asyncio.wait_for(
            scrape_clerk(start_dt, end_dt),
            timeout=1500   # 25 min hard cap on entire scrape
        )
        print(f"[clerk] total raw: {len(records)}")
    except asyncio.TimeoutError:
        print("[clerk] ⚠ overall scrape hit 25min cap — saving what we have")
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
