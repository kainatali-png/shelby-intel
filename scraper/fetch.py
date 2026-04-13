"""
Shelby County, TN — Motivated Seller Lead Scraper
v3 — uses requests session only (no Playwright form submit)
"""

import asyncio, csv, io, json, os, re, traceback, zipfile
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

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
    "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(re.sub(r"[^\d.]", "", str(v))) if v else None
    except: return None

def parse_date(s):
    if not s: return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
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

# ── SESSION SETUP ─────────────────────────────────────────────────────────────
def make_session():
    """Create a requests session that mimics a real browser."""
    s = requests.Session()
    s.headers.update(HEADERS)
    hidden = {}
    try:
        r = s.get(CLERK_SEARCH, timeout=30)
        print(f"[session] home page status: {r.status_code}")
        soup = BeautifulSoup(r.text, "lxml")
        save_debug("home_page", r.text)
        for inp in soup.find_all("input", type="hidden"):
            name = inp.get("name", "")
            val  = inp.get("value", "")
            if name:
                hidden[name] = val
        if hidden:
            print(f"[session] found hidden fields: {list(hidden.keys())}")
        else:
            print("[session] no hidden fields found")
    except Exception as e:
        print(f"[session] home page error: {e}")
    return s, hidden

# ── SEARCH ────────────────────────────────────────────────────────────────────
def search_doc_type(session, hidden_fields, code, cat, cat_label,
                    start_str, end_str):
    """Try every known endpoint/payload combo for one document type."""

    base = {**hidden_fields}

    payload_variants = [
        {**base, "searchType": "instrType", "inst_type1": code,
         "beg_date": start_str, "end_date": end_str, "submit": "Search"},
        {**base, "searchType": "instrType", "inst_type1": code,
         "beg_date": start_str, "end_date": end_str, "btnSearch": "Search"},
        {**base, "searchType": "instrType", "inst_type1": code,
         "beg_date": start_str, "end_date": end_str},
        {**base, "searchType": "instrType", "instrType": code,
         "beg_date": start_str, "end_date": end_str, "submit": "Search"},
        {**base, "searchType": "instrType", "inst_type": code,
         "beg_date": start_str, "end_date": end_str, "submit": "Search"},
    ]

    endpoints = [
        f"{CLERK_BASE}/search/SearchResults.php",
        f"{CLERK_BASE}/search/index.php",
        f"{CLERK_BASE}/search/InstrTypeResults.php",
        f"{CLERK_BASE}/search/results.php",
    ]

    post_headers = {
        "Referer":      CLERK_SEARCH,
        "Origin":       CLERK_BASE,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    for endpoint in endpoints:
        for payload in payload_variants:
            try:
                resp = session.post(endpoint, data=payload,
                                    headers=post_headers, timeout=30,
                                    allow_redirects=True)
                if resp.status_code != 200 or len(resp.text) < 200:
                    continue
                # If we just got the search form back, skip
                if ("Search One Name" in resp.text
                        or "Search For Both" in resp.text
                        or "search_form" in resp.text.lower()[:500]):
                    continue

                soup = BeautifulSoup(resp.text, "lxml")
                rows = _parse_table(soup, code, cat, cat_label)
                if rows:
                    print(f"[clerk] ✓ POST {endpoint}")
                    if code == "LP":
                        save_debug(f"success_{code}", resp.text)
                    return rows

                if code == "LP" and "<table" in resp.text.lower():
                    save_debug(f"attempt_{endpoint.split('/')[-1]}_{code}",
                               resp.text)
            except Exception as e:
                print(f"[clerk] POST {code} {endpoint} → {e}")

    # ── Fallback: GET ─────────────────────────────────────────────────────────
    get_variants = [
        {"searchType": "instrType", "inst_type1": code,
         "beg_date": start_str, "end_date": end_str},
        {"searchType": "instrType", "instrType": code,
         "beg_date": start_str, "end_date": end_str},
    ]
    for endpoint in endpoints[:3]:
        for params in get_variants:
            try:
                resp = session.get(endpoint, params=params,
                                   headers={"Referer": CLERK_SEARCH},
                                   timeout=30)
                if resp.status_code != 200 or len(resp.text) < 200:
                    continue
                if ("Search One Name" in resp.text
                        or "Search For Both" in resp.text):
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                rows = _parse_table(soup, code, cat, cat_label)
                if rows:
                    print(f"[clerk] ✓ GET {endpoint}")
                    return rows
            except Exception as e:
                print(f"[clerk] GET {code} {endpoint} → {e}")

    return []


def _parse_table(soup, code, cat, cat_label):
    records = []
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th", "td"])]
        if not any(k in " ".join(hdrs)
                   for k in ["doc", "date", "grantor", "name", "instr", "party"]):
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
            doc_num = _pick(row, ["instrument","doc","instr","number"]) or cells[0]
            owner   = _pick(row, ["grantor","owner","party 1","name"])
            if not doc_num and not owner:
                continue
            records.append({
                "doc_num":      doc_num or "",
                "doc_type":     code,
                "filed":        parse_date(_pick(row, ["date","filed"])) or "",
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner or "",
                "grantee":      _pick(row, ["grantee","party 2"]) or "",
                "amount":       safe_float(_pick(row, ["amount"])),
                "legal":        _pick(row, ["legal","description"]) or "",
                "prop_address": "", "prop_city": "Memphis",
                "prop_state":   "TN", "prop_zip": "",
                "mail_address": "", "mail_city": "",
                "mail_state":   "", "mail_zip": "",
                "clerk_url":    clerk_url, "flags": [], "score": 0,
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
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")
    print(f"[run] {start_dt.date()} → {end_dt.date()}")

    session, hidden_fields = make_session()

    records = []
    for code, (cat, cat_label) in DOC_TYPES.items():
        try:
            batch = search_doc_type(
                session, hidden_fields, code, cat, cat_label,
                start_str, end_str
            )
            records.extend(batch)
            print(f"[clerk] {code}: {len(batch)} records {'✓' if batch else ''}")
        except Exception as e:
            print(f"[clerk] {code} error: {e}")
            traceback.print_exc()

    print(f"[clerk] total raw: {len(records)}")

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
