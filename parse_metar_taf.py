import re
import json
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# CONFIG
input_folder = Path("data")
output_path = Path("parsed_reports.json")

# REGEX PATTERNS
OGIMET_REPORT_RE = re.compile(
    r'(?P<db_time_stamp>\d{10,12})\s'
    r'(?P<type>METAR|SPECI|TAF(?:\sAMD)?)\s'
    r'(?P<station>[A-Z]{4})\s'
    r'(?P<issue_time>\d{6}Z)\s'
    r'(?P<contents>.*?)'
    r'(?:\s(?P<remark>RMK.*?))?=',
    re.DOTALL
)


META_QUERY_RE = re.compile(r'(?m)^#\s*Query made at\s*(?P<query>.+)$')
META_INTERVAL_RE = re.compile(r'(?m)^#\s*Time interval:\s*(?P<interval>.+)$')
META_DETAIL_RE = re.compile(r'(?m)^#\s*Latitude\s*(?P<lat>[\d\-\w.]+)[.]\s*Longitude\s*(?P<lon>[\d\-\w.]+)[.]\s*Altitude\s*(?P<alt>.+).$')
META_STATION_RE = re.compile(r'(?m)^#\s*(?P<station>[A-Z]{4}),')

def read_file(path: Path) -> str:
    """Read text file safely (ignore bad characters) and strip HTML."""
    html = path.read_text(encoding='utf-8')
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()


def extract_meta(text: str) -> dict:
    """Extract station, query time, interval, and coordinates."""
    meta = {}
    if m := META_STATION_RE.search(text):
        meta["station"] = m.group("station")
    if q := META_QUERY_RE.search(text):
        meta["query_date"] = q.group("query").strip()
    if ti := META_INTERVAL_RE.search(text):
        meta["interval"] = ti.group("interval").strip()
    if d := META_DETAIL_RE.search(text):
        meta["latitude"] = d.group("lat").strip()
        meta["longitude"] = d.group("lon").strip()
        meta["elevation"] = d.group("alt").strip()
    return meta


def parse_issued_time(issued_ddhhmmZ: str, db_time_stamp: str, window_days: int = 3) -> datetime | None:

    """
    There are some instances where the db time stamp does not align with the issue day.

    For example:

    202404191800 TAF AMD CYBK 171317Z 1713/1724 32020G30KT P6SM FEW006 SCT050 TEMPO 1713/1715 3SM -SN BKN006 OVC050 FM171500 34020G30KT P6SM FEW010 FEW006 RMK NXT FCST BY 171800Z="

    --> "202404191800" is the ogimet timestamp (Apr 19)
    --> "TAF AMD CYBK 171317Z" is the issue time (17th day)
    --> these are not the same. since 17th day is repeated across the taf, assume this is correct.

    --> so take the ddhhmm from the issue time, and use the yyyymm from the db time stamp

    """

    anchor = datetime.strptime(db_time_stamp, "%Y%m%d%H%M")

    target_dd = int(issued_ddhhmmZ[:2])
    hh = int(issued_ddhhmmZ[2:4])
    mm = int(issued_ddhhmmZ[4:6])

    start_date = anchor.date() - timedelta(days=window_days)

    for i in range(2 * window_days + 1):
        d = start_date + timedelta(days=i)
        if d.day == target_dd:
            return datetime(d.year, d.month, d.day, hh, mm)

    return None

def normalize_ws(s: str | None) -> str | None:
    if s is None:
        return None
    return " ".join(s.split())

def extract_reports(text: str):
    """Return list of reports with station, type, issued timestamp, and cleaned raw string."""
    reports = []
    for m in OGIMET_REPORT_RE.finditer(text):
        full_match = m.group(0)
        full_match_clean = normalize_ws(full_match)

        db_time_stamp = m.group('db_time_stamp')

        remark = m.group('remark')
        remark = normalize_ws(remark) if remark else None

        contents = m.group('contents')
        contents = normalize_ws(contents) if contents else None

        m_issued = m.group('issue_time')
        issued_dt = None
        if m_issued:
            issued_dt = parse_issued_time(m_issued, db_time_stamp)

        issued_str_out = issued_dt.strftime("%Y%m%d%H%M") if issued_dt is not None else None

        reports.append({
            'station': m.group('station'),
            'type': m.group('type'),
            'db_time_stamp': db_time_stamp,
            'issued': issued_str_out,
            'contents': contents,
            'remark': remark,
            'raw': full_match_clean
        })
    return reports


def build_output_for_file(file: Path) -> dict:
    """Build JSON-friendly structure for a single input file."""
    text = read_file(file)
    meta = extract_meta(text)
    reports = extract_reports(text)
    tafs = [r for r in reports if r["type"] in ("TAF", "TAF AMD")]
    metars = [r for r in reports if r["type"] in ("METAR", "SPECI")]

    tafs.sort(key=lambda x: (x["station"], x["db_time_stamp"]))
    metars.sort(key=lambda x: (x["station"], x["db_time_stamp"]))

    return {
        "filename": file.name,     # top-level file name
        "meta": meta,
        "metars": metars,
        "tafs": tafs
    }


# MAIN EXECUTION
all_files = []
i = 0
for file in sorted(input_folder.glob("*.txt")):
    i += 1
    print(f"Parsing {file.name} - {i}")

    parsed = build_output_for_file(file)
    all_files.append(parsed)

with output_path.open("w", encoding="utf-8") as filehandle:
    json.dump(all_files, filehandle, indent=2, ensure_ascii=False)

print(f"Saved parsed output for {len(all_files)} files to {output_path}")



