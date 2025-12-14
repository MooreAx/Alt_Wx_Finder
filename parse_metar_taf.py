import re
import json
from pathlib import Path
from bs4 import BeautifulSoup

# CONFIG
input_folder = Path("data")
output_path = Path("parsed_reports.json")

# REGEX PATTERNS
REPORT_START_RE = re.compile(
    r'(?P<ts>\d{10,12})\s+'
    r'(?P<type>METAR|SPECI|TAF(?:\sAMD)?)\s+'
    r'(?P<station>[A-Z]{4})\s+'
    r'(?P<raw>\d{6}Z.*?)=',
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


def extract_reports(text: str):
    """Return list of reports with station, type, issued timestamp, and cleaned raw string."""
    reports = []
    for m in REPORT_START_RE.finditer(text):
        full_match = m.group(0)
        full_match_clean = ' '.join(full_match.split()) #clean up entire string
        reports.append({
            'station': m.group('station'),
            'type': m.group('type'),
            'issued': m.group('ts'),
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

    tafs.sort(key=lambda x: (x["station"], x["issued"]))
    metars.sort(key=lambda x: (x["station"], x["issued"]))

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



