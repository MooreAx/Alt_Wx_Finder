import json
from pathlib import Path
from datetime import datetime, timedelta, date


#RUN = "ANALYSIS1"
RUN = "ANALYSIS2"

# Paths
input_path = Path("parsed_reports.json")
output_path = Path("parsed_reports_dev.json")

# Filters

if RUN == "ANALYSIS1":
    keep_stations = {"CYYQ", "CYTH", "CYQD", "CYYL"}


    MIN_DATE = date(2024, 11, 1)
    MAX_DATE = date(2025, 11, 30)

elif RUN == "ANALYSIS2":
    keep_stations = {"CYYQ", "CYTH", "CYQD", "CYYL"}


    MIN_DATE = date(2022, 10, 1)
    MAX_DATE = date(2025, 10, 1)


def parse_issued_date(issued):
    try:
        return datetime.strptime(issued, "%Y%m%d%H%M").date()
    except Exception:
        return None


print(f"Loading {input_path}...")
with input_path.open(encoding="utf-8") as f:
    data = json.load(f)

filtered = []

for entry in data:
    meta_station = entry.get("meta", {}).get("station")

    # If the meta station isn't in our keep list, skip
    #if meta_station not in keep_stations:
    #    continue

    # Filter METARs and TAFs by year
    metars = [
        r for r in entry.get("metars", [])
        if ((d:= parse_issued_date(r.get("issued"))) is not None and MIN_DATE <= d <= MAX_DATE)
    ]

    tafs = [
        r for r in entry.get("tafs", [])
        if ((d:= parse_issued_date(r.get("issued"))) is not None and MIN_DATE <= d <= MAX_DATE)
    ]

    # Skip files with no remaining reports
    if not metars and not tafs:
        continue

    # Copy structure with filtered lists
    filtered.append({
        "filename": entry.get("filename"),
        "meta": entry.get("meta", {}),
        "metars": metars,
        "tafs": tafs
    })

# Save the smaller file
with output_path.open("w", encoding="utf-8") as f:
    json.dump(filtered, f, indent=2, ensure_ascii=False)

print(f"Saved filtered dataset with {len(filtered)} entries to {output_path}")
