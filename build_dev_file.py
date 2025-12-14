import json
from pathlib import Path

# Paths
input_path = Path("parsed_reports.json")
output_path = Path("parsed_reports_dev.json")

# Filters
keep_stations = {"CYYQ", "CYTH", "CYGX", "CYYL", "CYNE", "CYQD"}
keep_years = {"2025"}

print(f"Loading {input_path}...")
with input_path.open(encoding="utf-8") as f:
    data = json.load(f)

filtered = []

for entry in data:
    meta_station = entry.get("meta", {}).get("station")

    # If the meta station isn't in our keep list, skip
    if meta_station not in keep_stations:
        continue

    # Filter METARs and TAFs by year
    metars = [r for r in entry.get("metars", []) if r["issued"][:4] in keep_years]
    tafs = [r for r in entry.get("tafs", []) if r["issued"][:4] in keep_years]

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
