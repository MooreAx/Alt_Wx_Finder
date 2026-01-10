from datetime import datetime
import json
import pandas as pd
import re


INPUT = "nested_tafs.json"
OUTPUT = "nested_tafs_clean.json"
PROBLEMS = "nested_tafs_problems.json"

with open(INPUT) as f:
    tafs = json.load(f)

cleaned = []
dropped = []

i = 0
total = len(tafs)
for taf in tafs:

    i += 1
    print(f"{i} of {total} -- dropped {len(dropped)}")

    vf = pd.to_datetime(taf["valid_from"])
    vt = pd.to_datetime(taf["valid_to"])

    drop_taf = False

    raw_taf = taf.get("raw")
    print(raw_taf)

    if taf.get("status") in {"CANCELLED", "NIL"}:
        cleaned.append(taf)
        continue


    for seg in taf["segments"]:
        start = pd.to_datetime(seg["start"]) if seg["start"] else None
        end   = pd.to_datetime(seg["end"]) if seg["end"] else None

        # FM: only start must exist
        if seg["type"] == "FM":

            if start is None or not (vf <= start <= vt):
                drop_taf = True
                break

        # All others: start and end must exist and be inside TAF window
        else:
            if (
                start is None or end is None or
                start < vf or
                end   > vt or
                start >= end
            ):
                drop_taf = True
                break

    if drop_taf:
        dropped.append({
            "station": taf["station"],
            "issued": taf["issued"],
            "raw": taf["raw"]
        })
        continue

    cleaned.append(taf)

with open(OUTPUT, "w") as f:
    json.dump(cleaned, f, indent=2, default=str)

with open(PROBLEMS, "w") as f:
    json.dump(dropped, f, indent=2, default=str)

print(f"Dropped {len(dropped)} TAFs")
print(f"Kept    {len(cleaned)} TAFs")


