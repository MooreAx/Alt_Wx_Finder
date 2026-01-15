# build_tafs_hourly.py — builds hourly TAF DataFrame from parsed_reports.json

import json
import pandas as pd
import re
from datetime import datetime, timedelta
from fractions import Fraction


print("Running")

# ----------------------------
# Load parsed JSON
# ----------------------------
with open("parsed_reports_dev.json", "r") as f:
    data = json.load(f)

# ----------------------------
# Extract TAF reports
# ----------------------------
taf_records = []
taf_records_busted_issuedtime = []

for file in data:
    filename = file.get("filename")
    for taf in file.get("tafs", []):

        record = {
            "filename": filename,
            "station": taf.get("station"),
            "issued": taf.get("issued"),
            "db_time_stamp": taf.get("db_time_stamp"),
            "type": taf.get("type"),
            "contents": taf.get("contents"),
            "remark": taf.get("remark"),
            "raw": taf.get("raw")
        }

        if taf.get("issued") is None:
            taf_records_busted_issuedtime.append(record)
        else:
            taf_records.append(record)

#TAF DEDUPLICATION

def dedupe_tafs(df):
    before = len(df)

    df = (
        df
        .sort_values("issued")
        .drop_duplicates(
            subset=["station", "issued"],
            keep="last"
        )
        .reset_index(drop=True)
    )

    after = len(df)

    if after < before:
        print(f"Deduped TAFs: {before} --> {after}")

    return df

#deduplicate
df_tafs = pd.DataFrame(taf_records)
df_tafs = dedupe_tafs(df_tafs)
taf_records = df_tafs.to_dict(orient="records")


df_tafs_busted_issuedtime = pd.DataFrame(taf_records_busted_issuedtime)

#write bad ones for examination
df_tafs_busted_issuedtime.to_csv("tafs_busted_issued_time.csv", index=False)

print(f"Loaded {len(df_tafs)} TAFs from {len(data)} files")
print(df_tafs.head())

# ----------------------------
# Regex patterns
# ----------------------------

wind_pattern = re.compile(r'(?P<direction>\d{3}|VRB|000)(?P<speed>\d{2,3})(G(?P<gust>\d{2,3}))?KT')
vis_pattern = re.compile(r'\s(P6SM|\d+\s\d+/\d+SM|\d+/\d+SM|\d+SM)\b')
#key learning here: the literal ?: inside parantheses makes the group non capturing. when there are capturing groups, findall returns tuples.
sigwx_pattern = re.compile(
    r'(?:(?<=^)|(?<=\s))'                   #look behind: start with new line or space (do not consume)
    r'(?:[\+\-−–]|VC)?'                     # optional intensity / proximity
    r'(?:MI|BC|PR|DR|BL|SH|TS|FZ)?'         # optional descriptor
    r'(?:DZ|RA|SN|SG|IC|PL|GR|GS|BR|FG|FU|DU|SA|HZ|VA|PO|SQ|NSW|\+?FC|\+?SS|\+?DS)+\b'  # phenomenon
)
cloud_pattern = re.compile(r'\bSKC|(?:FEW|SCT|BKN|OVC)\d{3}(?:CB)?|VV\d{3}\b')

#FIX ME - missing the optional CB at the end of ceiling
ceilings_pattern = re.compile(r'\b(?:BKN|OVC|VV)\d{3}(?:CB)?\b')
ceiling_pattern = re.compile(r'\d{3}')

# ----------------------------
# Helper functions
# ----------------------------

def parse_ddhh(ddhh: str, issued_str, window_hrs = 48):
    #convert ddhh to datetime
    issue_dt = datetime.strptime(issued_str, "%Y%m%d%H%M")

    candidates = []
    for delta in range(-window_hrs, window_hrs + 1):
        dt = issue_dt + timedelta(hours=delta)
        short = dt.strftime("%d%H") #midnight = dd00 on new day
        
        #need to create an additional possible format for midnight
        if dt.hour == 0:
            dd_prev = (dt - timedelta(days=1)).strftime("%d")
            short_24 = f"{dd_prev}24" #midnight == dd24 on prev day
        else:
            short_24 = short

        candidates.append((dt.replace(minute=0, second=0, microsecond=0), short, short_24))

    ddhh_dt = min(
                (dt for dt, short, short_24 in candidates if ddhh in (short, short_24)),
                key=lambda dt: abs(dt - issue_dt),
                default=None)
    return ddhh_dt

def extract_wind(raw):
    match = wind_pattern.search(raw)
    if match:
        gust = int(match.group("gust")) if match.group("gust") else None
        return match.group("direction"), int(match.group("speed")), gust
    return None, None, None

# Visibility
def extract_visibility(raw):
    #fractional visibilities are missing the space in the taf
    match = vis_pattern.search(raw)
    if not match:
        return None

    vis = match.group(0).replace("SM", "").strip()

    if vis == "P6":
        return 6.1  # Flag >6 miles

    try:
        if " " in vis:  # e.g., '1 1/2'
            whole, frac = vis.split()
            return int(whole) + float(Fraction(frac))
        elif "/" in vis:  # e.g., '11/2' or '3/4'
            if len(vis.split("/")[0]) > 1:  # e.g., '11/2' → treat as '1 1/2'
                numerator, denominator = vis.split("/")
                whole = int(numerator[:-1])  # e.g., '1' from '11'
                fraction = f"{numerator[-1]}/{denominator}"  # e.g., '1/2' from '11/2'
                return whole + float(Fraction(fraction))
            else:  # e.g., '3/4'
                return float(Fraction(vis))
        else:  # Whole number, e.g., '5'
            return int(vis)
    except Exception as e:
        print(f"Error parsing visibility {vis}: {e}")
        return None

def extract_sigwx(raw):
    matches = [m.group(0) for m in sigwx_pattern.finditer(raw)]
    if not matches:
        return None
    return ", ".join(matches)

def extract_clouds(raw):
    matches = [m.group(0) for m in cloud_pattern.finditer(raw)]
    if not matches:
        return None
    # Extract the full match (group 0) from each tuple
    return ", ".join(matches)

def extract_ceilings(clouds):
    if not clouds:
        return None
    matches = [m.group(0) for m in ceilings_pattern.finditer(clouds)]
    if not matches:
        return None
    return ", ".join(matches)

def extract_ceiling(ceilings):
    if not ceilings:
        return None
    matches = [int(m.group(0)) for m in ceiling_pattern.finditer(ceilings)]
    if not matches:
        return None
    # Convert to feet by multiplying by 100
    return min(matches) * 100

def split_taf_segments(raw):
    """
    Split a TAF string into segments whenever a TAF, FM, BECMG, TEMPO, or PROB occurs.
    Returns a list of dictionaries with segment type and raw text.
    """

    #strip taf of db_time_stamp and rmk:
    pattern = r'(?<=\d{12}\s).*?(?=(?:\sRMK.*Z=|$))'
    tafmeat = re.search(pattern, raw).group(0)

    raw_marked = re.sub(r'(TAF|FM|BECMG|TEMPO|PROB)', r'*\1', tafmeat)
    parts = [s.strip() for s in raw_marked.split('*') if s.strip()]

    segments = []
    for seg in parts:
        m = re.search(r'^(TAF|FM|BECMG|TEMPO|PROB)', seg)
        seg_type = m.group(1) if m else "UNKNOWN"
        segments.append({"raw": seg, "type": seg_type})
    return segments

# ----------------------------
# Construct Segments
# ----------------------------

'''
a nested structure of tafs would be better for the looping / hourly expanding that i'm about to do...

structure is:
taf
    raw
    station
    issued
    valid_from
    valid_to
    remarks
    segments
        segment1
            raw
            type
            start
            end
            wind
            vis
            sigwx
            clouds
            ceilings
            ceiling
        segment2
        segment3
'''

nested_tafs = []

for taf in taf_records:
    filename = taf.get("filename")
    raw_taf = taf.get("raw")
    station = taf.get("station")
    issued = taf.get("issued")
    db_time_stamp = taf.get("db_time_stamp")
    remarks = taf.get("remark")
    type = taf.get("type")

    taf_status = "NORMAL"
    
    #look for cancelled or nil tafs:
    if bool(re.search(r'NIL', raw_taf)):
        taf_status = "NIL"

    elif (
        bool(re.search(r'FCST CNCLD', raw_taf))
        or bool(re.search(r'FCST NOT AVBL', raw_taf))
        or bool(re.search(r'CNL RMK NO OBS', raw_taf))
        or bool(re.search(r' CNL ', raw_taf))
    ):
        taf_status = "CANCELLED"

    valid_from, valid_to = None, None
    valid_period_match = re.search(r'\d{4}/\d{4}', raw_taf)
    if valid_period_match:
        valid_period = valid_period_match.group(0)
        valid_from_ddhh = valid_period[:4] #first 4 char
        valid_to_ddhh = valid_period[-4:] #last 4 char
        valid_from = parse_ddhh(valid_from_ddhh, issued_str = issued)
        valid_to = parse_ddhh(valid_to_ddhh, issued_str = issued)

    nested_segments = []

    segments = split_taf_segments(raw_taf)
    for seg in segments:
        seg_raw = seg.get('raw')
        seg_type = seg.get('type')
        start_dt, end_dt = None, None

        #get times
        if seg['type'] in {"TAF", "BECMG", "TEMPO", "PROB"}:
            valid_period_match = re.search(r'\d{4}\/\d{4}', seg_raw)

            if valid_period_match:
                ddhh_ddhh = valid_period_match.group(0)
                ddhh1 = ddhh_ddhh[:4]
                ddhh2 = ddhh_ddhh[-4:]

                start_dt = parse_ddhh(ddhh1, issued)
                end_dt = parse_ddhh(ddhh2, issued)

        elif seg['type'] == "FM":
            ddhh1 = re.search(r'^FM\d{4}', seg_raw).group(0)[2:6]
            start_dt = parse_ddhh(ddhh1, issued)

        w_dir, w_speed, w_gust = extract_wind(seg_raw)
        vis = extract_visibility(seg_raw)
        sigwx = extract_sigwx(seg_raw)
        clouds = extract_clouds(seg_raw)
        ceilings = extract_ceilings(clouds)
        ceiling = extract_ceiling(ceilings)

        nested_segments.append({
            "raw": seg_raw,
            "type": seg_type,
            "start": start_dt,
            "end": end_dt,
            "dir": w_dir,
            "speed": w_speed,
            "gust": w_gust,
            "vis": vis,
            "sigwx": sigwx,
            "clouds": clouds,
            "ceilings": ceilings,
            "ceiling": ceiling,
        })

    nested_tafs.append({
        "filename": filename,
        "station": station,
        "issued": issued,
        "db_time_stamp": db_time_stamp,
        "type": type,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "raw": raw_taf,
        "remarks": remarks,
        "segments": nested_segments,
        "status": taf_status
    })


print("complete")

import json

with open("nested_tafs.json", "w") as f:
    json.dump(nested_tafs, f, default=str)



#convert json to csv for viewing in excel:

rows = []
for taf in nested_tafs:
    for seg in taf["segments"]:
        rows.append({
            "filename": taf.get("filename"),
            "station": taf.get("station"),
            "issued": taf.get("issued"),
            "db_time_stamp": taf.get("db_time_stamp"),
            "type": taf.get("type"),
            "status": taf.get("status"),
            "fulltaf": taf.get("raw"),
            "valid_from": taf.get("valid_from"),
            "valid_to": taf.get("valid_to"),
            "remarks": taf.get("remarks"),
            "segment_raw": seg.get("raw"),
            "start_dt": seg.get("start"),
            "end_dt": seg.get("end"),
            "dir": seg.get("dir"),
            "speed": seg.get("speed"),
            "gust": seg.get("gust"),
            "vis": seg.get("vis"),
            "sigwx": seg.get("sigwx"),
            "clouds": seg.get("clouds"),
            "ceilings": seg.get("ceilings"),
            "ceiling": seg.get("ceiling"),
        })

# Create DataFrame
df_segments = pd.DataFrame(rows)

# Save to CSV
df_segments.to_csv("tafs_segments.csv", index=False)



