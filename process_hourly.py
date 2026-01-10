
import pandas as pd
from datetime import datetime, timedelta
import json
import re
import numpy as np

print("running")

# Load nested TAFs (produced from earlier step)
with open("nested_tafs_clean.json", "r") as f:
    tafs = json.load(f)


def expand_taf_to_hourly(taf):
    taf_status = taf.get("status")
    start_time = pd.to_datetime(taf.get("valid_from"))
    end_time = pd.to_datetime(taf.get("valid_to"))

    if taf_status in {"CANCELLED", "NIL"}:

        #debug step:
        print(taf.get("issued"))
        print(pd.to_datetime(taf.get("issued")),)

        return pd.DataFrame([{
            "raw_taf": taf.get("raw"),
            "station": taf.get("station"),
            "issued": taf.get("issued"),
            "status": taf_status,
            "time": pd.to_datetime(taf.get("issued")),
            "wind": "",
            "vis": "",
            "sigwx": "",
            "clouds": "",
            "ceiling": ""
            }])
    elif start_time is None or end_time is None:
        return None

    hours = pd.date_range(start=start_time, end=end_time, freq="1h", inclusive="left")
    df = pd.DataFrame({
        "raw_taf": taf["raw"],  # <--- add raw TAF column here
        "station": taf["station"],
        "issued": taf["issued"],
        "status": taf_status,
        "time": hours,
        "wind": "",
        "vis": "",
        "sigwx": "",
        "clouds": "",
        "ceiling": ""
    })

    # Helper functions for adding markers
    def add_tempo(base, val):
        if val and base:
            return f"{base} ({val})".strip()
        elif base:
            return base
        elif val:
            return f"({val})"
        else:
            return ""

    def add_prob(base, val, prob):
        return f"{base} [{prob}%: {val}]".strip() if val else base

    def add_becmg(base, val):
        return f"{base} -> {val}".strip() if val else base

    def add_becmg_after(base, val):
        return val or base

    def format_wind(drn, speed, gust):
        """Return wind as TAF-style string: e.g., 12010G20"""
        if drn is None or speed is None:
            return ""

        speed_str = f"{int(speed):02d}"  # 2-digit speed
        wind = drn + speed_str

        if gust:
            wind += "G" + f"{int(gust):02d}"

        return wind + "KT"

    # Iterate through all segments
    for seg in taf["segments"]:
        seg_type = seg["type"]
        seg_start = pd.to_datetime(seg["start"]) if seg["start"] else None
        seg_end = pd.to_datetime(seg["end"]) if seg["end"] else end_time

        #extract segment data
        wind = format_wind(seg.get("dir"), seg.get("speed"), seg.get("gust"))
        vis = seg.get("vis")
        sigwx = seg.get("sigwx")
        clouds = seg.get("clouds")
        ceiling = seg.get("ceiling")
        if ceiling:
            ceiling = str(ceiling)


        #throwing and error. trap:
        try:
            mask = (df["time"] >= seg_start) & (df["time"] < seg_end)
        except Exception as e:
            print("\nERROR CREATING MASK")
            print("Station:", taf.get("station"))
            print("Raw TAF:", taf.get("raw"))
            print("Segment dict:", seg)
            print("seg_start:", seg_start, type(seg_start))
            print("seg_end:", seg_end, type(seg_end))
            raise


        if seg_type in ["TAF", "FM"]:
            df.loc[mask, ["wind", "vis", "sigwx", "clouds", "ceiling"]] = [wind, vis, sigwx, clouds, ceiling]

        elif seg_type == "TEMPO":
            for col, val in zip(["wind", "vis", "sigwx", "clouds", "ceiling"], [wind, vis, sigwx, clouds, ceiling]):
                if val:
                    df.loc[mask, col] = df.loc[mask, col].apply(lambda x: add_tempo(x, val))

        elif seg_type.startswith("PROB"):
            prob = int(re.search(r"PROB(\d{2})", seg["raw"]).group(1))
            for col, val in zip(["wind", "vis", "sigwx", "clouds", "ceiling"], [wind, vis, sigwx, clouds, ceiling]):
                if val:
                    df.loc[mask, col] = df.loc[mask, col].apply(lambda x: add_prob(x, val, prob))

        elif seg_type == "BECMG":
            # During the transition
            during_mask = (df["time"] >= seg_start) & (df["time"] < seg_end)
            for col, val in zip(["wind", "vis", "sigwx", "clouds", "ceiling"], [wind, vis, sigwx, clouds, ceiling]):
                if val:
                    df.loc[during_mask, col] = df.loc[during_mask, col].apply(lambda x: add_becmg(x, val))

            # After transition complete
            after_mask = df["time"] >= seg_end
            for col, val in zip(["wind", "vis", "sigwx", "clouds", "ceiling"], [wind, vis, sigwx, clouds, ceiling]):
                if val:
                    df.loc[after_mask, col] = df.loc[after_mask, col].apply(lambda x: add_becmg_after(x, val))

    return df


# ---- Run for all TAFs ----
all_taf_hours = []
for taf in tafs:
    df_hourly = expand_taf_to_hourly(taf)
    all_taf_hours.append(df_hourly)

df_tafs_hourly = pd.concat(all_taf_hours, ignore_index=True)


#extract alternate minima data:

#initialize columns
df_tafs_hourly["altmin_ceiling"] = np.nan
df_tafs_hourly["altmin_vis"] = np.nan
df_tafs_hourly["prob_ceiling"] = np.nan

# Extract numeric values for ceilings
def extract_min_ceiling(text):
    if not text:
        return np.nan
    #exclude prob conditions
    text_no_prob = re.sub(r"\[\d{2}%: \d+\]", "", text)

    #extract ceilings from remaining string
    vals = [float(x) for x in re.findall(r"\d+", text_no_prob)]
    return min(vals) if vals else np.nan

def extract_prob_ceiling(text):
    if not text:
        return np.nan
    vals = [float(x) for x in re.findall(r"(?<=\[\d{2}%: )\d+(?=\])", text)]
    return min(vals) if vals else np.nan

# Extract numeric values for visibility
def extract_min_vis(text):
    if not text:
        return np.nan
    text = str(text)
    #exclude prob condition:
    text_no_prob = re.sub(r"\[\d{2}%: \d+(?:\.\d+)?\]", "", text)

    #extract ceilings from remaining string
    vals = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", text_no_prob)]
    return min(vals) if vals else np.nan

print("starting alt min processing")
# Apply vectorized (under the hood this is compiled regex run in C)
df_tafs_hourly["altmin_ceiling"] = df_tafs_hourly["ceiling"].apply(extract_min_ceiling)
df_tafs_hourly["altmin_vis"] = df_tafs_hourly["vis"].apply(extract_min_vis)
df_tafs_hourly["prob_ceiling"] = df_tafs_hourly["ceiling"].apply(extract_prob_ceiling)


df_tafs_hourly.to_csv("tafs_hourly.csv", index=False)
print(f"Saved {len(df_tafs_hourly)} hourly rows to tafs_hourly.csv")
