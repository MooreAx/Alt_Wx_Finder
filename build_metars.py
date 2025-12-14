#builds metar df from json file

import json
import pandas as pd
import re
from fractions import Fraction

# Load your parsed JSON file
with open("parsed_reports.json", "r") as f:
    data = json.load(f)

# Extract all METAR reports from all files
metar_records = []
for file_entry in data:
    filename = file_entry["filename"]
    for metar in file_entry.get("metars", []):
        metar_records.append({
            "filename": filename,
            "station": metar["station"],
            "issued": metar["issued"],
            "raw": metar["raw"]
        })

# Build DataFrame
df_metars = pd.DataFrame(metar_records)

print(df_metars.shape)
print(df_metars.head())



# ----------------------------
# Parsing functions
# ----------------------------

# Wind
wind_pattern = re.compile(r'(?P<direction>\d{3}|VRB|000)(?P<speed>\d{2,3})(G(?P<gust>\d{2,3}))?KT')
def parse_wind(raw):
    match = wind_pattern.search(raw)
    if match:
        return pd.Series({
            "wind_dir": match.group("direction"),
            "wind_speed": int(match.group("speed")),
            "wind_gust": int(match.group("gust")) if match.group("gust") else None
        })
    return pd.Series({"wind_dir": None, "wind_speed": None, "wind_gust": None})

# Visibility

vis_pattern = re.compile(r'(?<=\s)(?:P6SM|\d+\s\d+\/\d+SM|\d+\/\d+SM|\d+SM)(?=\s)')

def parse_visibility(raw):
    match = vis_pattern.search(raw)
    if not match:
        return None

    vis = match.group(0).replace("SM", "").strip()

    if vis == "P6":
        return 6.1  # just flag >6 miles

    try:
        if " " in vis:  # e.g., '1 1/2'
            whole, frac = vis.split()
            num = int(whole) + float(Fraction(frac))
        elif "/" in vis:  # e.g., '3/4'
            num = float(Fraction(vis))
        else:  # whole number
            num = int(vis)
    except Exception:
        return None

    return num

#sig wx
sigwx_pattern = re.compile(
    r'(?:(?<=^)|(?<=\s))'                   #look behind: start with new line or space (do not consume)
    r'(?:[\+\-−–]|VC)?'                     # optional intensity / proximity
    r'(?:MI|BC|PR|DR|BL|SH|TS|FZ)?'         # optional descriptor
    r'(?:DZ|RA|SN|SG|IC|PL|GR|GS|BR|FG|FU|DU|SA|HZ|VA|PO|SQ|NSW|\+?FC|\+?SS|\+?DS)+\b'  # phenomenon
)
def extract_sigwx(raw):
    matches = [m.group(0) for m in sigwx_pattern.finditer(raw)]
    if matches:
        return pd.Series({"sigwx": ", ".join(matches)})
    return pd.Series({"sigwx": None})

# Clouds
cloud_pattern = re.compile(r'\bSKC|(?:FEW|SCT|BKN|OVC)\d{3}(?:CB)?|VV\d{3}\b')
def parse_clouds(raw):
    matches = [m.group(0) for m in cloud_pattern.finditer(raw)]

    if matches:
        return pd.Series({"clouds": ", ".join(matches)})
    return pd.Series({"clouds": None})


#ceilings
ceilings_pattern = re.compile(r'\b(?:BKN|OVC|VV)\d{3}(?:CB)?\b')
ceiling_pattern = re.compile(r'\d{3}')

def extract_ceilings(clouds):
    if not clouds:
        return pd.Series({"ceilings": None})
    matches = [m.group(0) for m in ceilings_pattern.finditer(clouds)]

    if matches:
        return pd.Series({"ceilings": ", ".join(matches)})
    return pd.Series({"ceilings": None})


def extract_ceiling(ceilings):

    if not ceilings:
        return pd.Series({"ceiling": None})
    matches = [int(m.group(0)) for m in ceiling_pattern.finditer(ceilings)]

    if matches:
        # Convert to feet by multiplying by 100
        return pd.Series({"ceiling": min(matches)*100})
    return pd.Series({"ceilings": None})


# Temperature/Dewpoint
temp_pattern = re.compile(r'\bM?\d{2}/M?\d{2}\b')
def parse_temp_dew(raw):
    match = temp_pattern.search(raw)
    if match:
        segment = match.group(0)  # e.g., "M05/M10" or "03/M02"
        temp_part, dew_part = segment.split("/")
        
        temp = int(temp_part.replace("M", "-"))
        dew = int(dew_part.replace("M", "-"))
        
        return pd.Series({"temp_c": temp, "dewpoint_c": dew})
    
    return pd.Series({"temp_c": None, "dewpoint_c": None})



# Altimeter
alt_pattern = re.compile(r'\bA(?P<alt>\d{4})\b')
def parse_altimeter(raw):
    match = alt_pattern.search(raw)
    if match:
        return pd.Series({"altimeter_inhg": float(match.group("alt"))/100})
    return pd.Series({"altimeter_inhg": None})

# ----------------------------
# METAR pipe
# ----------------------------

print('processing')
'''
df_metars_parsed = (
    df_metars
    .pipe(lambda df: pd.concat(
        [
            df,
            df['raw'].apply(parse_wind).apply(pd.Series),
            df['raw'].apply(parse_visibility).rename('visibility'),
            df['raw'].apply(extract_sigwx).apply(pd.Series),
            df['raw'].apply(parse_clouds).apply(pd.Series),
            df['clouds'].apply(extract_ceilings).apply(pd.Series),
            df['ceilings'].apply(extract_ceiling).apply(pd.Series),
            df['raw'].apply(parse_temp_dew).apply(pd.Series),
            df['raw'].apply(parse_altimeter).apply(pd.Series),
        ],
        axis=1
    ))
)
'''

df_metars_parsed = (
    df_metars
    .pipe(lambda df: df.join(df['raw'].apply(parse_wind).apply(pd.Series)))
    .pipe(lambda df: df.assign(visibility=df['raw'].apply(parse_visibility)))
    .pipe(lambda df: df.join(df['raw'].apply(extract_sigwx).apply(pd.Series)))
    .pipe(lambda df: df.join(df['raw'].apply(parse_clouds).apply(pd.Series)))
    .pipe(lambda df: df.join(df['clouds'].apply(extract_ceilings).apply(pd.Series)))
    .pipe(lambda df: df.join(df['ceilings'].apply(extract_ceiling).apply(pd.Series)))
    .pipe(lambda df: df.join(df['raw'].apply(parse_temp_dew).apply(pd.Series)))
    .pipe(lambda df: df.join(df['raw'].apply(parse_altimeter).apply(pd.Series)))
)



print(df_metars_parsed.head())

print('done')

# Save to CSV
df_metars_parsed.to_csv("metars_parsed.csv", index=False)