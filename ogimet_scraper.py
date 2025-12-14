import os
import time
import datetime
import requests
from dateutil.relativedelta import relativedelta

# --------------- CONFIG ---------------

AIRPORTS = "CYFO CYEK CYRT CYXN CYCS CYBK CYZS CYUT CYUX CYGT CYBB CYHK CYYH CYRB CYAB CYIO CYBR CYTE CYQK CYVC".split(" ")   # Add your airports here
START_YEAR = 2021
START_MONTH = 1
END_YEAR = 2021
END_MONTH = 2
SAVE_DIR = "data"
REQUEST_DELAY = 10        # seconds between requests
MAX_RETRIES = 3          # retry if connection fails
INCLUDE_NIL = "NO"       # can be "YES" or "NO"
OUTPUT_FORMAT = "TXT"    # "TXT" or "HTML"
REPORT_TYPE = "ALL"      # "ALL", "SA", "SP", "FC", or "FT"

# --------------- FUNCTIONS ---------------

def daterange_months(start_year, start_month, end_year, end_month):
    """Yield (year, month) pairs inclusive."""
    current = datetime.date(start_year, start_month, 1)
    end = datetime.date(end_year, end_month, 1)
    while current <= end:
        yield current.year, current.month
        current += relativedelta(months=1)

def get_last_day(year, month):
    """Return last day of a month."""
    next_month = datetime.date(year, month, 1) + relativedelta(months=1)
    return (next_month - datetime.timedelta(days=1)).day

def fetch_ogimet(icao, year, month):
    """Fetch one month of METAR/TAF data for an airport."""
    url = "https://www.ogimet.com/display_metars2.php"
    params = {
        "lang": "en",
        "lugar": icao,
        "tipo": "ALL",
        "ord": "DIR",
        "nil": "SI",
        "fmt": "txt",
        "ano": year,
        "mes": month,
        "day": 1,
        "hora": 0,
        "anof": year,
        "mesf": month,
        "dayf": get_last_day(year, month),
        "horaf": 23,
        "minf": 59,
        "send": "send"
    }

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Referer": "https://www.ogimet.com/display_metars2.php",
    "Accept-Language": "en-US,en;q=0.9",
}


    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, headers = headers, timeout=30)
            print("Fetching:", r.url)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            print(f"Error fetching {icao} {year}-{month:02d}: {e}")
            if attempt < MAX_RETRIES - 1:
                print("Retrying...")
                time.sleep(3)
            else:
                print("Giving up on this month.")
                return None

def save_text(icao, year, month, text):
    """Save the response text to a file."""
    os.makedirs(SAVE_DIR, exist_ok=True)
    filename = os.path.join(SAVE_DIR, f"{icao}_{year}-{month:02d}.txt")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)

# --------------- MAIN SCRIPT ---------------

if __name__ == "__main__":
    for icao in AIRPORTS:
        for year, month in daterange_months(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
            filename = os.path.join(SAVE_DIR, f"{icao}_{year}-{month:02d}.txt")
            if os.path.exists(filename):
                print(f"Skipping {icao} {year}-{month:02d} (already exists)")
                continue

            print(f"Fetching {icao} {year}-{month:02d}...")
            text = fetch_ogimet(icao, year, month)
            if text:
                save_text(icao, year, month, text)
                print(f"Saved {icao}_{year}-{month:02d}.txt")
            else:
                print(f"Failed {icao} {year}-{month:02d}")

            time.sleep(REQUEST_DELAY)

    print("All done!")
