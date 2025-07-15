#!/usr/bin/env python3
import os
import time
import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# === CONFIGURATION ===
STATS_URL = "https://ocean.xyz/stats/bc1qf6kklyuzuq7sg9dw8duqr7uu5xhl07xvvny4dk"
CSV_FILE  = "ocean_hashrate.csv"
INTERVAL  = 60  # seconds between measurements

FIELDNAMES = [
    "timestamp",
    "hashrate_24h",
    "hashrate_3h",
    "hashrate_10m",
    "hashrate_5m",
    "hashrate_60s",
]

# Mapping label on page → CSV column
LABEL_MAP = {
    "24 hrs": "hashrate_24h",
    "3 hrs":  "hashrate_3h",
    "10 min": "hashrate_10m",
    "5 min":  "hashrate_5m",
    "60 sec": "hashrate_60s",
}

def init_csv():
    """Create CSV with header if it doesn't exist."""
    if not os.path.isfile(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()

def parse_hashrates(html: str) -> dict:
    """
    Parse the five hashrate averages from the stats page HTML.
    Returns a dict mapping CSV fields → float value (in Th/s).
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the section header
    header = soup.find(string="Hashrate Average")
    if not header:
        raise RuntimeError("Couldn't locate Hashrate Average section")

    # Assume the next <table> contains the data
    table = header.find_next("table")
    if not table:
        raise RuntimeError("Couldn't find the table after Hashrate Average")

    # Collect the results
    result = {}
    for tr in table.find_all("tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) < 2:
            continue
        label, hr_str = cols[0], cols[1]
        if label in LABEL_MAP:
            # hr_str looks like "99.91 Th/s"
            value, unit = hr_str.split()
            result[LABEL_MAP[label]] = float(value)
    return result

def record():
    """Fetch the page, parse hashrates, and append a row to CSV."""
    html = requests.get(STATS_URL).text
    rates = parse_hashrates(html)
    row = {"timestamp": datetime.utcnow().isoformat()}
    row.update(rates)

    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writerow(row)
    print(f"[{row['timestamp']}] Recorded: {rates}")

def main():
    init_csv()
    print(f"Starting 1-minute polling of Ocean stats → {CSV_FILE}")
    try:
        while True:
            record()
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("Stopped by user.")

if __name__ == "__main__":
    main()
