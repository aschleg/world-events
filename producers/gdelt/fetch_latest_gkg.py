import zipfile
import io
import datetime
import os
import csv

import requests


GKG_BASE_URL = "http://data.gdeltproject.org/gdeltv2/"
GKG_FILELIST = "lastupdate.txt"

def get_latest_gkg_url():
    r = requests.get(os.path.join(GKG_BASE_URL, GKG_FILELIST))
    r.raise_for_status()
    latest_line = r.text.strip().splitlines()[0]
    latest_filename = latest_line.split()[2]
    return os.path.join(GKG_BASE_URL, latest_filename)


def parse_gkg_line(row):
    try:
        if len(row) < 14:
            return None

        raw_tone = row[13].split(",")[0] if row[13] else "0"
        return {
            "event_id": row[0],
            "timestamp": datetime.datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%S").isoformat(),
            "themes": row[9].split(";") if row[9] else [],
            "organizations": row[12].split(";") if row[12] else [],
            "locations": row[10].split(";") if row[10] else [],
            "tone": float(raw_tone)
        }
    except Exception:
        return None


def fetch_and_parse_gkg(max_records=1000):
    url = get_latest_gkg_url()
    print(f"Fetching latest GKG..., downloading: {url}")
    r = requests.get(url)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = z.namelist()[0]

    parsed_rows = []
    with z.open(csv_name) as raw_file:
        text_stream = io.TextIOWrapper(raw_file, encoding="utf-8", errors="replace")
        reader = csv.reader(text_stream, delimiter="\t")

        for row in reader:
            event = parse_gkg_line(row)
            if not event:
                continue

            parsed_rows.append(event)
            if len(parsed_rows) >= max_records:
                break

    return parsed_rows

