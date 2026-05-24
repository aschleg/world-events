import zipfile
import io
import datetime
import os

import requests


GKG_BASE_URL = "http://data.gdeltproject.org/gdeltv2/"
GKG_FILELIST = "lastupdate.txt"

def get_latest_gkg_url():
    r = requests.get(os.path.join(GKG_BASE_URL, GKG_FILELIST))
    r.raise_for_status()
    latest_filename = r.text.strip()
    return os.path.join(GKG_BASE_URL, latest_filename)


def parse_gkg_line(row):
    try:
        return {
            "event_id": row[0],
            "timestamp": datetime.datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%S").isoformat(),
            "themes": row[9].split(";") if row[9] else [],
            "organizations": row[12].split(";") if row[12] else [],
            "locations": row[10].split(";") if row[10] else [],
            "tone": float(row[13].split(";")) if row[13] else 0.0
        }
    except Exception as e:
        return None


def fetch_and_parse_gkg():
    url = get_latest_gkg_url()
    print(f"Fetching latest GKG..., downloading: {url}")
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = z.namelist()[0]

