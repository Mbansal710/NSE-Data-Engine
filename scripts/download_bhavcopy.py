import pandas as pd
import requests
import io
import os
from datetime import datetime

def fetch_bhavcopy(date: str):
    """
    date: 'YYYY-MM-DD'
    Downloads NSE equity bhavcopy and returns a cleaned DataFrame.
    """
    d = datetime.strptime(date, "%Y-%m-%d")
    url = f"https://archives.nseindia.com/content/historical/EQUITIES/{d.year}/{d.strftime('%b').upper()}/cm{d.strftime('%d')}{d.strftime('%b').upper()}{d.year}bhav.csv.zip"

    print(f"Downloading: {url}")

    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        raise Exception(f"Failed to download for {date}: HTTP {r.status_code}")

    zf = io.BytesIO(r.content)
    df = pd.read_csv(zf, compression="zip")

    # Basic cleaning
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["timestamp"])

    return df


def save_parquet(df, date: str):
    """Save cleaned bhavcopy as parquet."""
    folder = "data/equities"
    os.makedirs(folder, exist_ok=True)

    path = f"{folder}/{date}.parquet"
    df.to_parquet(path, index=False)
    print(f"Saved: {path}")


if __name__ == "__main__":
    date = "2025-01-03"  # example
    df = fetch_bhavcopy(date)
    save_parquet(df, date)
