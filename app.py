import io
import os
import logging
from datetime import datetime, timezone

import boto3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# Required environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
LATITUDE = os.environ["LATITUDE"]
LONGITUDE = os.environ["LONGITUDE"]

# Optional environment variables
TIMEZONE = os.environ.get("TIMEZONE", "auto")
DATA_KEY = os.environ.get("DATA_KEY", "data.csv")
PLOT_KEY = os.environ.get("PLOT_KEY", "plot.png")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "cloud_cover",
]


def fetch_latest_weather() -> pd.DataFrame:
    """
    Fetch the next 1 hour of hourly data and keep the first hourly row.
    This gives one time-stamped record per pipeline run.
    """
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": TIMEZONE,
        "forecast_hours": 1,
    }

    log.info("Requesting Open-Meteo data with params=%s", params)
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    hourly = data["hourly"]
    row = {
        "timestamp": hourly["time"][0],
        "temperature_2m": hourly["temperature_2m"][0],
        "relative_humidity_2m": hourly["relative_humidity_2m"][0],
        "precipitation": hourly["precipitation"][0],
        "wind_speed_10m": hourly["wind_speed_10m"][0],
        "cloud_cover": hourly["cloud_cover"][0],
        "latitude": float(data["latitude"]),
        "longitude": float(data["longitude"]),
        "timezone": data.get("timezone", TIMEZONE),
    }

    df = pd.DataFrame([row])
    log.info("Fetched row: %s", row)
    return df


def load_existing_csv(s3_client) -> pd.DataFrame:
    """
    Load existing CSV from S3 if it exists.
    If not, return an empty DataFrame.
    """
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=DATA_KEY)
        existing = pd.read_csv(obj["Body"])
        log.info("Loaded existing CSV with %d rows", len(existing))
        return existing
    except s3_client.exceptions.NoSuchKey:
        log.info("No existing CSV found at s3://%s/%s", S3_BUCKET, DATA_KEY)
        return pd.DataFrame()
    except Exception as e:
        # If bucket/key is missing or first run, keep going cleanly
        if "NoSuchKey" in str(e):
            log.info("No existing CSV found at s3://%s/%s", S3_BUCKET, DATA_KEY)
            return pd.DataFrame()
        raise


def combine_and_deduplicate(existing: pd.DataFrame, new_row: pd.DataFrame) -> pd.DataFrame:
    """
    Append new row, drop duplicate timestamps, sort chronologically.
    """
    if existing.empty:
        df = new_row.copy()
    else:
        df = pd.concat([existing, new_row], ignore_index=True)

    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df


def upload_csv(s3_client, df: pd.DataFrame) -> None:
    """
    Upload full CSV history back to S3.
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=DATA_KEY,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    log.info("Uploaded CSV to s3://%s/%s with %d rows", S3_BUCKET, DATA_KEY, len(df))


def generate_plot(df: pd.DataFrame) -> io.BytesIO:
    """
    Generate a temperature-over-time plot from the full CSV history.
    """
    plot_df = df.copy()
    plot_df["timestamp"] = pd.to_datetime(plot_df["timestamp"])
    plot_df = plot_df.sort_values("timestamp")

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(plot_df["timestamp"], plot_df["temperature_2m"], linewidth=2)
    ax.set_title("Open-Meteo Hourly Temperature Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Temperature (°C)")
    fig.autofmt_xdate(rotation=25)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)

    log.info("Generated plot with %d points", len(plot_df))
    return buf


def upload_plot(s3_client, plot_buf: io.BytesIO) -> None:
    """
    Upload plot.png to S3.
    """
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=PLOT_KEY,
        Body=plot_buf.getvalue(),
        ContentType="image/png",
    )
    log.info("Uploaded plot to s3://%s/%s", S3_BUCKET, PLOT_KEY)


def main() -> None:
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    new_row = fetch_latest_weather()
    existing = load_existing_csv(s3_client)
    full_df = combine_and_deduplicate(existing, new_row)

    upload_csv(s3_client, full_df)
    plot_buf = generate_plot(full_df)
    upload_plot(s3_client, plot_buf)

    log.info("Pipeline run completed successfully at %s", datetime.now(timezone.utc).isoformat())


if __name__ == "__main__":
    main()