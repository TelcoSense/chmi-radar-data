import argparse
import logging
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry

from conversions import hdf_to_png, merge1h_to_png

FOLDER_MAPPINGS = {
    "https://opendata.chmi.cz/meteorology/weather/radar/composite/pseudocappi2km/hdf5/": Path(
        "./pseudocappi2km"
    ),
    "https://opendata.chmi.cz/meteorology/weather/radar/composite/maxz/hdf5/": Path(
        "./maxz"
    ),
    "https://opendata.chmi.cz/meteorology/weather/radar/composite/merge1h/hdf5/": Path(
        "./merge1h"
    ),
}

MAXZ_PNG_FOLDER = Path("maxz_png")
MAXZ_PNG_FOLDER.mkdir(parents=True, exist_ok=True)

MERGE1H_PNG_FOLDER = Path("merge1h_png")
MERGE1H_PNG_FOLDER.mkdir(parents=True, exist_ok=True)

CAPPI2KM_PNG_FOLDER = Path("pseudocappi2km_png")
CAPPI2KM_PNG_FOLDER.mkdir(parents=True, exist_ok=True)

DEFAULT_CHECK_INTERVAL = 30  # seconds
DEFAULT_LOG_FILE = Path("radar_data_realtime.log")
DISCORD_WEBHOOK_URL = ""

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)


def notify_discord(message: str):
    try:
        response = session.post(
            DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10
        )
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send Discord notification: {e}")


def convert_maxz_to_png(hdf_path: Path, output_folder: Path):
    temp_output = output_folder / hdf_path.with_suffix(".png").name
    logging.info(f"Converting {hdf_path.name} -> temporary {temp_output.name}")
    rain_score = hdf_to_png(hdf_path, temp_output, raw_visible_min=None)
    score_str = f"{rain_score:.3f}"
    final_name = hdf_path.with_suffix("").name + f"_{score_str}.png"
    final_path = output_folder / final_name
    logging.info(f"Renaming to final output: {final_path.name}")
    temp_output.rename(final_path)
    return final_path


def convert_cappi_to_png(hdf_path: Path, output_folder: Path):
    temp_output = output_folder / hdf_path.with_suffix(".png").name
    logging.info(f"Converting {hdf_path.name} -> temporary {temp_output.name}")
    rain_score = hdf_to_png(hdf_path, temp_output, raw_visible_min=78)
    score_str = f"{rain_score:.3f}"
    final_name = hdf_path.with_suffix("").name + f"_{score_str}.png"
    final_path = output_folder / final_name
    logging.info(f"Renaming to final output: {final_path.name}")
    temp_output.rename(final_path)
    return final_path


def convert_merge1h_to_png(hdf_path: Path, output_folder: Path):
    temp_png_name = hdf_path.with_suffix(".png").name
    temp_output_path = output_folder / temp_png_name
    logging.info(f"Converting {hdf_path.name} -> temporary {temp_output_path.name}")
    rain_score = merge1h_to_png(str(hdf_path), str(temp_output_path))
    score_str = f"{rain_score:.3f}"
    final_name = hdf_path.with_suffix("").name + f"_{score_str}.png"
    final_output_path = output_folder / final_name
    logging.info(f"Renaming {temp_output_path.name} -> {final_output_path.name}")
    temp_output_path.rename(final_output_path)
    return final_output_path


def setup_logging(level, log_file):
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3  # 5 MB
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)


def parse_filename_utc(filename):
    try:
        datetime_str = filename.split("_")[-1].replace(".hdf", "")
        return datetime.strptime(datetime_str, "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        )
    except Exception as e:
        logging.warning(f"Failed to parse datetime from {filename}: {e}")
        return None


def get_file_links(folder_url):
    try:
        response = session.get(folder_url, timeout=10)
        response.raise_for_status()
        return [
            folder_url + line.split('"')[1]
            for line in response.text.splitlines()
            if ".hdf" in line and 'href="' in line
        ]
    except Exception as e:
        error_msg = f"Failed to fetch from {folder_url}: {e}"
        logging.error(error_msg)
        notify_discord(error_msg)
        return []


def download_file(file_url, local_folder: Path):
    file_name = Path(file_url).name
    local_path = local_folder / file_name

    if local_path.exists():
        logging.debug(f"File already exists: {local_path.as_posix()}")
        return False

    try:
        with session.get(file_url, stream=True, timeout=30) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0))
            logging.info(f"Downloading {file_name} ({total} bytes)")
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"Downloaded: {local_path.as_posix()}")
        return True
    except Exception as e:
        error_msg = f"Failed to download {file_url}: {e}"
        logging.error(error_msg)
        notify_discord(error_msg)
        return False


def sleep_until_next_interval(interval_seconds):
    now = time.time()
    next_time = ((now // interval_seconds) + 1) * interval_seconds
    sleep_duration = next_time - now
    mins, secs = divmod(int(sleep_duration), 60)
    logging.info(f"Sleeping {mins:02d}:{secs:02d} until next check...")
    time.sleep(sleep_duration)


def main(check_every):
    for folder in FOLDER_MAPPINGS.values():
        folder.mkdir(parents=True, exist_ok=True)

    downloaded_files = {
        url: {f.name for f in folder.iterdir() if f.is_file()}
        for url, folder in FOLDER_MAPPINGS.items()
    }

    logging.info("Started CHMI radar data fetcher (interval: %d s)", check_every)
    # notify_discord(":satellite: CHMI radar data fetcher started.")

    while True:
        for folder_url, local_folder in FOLDER_MAPPINGS.items():
            logging.info(f"Checking: {folder_url}")
            links = get_file_links(folder_url)

            for file_url in links:
                file_name = Path(file_url).name
                if file_name not in downloaded_files[folder_url]:
                    success = download_file(file_url, local_folder)
                    if success:
                        downloaded_files[folder_url].add(file_name)
                        if local_folder.name == "maxz":
                            try:
                                convert_maxz_to_png(
                                    local_folder / file_name, MAXZ_PNG_FOLDER
                                )
                            except Exception as e:
                                error_msg = f"Failed to convert {file_name} to PNG: {e}"
                                logging.error(error_msg)
                                notify_discord(error_msg)
                        if local_folder.name == "merge1h":
                            try:
                                convert_merge1h_to_png(
                                    local_folder / file_name, MERGE1H_PNG_FOLDER
                                )
                            except Exception as e:
                                error_msg = f"Failed to convert {file_name} to PNG: {e}"
                                logging.error(error_msg)
                                notify_discord(error_msg)
                        if local_folder.name == "pseudocappi2km":
                            try:
                                convert_cappi_to_png(
                                    local_folder / file_name, CAPPI2KM_PNG_FOLDER
                                )
                            except Exception as e:
                                error_msg = f"Failed to convert {file_name} to PNG: {e}"
                                logging.error(error_msg)
                                notify_discord(error_msg)
        sleep_until_next_interval(check_every)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CHMI radar data fetching.")
    parser.add_argument(
        "--check-every",
        type=int,
        default=DEFAULT_CHECK_INTERVAL,
        help="Polling interval in seconds (default: 30)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=DEFAULT_LOG_FILE,
        help="Path to the log file (default: radar_data_realtime.log)",
    )

    args = parser.parse_args()
    setup_logging(args.log_level.upper(), args.log_file)

    try:
        main(args.check_every)
    except Exception as e:
        error_msg = f"Unhandled exception in main(): {e}"
        logging.critical(error_msg, exc_info=True)
        notify_discord(f":rotating_light: FATAL ERROR: {error_msg}")
        raise
