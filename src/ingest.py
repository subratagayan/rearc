"""Step 1: land BLS PR files and DataUSA population JSON as raw files in a Unity Catalog Volume.

Layout (all in one Volume):
    bls/pr/<file_name>/<version>/<file_name>             immutable versions of each BLS file
    datausa/population/<version>/population.json         immutable versions of the DataUSA response
    _state/active_files.json                             which version of each file is live (JSON lines)

* The BLS directory listing is read on every run - nothing is hardcoded about which files exist.
* Unchanged files (same listed timestamp + size, or same JSON content) are not downloaded/written again.
* New or changed files are written as a new version; Auto Loader in the pipeline ingests each version once.
* Files removed upstream drop out of active_files.json, so the pipeline's silver layer stops using them.
* BLS blocks robots without contact info (https://www.bls.gov/bls/pss.htm), so every request sends a
  User-Agent containing a contact email.

Usage: python ingest.py --target /Volumes/<catalog>/<schema>/raw --contact-email you@example.com
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
# Listing rows look like: " 11/5/2026  8:30 AM      1943714 <A HREF="/pub/time.series/pr/pr.data.0.Current">pr.data.0.Current</A>"
LISTING_ROW = re.compile(r"(\d+/\d+/\d{4}\s+\d+:\d+\s+[AP]M)\s+(\d+)\s+<A HREF=\"([^\"]+)\">([^<]+)</A>", re.I)


def get(session: requests.Session, url: str, retries: int = 4, stream: bool = False) -> requests.Response:
    """GET with a polite pause and exponential backoff on throttling / server errors."""
    for attempt in range(retries + 1):
        time.sleep(0.5)
        resp = session.get(url, timeout=60, stream=stream)
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
            time.sleep(int(resp.headers.get("Retry-After", 2 ** (attempt + 1))))
            continue
        resp.raise_for_status()
        return resp


def save(path: str, chunks) -> None:
    """Write to a local temp file first, then move into place, so a half-written file never lands."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        for chunk in chunks:
            tmp.write(chunk)
    shutil.move(tmp.name, path)


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    rows = [json.loads(line) for line in open(path) if line.strip()]
    return {(r["source"], r["file_name"]): r for r in rows}


def save_state(path: str, state: dict) -> None:
    lines = [json.dumps(state[k]) for k in sorted(state)]
    save(path, ["\n".join(lines).encode() + b"\n"])


def sync_bls(session: requests.Session, target: str, state: dict, version: str) -> None:
    listing = {
        name: {"modified": modified, "size": int(size), "url": urljoin(BLS_URL, href)}
        for modified, size, href, name in LISTING_ROW.findall(get(session, BLS_URL).text)
    }
    if not listing:
        raise RuntimeError("Could not parse BLS listing - refusing to treat it as 'all files removed'")

    for name, meta in listing.items():
        key, prev = ("bls_pr", name), state.get(("bls_pr", name))
        if prev and (prev["modified"], prev["size"]) == (meta["modified"], meta["size"]):
            print(f"unchanged  {name}")
            continue
        path = f"{target}/bls/pr/{name}/{version}/{name}"
        save(path, get(session, meta["url"], stream=True).iter_content(1 << 20))
        print(f"{'updated' if prev else 'added':10} {name} -> {path}")
        state[key] = {"source": "bls_pr", "file_name": name, "version": version,
                      "modified": meta["modified"], "size": meta["size"], "path": path}

    for key in [k for k in state if k[0] == "bls_pr" and k[1] not in listing]:
        del state[key]
        print(f"removed    {key[1]}")


def fetch_population(session: requests.Session, target: str, state: dict, version: str) -> None:
    resp = get(session, POPULATION_URL)
    payload = resp.json()
    if not payload.get("data"):
        raise RuntimeError("DataUSA response has no data")
    sha = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    key = ("datausa", "population.json")
    if state.get(key, {}).get("sha256") == sha:
        print("unchanged  population.json")
        return
    path = f"{target}/datausa/population/{version}/population.json"
    save(path, [resp.content])
    print(f"written    population.json ({len(payload['data'])} records) -> {path}")
    state[key] = {"source": "datausa", "file_name": "population.json", "version": version, "sha256": sha, "path": path}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="Volume path, e.g. /Volumes/main/rearc_quest/raw")
    parser.add_argument("--contact-email", required=True)
    args = parser.parse_args()

    target = args.target.rstrip("/")
    session = requests.Session()
    session.headers["User-Agent"] = f"rearc-data-quest/1.0 (contact: {args.contact_email})"
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    state_path = f"{target}/_state/active_files.json"

    state = load_state(state_path)
    sync_bls(session, target, state, version)
    fetch_population(session, target, state, version)
    save_state(state_path, state)  # written last: if anything fails, the next run safely redoes the work
