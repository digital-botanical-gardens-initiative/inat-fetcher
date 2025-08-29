"""
Create iNaturalist observations from a CSV and local photos.

Scaffold notes:
- Reads rows from the JBC-formatted CSV (EPSG:4326) and builds
  parameters for `pyinaturalist.v1.observations.create_observation()`.
- Photos are resolved from subfolders under the images root using the
  `sample_id` column (e.g., data/inat_pictures/dbgi_008572/*.jpg).
- Coordinates in the supplied CSV appear swapped (columns labeled
  'latitude' and 'longitude' look reversed in examples). This scaffold
  includes a simple safeguard to auto-swap if values look reversed;
  we will confirm/adjust in the next iteration.

Usage (dry run by default):
    python -m inat_fetcher.src.pusher \
        --csv data/inat_pictures/jbc_formatted_csv/jbc_EPSG:4326.csv \
        --images-root data/inat_pictures \
        --limit 3 --verbose

To actually create observations, pass `--no-dry-run` and ensure
`INATURALIST_ACCESS_TOKEN_TODAY` is set in your environment or .env file.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict, Any

import pandas as pd
from dotenv import load_dotenv
from rich import print  # noqa: T201

# pyinaturalist helpers
from pyinaturalist import (
    create_observation,
    get_observations_by_id,
    get_observations,
)
import requests
import json


DEFAULT_CSV = "data/inat_pictures/jbc_formatted_csv/subset.csv"
DEFAULT_IMAGES_ROOT = "data/inat_pictures"
ENV_TOKEN_KEY = "INATURALIST_ACCESS_TOKEN_TODAY"


@dataclass
class RowData:
    sample_id: str
    taxon_name: Optional[str]
    observed_on: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    inat_upload: Optional[int]
    is_wild: Optional[int]
    collector_inat: Optional[str]


def load_env() -> None:
    # Load .env from the package directory if present
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")


def parse_datetime(dt_value: object) -> Optional[str]:
    """Parse date/time from CSV into an ISO-like string accepted by iNat.

    Accepts formats like YYYYMMDDHHMMSS or YYYYMMDD; returns string or None.
    """
    if dt_value is None or (isinstance(dt_value, float) and pd.isna(dt_value)):
        return None
    s = str(dt_value).strip()
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).isoformat()
        except ValueError:
            continue
    # Fallback: return as-is; iNat may still accept it
    return s or None


def coerce_float(val: object) -> Optional[float]:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)
    except Exception:
        return None


def get_lat_lon(row_lat: Optional[float], row_lon: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """Return (lat, lon) with a guard for swapped CSV columns.

    The provided CSV appears to have 'latitude' and 'longitude' values swapped
    in some rows (example shows ~7 and ~46 respectively). As a temporary
    safeguard, if lat looks like ~7 and lon like ~46, swap them.
    """
    lat, lon = row_lat, row_lon
    if lat is None or lon is None:
        return lat, lon
    # If lat is small (<= 15) and lon is large (>= 30), assume swapped
    if abs(lat) <= 15 and abs(lon) >= 30:
        return lon, lat
    return lat, lon


def collect_photos(images_root: Path, sample_id: str) -> List[Path]:
    folder = images_root / sample_id
    if not folder.exists() or not folder.is_dir():
        return []
    exts = ("*.jpg", "*.jpeg", "*.png")
    paths: List[Path] = []
    for pattern in exts:
        paths.extend(folder.glob(pattern))

    def natural_key(p: Path):
        name = p.name.lower()
        return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", name)]

    return sorted(paths, key=natural_key)


def to_row_data(row: pd.Series) -> RowData:
    return RowData(
        sample_id=str(row.get("sample_id", "")).strip(),
        taxon_name=str(row.get("taxon_name", "")).strip() or None,
        observed_on=parse_datetime(row.get("date")),
        latitude=coerce_float(row.get("latitude")),
        longitude=coerce_float(row.get("longitude")),
        inat_upload=int(row.get("inat_upload", 0)) if pd.notna(row.get("inat_upload")) else 0,
        is_wild=int(row.get("is_wild")) if pd.notna(row.get("is_wild")) else None,
        collector_inat=(
            (str(row.get("collector_inat")).strip() if pd.notna(row.get("collector_inat")) else None)
        ),
    )


def build_params(
    rowd: RowData, photos: Iterable[Path], access_token: str
) -> dict:
    lat, lon = get_lat_lon(rowd.latitude, rowd.longitude)
    # Join tags into a comma-separated string to match API expectation
    tags = [
        "emi_source:JBC",
        f"emi_sample_id:{rowd.sample_id}",
    ]
    params: dict = {
        "access_token": access_token,
        "species_guess": rowd.taxon_name,
        # observed_on_string is accepted; passing ISO-like string
        "observed_on": rowd.observed_on,
        "latitude": lat,
        "longitude": lon,
        # Basic tags; we can extend/update next iteration
        "tag_list": ",".join(tags),
        # Attach local photo paths
        "photos": [str(p) for p in photos],
    }
    # Description from collector_inat, normalized to single '@'
    if rowd.collector_inat:
        handle = rowd.collector_inat
        if not handle.startswith("@"):  # ensure single '@'
            handle = "@" + handle
        params["description"] = f"Original observer: {handle}"
    # Drop None values to avoid sending empty fields
    return {k: v for k, v in params.items() if v not in (None, [], "", ())}


def run(
    csv_path: Path,
    images_root: Path,
    *,
    limit: Optional[int] = None,
    dry_run: bool = True,
    verbose: bool = False,
    verify: bool = False,
    state_file: Optional[Path] = None,
    user: Optional[str] = None,
    dedupe_remote: bool = True,
) -> None:
    load_env()
    token = os.getenv(ENV_TOKEN_KEY)
    if not token:
        print(f"[yellow]Warning:[/yellow] {ENV_TOKEN_KEY} not set; only dry-run will work.")
        if not dry_run:
            raise SystemExit("Access token required for non-dry-run.")

    # State for idempotency
    state: Dict[str, Any] = {}
    if state_file:
        try:
            if state_file.exists():
                state = json.loads(state_file.read_text())
        except Exception:  # noqa: BLE001
            state = {}

    df = pd.read_csv(csv_path)
    rows = (to_row_data(r) for _, r in df.iterrows())
    # Keep only rows marked for upload and with a sample_id
    filtered = [r for r in rows if r.inat_upload and r.sample_id]
    if limit is not None:
        filtered = filtered[:limit]

    print(f"Found {len(filtered)} row(s) to process from {csv_path}.")

    for idx, rowd in enumerate(filtered, start=1):
        # Idempotency: skip if recorded in local state
        if state_file and rowd.sample_id in state:
            if verbose:
                print(f"[yellow]Skip[/yellow] {rowd.sample_id}: already uploaded (state file)")
            continue

        # Idempotency: optional remote dedupe via tag search
        unique_tag = f"emi_sample_id:{rowd.sample_id}"
        if dedupe_remote and token:
            try:
                resp = get_observations(q=unique_tag, search_on="tags", user_id=user, page="all")
                results = resp.get("results") if isinstance(resp, dict) else resp
                if results and len(results) > 0:
                    # Record first match to state and skip
                    first = results[0]
                    if state_file:
                        state[rowd.sample_id] = {"id": first.get("id") if isinstance(first, dict) else None}
                    if verbose:
                        print(f"[yellow]Skip[/yellow] {rowd.sample_id}: found existing on iNat")
                    continue
            except Exception:  # noqa: BLE001
                # Non-fatal; proceed without remote dedupe
                pass

        photos = collect_photos(images_root, rowd.sample_id)
        if not photos:
            print(f"[yellow]Skip[/yellow] {rowd.sample_id}: No photos under {images_root / rowd.sample_id}")
            continue

        params = build_params(rowd, photos, access_token=token or "")
        if verbose:
            print({k: (v if k != "photos" else [Path(p).name for p in v]) for k, v in params.items()})

        if dry_run:
            print(
                f"[cyan]Dry-run[/cyan] {idx}/{len(filtered)}: sample_id={rowd.sample_id}, "
                f"taxon={rowd.taxon_name}, lat/lon={params.get('latitude')},{params.get('longitude')} "
                f"photos={len(photos)}"
            )
            continue

        try:
            resp = create_observation(**params)
            # Set DQA 'wild' vote once; is_wild==0 -> agree=false (captive)
            if rowd.is_wild is not None and isinstance(resp, dict) and resp.get("id") and token:
                agree_str = "true" if rowd.is_wild == 1 else "false"
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                url = f"https://www.inaturalist.org/observations/{resp['id']}/quality/wild.json"
                r = requests.post(url, data={"agree": agree_str}, headers=headers, timeout=30)
                if r.status_code >= 300 and verbose:
                    print(f"[yellow]Warn[/yellow] DQA vote failed: status={r.status_code} body={r.text[:200]}")
            print(f"[green]Created[/green] {rowd.sample_id}: response={resp}")
            # Update local state
            if state_file and isinstance(resp, dict) and resp.get("id") is not None:
                state[rowd.sample_id] = {"id": resp["id"], "uuid": resp.get("uuid")}
                try:
                    state_file.parent.mkdir(parents=True, exist_ok=True)
                    state_file.write_text(json.dumps(state, indent=2))
                except Exception as write_exc:  # noqa: BLE001
                    if verbose:
                        print(f"[yellow]Warn[/yellow] failed to write state: {write_exc}")
            if verify and isinstance(resp, dict) and resp.get("id") and token:
                try:
                    check = get_observations_by_id(resp["id"], access_token=token, refresh=True)
                    final = check["results"][0] if isinstance(check, dict) and check.get("results") else check
                    print(
                        f"[blue]Verify[/blue] id={resp['id']} captive={getattr(final, 'captive', None) if not isinstance(final, dict) else final.get('captive')}"
                    )
                except Exception as inner_exc:  # noqa: BLE001
                    print(f"[yellow]Warn[/yellow] verify failed for {rowd.sample_id}: {inner_exc}")
        except Exception as exc:  # noqa: BLE001
            print(f"[red]Error[/red] {rowd.sample_id}: {exc}")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=DEFAULT_CSV, type=Path, help="Path to input CSV")
    parser.add_argument(
        "--images-root",
        default=DEFAULT_IMAGES_ROOT,
        type=Path,
        help="Root directory containing per-sample photo subfolders",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max rows to process")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--verify", action="store_true", help="Re-fetch observation to verify captive flag")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("data/inat_pictures/upload_state.json"),
        help="Path to JSON file to track uploaded sample_ids for idempotency",
    )
    parser.add_argument("--user", type=str, default=None, help="iNat username to restrict remote dedupe")
    parser.add_argument(
        "--dedupe-remote",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Query iNat by tag to skip already-uploaded samples",
    )
    args = parser.parse_args(argv)

    run(
        csv_path=args.csv,
        images_root=args.images_root,
        limit=args.limit,
        dry_run=args.dry_run,
        verbose=args.verbose,
        verify=args.verify,
        state_file=args.state_file,
        user=args.user,
        dedupe_remote=args.dedupe_remote,
    )


if __name__ == "__main__":
    main()
