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
import logging
import time
import hashlib

import pandas as pd
from dotenv import load_dotenv
from rich import print  # noqa: T201

# pyinaturalist helpers
from pyinaturalist import (
    create_observation,
    get_observations_by_id,
    get_observations,
    upload as upload_media,
)
from pyinaturalist.session import get_refresh_params
import requests
import json


DEFAULT_CSV = "data/inat_pictures/jbc_formatted_csv/jbc_EPSG:2056.csv"
DEFAULT_IMAGES_ROOT = "/Users/pma/02_tmp/inat_pictures/"
ENV_TOKEN_KEY = "INATURALIST_ACCESS_TOKEN_TODAY"


def setup_logger(log_file: Optional[Path], verbose: bool) -> logging.Logger:
    logger = logging.getLogger("inat_pusher")
    logger.setLevel(logging.DEBUG)
    # Clear existing handlers to avoid duplicate logs on repeated runs
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    if log_file:
        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.INFO)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:  # noqa: BLE001
            # Fallback to console only if file can't be opened
            pass

    return logger


@dataclass
class RowData:
    sample_id: str
    taxon_name: Optional[str]
    observed_on: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    x_coord: Optional[float]
    y_coord: Optional[float]
    inat_upload: Optional[int]
    is_wild: Optional[int]
    collector_inat: Optional[str]
    collector_fullname: Optional[str]
    collector_orcid: Optional[str]
    project: Optional[str]


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
    # Prefer explicit taxon_name; fallback to name_proposition when missing
    taxon = str(row.get("taxon_name", "")).strip()
    if not taxon:
        alt = str(row.get("name_proposition", "")).strip()
        taxon = alt or None

    return RowData(
        sample_id=str(row.get("sample_id", "")).strip(),
        taxon_name=taxon,
        observed_on=parse_datetime(row.get("date")),
        latitude=coerce_float(row.get("latitude")),
        longitude=coerce_float(row.get("longitude")),
        x_coord=coerce_float(row.get("x_coord")),
        y_coord=coerce_float(row.get("y_coord")),
        inat_upload=int(row.get("inat_upload", 0)) if pd.notna(row.get("inat_upload")) else 0,
        is_wild=int(row.get("is_wild")) if pd.notna(row.get("is_wild")) else None,
        collector_inat=(
            (str(row.get("collector_inat")).strip() if pd.notna(row.get("collector_inat")) else None)
        ),
        collector_fullname=(
            str(row.get("collector_fullname")).strip() if pd.notna(row.get("collector_fullname")) else None
        ),
        collector_orcid=(
            str(row.get("collector_orcid")).strip() if pd.notna(row.get("collector_orcid")) else None
        ),
        project=(str(row.get("project")).strip() if pd.notna(row.get("project")) else None),
    )


def build_params(
    rowd: RowData, photos: Iterable[Path], access_token: str
) -> dict:
    lat, lon = rowd.latitude, rowd.longitude
    # Join tags into a comma-separated string to match API expectation
    tags = [
        f"emi_external_id:{rowd.sample_id}",
    ]
    if rowd.project:
        tags.append(f"emi_project:dbgi_{rowd.project}")
    # Optional emitter metadata tags from CSV
    if rowd.collector_fullname:
        tags.append(f"emi_collector:{rowd.collector_fullname}")
    if rowd.collector_inat:
        handle = rowd.collector_inat if rowd.collector_inat.startswith("@") else f"@{rowd.collector_inat}"
        tags.append(f"emi_collector_inat:{handle}")
    if rowd.collector_orcid:
        tags.append(f"emi_collector_orcid:{rowd.collector_orcid}")
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


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _get_photo_count(obs_id: int, token: str) -> int:
    try:
        check = get_observations_by_id(obs_id, access_token=token, refresh=True)
        result = check["results"][0] if isinstance(check, dict) and check.get("results") else check
        if isinstance(result, dict):
            photos = result.get('photos') or []
            return len(photos)
    except Exception:
        pass
    return -1


def resolve_lat_lon_from_xy(rowd: RowData, xy_epsg: str, logger: logging.Logger) -> Tuple[Optional[float], Optional[float]]:
    """Resolve latitude/longitude strictly from x_coord/y_coord.

    - If xy_epsg == '4326', interpret x_coord as longitude and y_coord as latitude.
    - Otherwise, transform EPSG:{xy_epsg} -> EPSG:4326 using pyproj.
    Returns (lat, lon).
    """
    if rowd.x_coord is None or rowd.y_coord is None:
        logger.error("Missing x_coord/y_coord for %s; cannot compute coordinates", rowd.sample_id)
        return None, None

    if xy_epsg == "4326":
        return rowd.y_coord, rowd.x_coord

    try:
        from pyproj import Transformer  # type: ignore

        transformer = Transformer.from_crs(f"EPSG:{xy_epsg}", "EPSG:4326", always_xy=True)
        lon, lat = transformer.transform(rowd.x_coord, rowd.y_coord)
        return lat, lon
    except ModuleNotFoundError:
        logger.error(
            "pyproj is required to transform x/y from EPSG:%s to EPSG:4326. Install with 'poetry add pyproj' or 'pip install pyproj'",
            xy_epsg,
        )
        return None, None
    except Exception as exc:  # noqa: BLE001
        logger.error("XY transform failed for %s: %s", rowd.sample_id, exc)
        return None, None


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
    refresh_remote: bool = False,
    upload_timeout: int = 180,
    upload_retries: int = 2,
    log_file: Optional[Path] = None,
    xy_epsg: str = "auto",
    index_wait_seconds: int = 120,
    index_wait_interval: int = 5,
) -> None:
    load_env()
    logger = setup_logger(log_file, verbose)
    token = os.getenv(ENV_TOKEN_KEY)
    if not token:
        print(f"[yellow]Warning:[/yellow] {ENV_TOKEN_KEY} not set; only dry-run will work.")
        logger.warning("%s not set; only dry-run will work", ENV_TOKEN_KEY)
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
    logger.info("Found %d rows to process from %s", len(filtered), csv_path)

    for idx, rowd in enumerate(filtered, start=1):
        # Idempotency: skip only if marked complete in local state
        if state_file and rowd.sample_id in state and state[rowd.sample_id].get("complete") is True:
            msg = f"Skip {rowd.sample_id}: already uploaded (complete in state file)"
            logger.info(msg)
            if verbose:
                print(f"[yellow]Skip[/yellow] {rowd.sample_id}: already uploaded (complete)")
            continue

        # Idempotency: optional remote dedupe via tag search
        unique_tag = f"emi_external_id:{rowd.sample_id}"
        if dedupe_remote:
            try:
                logger.debug("Checking remote for %s", unique_tag)
                resp = get_observations(
                    q=unique_tag,
                    search_on="tags",
                    user_id=user,
                    page="all",
                    refresh=refresh_remote,
                )
                results = resp.get("results") if isinstance(resp, dict) else resp
                if results and len(results) > 0:
                    # Record first match to state and skip
                    first = results[0]
                    if state_file:
                        state[rowd.sample_id] = {"id": first.get("id") if isinstance(first, dict) else None}
                    logger.info(
                        "Skip %s: found existing on iNat (matches=%s, first_id=%s)",
                        rowd.sample_id,
                        len(results),
                        first.get("id") if isinstance(first, dict) else None,
                    )
                    if verbose:
                        print(f"[yellow]Skip[/yellow] {rowd.sample_id}: found existing on iNat")
                    continue
            except Exception:  # noqa: BLE001
                # Non-fatal; proceed without remote dedupe
                logger.warning("Remote dedupe check failed for %s", rowd.sample_id)
                pass

        photos = collect_photos(images_root, rowd.sample_id)
        if not photos:
            logger.warning(f"[yellow]Skip[/yellow] {rowd.sample_id}: No photos under {images_root / rowd.sample_id}")
            continue

        # Determine EPSG for x/y: auto-detect from CSV filename if requested
        epsg_to_use = xy_epsg
        if xy_epsg.lower() == "auto":
            name = csv_path.name
            epsg_to_use = "2056"
            if "EPSG:" in name:
                try:
                    epsg_to_use = name.split("EPSG:")[-1].split(".")[0]
                except Exception:
                    epsg_to_use = "2056"

        # Resolve coordinates strictly from x/y
        lat, lon = resolve_lat_lon_from_xy(rowd, epsg_to_use, logger)
        if lat is None or lon is None:
            logger.warning("Skipping %s: could not resolve latitude/longitude", rowd.sample_id)
            print(f"[yellow]Skip[/yellow] {rowd.sample_id}: could not resolve latitude/longitude")
            continue

        # Override legacy lat/lon so build_params uses the resolved values
        rowd.latitude, rowd.longitude = lat, lon
        logger.info("%s geolocation from x/y using EPSG:%s -> lat=%.8f lon=%.8f", rowd.sample_id, epsg_to_use, lat, lon)

        params = build_params(rowd, photos, access_token=token or "")
        # Ensure longer timeout for uploads
        params["timeout"] = upload_timeout
        if verbose:
            print(
                {
                    k: (
                        "***"
                        if k == "access_token"
                        else ([Path(p).name for p in v] if k == "photos" else v)
                    )
                    for k, v in params.items()
                }
            )
        logger.debug("Params for %s ready (photos=%d)", rowd.sample_id, len(photos))

        if dry_run:
            print(
                f"[cyan]Dry-run[/cyan] {idx}/{len(filtered)}: sample_id={rowd.sample_id}, "
                f"taxon={rowd.taxon_name}, lat/lon={params.get('latitude')},{params.get('longitude')} "
                f"photos={len(photos)}"
            )
            continue

        try:
            # Separate create and media upload for safer retries
            photos_list = params.pop("photos", [])

            # Helper: poll iNat for an existing obs with our unique tag
            def wait_for_existing(timeout_s: int) -> Optional[Dict[str, Any]]:
                deadline = time.time() + timeout_s
                tries = 0
                while time.time() < deadline:
                    tries += 1
                    try:
                        refresh_kwargs = get_refresh_params('observations')
                        check = get_observations(
                            q=unique_tag,
                            search_on="tags",
                            user_id=user,
                            page="all",
                            **refresh_kwargs,
                        )
                        results = check.get("results") if isinstance(check, dict) else check
                        if results:
                            return results[0] if isinstance(results[0], dict) else None
                    except Exception:
                        pass
                    time.sleep(index_wait_interval)
                return None

            # Retry create on transient errors, but wait + re-check before retrying to avoid duplicates
            attempt = 0
            resp: Dict[str, Any]
            while True:
                # Re-check for an existing observation before each create attempt
                existing = wait_for_existing(0)
                if existing:
                    resp = existing
                    break

                try:
                    resp = create_observation(**params)
                    break
                except Exception as e:  # noqa: BLE001
                    attempt += 1
                    logger.warning(
                        "Create attempt failed for %s (attempt %d/%d): %s",
                        rowd.sample_id,
                        attempt,
                        upload_retries,
                        e,
                    )
                    # After a failed create, wait for indexing and check again before retrying
                    existing = wait_for_existing(index_wait_seconds)
                    if existing:
                        resp = existing
                        break
                    if attempt > upload_retries:
                        raise

            # At this point, we have an observation (either newly created or found). Ensure media is attached.
            obs_id = resp.get("id") if isinstance(resp, dict) else None
            if obs_id and photos_list:
                # Ensure state entry exists and persist obs id early
                if state_file:
                    state.setdefault(rowd.sample_id, {})
                    state[rowd.sample_id].update({"id": obs_id, "uuid": resp.get("uuid")})
                    try:
                        state_file.parent.mkdir(parents=True, exist_ok=True)
                        state_file.write_text(json.dumps(state, indent=2))
                    except Exception:
                        pass

                # Check current photo count on server to avoid overshooting
                server_count = _get_photo_count(obs_id, token)
                local_total = len(photos_list)
                if server_count >= local_total:
                    logger.info(
                        "%s already has %d photos on iNat; skipping uploads to avoid duplicates",
                        rowd.sample_id,
                        server_count,
                    )
                    # Mark all local photos as uploaded in state to prevent future re-uploads
                    if state_file:
                        uploaded = state[rowd.sample_id].get("uploaded_photos", []) if state.get(rowd.sample_id) else []
                        all_hashes = []
                        try:
                            all_hashes = [_sha256_file(Path(p)) for p in photos_list]
                        except Exception:
                            pass
                        state[rowd.sample_id]["uploaded_photos"] = sorted(set(uploaded) | set(all_hashes))
                        state[rowd.sample_id]["complete"] = True
                        try:
                            state_file.write_text(json.dumps(state, indent=2))
                        except Exception:
                            pass
                else:
                    # Upload photos one-by-one with per-file retries and state tracking
                    uploaded_hashes = set()
                    if state.get(rowd.sample_id) and state[rowd.sample_id].get("uploaded_photos"):
                        uploaded_hashes = set(state[rowd.sample_id]["uploaded_photos"])  # type: ignore[index]

                    for photo_path in photos_list:
                        p = Path(photo_path)
                        try:
                            h = _sha256_file(p)
                        except Exception:
                            h = f"name:{p.name}|size:{p.stat().st_size}"

                        # Skip if this exact file was already recorded as uploaded
                        if h in uploaded_hashes:
                            continue

                        # If server already has at least local_total photos, stop to avoid duplicates
                        if server_count >= local_total:
                            break

                        per_attempt = 0
                        while True:
                            before = _get_photo_count(obs_id, token)
                            try:
                                upload_media(
                                    obs_id,
                                    photos=[str(p)],
                                    access_token=token,
                                    timeout=upload_timeout,
                                )
                            except Exception as e:  # noqa: BLE001
                                per_attempt += 1
                                # Wait for indexing; success may have happened
                                time.sleep(index_wait_interval)
                                after = _get_photo_count(obs_id, token)
                                if after > before:
                                    server_count = after
                                    uploaded_hashes.add(h)
                                    if state_file:
                                        state.setdefault(rowd.sample_id, {})
                                        entry = state[rowd.sample_id]
                                        entry["uploaded_photos"] = sorted(set(entry.get("uploaded_photos", [])) | {h})
                                        try:
                                            state_file.write_text(json.dumps(state, indent=2))
                                        except Exception:
                                            pass
                                    break
                                if per_attempt > upload_retries:
                                    raise
                                delay = min(10, 3 * per_attempt)
                                logger.warning(
                                    "Photo %s upload failed for %s (attempt %d/%d): %s; retrying in %ss",
                                    p.name,
                                    rowd.sample_id,
                                    per_attempt,
                                    upload_retries,
                                    e,
                                    delay,
                                )
                                time.sleep(delay)
                                continue

                            # If no exception, still confirm photo count increased before marking success
                            time.sleep(index_wait_interval)
                            after = _get_photo_count(obs_id, token)
                            if after > before:
                                server_count = after
                                uploaded_hashes.add(h)
                                if state_file:
                                    state.setdefault(rowd.sample_id, {})
                                    entry = state[rowd.sample_id]
                                    entry["uploaded_photos"] = sorted(set(entry.get("uploaded_photos", [])) | {h})
                                    try:
                                        state_file.write_text(json.dumps(state, indent=2))
                                    except Exception:
                                        pass
                                break
                            else:
                                # Treat as transient; let retry loop handle
                                per_attempt += 1
                                if per_attempt > upload_retries:
                                    raise RuntimeError(
                                        f"Upload completed but photo count did not change for {p.name}"
                                    )
                                time.sleep(min(10, 3 * per_attempt))
                    # After attempting all photos, if server has at least local_total, mark complete
                    if state_file:
                        final_count = _get_photo_count(obs_id, token)
                        if final_count >= local_total:
                            state[rowd.sample_id]["complete"] = True
                            try:
                                state_file.write_text(json.dumps(state, indent=2))
                            except Exception:
                                pass
            # Set DQA 'wild' vote once; is_wild==0 -> agree=false (captive)
            if rowd.is_wild is not None and isinstance(resp, dict) and resp.get("id") and token:
                agree_str = "true" if rowd.is_wild == 1 else "false"
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                url = f"https://www.inaturalist.org/observations/{resp['id']}/quality/wild.json"
                r = requests.post(url, data={"agree": agree_str}, headers=headers, timeout=30)
                if r.status_code >= 300:
                    logger.warning(
                        "DQA vote failed for %s (id=%s): status=%s body=%s",
                        rowd.sample_id,
                        resp.get("id"),
                        r.status_code,
                        r.text[:200],
                    )
                else:
                    logger.info("DQA vote set for %s (id=%s, wild=%s)", rowd.sample_id, resp.get("id"), agree_str)
            print(f"[green]Created[/green] {rowd.sample_id}: response={resp}")
            logger.info("Created %s: id=%s uuid=%s", rowd.sample_id, resp.get("id"), resp.get("uuid"))
            # Update local state
            if state_file and isinstance(resp, dict) and resp.get("id") is not None:
                state[rowd.sample_id] = {"id": resp["id"], "uuid": resp.get("uuid")}
                try:
                    state_file.parent.mkdir(parents=True, exist_ok=True)
                    state_file.write_text(json.dumps(state, indent=2))
                except Exception as write_exc:  # noqa: BLE001
                    logger.warning("Failed to write state file: %s", write_exc)
                    if verbose:
                        print(f"[yellow]Warn[/yellow] failed to write state: {write_exc}")
            if verify and isinstance(resp, dict) and resp.get("id") and token:
                try:
                    check = get_observations_by_id(resp["id"], access_token=token, refresh=True)
                    final = check["results"][0] if isinstance(check, dict) and check.get("results") else check
                    print(
                        f"[blue]Verify[/blue] id={resp['id']} captive={getattr(final, 'captive', None) if not isinstance(final, dict) else final.get('captive')}"
                    )
                    logger.info(
                        "Verify id=%s captive=%s",
                        resp.get("id"),
                        (getattr(final, "captive", None) if not isinstance(final, dict) else final.get("captive")),
                    )
                except Exception as inner_exc:  # noqa: BLE001
                    print(f"[yellow]Warn[/yellow] verify failed for {rowd.sample_id}: {inner_exc}")
                    logger.warning("Verify failed for %s: %s", rowd.sample_id, inner_exc)
        except Exception as exc:  # noqa: BLE001
            print(f"[red]Error[/red] {rowd.sample_id}: {exc}")
            logger.exception("Error processing %s", rowd.sample_id)


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
        "--log-file",
        type=Path,
        default=Path("data/inat_pictures/pusher.log"),
        help="Path to write a run log with created/skipped/errors",
    )
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
    parser.add_argument(
        "--refresh-remote",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Bypass any HTTP cache for remote dedupe checks",
    )
    parser.add_argument(
        "--xy-epsg",
        type=str,
        default="auto",
        help="EPSG code for x_coord/y_coord, or 'auto' to infer from CSV filename (_EPSG:####)",
    )
    parser.add_argument(
        "--upload-timeout",
        type=int,
        default=180,
        help="Timeout in seconds for each create/upload request",
    )
    parser.add_argument(
        "--upload-retries",
        type=int,
        default=2,
        help="Number of retries for create/upload on timeout",
    )
    parser.add_argument(
        "--index-wait-seconds",
        type=int,
        default=120,
        help="After a failed create, wait up to this many seconds for iNat to index the new observation before retrying",
    )
    parser.add_argument(
        "--index-wait-interval",
        type=int,
        default=5,
        help="Polling interval (seconds) while waiting for the observation to appear",
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
        refresh_remote=args.refresh_remote,
        upload_timeout=args.upload_timeout,
        upload_retries=args.upload_retries,
        log_file=args.log_file,
        xy_epsg=args.xy_epsg,
        index_wait_seconds=args.index_wait_seconds,
        index_wait_interval=args.index_wait_interval,
    )


if __name__ == "__main__":
    main()
