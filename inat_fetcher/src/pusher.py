"""
Create iNaturalist observations from a CSV and local photos.

Notes:
- Reads rows from the JBP/JBC-formatted CSV and builds
  parameters for `pyinaturalist.v1.observations.create_observation()`.
- Photos are resolved from explicit picture columns first, then from
  per-sample folders and recursive sample-id matches under the images root.

Usage (dry run by default):
    uv run python -m inat_fetcher.src.pusher \
        --limit 3 --verbose

To actually create observations, pass `--no-dry-run` and ensure
`INATURALIST_ACCESS_TOKEN_TODAY` is set in your environment or .env file.
See https://www.inaturalist.org/users/api_token for generating a token.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
import requests

# pyinaturalist helpers
from pyinaturalist import (
    create_observation,
    get_observations,
    get_observations_by_id,
    get_taxa,
    upload as upload_media,
)
from pyinaturalist.session import get_refresh_params


DEFAULT_CSV = "/media/data/nextcloud_data/emi/files/output/csv/jbp-new"
DEFAULT_IMAGES_ROOT = "/media/data/nextcloud_data/emi/files/output/pictures/jbp-new/jbp-new"
ENV_TOKEN_KEY = "INATURALIST_ACCESS_TOKEN_TODAY"
PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png"}
PICTURE_COLUMN_PREFIX = "picture_"


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
    photo_refs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TaxonResolution:
    original_name: str
    taxon_id: Optional[int]
    resolved_name: Optional[str]
    rank: Optional[str]
    source: str

    @property
    def used_fallback(self) -> bool:
        return self.source == "genus_fallback"


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


def get_lat_lon(row_lat: Optional[float], row_lon: Optional[float]) -> tuple[Optional[float], Optional[float]]:
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


def natural_key(path: Path) -> list[object]:
    name = path.name.lower()
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", name)]


def is_photo_path(path: Path) -> bool:
    return path.suffix.lower() in PHOTO_EXTENSIONS


def _clean_photo_ref(value: object) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ref = str(value).strip()
    return ref or None


def photo_refs_from_row(row: pd.Series) -> list[str]:
    refs: list[str] = []
    for col in row.index:
        if str(col).startswith(PICTURE_COLUMN_PREFIX):
            ref = _clean_photo_ref(row.get(col))
            if ref:
                refs.append(ref)
    return refs


class PhotoResolver:
    def __init__(self, images_root: Path) -> None:
        self.images_root = images_root
        self._basename_index: Optional[dict[str, list[Path]]] = None

    def _iter_photos(self) -> Iterable[Path]:
        if not self.images_root.exists():
            return []
        return (path for path in self.images_root.rglob("*") if path.is_file() and is_photo_path(path))

    def _index_by_basename(self) -> dict[str, list[Path]]:
        if self._basename_index is None:
            index: dict[str, list[Path]] = {}
            for path in self._iter_photos():
                index.setdefault(path.name.lower(), []).append(path)
            self._basename_index = {key: sorted(paths, key=natural_key) for key, paths in index.items()}
        return self._basename_index

    def resolve_ref(self, ref: str) -> list[Path]:
        ref_path = Path(ref)
        candidates = []
        if ref_path.is_absolute():
            candidates.append(ref_path)
        else:
            candidates.append(self.images_root / ref_path)
            candidates.append(self.images_root / ref_path.name)

        matches = [path for path in candidates if path.is_file() and is_photo_path(path)]
        if matches:
            return matches

        return self._index_by_basename().get(ref_path.name.lower(), [])

    def collect(self, sample_id: str, photo_refs: Optional[Iterable[str]] = None) -> list[Path]:
        paths: list[Path] = []
        for ref in photo_refs or []:
            paths.extend(self.resolve_ref(ref))

        paths.extend(collect_photos(self.images_root, sample_id))

        if not paths and self.images_root.exists():
            sample_key = sample_id.lower()
            paths.extend(path for path in self._iter_photos() if sample_key in path.name.lower())

        return dedupe_and_sort_photos(paths)


def dedupe_and_sort_photos(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        resolved = path.resolve() if path.exists() else path
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return sorted(unique, key=natural_key)


def collect_photos(images_root: Path, sample_id: str) -> list[Path]:
    folder = images_root / sample_id
    if not folder.exists() or not folder.is_dir():
        return []
    paths: list[Path] = []
    for pattern in ("*.jpg", "*.jpeg", "*.png"):
        paths.extend(folder.glob(pattern))
    return dedupe_and_sort_photos(paths)


def resolve_csv_path(csv_path: Path) -> Path:
    if csv_path.is_file():
        return csv_path
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV path does not exist: {csv_path}")
    if not csv_path.is_dir():
        raise ValueError(f"CSV path is neither a file nor a directory: {csv_path}")

    candidates = sorted(path for path in csv_path.glob("*.csv") if path.is_file())
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise FileNotFoundError(f"No .csv files found in directory: {csv_path}")

    candidate_list = "\n".join(f"  - {path}" for path in candidates)
    raise ValueError(f"Expected exactly one .csv in {csv_path}, found {len(candidates)}:\n{candidate_list}")


def load_sample_id_allowlist(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Sample ID allow-list does not exist: {path}")

    allowed: set[str] = set()
    for line in path.read_text().splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        allowed.add(value)

    if not allowed:
        raise ValueError(f"Sample ID allow-list is empty: {path}")
    return allowed


def _normalize_taxon_name(name: str) -> str:
    return " ".join(name.casefold().split())


def _first_matching_taxon(results: Iterable[dict[str, Any]], expected_name: str, *, rank: Optional[str] = None) -> Optional[dict[str, Any]]:
    expected = _normalize_taxon_name(expected_name)
    for result in results:
        name = result.get("name")
        if not isinstance(name, str) or _normalize_taxon_name(name) != expected:
            continue
        if rank and result.get("rank") != rank:
            continue
        return result
    return None


class TaxonResolver:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self._cache: dict[str, TaxonResolution] = {}

    def resolve(self, taxon_name: Optional[str]) -> Optional[TaxonResolution]:
        if not taxon_name:
            return None

        name = " ".join(taxon_name.split())
        key = _normalize_taxon_name(name)
        if key in self._cache:
            return self._cache[key]

        resolution = self._resolve_uncached(name)
        self._cache[key] = resolution
        return resolution

    def _resolve_uncached(self, name: str) -> TaxonResolution:
        try:
            exact = get_taxa(q=name, is_active=True, per_page=10)
            match = _first_matching_taxon(exact.get("results", []), name)
            if match:
                return TaxonResolution(
                    original_name=name,
                    taxon_id=match.get("id"),
                    resolved_name=match.get("name"),
                    rank=match.get("rank"),
                    source="exact",
                )

            genus = name.split()[0] if name.split() else ""
            if genus and genus != name:
                genus_resp = get_taxa(q=genus, is_active=True, per_page=10)
                genus_match = _first_matching_taxon(genus_resp.get("results", []), genus, rank="genus")
                if genus_match:
                    return TaxonResolution(
                        original_name=name,
                        taxon_id=genus_match.get("id"),
                        resolved_name=genus_match.get("name"),
                        rank=genus_match.get("rank"),
                        source="genus_fallback",
                    )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Taxon lookup failed for %s: %s", name, exc)
            return TaxonResolution(name, None, None, None, "lookup_failed")

        return TaxonResolution(name, None, None, None, "unresolved")


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
        photo_refs=photo_refs_from_row(row),
    )


def build_params(
    rowd: RowData,
    photos: Iterable[Path],
    access_token: str,
    taxon_resolution: Optional[TaxonResolution] = None,
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
    if taxon_resolution and taxon_resolution.used_fallback:
        tags.append(f"emi_original_taxon:{taxon_resolution.original_name}")
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
    if taxon_resolution and taxon_resolution.taxon_id:
        params["taxon_id"] = taxon_resolution.taxon_id
    # Description from collector_inat, normalized to single '@'
    if rowd.collector_inat:
        handle = rowd.collector_inat
        if not handle.startswith("@"):  # ensure single '@'
            handle = "@" + handle
        params["description"] = f"Original observer: {handle}"
    if taxon_resolution and taxon_resolution.used_fallback:
        fallback_note = (
            f"Original CSV taxon: {taxon_resolution.original_name}; "
            f"uploaded with iNaturalist taxon: {taxon_resolution.resolved_name} ({taxon_resolution.rank})."
        )
        params["description"] = (
            f"{params['description']}\n{fallback_note}" if params.get("description") else fallback_note
        )
    # Drop None values to avoid sending empty fields
    return {k: v for k, v in params.items() if v not in (None, [], "", ())}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_photo_count(obs_id: int, token: str) -> int:
    try:
        check = get_observations_by_id(obs_id, access_token=token, refresh=True)
        result = check["results"][0] if isinstance(check, dict) and check.get("results") else check
        if isinstance(result, dict):
            photos = result.get("photos") or []
            return len(photos)
    except Exception:
        pass
    return -1


def resolve_lat_lon_from_xy(
    rowd: RowData, xy_epsg: str, logger: logging.Logger
) -> tuple[Optional[float], Optional[float]]:
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
            "pyproj is required to transform x/y from EPSG:%s to EPSG:4326. Install with 'uv add pyproj' or 'pip install pyproj'",
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
    resolve_taxa: bool = True,
    allow_sample_ids: Optional[Path] = None,
) -> None:
    load_env()
    logger = setup_logger(log_file, verbose)
    csv_path = resolve_csv_path(csv_path)
    token = os.getenv(ENV_TOKEN_KEY)
    if not token:
        print(f"Warning: {ENV_TOKEN_KEY} not set; only dry-run will work.")
        logger.warning("%s not set; only dry-run will work", ENV_TOKEN_KEY)
        if not dry_run:
            raise SystemExit("Access token required for non-dry-run.")

    # State for idempotency
    state: dict[str, Any] = {}
    if state_file:
        try:
            if state_file.exists():
                state = json.loads(state_file.read_text())
        except Exception:  # noqa: BLE001
            state = {}

    df = pd.read_csv(csv_path)
    rows = [to_row_data(r) for _, r in df.iterrows()]
    # Keep only rows marked for upload and with a sample_id
    filtered = [r for r in rows if r.inat_upload and r.sample_id]
    uploadable_count = len(filtered)

    allowed_ids: Optional[set[str]] = None
    if allow_sample_ids:
        allowed_ids = load_sample_id_allowlist(allow_sample_ids)
        before_filter = len(filtered)
        filtered = [r for r in filtered if r.sample_id in allowed_ids]
        missing_allowed = allowed_ids - {r.sample_id for r in rows if r.sample_id}
        print(
            f"Loaded {len(allowed_ids)} allowed sample_id(s) from {allow_sample_ids}; "
            f"matched {len(filtered)} uploadable row(s), excluded {before_filter - len(filtered)} uploadable row(s)."
        )
        logger.info(
            "Allow-list %s loaded: allowed=%d uploadable_before=%d matched=%d excluded=%d missing_allowed=%d",
            allow_sample_ids,
            len(allowed_ids),
            before_filter,
            len(filtered),
            before_filter - len(filtered),
            len(missing_allowed),
        )
        if missing_allowed:
            logger.warning(
                "Allow-list has %d sample_id(s) not present in CSV, first values: %s",
                len(missing_allowed),
                sorted(missing_allowed)[:20],
            )

    print(f"Found {len(filtered)} uploadable row(s) in {csv_path}.")
    logger.info("Found %d uploadable rows in %s (before allow-list=%d)", len(filtered), csv_path, uploadable_count)

    photo_resolver = PhotoResolver(images_root)
    taxon_resolver = TaxonResolver(logger) if resolve_taxa else None
    processed = 0
    for idx, rowd in enumerate(filtered, start=1):
        if limit is not None and processed >= limit:
            break

        # Idempotency: skip only if marked complete in local state
        if state_file and rowd.sample_id in state and state[rowd.sample_id].get("complete") is True:
            msg = f"Skip {rowd.sample_id}: already uploaded (complete in state file)"
            logger.info(msg)
            if verbose:
                print(f"Skip {rowd.sample_id}: already uploaded (complete)")
            continue

        # Idempotency: optional remote dedupe via tag search
        unique_tag = f"emi_external_id:{rowd.sample_id}"
        should_dedupe_remote = dedupe_remote and not dry_run
        if should_dedupe_remote:
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
                        state.setdefault(rowd.sample_id, {})
                        state[rowd.sample_id].update(
                            {
                                "id": first.get("id") if isinstance(first, dict) else None,
                                "complete": True,
                            }
                        )
                        try:
                            state_file.parent.mkdir(parents=True, exist_ok=True)
                            state_file.write_text(json.dumps(state, indent=2))
                        except Exception:
                            pass
                    logger.info(
                        "Skip %s: found existing on iNat (matches=%s, first_id=%s)",
                        rowd.sample_id,
                        len(results),
                        first.get("id") if isinstance(first, dict) else None,
                    )
                    if verbose:
                        print(f"Skip {rowd.sample_id}: found existing on iNat")
                    continue
            except Exception:  # noqa: BLE001
                # Non-fatal; proceed without remote dedupe
                logger.warning("Remote dedupe check failed for %s", rowd.sample_id)
                pass

        photos = photo_resolver.collect(rowd.sample_id, rowd.photo_refs)
        if not photos:
            logger.warning("Skip %s: no photos found under %s", rowd.sample_id, images_root)
            if verbose:
                print(f"Skip {rowd.sample_id}: no photos found under {images_root}")
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
            print(f"Skip {rowd.sample_id}: could not resolve latitude/longitude")
            continue

        # Override legacy lat/lon so build_params uses the resolved values
        rowd.latitude, rowd.longitude = lat, lon
        logger.info("%s geolocation from x/y using EPSG:%s -> lat=%.8f lon=%.8f", rowd.sample_id, epsg_to_use, lat, lon)

        taxon_resolution = taxon_resolver.resolve(rowd.taxon_name) if taxon_resolver else None
        if taxon_resolution:
            logger.info(
                "%s taxon resolution: original=%s taxon_id=%s resolved=%s rank=%s source=%s",
                rowd.sample_id,
                taxon_resolution.original_name,
                taxon_resolution.taxon_id,
                taxon_resolution.resolved_name,
                taxon_resolution.rank,
                taxon_resolution.source,
            )

        params = build_params(rowd, photos, access_token=token or "", taxon_resolution=taxon_resolution)
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
        processed += 1

        if dry_run:
            print(
                f"Dry-run {processed}{f'/{limit}' if limit else ''}: csv_row={idx}, sample_id={rowd.sample_id}, "
                f"taxon={rowd.taxon_name}, lat/lon={params.get('latitude')},{params.get('longitude')} "
                f"taxon_id={params.get('taxon_id')} "
                f"taxon_source={taxon_resolution.source if taxon_resolution else 'disabled'} "
                f"resolved_taxon={taxon_resolution.resolved_name if taxon_resolution else None} "
                f"photos={len(photos)} photo_files={[Path(p).name for p in params.get('photos', [])]}"
            )
            continue

        try:
            # Separate create and media upload for safer retries
            photos_list = params.pop("photos", [])

            # Helper: poll iNat for an existing obs with our unique tag
            def wait_for_existing(timeout_s: int) -> Optional[dict[str, Any]]:
                deadline = time.time() + timeout_s
                tries = 0
                while time.time() < deadline:
                    tries += 1
                    try:
                        refresh_kwargs = get_refresh_params("observations")
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
            resp: dict[str, Any]
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
                    # Batch upload all remaining photos at once with limited retries
                    uploaded_hashes = set()
                    if state.get(rowd.sample_id) and state[rowd.sample_id].get("uploaded_photos"):
                        uploaded_hashes = set(state[rowd.sample_id]["uploaded_photos"])  # type: ignore[index]

                    # Filter to photos not yet recorded as uploaded in state
                    to_upload: list[str] = []
                    hashes_to_add: list[str] = []
                    for photo_path in photos_list:
                        p = Path(photo_path)
                        try:
                            h = _sha256_file(p)
                        except Exception:
                            h = f"name:{p.name}|size:{p.stat().st_size}"
                        if h in uploaded_hashes:
                            continue
                        to_upload.append(str(p))
                        hashes_to_add.append(h)

                    if not to_upload:
                        # Nothing new to upload; if server already has enough, mark complete
                        if state_file:
                            final_count = _get_photo_count(obs_id, token)
                            if final_count >= local_total:
                                state.setdefault(rowd.sample_id, {})
                                entry = state[rowd.sample_id]
                                entry["uploaded_photos"] = sorted(set(entry.get("uploaded_photos", [])) | set(hashes_to_add))
                                entry["complete"] = True
                                try:
                                    state_file.write_text(json.dumps(state, indent=2))
                                except Exception:
                                    pass
                        # Proceed to DQA step
                    else:
                        attempt = 0
                        while True:
                            before = server_count if server_count >= 0 else _get_photo_count(obs_id, token)
                            try:
                                upload_media(
                                    obs_id,
                                    photos=to_upload,
                                    access_token=token,
                                    timeout=upload_timeout,
                                )
                            except Exception as e:  # noqa: BLE001
                                attempt += 1
                                if attempt > upload_retries:
                                    raise
                                delay = min(15, 5 * attempt)
                                logger.warning(
                                    "Batch upload failed for %s (attempt %d/%d): %s; retrying in %ss",
                                    rowd.sample_id,
                                    attempt,
                                    upload_retries,
                                    e,
                                    delay,
                                )
                                time.sleep(delay)
                                continue

                            # Success path: verify count increased to expected (best effort)
                            time.sleep(index_wait_interval)
                            after = _get_photo_count(obs_id, token)
                            if after >= local_total or after > before:
                                server_count = after
                                if state_file:
                                    state.setdefault(rowd.sample_id, {})
                                    entry = state[rowd.sample_id]
                                    entry["uploaded_photos"] = sorted(set(entry.get("uploaded_photos", [])) | set(hashes_to_add))
                                    if after >= local_total:
                                        entry["complete"] = True
                                    try:
                                        state_file.write_text(json.dumps(state, indent=2))
                                    except Exception:
                                        pass
                                break
                            else:
                                attempt += 1
                                if attempt > upload_retries:
                                    raise RuntimeError(
                                        f"Upload completed but photo count did not change for {rowd.sample_id}"
                                    )
                                time.sleep(min(15, 5 * attempt))
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
            print(f"Created {rowd.sample_id}: response={resp}")
            logger.info("Created %s: id=%s uuid=%s", rowd.sample_id, resp.get("id"), resp.get("uuid"))
            # Update local state
            if state_file and isinstance(resp, dict) and resp.get("id") is not None:
                state.setdefault(rowd.sample_id, {})
                state[rowd.sample_id].update(
                    {
                        "id": resp["id"],
                        "uuid": resp.get("uuid"),
                        "complete": True,
                    }
                )
                try:
                    state_file.parent.mkdir(parents=True, exist_ok=True)
                    state_file.write_text(json.dumps(state, indent=2))
                except Exception as write_exc:  # noqa: BLE001
                    logger.warning("Failed to write state file: %s", write_exc)
                    if verbose:
                        print(f"Warn: failed to write state: {write_exc}")
            if verify and isinstance(resp, dict) and resp.get("id") and token:
                try:
                    check = get_observations_by_id(resp["id"], access_token=token, refresh=True)
                    final = check["results"][0] if isinstance(check, dict) and check.get("results") else check
                    print(
                        f"Verify id={resp['id']} captive={getattr(final, 'captive', None) if not isinstance(final, dict) else final.get('captive')}"
                    )
                    logger.info(
                        "Verify id=%s captive=%s",
                        resp.get("id"),
                        (getattr(final, "captive", None) if not isinstance(final, dict) else final.get("captive")),
                    )
                except Exception as inner_exc:  # noqa: BLE001
                    print(f"Warn: verify failed for {rowd.sample_id}: {inner_exc}")
                    logger.warning("Verify failed for %s: %s", rowd.sample_id, inner_exc)
        except Exception as exc:  # noqa: BLE001
            print(f"Error {rowd.sample_id}: {exc}")
            logger.exception("Error processing %s", rowd.sample_id)


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=DEFAULT_CSV, type=Path, help="Path to input CSV")
    parser.add_argument(
        "--images-root",
        default=DEFAULT_IMAGES_ROOT,
        type=Path,
        help="Root directory containing photos",
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
        help="Query iNat by tag to skip already-uploaded samples. Ignored during dry-runs.",
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
    parser.add_argument(
        "--resolve-taxa",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resolve CSV taxon names to iNaturalist taxon_id, falling back to genus when exact name is missing",
    )
    parser.add_argument(
        "--allow-sample-ids",
        type=Path,
        default=None,
        help="Optional file containing one sample_id per line; only these sample IDs will be processed",
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
        resolve_taxa=args.resolve_taxa,
        allow_sample_ids=args.allow_sample_ids,
    )


if __name__ == "__main__":
    main()
