#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from urllib.request import urlopen

import pandas as pd


DEFAULT_ACCEPT_STATUSES = ("OK", "no photo_05")
PROFILES = {
    "kew": {
        "id_column": "sample_id",
        "status_column": "Notes",
        "accept_statuses": DEFAULT_ACCEPT_STATUSES,
    }
}


def normalize_status(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return "".join(str(value).casefold().split())


def read_curation_csv(source: str) -> pd.DataFrame:
    if source.startswith(("http://", "https://")):
        with urlopen(source, timeout=60) as response:
            return pd.read_csv(response)
    return pd.read_csv(Path(source))


def build_allowlist(
    df: pd.DataFrame,
    *,
    id_column: str,
    status_column: str,
    accept_statuses: list[str],
) -> list[str]:
    missing = [col for col in (id_column, status_column) if col not in df.columns]
    if missing:
        raise SystemExit(f"Missing required column(s): {', '.join(missing)}")

    accepted = {normalize_status(status) for status in accept_statuses}
    allowed = df[df[status_column].map(normalize_status).isin(accepted)][id_column]
    sample_ids = sorted({str(value).strip() for value in allowed if str(value).strip() and str(value) != "nan"})
    if not sample_ids:
        raise SystemExit("No sample IDs matched the accepted curation statuses")
    return sample_ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a pusher sample_id allow-list from a curation CSV.")
    parser.add_argument("source", help="Curation CSV path or URL")
    parser.add_argument("--output", type=Path, required=True, help="Output file with one sample_id per line")
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default="kew",
        help="Curation report profile. Defaults to kew.",
    )
    parser.add_argument("--id-column", default=None, help="Column containing sample IDs")
    parser.add_argument("--status-column", default=None, help="Column containing curator status")
    parser.add_argument(
        "--accept-status",
        action="append",
        default=None,
        help="Accepted status value; may be repeated. Defaults to OK and no photo_05.",
    )
    args = parser.parse_args()

    profile = PROFILES[args.profile]
    id_column = args.id_column or str(profile["id_column"])
    status_column = args.status_column or str(profile["status_column"])
    accept_statuses = args.accept_status or list(profile["accept_statuses"])
    df = read_curation_csv(args.source)
    sample_ids = build_allowlist(
        df,
        id_column=id_column,
        status_column=status_column,
        accept_statuses=accept_statuses,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(sample_ids) + "\n")
    print(f"Wrote {len(sample_ids)} sample_id(s) to {args.output}")


if __name__ == "__main__":
    main()
