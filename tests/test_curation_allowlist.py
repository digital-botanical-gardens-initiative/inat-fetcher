import pandas as pd

from scripts.make_allowlist_from_curation import build_allowlist, normalize_status


def test_normalize_status_ignores_case_and_whitespace():
    assert normalize_status("OK") == "ok"
    assert normalize_status(" no photo _05 ") == "nophoto_05"
    assert normalize_status("No Photo_05") == "nophoto_05"


def test_build_allowlist_accepts_kew_statuses_only():
    df = pd.DataFrame(
        [
            {"sample_id": "dbgi_001", "Notes": "OK"},
            {"sample_id": "dbgi_002", "Notes": "no photo_05"},
            {"sample_id": "dbgi_003", "Notes": "no photo _05"},
            {"sample_id": "dbgi_004", "Notes": "TEST"},
            {"sample_id": "dbgi_005", "Notes": "NO"},
            {"sample_id": "dbgi_006", "Notes": "no asterisk"},
            {"sample_id": "dbgi_002", "Notes": "OK"},
        ]
    )

    assert build_allowlist(
        df,
        id_column="sample_id",
        status_column="Notes",
        accept_statuses=["OK", "no photo_05"],
    ) == ["dbgi_001", "dbgi_002", "dbgi_003"]
