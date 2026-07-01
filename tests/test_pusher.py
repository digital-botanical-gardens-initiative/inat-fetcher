from pathlib import Path

import pandas as pd
import pytest

from inat_fetcher.src import pusher


def touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"image")
    return path


def test_resolve_csv_path_accepts_file(tmp_path: Path):
    csv_path = touch(tmp_path / "input.csv")

    assert pusher.resolve_csv_path(csv_path) == csv_path


def test_resolve_csv_path_accepts_directory_with_single_csv(tmp_path: Path):
    csv_path = touch(tmp_path / "input.csv")
    touch(tmp_path / "notes.txt")

    assert pusher.resolve_csv_path(tmp_path) == csv_path


def test_resolve_csv_path_rejects_directory_with_multiple_csvs(tmp_path: Path):
    touch(tmp_path / "a.csv")
    touch(tmp_path / "b.csv")

    with pytest.raises(ValueError, match="Expected exactly one .csv"):
        pusher.resolve_csv_path(tmp_path)


def test_photo_resolver_uses_picture_columns_by_basename(tmp_path: Path):
    images_root = tmp_path / "images"
    photo = touch(images_root / "nested" / "Plant_dbgi_001_01.jpg")
    row = pd.Series(
        {
            "sample_id": "dbgi_001",
            "picture_panel": "DCIM/jbp/Plant_dbgi_001_01.jpg",
            "picture_detail": "",
        }
    )
    row_data = pusher.to_row_data(row)

    assert pusher.PhotoResolver(images_root).collect(row_data.sample_id, row_data.photo_refs) == [photo]


def test_photo_resolver_uses_sample_folder_fallback(tmp_path: Path):
    images_root = tmp_path / "images"
    photo_2 = touch(images_root / "dbgi_001" / "Plant_dbgi_001_02.jpg")
    photo_10 = touch(images_root / "dbgi_001" / "Plant_dbgi_001_10.jpg")

    assert pusher.PhotoResolver(images_root).collect("dbgi_001") == [photo_2, photo_10]


def test_photo_resolver_uses_recursive_sample_id_fallback(tmp_path: Path):
    images_root = tmp_path / "images"
    photo = touch(images_root / "flat" / "Plant_dbgi_001_01.jpg")

    assert pusher.PhotoResolver(images_root).collect("dbgi_001") == [photo]


def test_photo_resolver_deduplicates_and_sorts(tmp_path: Path):
    images_root = tmp_path / "images"
    photo_1 = touch(images_root / "dbgi_001" / "Plant_dbgi_001_01.jpg")
    photo_2 = touch(images_root / "dbgi_001" / "Plant_dbgi_001_02.jpg")

    photos = pusher.PhotoResolver(images_root).collect(
        "dbgi_001",
        ["Plant_dbgi_001_02.jpg", "Plant_dbgi_001_01.jpg"],
    )

    assert photos == [photo_1, photo_2]


def test_dry_run_does_not_call_remote_dedupe_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    images_root = tmp_path / "images"
    touch(images_root / "dbgi_001" / "Plant_dbgi_001_01.jpg")
    csv_path = tmp_path / "input_EPSG:4326.csv"
    csv_path.write_text(
        "\n".join(
            [
                "sample_id,inat_upload,is_wild,taxon_name,date,x_coord,y_coord",
                "dbgi_001,1,0,Plantago major,20250101,7.1,46.0",
            ]
        )
    )

    def fail_remote_dedupe(*args, **kwargs):
        raise AssertionError("remote dedupe should not run during dry-run")

    monkeypatch.setattr(pusher, "get_observations", fail_remote_dedupe)
    monkeypatch.delenv(pusher.ENV_TOKEN_KEY, raising=False)

    pusher.run(csv_path, images_root, limit=1, dry_run=True, log_file=None, state_file=None)
