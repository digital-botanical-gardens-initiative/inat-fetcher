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


def test_load_sample_id_allowlist_ignores_comments_blanks_and_duplicates(tmp_path: Path):
    allowlist = tmp_path / "allow.txt"
    allowlist.write_text("\n# comment\ndbgi_001\n\ndbgi_002\ndbgi_001\n")

    assert pusher.load_sample_id_allowlist(allowlist) == {"dbgi_001", "dbgi_002"}


def test_load_sample_id_allowlist_fails_closed_for_missing_or_empty_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        pusher.load_sample_id_allowlist(tmp_path / "missing.txt")

    empty = tmp_path / "empty.txt"
    empty.write_text("# no ids\n\n")
    with pytest.raises(ValueError, match="empty"):
        pusher.load_sample_id_allowlist(empty)


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


def test_dry_run_filters_by_allow_sample_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    images_root = tmp_path / "images"
    touch(images_root / "dbgi_001" / "Plant_dbgi_001_01.jpg")
    touch(images_root / "dbgi_002" / "Plant_dbgi_002_01.jpg")
    csv_path = tmp_path / "input_EPSG:4326.csv"
    csv_path.write_text(
        "\n".join(
            [
                "sample_id,inat_upload,is_wild,taxon_name,date,x_coord,y_coord",
                "dbgi_001,1,0,Plantago major,20250101,7.1,46.0",
                "dbgi_002,1,0,Fascicularia kirchhoffiana,20250101,7.2,46.1",
            ]
        )
    )
    allowlist = tmp_path / "allow.txt"
    allowlist.write_text("dbgi_002\n")

    monkeypatch.delenv(pusher.ENV_TOKEN_KEY, raising=False)
    pusher.run(
        csv_path,
        images_root,
        limit=1,
        dry_run=True,
        log_file=None,
        state_file=None,
        allow_sample_ids=allowlist,
        resolve_taxa=False,
    )

    output = capsys.readouterr().out
    assert "matched 1 uploadable row(s), excluded 1 uploadable row(s)" in output
    assert "sample_id=dbgi_002" in output
    assert "sample_id=dbgi_001" not in output


def test_taxon_resolver_exact_match(monkeypatch: pytest.MonkeyPatch):
    def fake_get_taxa(**kwargs):
        assert kwargs["q"] == "Plantago major"
        return {"results": [{"id": 123, "name": "Plantago major", "rank": "species"}]}

    monkeypatch.setattr(pusher, "get_taxa", fake_get_taxa)
    resolver = pusher.TaxonResolver(pusher.setup_logger(None, False))

    resolution = resolver.resolve("Plantago major")

    assert resolution == pusher.TaxonResolution("Plantago major", 123, "Plantago major", "species", "exact")


def test_taxon_resolver_falls_back_to_genus(monkeypatch: pytest.MonkeyPatch):
    calls = []

    def fake_get_taxa(**kwargs):
        calls.append(kwargs["q"])
        if kwargs["q"] == "Fascicularia kirchhoffiana":
            return {"results": []}
        if kwargs["q"] == "Fascicularia":
            return {
                "results": [
                    {"id": 244190, "name": "Fascicularia", "rank": "genus"},
                    {"id": 244184, "name": "Fascicularia bicolor", "rank": "species"},
                ]
            }
        raise AssertionError(kwargs["q"])

    monkeypatch.setattr(pusher, "get_taxa", fake_get_taxa)
    resolver = pusher.TaxonResolver(pusher.setup_logger(None, False))

    resolution = resolver.resolve("Fascicularia kirchhoffiana")

    assert calls == ["Fascicularia kirchhoffiana", "Fascicularia"]
    assert resolution == pusher.TaxonResolution(
        "Fascicularia kirchhoffiana",
        244190,
        "Fascicularia",
        "genus",
        "genus_fallback",
    )


def test_taxon_resolver_unresolved(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pusher, "get_taxa", lambda **kwargs: {"results": []})
    resolver = pusher.TaxonResolver(pusher.setup_logger(None, False))

    assert resolver.resolve("Notataxon nowhere") == pusher.TaxonResolution(
        "Notataxon nowhere",
        None,
        None,
        None,
        "unresolved",
    )


def test_taxon_resolver_lookup_failure(monkeypatch: pytest.MonkeyPatch):
    def fake_get_taxa(**kwargs):
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(pusher, "get_taxa", fake_get_taxa)
    resolver = pusher.TaxonResolver(pusher.setup_logger(None, False))

    assert resolver.resolve("Plantago major") == pusher.TaxonResolution(
        "Plantago major",
        None,
        None,
        None,
        "lookup_failed",
    )


def test_build_params_includes_taxon_id_and_original_taxon_tag_on_fallback():
    rowd = pusher.RowData(
        sample_id="dbgi_001",
        taxon_name="Fascicularia kirchhoffiana",
        observed_on="2026-01-01T12:00:00",
        latitude=50.0,
        longitude=14.0,
        x_coord=None,
        y_coord=None,
        inat_upload=1,
        is_wild=0,
        collector_inat="@observer",
        collector_fullname="Observer Name",
        collector_orcid=None,
        project=None,
    )
    resolution = pusher.TaxonResolution(
        "Fascicularia kirchhoffiana",
        244190,
        "Fascicularia",
        "genus",
        "genus_fallback",
    )

    params = pusher.build_params(rowd, [Path("dbgi_001_01.jpg")], "token", resolution)

    assert params["taxon_id"] == 244190
    assert params["species_guess"] == "Fascicularia kirchhoffiana"
    assert "emi_original_taxon:Fascicularia kirchhoffiana" in params["tag_list"]
    assert "Original CSV taxon: Fascicularia kirchhoffiana" in params["description"]
