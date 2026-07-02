# iNaturalist Pusher Notes

## Current JBP inputs

- CSV directory: `/media/data/nextcloud_data/emi/files/output/csv/jbp-new`
- Pictures root: `/media/data/nextcloud_data/emi/files/output/pictures/jbp-new/jbp-new`
- Runtime state/log directory: `/media/data/nextcloud_data/emi/files/output/inat-pusher/jbp-new`
- These paths are readable by `cronuser`; the current interactive user may get `Permission denied`.

## uv workflow

Install dependencies:

```bash
uv sync --dev
```

Run tests:

```bash
uv run pytest -q
```

Show pusher help:

```bash
uv run python -m inat_fetcher.src.pusher --help
```

## First-three validation

Preferred wrapper:

```bash
scripts/push_project.sh jbp-new 3
```

Live upload with the wrapper:

```bash
scripts/push_project.sh jbp-new 20 --live --user dbgi
```

Use `0`, `all`, or `unlimited` as the limit to process all remaining observations allowed by the CSV, state file, and optional curation allow-list.

Dry-run the first three uploadable rows that have matched photos and usable coordinates:

```bash
sudo -u cronuser uv run python -m inat_fetcher.src.pusher \
  --csv /media/data/nextcloud_data/emi/files/output/csv/jbp-new \
  --images-root /media/data/nextcloud_data/emi/files/output/pictures/jbp-new/jbp-new \
  --limit 3 \
  --dry-run \
  --verbose \
  --state-file /media/data/nextcloud_data/emi/files/output/inat-pusher/jbp-new/upload_state.json \
  --log-file /media/data/nextcloud_data/emi/files/output/inat-pusher/jbp-new/pusher.log
```

Live upload the same first three only after dry-run review and with an iNaturalist token available:

```bash
sudo -u cronuser env INATURALIST_ACCESS_TOKEN_TODAY="$INATURALIST_ACCESS_TOKEN_TODAY" \
  uv run python -m inat_fetcher.src.pusher \
  --csv /media/data/nextcloud_data/emi/files/output/csv/jbp-new \
  --images-root /media/data/nextcloud_data/emi/files/output/pictures/jbp-new/jbp-new \
  --limit 3 \
  --no-dry-run \
  --verify \
  --state-file /media/data/nextcloud_data/emi/files/output/inat-pusher/jbp-new/upload_state.json \
  --log-file /media/data/nextcloud_data/emi/files/output/inat-pusher/jbp-new/pusher.log
```

## Further improvements

- Add `--sample-id` to target one or more exact samples for reruns.
- Add an explicit `--csv-glob` or `--csv-latest` option if the JBP CSV directory commonly contains multiple exports.
- Emit structured JSON logs for production cron runs.
- Consider sharing the pusher photo resolver with the fetch/format pipeline so CSV picture columns and disk layout stay aligned.

## Taxon resolution fallback

The pusher resolves CSV taxon names to iNaturalist `taxon_id` values before upload. It first tries the full CSV taxon name. If iNaturalist has no active exact match, it tries the first word as a genus fallback.

Example:

- CSV taxon: `Fascicularia kirchhoffiana`
- Exact iNaturalist lookup: no active match
- Genus fallback: `Fascicularia`
- Upload: sends `taxon_id` for `Fascicularia`, keeps `species_guess` as `Fascicularia kirchhoffiana`, and adds `emi_original_taxon:Fascicularia kirchhoffiana`

This avoids observations becoming fully Unknown when the CSV name is a cultivar, unpublished name, or otherwise absent from iNaturalist, while preserving the original label. Disable with `--no-resolve-taxa` if needed.

## Curation-gated uploads

Curation reports should be normalized to a simple `sample_id` allow-list before upload. This keeps the pusher independent from report-specific spreadsheet formats.

Kew report policy:

- join key: `sample_id`
- status column: `Notes`
- accepted statuses after normalization: `OK`, `no photo_05`, `no photo _05`
- current report inspection: 642 curation rows, 410 accepted sample IDs, all 410 present in the local `kew-botanical-gardens` upload CSV

Generate the allow-list from a downloaded report:

```bash
scripts/make_allowlist_from_curation.py /path/to/curation.csv --output /media/data/nextcloud_data/emi/files/output/inat-pusher/kew-botanical-gardens/allow_sample_ids.txt
```

The generator defaults to `--profile kew`; use `--id-column`, `--status-column`, and repeated `--accept-status` flags when a future curation report uses different column names or accepted states.

Dry-run with the default allow-list:

```bash
scripts/push_project.sh kew-botanical-gardens 20
```

Live upload:

```bash
scripts/push_project.sh kew-botanical-gardens 20 --live --user dbgi
```

Live upload without a limit:

```bash
scripts/push_project.sh kew-botanical-gardens all --live --user dbgi
```

The wrapper also accepts an explicit allow-list:

```bash
scripts/push_project.sh kew-botanical-gardens 20 --allow-sample-ids /path/to/allow_sample_ids.txt
```

## Multiple observations of the same specimen

iNaturalist's help page says an observation records an encounter between an observer and an organism, so multiple people may each make observations of the same organism. The practical caveat is that iNaturalist observation counts should not be treated as exact abundance counts. Community discussion also supports this interpretation: duplicate observations by different observers are generally acceptable, especially when each observer had the encounter and is building their own record.

For DBGI this means it is acceptable to have both:

- an observation uploaded by the original observer, so the specimen appears on their iNaturalist account and credits their field encounter;
- a DBGI-owned observation of the same specimen, so DBGI can keep a managed institutional copy for batch edits, continuity, and recovery if an original observer later deletes or changes their observation.

When DBGI uploads a managed copy, keep the provenance explicit:

- preserve `emi_external_id:<sample_id>` as the stable dedupe tag for DBGI uploads;
- include the original observer in the description and tags, for example `Original observer: @handle`, `emi_collector:<name>`, `emi_collector_inat:@handle`, and `emi_collector_orcid:<orcid>`;
- avoid interpreting the resulting DBGI plus observer records as abundance; they are separate observer/specimen encounters or managed copies, not separate individuals.

References:

- https://help.inaturalist.org/en/support/solutions/articles/151000209283-can-two-or-more-people-make-an-observation-of-the-same-organism-
- https://forum.inaturalist.org/t/whats-your-opinion-on-groups-uploading-multiple-observations-of-the-same-individual/74400
