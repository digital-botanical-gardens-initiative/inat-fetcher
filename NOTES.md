# iNaturalist Pusher Notes

## Current JBP inputs

- CSV directory: `/media/data/nextcloud_data/emi/files/output/csv/jbp-new`
- Pictures root: `/media/data/nextcloud_data/emi/files/output/pictures/jbp-new/jbp-new`
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

Dry-run the first three uploadable rows that have matched photos and usable coordinates:

```bash
sudo -u cronuser uv run python -m inat_fetcher.src.pusher \
  --csv /media/data/nextcloud_data/emi/files/output/csv/jbp-new \
  --images-root /media/data/nextcloud_data/emi/files/output/pictures/jbp-new/jbp-new \
  --limit 3 \
  --dry-run \
  --verbose \
  --state-file /tmp/inat-pusher-jbp-state.json \
  --log-file /tmp/inat-pusher-jbp.log
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
  --state-file /tmp/inat-pusher-jbp-state.json \
  --log-file /tmp/inat-pusher-jbp.log
```

## Further improvements

- Add `--sample-id` to target one or more exact samples for reruns.
- Add an explicit `--csv-glob` or `--csv-latest` option if the JBP CSV directory commonly contains multiple exports.
- Move pusher state/log defaults out of `data/inat_pictures` and into a configurable runtime directory.
- Emit structured JSON logs for production cron runs.
- Consider sharing the pusher photo resolver with the fetch/format pipeline so CSV picture columns and disk layout stay aligned.

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
