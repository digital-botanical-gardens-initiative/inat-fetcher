# inat-fetcher

Utilities for preparing, fetching, and pushing DBGI observation data to iNaturalist.

## Setup

Install dependencies with uv:

```bash
uv sync --dev
```

Run tests:

```bash
uv run pytest -q
```

## Pushing observations

Dry-run the first three uploadable observations from the configured JBP inputs:

```bash
uv run python -m inat_fetcher.src.pusher --limit 3 --dry-run --verbose
```

### iNaturalist token

For a live upload, the pusher needs an iNaturalist API token for the account that will own the observations.

1. Log in to iNaturalist in a browser.
2. Open <https://www.inaturalist.org/users/api_token>.
3. Copy the token shown on that page.
4. Provide it as `INATURALIST_ACCESS_TOKEN_TODAY`.

You can put it in `inat_fetcher/src/.env`:

```bash
INATURALIST_ACCESS_TOKEN_TODAY=your_token_here
```

Or export it for the current shell:

```bash
export INATURALIST_ACCESS_TOKEN_TODAY=your_token_here
```

Then pass `--no-dry-run`:

```bash
INATURALIST_ACCESS_TOKEN_TODAY=<token> uv run python -m inat_fetcher.src.pusher --limit 3 --no-dry-run --verify
```

### Taxon resolution

Before upload, the pusher resolves the CSV taxon name against iNaturalist and sends `taxon_id` when possible. It first tries the full name, then falls back to the genus when the full name is not known to iNaturalist.

For example, `Fascicularia kirchhoffiana` is not an iNaturalist taxon, so the pusher falls back to `Fascicularia` if that genus exists. The original CSV taxon is still preserved in the observation description and tags.

Disable this behavior with:

```bash
uv run python -m inat_fetcher.src.pusher --no-resolve-taxa
```

## Project wrapper

Use the wrapper script to avoid long copy-paste commands. It takes the project folder name and number of observations to process. Dry-run is the default:

```bash
scripts/push_project.sh jbp-new 3
```

Live upload:

```bash
scripts/push_project.sh jbp-new 20 --live --user dbgi
```

The wrapper maps `jbp-new` to:

```text
csv:     /media/data/nextcloud_data/emi/files/output/csv/jbp-new
photos:  /media/data/nextcloud_data/emi/files/output/pictures/jbp-new/jbp-new
runtime: /media/data/nextcloud_data/emi/files/output/inat-pusher/jbp-new
```

The runtime directory contains durable upload state and logs.
