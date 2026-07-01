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
