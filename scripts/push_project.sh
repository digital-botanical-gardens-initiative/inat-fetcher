#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/push_project.sh <project> <limit> [--dry-run|--live] [--user <inat_user>]

Examples:
  scripts/push_project.sh jbp-new 3
  scripts/push_project.sh jbp-new 20 --live --user dbgi

Notes:
  - Defaults to --dry-run.
  - Runs the pusher as cronuser when launched by another user.
  - Expects uv at /usr/local/bin/uv.
  - Live upload requires INATURALIST_ACCESS_TOKEN_TODAY in inat_fetcher/src/.env
    or otherwise visible to cronuser.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 ]]; then
  usage
  exit 2
fi

project="$1"
limit="$2"
shift 2

mode="--dry-run"
inat_user="dbgi"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      mode="--dry-run"
      shift
      ;;
    --live|--no-dry-run)
      mode="--no-dry-run"
      shift
      ;;
    --user)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --user" >&2
        exit 2
      fi
      inat_user="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! [[ "$limit" =~ ^[0-9]+$ ]] || [[ "$limit" -lt 1 ]]; then
  echo "<limit> must be a positive integer" >&2
  exit 2
fi

repo_dir="/git_repos/inat-fetcher"
csv_dir="/media/data/nextcloud_data/emi/files/output/csv/${project}"
images_root="/media/data/nextcloud_data/emi/files/output/pictures/${project}/${project}"
safe_project="${project//[^A-Za-z0-9_.-]/_}"
runtime_dir="/media/data/nextcloud_data/emi/files/output/inat-pusher/${safe_project}"
state_file="${runtime_dir}/upload_state.json"
log_file="${runtime_dir}/pusher.log"

pusher_args=(
  python -m inat_fetcher.src.pusher
  --csv "$csv_dir"
  --images-root "$images_root"
  --limit "$limit"
  "$mode"
  --verbose
  --verify
  --user "$inat_user"
  --state-file "$state_file"
  --log-file "$log_file"
)

cmd="mkdir -p '$runtime_dir' && cd '$repo_dir' && PATH=/usr/local/bin:/usr/bin:/bin UV_PROJECT_ENVIRONMENT=/home/cronuser/.venvs/inat-fetcher uv run ${pusher_args[*]@Q}"

echo "Project: $project"
echo "Limit: $limit"
echo "Mode: $mode"
echo "CSV: $csv_dir"
echo "Images: $images_root"
echo "Runtime: $runtime_dir"
echo "State: $state_file"
echo "Log: $log_file"

if [[ "$(id -un)" == "cronuser" ]]; then
  bash -lc "$cmd"
else
  su - cronuser -c "$cmd"
fi
