#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/push_project.sh <project> <limit> [--dry-run|--live] [--user <inat_user>] [--allow-sample-ids <path>]

Examples:
  scripts/push_project.sh jbp-new 3
  scripts/push_project.sh jbp-new 20 --live --user dbgi
  scripts/push_project.sh kew-botanical-gardens all --live --user dbgi
  scripts/push_project.sh kew-botanical-gardens 20 --allow-sample-ids /path/to/allow_sample_ids.txt

Notes:
  - Use 0, all, or unlimited for no limit.
  - Defaults to --dry-run.
  - Runs the pusher as cronuser when launched by another user.
  - Expects uv at /usr/local/bin/uv.
  - Live upload requires INATURALIST_ACCESS_TOKEN_TODAY in inat_fetcher/src/.env
    or otherwise visible to cronuser.
EOF
}

original_args=("$@")

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
limit_label="$limit"
limit_args=()

mode="--dry-run"
inat_user="dbgi"
allow_sample_ids=""

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
    --allow-sample-ids)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --allow-sample-ids" >&2
        exit 2
      fi
      allow_sample_ids="$2"
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

case "$limit" in
  0|all|unlimited)
    limit_label="unlimited"
    ;;
  *)
    if ! [[ "$limit" =~ ^[0-9]+$ ]] || [[ "$limit" -lt 1 ]]; then
      echo "<limit> must be a positive integer, 0, all, or unlimited" >&2
      exit 2
    fi
    limit_args=(--limit "$limit")
    ;;
esac

repo_dir="/git_repos/inat-fetcher"
csv_dir="/media/data/nextcloud_data/emi/files/output/csv/${project}"
images_root="/media/data/nextcloud_data/emi/files/output/pictures/${project}/${project}"
safe_project="${project//[^A-Za-z0-9_.-]/_}"
runtime_dir="/media/data/nextcloud_data/emi/files/output/inat-pusher/${safe_project}"
state_file="${runtime_dir}/upload_state.json"
log_file="${runtime_dir}/pusher.log"
default_allow_sample_ids="${runtime_dir}/allow_sample_ids.txt"

if [[ -z "$allow_sample_ids" && "$project" == "kew-botanical-gardens" ]]; then
  allow_sample_ids="$default_allow_sample_ids"
elif [[ -z "$allow_sample_ids" && -f "$default_allow_sample_ids" ]]; then
  allow_sample_ids="$default_allow_sample_ids"
fi

pusher_args=(
  -m inat_fetcher.src.pusher
  --csv "$csv_dir"
  --images-root "$images_root"
  "${limit_args[@]}"
  "$mode"
  --verbose
  --verify
  --user "$inat_user"
  --state-file "$state_file"
  --log-file "$log_file"
)

if [[ -n "$allow_sample_ids" ]]; then
  pusher_args+=(--allow-sample-ids "$allow_sample_ids")
fi

if [[ "$(id -un)" != "cronuser" ]]; then
  quoted_args=$(printf ' %q' "${original_args[@]}")
  exec su - cronuser -c "cd '$repo_dir' && PATH=/usr/local/bin:/usr/bin:/bin UV_PROJECT_ENVIRONMENT=/home/cronuser/.venvs/inat-fetcher scripts/push_project.sh$quoted_args"
fi

echo "Project: $project"
echo "Limit: $limit_label"
echo "Mode: $mode"
echo "CSV: $csv_dir"
echo "Images: $images_root"
echo "Runtime: $runtime_dir"
echo "State: $state_file"
echo "Log: $log_file"
if [[ -n "$allow_sample_ids" ]]; then
  echo "Allow-list: $allow_sample_ids"
fi

mkdir -p "$runtime_dir"
cd "$repo_dir"
export PATH=/usr/local/bin:/usr/bin:/bin
export UV_PROJECT_ENVIRONMENT=/home/cronuser/.venvs/inat-fetcher
uv run python "${pusher_args[@]}"
