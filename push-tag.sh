#!/usr/bin/env bash
set -euo pipefail

TAG_PREFIX="${TAG_PREFIX:-v}"
REGISTER_DELAY_SECONDS="${REGISTER_DELAY_SECONDS:-15}"
POLL_SECONDS="${POLL_SECONDS:-20}"
EMPTY_POLLS_BEFORE_FAIL="${EMPTY_POLLS_BEFORE_FAIL:-6}"
VISIBILITY_SETTLE_SECONDS="${VISIBILITY_SETTLE_SECONDS:-6}"
PUSH_RETRY_ATTEMPTS="${PUSH_RETRY_ATTEMPTS:-5}"
PUSH_RETRY_INITIAL_SECONDS="${PUSH_RETRY_INITIAL_SECONDS:-5}"
VISIBILITY_RETRY_ATTEMPTS="${VISIBILITY_RETRY_ATTEMPTS:-6}"
VISIBILITY_RETRY_INITIAL_SECONDS="${VISIBILITY_RETRY_INITIAL_SECONDS:-4}"
API_ACCEPT_HEADER="Accept: application/vnd.github+json"
API_VERSION_HEADER="X-GitHub-Api-Version: 2022-11-28"

BUMP_KIND="${1:-}"
REPO=""
ORIGINAL_VISIBILITY=""
VISIBILITY_CHANGED=0

usage() {
  echo "Usage: ./push-tag.sh <patch|minor|major>" >&2
}

die() {
  echo "error: $*" >&2
  exit 1
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

api() {
  gh api -H "$API_ACCEPT_HEADER" -H "$API_VERSION_HEADER" "$@"
}

parse_repo_from_remote() {
  local remote_url="$1"
  local path=""

  remote_url="${remote_url%/}"
  remote_url="${remote_url%.git}"

  if [[ "$remote_url" =~ ^https://github\.com/(.+)$ ]]; then
    path="${BASH_REMATCH[1]}"
  elif [[ "$remote_url" =~ ^git@github\.com:(.+)$ ]]; then
    path="${BASH_REMATCH[1]}"
  elif [[ "$remote_url" =~ ^ssh://git@github\.com/(.+)$ ]]; then
    path="${BASH_REMATCH[1]}"
  else
    return 1
  fi

  if [[ "$path" =~ ^([^/]+)/([^/]+)$ ]]; then
    echo "${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    return 0
  fi

  return 1
}

latest_semver_tag() {
  git tag --list "${TAG_PREFIX}[0-9]*.[0-9]*.[0-9]*" --sort=-v:refname | head -n 1
}

version_gt() {
  local left="$1"
  local right="$2"
  node -e '
    const [a, b] = process.argv.slice(1).map((v) => v.split(".").map(Number));
    const cmp = (x, y) => x[0] !== y[0] ? x[0] - y[0] : x[1] !== y[1] ? x[1] - y[1] : x[2] - y[2];
    process.exit(cmp(a, b) > 0 ? 0 : 1);
  ' "$left" "$right"
}

preview_bumped_version() {
  local version="$1"
  local bump_kind="$2"
  node -e '
    const version = process.argv[1];
    const bump = process.argv[2];
    let [major, minor, patch] = version.split(".").map(Number);
    if (bump === "patch") patch += 1;
    else if (bump === "minor") { minor += 1; patch = 0; }
    else if (bump === "major") { major += 1; minor = 0; patch = 0; }
    else process.exit(2);
    console.log(`${major}.${minor}.${patch}`);
  ' "$version" "$bump_kind"
}

set_visibility() {
  local visibility="$1"
  api --method PATCH "/repos/$REPO" -f "visibility=$visibility" >/dev/null
}

set_visibility_with_retry() {
  local visibility="$1"
  local attempts="$VISIBILITY_RETRY_ATTEMPTS"
  local delay="$VISIBILITY_RETRY_INITIAL_SECONDS"
  local i
  for ((i=1; i<=attempts; i++)); do
    if set_visibility "$visibility" 2>/dev/null; then
      return 0
    fi
    if [[ $i -lt $attempts ]]; then
      echo "  visibility flip to '$visibility' failed (attempt $i/$attempts); retrying in ${delay}s..." >&2
      sleep "$delay"
      delay=$((delay * 2))
    fi
  done
  return 1
}

git_push_with_retry() {
  local ref="$1"
  local attempts="$PUSH_RETRY_ATTEMPTS"
  local delay="$PUSH_RETRY_INITIAL_SECONDS"
  local i
  for ((i=1; i<=attempts; i++)); do
    if git push origin "$ref"; then
      return 0
    fi
    if [[ $i -lt $attempts ]]; then
      echo "  push of '$ref' failed (attempt $i/$attempts); retrying in ${delay}s..." >&2
      sleep "$delay"
      delay=$((delay * 2))
    fi
  done
  return 1
}

cleanup() {
  local status="$1"
  trap - EXIT INT TERM
  set +e

  if [[ "$VISIBILITY_CHANGED" == "1" && -n "$ORIGINAL_VISIBILITY" ]]; then
    echo "→ Restoring repo visibility to ${ORIGINAL_VISIBILITY}..."
    if set_visibility_with_retry "$ORIGINAL_VISIBILITY"; then
      echo "✓ Repo restored to ${ORIGINAL_VISIBILITY}"
    else
      echo "warning: failed to restore repo visibility to ${ORIGINAL_VISIBILITY}" >&2
      echo "warning: run this manually after re-authenticating if needed:" >&2
      echo "  gh api --method PATCH /repos/$REPO -f visibility=${ORIGINAL_VISIBILITY}" >&2
      status=1
    fi
  fi

  exit "$status"
}

trap 'cleanup "$?"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

[[ -n "$BUMP_KIND" ]] || {
  usage
  exit 2
}

have_cmd gh || die "GitHub CLI (gh) is not installed."
have_cmd git || die "git is not installed."
have_cmd node || die "node is not installed."
have_cmd npm || die "npm is not installed."

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "Run this from inside the git repository."
git remote get-url origin >/dev/null 2>&1 || die "Git remote 'origin' is not configured."
gh auth status >/dev/null 2>&1 || die "gh is not authenticated. Run: gh auth login -h github.com"
[[ -f package.json ]] || die "package.json not found in repo root."

if [[ "${ALLOW_DIRTY:-0}" != "1" ]] && [[ -n "$(git status --porcelain)" ]]; then
  die "working tree has uncommitted changes. Commit or stash them first, or run with ALLOW_DIRTY=1."
fi

REMOTE_URL="$(git remote get-url origin)"
REPO="$(parse_repo_from_remote "$REMOTE_URL")" || die "could not parse a GitHub owner/repo from origin: $REMOTE_URL"
CURRENT_BRANCH="$(git symbolic-ref --quiet --short HEAD 2>/dev/null)" || die "detached HEAD is not supported for releases."
PREVIOUS_VERSION="$(node -p "require('./package.json').version")"
[[ -n "$PREVIOUS_VERSION" ]] || die "could not read package.json version"
LATEST_TAG="$(latest_semver_tag)"
LATEST_TAG_VERSION="${LATEST_TAG#${TAG_PREFIX}}"
BASE_VERSION="$PREVIOUS_VERSION"

if [[ ! -d .github/workflows ]]; then
  echo "warning: no .github/workflows directory found; the workflow wait step may fail if this repo has no tag-triggered workflows." >&2
fi

case "$BUMP_KIND" in
  patch|minor|major)
    ;;
  *)
    die "unsupported bump kind '$BUMP_KIND' (expected patch, minor, or major)"
    ;;
esac

if [[ -n "$LATEST_TAG" ]]; then
  if version_gt "$LATEST_TAG_VERSION" "$PREVIOUS_VERSION"; then
    BASE_VERSION="$LATEST_TAG_VERSION"
  elif version_gt "$PREVIOUS_VERSION" "$LATEST_TAG_VERSION"; then
    echo "warning: package.json version ($PREVIOUS_VERSION) is ahead of latest tag ($LATEST_TAG)." >&2
  fi
fi

TARGET_VERSION="$(preview_bumped_version "$BASE_VERSION" "$BUMP_KIND")"
TAG="${TAG_PREFIX}${TARGET_VERSION}"

if git rev-parse --verify "refs/tags/$TAG" >/dev/null 2>&1; then
  die "computed tag '$TAG' already exists locally."
fi

echo "→ Repo: $REPO"
echo "→ Branch: $CURRENT_BRANCH"
echo "→ Current package version: $PREVIOUS_VERSION"
if [[ -n "$LATEST_TAG" ]]; then
  echo "→ Latest git tag: $LATEST_TAG"
fi
if [[ "$BASE_VERSION" != "$PREVIOUS_VERSION" ]]; then
  echo "→ Syncing package version forward to $BASE_VERSION before bumping"
  npm version --no-git-tag-version "$BASE_VERSION" >/dev/null
fi
echo "→ Running npm version $BUMP_KIND..."
npm version --no-git-tag-version "$BUMP_KIND" >/dev/null

NEW_VERSION="$(node -p "require('./package.json').version")"
[[ -n "$NEW_VERSION" ]] || die "could not read updated package.json version"
[[ "$NEW_VERSION" == "$TARGET_VERSION" ]] || die "expected version $TARGET_VERSION after npm bump, got $NEW_VERSION"

git add -A
git commit -m "chore: release $TAG" >/dev/null
echo "→ Created release commit for $TAG"
echo "→ Pushing branch $CURRENT_BRANCH..."
git push origin "$CURRENT_BRANCH"

ORIGINAL_VISIBILITY="$(api "/repos/$REPO" --jq '.visibility')"
[[ -n "$ORIGINAL_VISIBILITY" ]] || die "could not determine current repo visibility for $REPO"

echo "→ Current visibility: $ORIGINAL_VISIBILITY"
echo "→ New package version: $NEW_VERSION"
echo "→ Release tag: $TAG"
echo "→ Making repo public..."
if ! set_visibility_with_retry public; then
  die "could not flip repo visibility to public"
fi
VISIBILITY_CHANGED=1

echo "→ Waiting ${VISIBILITY_SETTLE_SECONDS}s for GitHub to propagate the visibility change to git endpoints..."
sleep "$VISIBILITY_SETTLE_SECONDS"

echo "→ Creating and pushing tag $TAG..."
git tag "$TAG"
if ! git_push_with_retry "$TAG"; then
  die "failed to push tag $TAG after $PUSH_RETRY_ATTEMPTS attempts"
fi

echo "→ Waiting ${REGISTER_DELAY_SECONDS}s for workflows to register..."
sleep "$REGISTER_DELAY_SECONDS"

SHA="$(git rev-list -n 1 "$TAG")"
echo "→ Waiting for workflows on commit $SHA to finish..."

empty_polls=0
while true; do
  total_runs="$(
    api "/repos/$REPO/actions/runs?head_sha=$SHA&event=push&per_page=100" \
      --jq '.total_count'
  )"

  active_runs="$(
    api "/repos/$REPO/actions/runs?head_sha=$SHA&event=push&per_page=100" \
      --jq '[.workflow_runs[] | select(.status == "queued" or .status == "in_progress" or .status == "pending" or .status == "waiting" or .status == "requested")] | length'
  )"

  echo "  Workflow runs seen: $total_runs | active: $active_runs"

  if [[ "$total_runs" -eq 0 ]]; then
    empty_polls=$((empty_polls + 1))
    if [[ "$empty_polls" -ge "$EMPTY_POLLS_BEFORE_FAIL" ]]; then
      die "no workflow runs appeared for $SHA. Check .github/workflows for a tags trigger and verify the tag pattern."
    fi
  else
    empty_polls=0
    if [[ "$active_runs" -eq 0 ]]; then
      break
    fi
  fi

  sleep "$POLL_SECONDS"
done

failed_runs="$(
  api "/repos/$REPO/actions/runs?head_sha=$SHA&event=push&per_page=100" \
    --jq '[.workflow_runs[] | select(.conclusion != null and .conclusion != "success")] | length'
)"

if [[ "$failed_runs" -gt 0 ]]; then
  echo "warning: one or more workflows completed without success." >&2
  api "/repos/$REPO/actions/runs?head_sha=$SHA&event=push&per_page=100" \
    --jq '.workflow_runs[] | select(.conclusion != null and .conclusion != "success") | "  - \(.name): \(.conclusion)"'
else
  echo "✓ All workflows completed successfully"
fi

echo "Workflow summary:"
api "/repos/$REPO/actions/runs?head_sha=$SHA&event=push&per_page=100" \
  --jq '.workflow_runs[] | "  - \(.name) | status=\(.status) | conclusion=\(.conclusion // "n/a") | \(.html_url)"'

RELEASE_URL="$(gh release view "$TAG" --repo "$REPO" --json url --jq '.url' 2>/dev/null || true)"
echo "Release links:"
if [[ -n "$RELEASE_URL" ]]; then
  echo "  - $RELEASE_URL"
else
  echo "  - No GitHub Release found yet for $TAG"
fi
