# SPDX-FileCopyrightText: 2026 Mika Tammi
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# publish-releases [--check] [--skip-crates]
#
# Master's pipeline, its release job: every release tag vVERSION merged into
# HEAD and not released yet, oldest first, is checked, pushed to GitHub, and
# its commit's crates published to crates.io. Released means the tag is on
# GitHub and every crate of the commit is on crates.io at VERSION, so a run
# after a failure carries on where the last one stopped.
#
# A tag must be annotated, signed with the key whose fingerprint is
# RELEASE_SIGNER (the public key in the file RELEASE_SIGNING_KEY), and name
# the version of its commit's crates.
#
# GITHUB_REMOTE is the GitHub repository, GITHUB_SSH_COMMAND the ssh
# command that reaches it with the deploy key; cargo reads
# CARGO_REGISTRY_TOKEN. PUBLISH_FLAGS are the repository's flags for cargo
# publish (release.nix).
#
# --check, in any checkout: what a release of the checkout's crates would
# do, short of the upload. cargo publish --dry-run packages them and builds
# them from their packages, in this script's environment, the release
# job's; CI runs it before every merge. --skip-crates is for the tests:
# everything but cargo publish.

usage() {
  echo "usage: publish-releases [--check] [--skip-crates]" >&2
}

check=0
skip_crates=0
while [ "$#" -gt 0 ]; do
  case "$1" in
  --check)
    check=1
    shift
    ;;
  --skip-crates)
    skip_crates=1
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage
    exit 2
    ;;
  esac
done
read -r -a publish_flags <<<"${PUBLISH_FLAGS:-}"

root=$(git rev-parse --show-toplevel)
cd "$root"

# The crates of the checkout in the current directory: those crates.io
# gets, as "name version" lines, and the others' names.
publishable() {
  cargo metadata --no-deps --offline --format-version 1 |
    jq -r '.packages[] | select(.publish != []) | "\(.name) \(.version)"'
}
unpublishable() {
  cargo metadata --no-deps --offline --format-version 1 |
    jq -r '.packages[] | select(.publish == []) | .name'
}

# The versions of crate $1 in the crates.io index, one per line.
index_versions() {
  local name=$1 path body status
  case "${#name}" in
  1) path=1/$name ;;
  2) path=2/$name ;;
  3) path=3/${name:0:1}/$name ;;
  *) path=${name:0:2}/${name:2:2}/$name ;;
  esac
  body=$(mktemp)
  if ! status=$(curl -sS -o "$body" -w '%{http_code}' "${CRATES_INDEX:-https://index.crates.io}/$path"); then
    echo "publish-releases: the crates.io index didn't answer for $name" >&2
    exit 1
  fi
  case "$status" in
  # 000: a file:// index, as the tests use.
  200 | 000) jq -r '.vers' "$body" ;;
  404) ;;
  *)
    echo "publish-releases: the crates.io index answered $status for $name" >&2
    exit 1
    ;;
  esac
  rm -f "$body"
}

if [ "$check" = 1 ]; then
  excludes=()
  while IFS= read -r name; do
    excludes+=(--exclude "$name")
  done < <(unpublishable)
  cargo package --workspace --list --allow-dirty "${excludes[@]}"
  cargo publish --workspace --dry-run --allow-dirty "${publish_flags[@]}" "${excludes[@]}"
  exit 0
fi

: "${RELEASE_SIGNER:?the fingerprint of the release signing key}"
: "${RELEASE_SIGNING_KEY:?a file with the release signing key}"
: "${GITHUB_REMOTE:?the GitHub repository}"
github_ssh=${GITHUB_SSH_COMMAND:-ssh}

work=$(mktemp -d)
GNUPGHOME=$work/gnupg
export GNUPGHOME
cleanup() {
  gpgconf --kill gpg-agent 2>/dev/null || true
  git -C "$root" worktree remove --force "$work/tree" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT
mkdir -m 700 "$GNUPGHOME"
gpg --batch --quiet --import "$RELEASE_SIGNING_KEY"

# The runner fetches only the pipeline's ref.
git fetch --quiet origin '+refs/tags/v*:refs/tags/v*'
github_tags=$(git -c core.sshCommand="$github_ssh" ls-remote --tags "$GITHUB_REMOTE" 'v*' | cut -f2)

release_tag() {
  local tag=$1 version=${1#v} commit name crate_version status missing=() published=() excludes=()
  commit=$(git rev-parse "$tag^{commit}")
  git worktree add --quiet --detach "$work/tree" "$commit"
  cd "$work/tree"

  while read -r name crate_version; do
    if index_versions "$name" | grep -Fqx "$version"; then
      published+=("$name")
    else
      missing+=("$name")
    fi
    if [ "$crate_version" != "$version" ]; then
      echo "publish-releases: $tag is on a commit whose $name is $crate_version" >&2
      exit 1
    fi
  done < <(publishable)

  if grep -Fqx "refs/tags/$tag" <<<"$github_tags" && [ "${#missing[@]}" = 0 ]; then
    echo "publish-releases: $tag is released already"
  else
    if [ "$(git cat-file -t "$tag")" != tag ]; then
      echo "publish-releases: $tag is not an annotated tag" >&2
      exit 1
    fi
    if ! status=$(git verify-tag --raw "$tag" 2>&1 >/dev/null); then
      echo "publish-releases: $tag's signature doesn't verify with the release signing key:" >&2
      echo "$status" >&2
      exit 1
    fi
    # VALIDSIG: the key that signed, ..., and last its primary key.
    if ! awk -v signer="$RELEASE_SIGNER" '$2 == "VALIDSIG" && ($3 == signer || $NF == signer) { found = 1 } END { exit !found }' <<<"$status"; then
      echo "publish-releases: $tag is not signed with $RELEASE_SIGNER" >&2
      exit 1
    fi

    if ! grep -Fqx "refs/tags/$tag" <<<"$github_tags"; then
      git -c core.sshCommand="$github_ssh" push --quiet "$GITHUB_REMOTE" "refs/tags/$tag"
      echo "publish-releases: pushed $tag to GitHub"
    fi

    if [ "${#missing[@]}" = 0 ]; then
      :
    elif [ "$skip_crates" = 1 ]; then
      echo "publish-releases: $tag would publish ${missing[*]}"
    else
      for name in "${published[@]}"; do
        excludes+=(--exclude "$name")
      done
      while IFS= read -r name; do
        excludes+=(--exclude "$name")
      done < <(unpublishable)
      cargo publish --workspace "${publish_flags[@]}" "${excludes[@]}"
      echo "publish-releases: published ${missing[*]} at $version"
    fi
  fi

  cd "$root"
  git worktree remove --force "$work/tree"
}

while IFS= read -r tag; do
  release_tag "$tag"
done < <(git tag --merged HEAD --list 'v*' --sort=creatordate)
