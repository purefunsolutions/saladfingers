# SPDX-FileCopyrightText: 2026 Mika Tammi
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# release [--no-push] [--yes] VERSION
# release --abort VERSION
#
# Starts a release of the repository's crates at VERSION. On origin/master,
# a release commit changes the crates' version and nothing else. The commit
# is signed and gets a signed tag vVERSION; both go to GitLab with a merge
# request that merges when its pipeline passes. Once merged, master's
# pipeline pushes the tag to GitHub and publishes the crates to crates.io
# (publish-releases).
#
# The release commit skips the per-commit gate: it changes only version
# strings in what master has passed, so the script checks that edit
# instead. It works in a worktree of its own, so the checkout's state
# doesn't matter.
#
# --abort VERSION closes the release's merge request and deletes its branch
# and tag, for a release that won't be merged. --no-push and --yes are for
# the tests: everything up to the push, without asking.

usage() {
  echo "usage: release [--no-push] [--yes] VERSION" >&2
  echo "       release --abort VERSION" >&2
}

push=1
ask=1
abort=0
version=
while [ "$#" -gt 0 ]; do
  case "$1" in
  --no-push)
    push=0
    shift
    ;;
  --yes)
    ask=0
    shift
    ;;
  --abort)
    abort=1
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  -*)
    usage
    exit 2
    ;;
  *)
    if [ -n "$version" ]; then
      usage
      exit 2
    fi
    version=$1
    shift
    ;;
  esac
done
if [ -z "$version" ]; then
  usage
  exit 2
fi

# A release is MAJOR.MINOR.PATCH, or one of its pre-releases: alpha.N, then
# beta.N, then rc.N.
if ! [[ $version =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|rc)\.[1-9][0-9]*)?$ ]]; then
  echo "release: $version is not a version such as 1.2.3 or 1.2.3-alpha.1 (alpha, beta, rc)" >&2
  exit 2
fi
tag=v$version
branch=release/$tag

root=$(git rev-parse --show-toplevel)
cd "$root"
origin=$(git remote get-url origin)
repo=$(basename "$origin" .git)

# The GitLab host and project, from origin's URL: scp-like
# (git@host:group/name.git), ssh:// or https://.
gitlab() {
  local rest
  case "$origin" in
  *://*)
    rest=${origin#*://}
    rest=${rest#*@}
    host=${rest%%/*}
    host=${host%%:*}
    project=${rest#*/}
    ;;
  *:*)
    rest=${origin#*@}
    host=${rest%%:*}
    project=${rest#*:}
    ;;
  *)
    echo "release: origin ($origin) is not a GitLab repository's URL" >&2
    exit 1
    ;;
  esac
  project=${project%.git}
  api=projects/${project//\//%2F}
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
    echo "release: the crates.io index didn't answer for $name" >&2
    exit 1
  fi
  case "$status" in
  # 000: a file:// index, as the tests use.
  200 | 000) jq -r '.vers' "$body" ;;
  404) ;;
  *)
    echo "release: the crates.io index answered $status for $name" >&2
    exit 1
    ;;
  esac
  rm -f "$body"
}

if [ "$abort" = 1 ]; then
  gitlab
  git fetch --quiet origin master
  released=$(git ls-remote origin "refs/tags/$tag^{}" | cut -f1)
  if [ -n "$released" ] && git merge-base --is-ancestor "$released" origin/master 2>/dev/null; then
    echo "release: $tag is merged into master, which releases it: nothing to abort" >&2
    exit 1
  fi
  for iid in $(glab api --hostname "$host" "$api/merge_requests?state=opened&source_branch=${branch//\//%2F}" | jq -r '.[].iid'); do
    glab api --hostname "$host" --method PUT "$api/merge_requests/$iid" -f state_event=close >/dev/null
    echo "release: closed merge request !$iid"
  done
  for ref in "refs/heads/$branch" "refs/tags/$tag"; do
    if [ -n "$(git ls-remote origin "$ref")" ]; then
      git push --quiet origin --delete "$ref"
      echo "release: deleted $ref on GitLab"
    fi
  done
  git tag -d "$tag" >/dev/null 2>&1 || true
  git branch -D "$branch" >/dev/null 2>&1 || true
  exit 0
fi

if [ "$push" = 1 ]; then
  gitlab
fi
git fetch --quiet --tags origin
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  echo "release: $tag exists already" >&2
  exit 1
fi
if git rev-parse -q --verify "refs/heads/$branch" >/dev/null; then
  echo "release: the branch $branch exists already (release --abort $version removes it)" >&2
  exit 1
fi

work=$(mktemp -d)
finished=0
cleanup() {
  git -C "$root" worktree remove --force "$work/tree" 2>/dev/null || true
  rm -rf "$work"
  if [ "$finished" = 0 ]; then
    git -C "$root" tag -d "$tag" >/dev/null 2>&1 || true
  fi
  if [ "$finished" = 0 ] || [ "$push" = 1 ]; then
    git -C "$root" branch -D "$branch" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
git worktree add --quiet -b "$branch" "$work/tree" origin/master
cd "$work/tree"

metadata() {
  cargo metadata --no-deps --offline --format-version 1
}
before=$(metadata)
mapfile -t members < <(jq -r '.packages[].name' <<<"$before")
mapfile -t publishable < <(jq -r '.packages[] | select(.publish != []) | .name' <<<"$before")
old=$(jq -r '[.packages[].version] | unique | if length == 1 then .[0] else "" end' <<<"$before")
if [ -z "$old" ]; then
  echo "release: the crates don't share one version, which a release needs" >&2
  exit 1
fi
if [ "$old" = "$version" ]; then
  echo "release: master's crates are at $version already" >&2
  exit 1
fi
for name in "${publishable[@]}"; do
  if index_versions "$name" | grep -Fqx "$version"; then
    echo "release: $name $version is on crates.io already" >&2
    exit 1
  fi
done

# The edit: the version in [package] or [workspace.package], and the version
# requirement of every dependency on a member, in the manifests; and the
# members' entries in Cargo.lock.
manifests=(Cargo.toml)
while IFS= read -r manifest; do
  manifests+=("$(realpath --relative-to=. "$manifest")")
done < <(jq -r '.packages[].manifest_path' <<<"$before")
mapfile -t manifests < <(printf '%s\n' "${manifests[@]}" | sort -u)
for manifest in "${manifests[@]}"; do
  awk -v old="$old" -v new="$version" -v names="${members[*]}" '
    BEGIN {
      split(names, list, " ")
      for (i in list) member[list[i]] = 1
    }
    /^\[/ { section = $0 }
    (section == "[package]" || section == "[workspace.package]") && $0 == "version = \"" old "\"" {
      $0 = "version = \"" new "\""
    }
    {
      name = $0
      sub(/ .*/, "", name)
      if ((name in member) && index($0, " = {") && index($0, "path = ")) {
        for (op = 1; op <= 4; op++) {
          prefix = substr("x=^~", op, 1)
          if (prefix == "x") prefix = ""
          from = "version = \"" prefix old "\""
          at = index($0, from)
          if (at) {
            $0 = substr($0, 1, at - 1) "version = \"" prefix new "\"" substr($0, at + length(from))
            break
          }
        }
      }
    }
    { print }
  ' "$manifest" >"$work/edited"
  cat "$work/edited" >"$manifest"
done
awk -v old="$old" -v new="$version" -v names="${members[*]}" '
  BEGIN {
    split(names, list, " ")
    for (i in list) member[list[i]] = 1
  }
  /^\[\[package\]\]$/ { name = "" }
  /^name = "/ {
    name = $0
    sub(/^name = "/, "", name)
    sub(/"$/, "", name)
  }
  (name in member) && $0 == "version = \"" old "\"" { $0 = "version = \"" new "\"" }
  { print }
' Cargo.lock >"$work/edited"
cat "$work/edited" >Cargo.lock

# The check of the edit, which stands in for the gate: every member at the
# new version, every requirement on a member naming it (dev-dependencies may
# name none), every member's Cargo.lock entry with it, and no line changed
# but these.
fail() {
  echo "release: $*" >&2
  git --no-pager diff >&2
  exit 1
}
after=$(metadata)
stale=$(jq -r --arg v "$version" '.packages[] | select(.version != $v) | .name' <<<"$after")
[ -z "$stale" ] || fail "still at another version than $version: $stale"
names_json=$(printf '%s\n' "${members[@]}" | jq -R . | jq -s .)
stale=$(jq -r --arg v "$version" --argjson names "$names_json" '
  .packages[] | .name as $from | .dependencies[]
  | select(.path != null and (.name | IN($names[])))
  | select((.req | ltrimstr("=") | ltrimstr("^") | ltrimstr("~")) != $v)
  | select(.req != "*" or .kind != "dev")
  | "\($from) -> \(.name) \(.req)"
' <<<"$after")
[ -z "$stale" ] || fail "requirements on members that don't name $version: $stale"
locked=$(yq -p toml -o json -I0 '[.package[] | select(.source == null) | {"name": .name, "version": .version}]' Cargo.lock)
for name in "${members[@]}"; do
  jq -e --arg n "$name" --arg v "$version" 'any(.[]; .name == $n and .version == $v)' <<<"$locked" >/dev/null ||
    fail "Cargo.lock has no $name $version"
done
changed=$(git diff --name-only | grep -Ev '(^|/)Cargo\.(toml|lock)$' || true)
[ -z "$changed" ] || fail "files changed besides the Cargo files: $changed"
other=$(git diff -U0 | grep -E '^[-+]' | grep -Ev '^(\+\+\+|---) ' | grep -Ev '^[-+]([A-Za-z0-9_-]+ = \{.*)?version = "' || true)
[ -z "$other" ] || fail "lines changed besides versions: $other"
git diff --quiet && fail "nothing changed"

git --no-pager diff --stat
git --no-pager diff
if [ "$ask" = 1 ]; then
  read -r -p "Sign the release commit and $tag, and push them for the merge request? [y/N] " answer
  case "$answer" in
  y | Y | yes) ;;
  *)
    echo "release: stopped, nothing pushed" >&2
    exit 1
    ;;
  esac
fi

git commit --quiet --all --signoff --gpg-sign --message "$repo: $version"
git tag --sign --message "$repo $version" "$tag"
commit=$(git rev-parse HEAD)

if [ "$push" = 0 ]; then
  finished=1
  echo "release: $tag on $branch, not pushed"
  exit 0
fi

git push --quiet --atomic origin "refs/heads/$branch" "refs/tags/$tag"
finished=1
mr=$(glab api --hostname "$host" --method POST "$api/merge_requests" \
  -f source_branch="$branch" -f target_branch=master -f title="$repo: $version" \
  -F remove_source_branch=true \
  -f description="The release commit of $tag. Once merged, master's pipeline pushes $tag to GitHub and publishes the crates to crates.io.")
iid=$(jq -r .iid <<<"$mr")
url=$(jq -r .web_url <<<"$mr")

# Auto-merge waits for the merge request's pipeline, which must exist first:
# without one, there would be nothing to wait for.
pipeline=
for _ in $(seq 60); do
  pipeline=$(glab api --hostname "$host" "$api/merge_requests/$iid" |
    jq -r --arg sha "$commit" 'select(.head_pipeline.sha == $sha) | .head_pipeline.id')
  [ -z "$pipeline" ] || break
  sleep 5
done
if [ -z "$pipeline" ]; then
  echo "release: $url has no pipeline after 5 minutes: set it to auto-merge in GitLab" >&2
  exit 1
fi
glab api --hostname "$host" --method PUT "$api/merge_requests/$iid/merge" \
  -F auto_merge=true -f sha="$commit" -F should_remove_source_branch=true >/dev/null
echo "release: $url merges when its pipeline passes; master's pipeline then releases $tag"
