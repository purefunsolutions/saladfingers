# SPDX-FileCopyrightText: 2026 Mika Tammi
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# The release scripts against throwaway repositories, a file:// crates.io
# index and throwaway keys. release refuses what isn't a new version and
# makes a release commit that changes only versions, signed, with its
# signed tag; publish-releases pushes a release tag to GitHub once, and
# refuses a tag that isn't annotated, signed with the release signing key
# and named after its commit's version. Nothing is pushed to GitLab or
# published.
{
  git,
  gnupg,
  jq,
  publish-releases,
  release,
  runCommand,
  rustToolchain,
}:
runCommand "release-test" {
  nativeBuildInputs = [git gnupg jq publish-releases release rustToolchain];
} ''
  export HOME=$TMPDIR
  export GNUPGHOME=$TMPDIR/gnupg
  mkdir -m 700 "$GNUPGHOME"
  gpg --batch --quiet --passphrase "" --quick-gen-key "Release Test <release@example.invalid>" ed25519 sign never
  gpg --batch --quiet --passphrase "" --quick-gen-key "Someone Else <else@example.invalid>" ed25519 sign never
  fingerprint() {
    gpg --list-keys --with-colons "$1" | awk -F: '$1 == "fpr" { print $10; exit }'
  }
  signer=$(fingerprint release@example.invalid)
  other=$(fingerprint else@example.invalid)
  gpg --armor --export "$signer" > signer.asc

  git config --global user.name "Release Test"
  git config --global user.email release@example.invalid
  git config --global user.signingkey "$signer"
  git config --global init.defaultBranch master

  # expect STATUS COMMAND...: COMMAND exits with STATUS.
  expect() {
    want=$1
    shift
    got=0
    "$@" > out 2>&1 || got=$?
    if [ "$got" -ne "$want" ]; then
      cat out
      echo "expected exit status $want, got $got: $*" >&2
      exit 1
    fi
  }
  # shows TEXT: the last command's output has TEXT.
  shows() {
    if ! grep -Fq -- "$1" out; then
      cat out
      echo "expected the output to show: $1" >&2
      exit 1
    fi
  }

  # A workspace at 0.2.0: a library, a tool that depends on it by path and
  # version, and a crate that isn't published.
  git init --quiet --bare demo.git
  git init --quiet --bare github.git
  git init --quiet demo
  cd demo
  mkdir -p crates/demo-lib/src crates/demo-cli/src crates/demo-shim/src
  cat > Cargo.toml <<'EOF'
  [workspace]
  resolver = "3"
  members = ["crates/demo-lib", "crates/demo-cli", "crates/demo-shim"]

  [workspace.package]
  version = "0.2.0"
  edition = "2024"
  license = "MIT OR Apache-2.0"

  [workspace.dependencies]
  demo-lib = { path = "crates/demo-lib", version = "0.2.0" }
  EOF
  for crate in demo-lib demo-cli demo-shim; do
    cat > crates/$crate/Cargo.toml <<EOF
  [package]
  name = "$crate"
  version.workspace = true
  edition.workspace = true
  license.workspace = true
  EOF
  done
  echo 'publish = false' >> crates/demo-shim/Cargo.toml
  printf '\n[dependencies]\ndemo-lib.workspace = true\n' >> crates/demo-cli/Cargo.toml
  echo 'pub fn f() {}' > crates/demo-lib/src/lib.rs
  echo 'fn main() { demo_lib::f(); }' > crates/demo-cli/src/main.rs
  echo 'fn main() {}' > crates/demo-shim/src/main.rs
  cargo generate-lockfile --offline --quiet
  git add -A
  git commit --quiet -m "demo: init"
  git remote add origin "$TMPDIR/demo.git"
  git push --quiet origin master

  # Neither crate is on crates.io yet.
  export CRATES_INDEX=file://$TMPDIR/index
  mkdir -p "$TMPDIR/index/de/mo"
  touch "$TMPDIR/index/de/mo/demo-lib" "$TMPDIR/index/de/mo/demo-cli"

  # Not a release version, or not a new one.
  expect 2 release --no-push --yes 1.2
  expect 2 release --no-push --yes 0.2.1-pre.1
  expect 1 release --no-push --yes 0.2.0
  shows "at 0.2.0 already"
  echo '{"name":"demo-lib","vers":"0.4.0"}' > "$TMPDIR/index/de/mo/demo-lib"
  expect 1 release --no-push --yes 0.4.0
  shows "demo-lib 0.4.0 is on crates.io already"
  : > "$TMPDIR/index/de/mo/demo-lib"

  # The release commit: versions only, signed, and its signed tag.
  expect 0 release --no-push --yes 0.2.0-alpha.1
  [ "$(git log -1 --format=%s release/v0.2.0-alpha.1)" = "demo: 0.2.0-alpha.1" ]
  git log -1 --format=%b release/v0.2.0-alpha.1 | grep -Fqx "Signed-off-by: Release Test <release@example.invalid>"
  git verify-commit release/v0.2.0-alpha.1 2> /dev/null
  git verify-tag v0.2.0-alpha.1 2> /dev/null
  [ "$(git rev-parse 'v0.2.0-alpha.1^{commit}')" = "$(git rev-parse release/v0.2.0-alpha.1)" ]
  [ "$(git diff --name-only master release/v0.2.0-alpha.1 | tr '\n' ' ')" = "Cargo.lock Cargo.toml " ]
  [ "$(git show release/v0.2.0-alpha.1:Cargo.toml | grep -c '"0.2.0-alpha.1"')" = 2 ]
  [ "$(git show release/v0.2.0-alpha.1:Cargo.lock | grep -c '^version = "0.2.0-alpha.1"$')" = 3 ]
  [ "$(git worktree list | wc -l)" = 1 ]
  expect 1 release --no-push --yes 0.2.0-alpha.1
  shows "v0.2.0-alpha.1 exists already"

  # Master as the merge request leaves it.
  git merge --quiet --no-ff -m "Merge branch 'release/v0.2.0-alpha.1' into 'master'" release/v0.2.0-alpha.1
  git push --quiet origin master v0.2.0-alpha.1
  export GITHUB_REMOTE=$TMPDIR/github.git RELEASE_SIGNER=$signer RELEASE_SIGNING_KEY=$TMPDIR/signer.asc
  expect 0 publish-releases --skip-crates
  shows "pushed v0.2.0-alpha.1 to GitHub"
  shows "v0.2.0-alpha.1 would publish"
  shows demo-lib
  shows demo-cli
  git ls-remote --tags "$GITHUB_REMOTE" | grep -Fq refs/tags/v0.2.0-alpha.1
  echo '{"name":"demo-lib","vers":"0.2.0-alpha.1"}' > "$TMPDIR/index/de/mo/demo-lib"
  echo '{"name":"demo-cli","vers":"0.2.0-alpha.1"}' > "$TMPDIR/index/de/mo/demo-cli"
  expect 0 publish-releases --skip-crates
  shows "v0.2.0-alpha.1 is released already"

  # A second release, its tag replaced by ones that must not release.
  expect 0 release --no-push --yes 0.2.0-alpha.2
  git merge --quiet --no-ff -m "Merge branch 'release/v0.2.0-alpha.2' into 'master'" release/v0.2.0-alpha.2
  git push --quiet origin master
  commit=$(git rev-parse 'v0.2.0-alpha.2^{commit}')
  git tag -f v0.2.0-alpha.2 "$commit" > /dev/null
  expect 1 publish-releases --skip-crates
  shows "v0.2.0-alpha.2 is not an annotated tag"
  git tag -f -a -m "demo 0.2.0-alpha.2" v0.2.0-alpha.2 "$commit" > /dev/null
  expect 1 publish-releases --skip-crates
  shows "signature doesn't verify"
  git tag -f -s -u "$other" -m "demo 0.2.0-alpha.2" v0.2.0-alpha.2 "$commit" > /dev/null
  expect 1 publish-releases --skip-crates
  shows "signature doesn't verify"
  git tag -d v0.2.0-alpha.2 > /dev/null
  git tag -s -m "demo 0.2.0-alpha.3" v0.2.0-alpha.3 "$commit"
  expect 1 publish-releases --skip-crates
  shows "v0.2.0-alpha.3 is on a commit whose demo-"
  git tag -d v0.2.0-alpha.3 > /dev/null
  git tag -s -m "demo 0.2.0-alpha.2" v0.2.0-alpha.2 "$commit"
  expect 0 publish-releases --skip-crates
  shows "pushed v0.2.0-alpha.2 to GitHub"
  if git ls-remote --tags "$GITHUB_REMOTE" | grep -Fq alpha.3; then
    echo "a refused tag reached GitHub" >&2
    exit 1
  fi

  gpgconf --kill all
  touch $out
''
