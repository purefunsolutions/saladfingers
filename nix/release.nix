# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
#
# Releases, as in the infurer project's repositories. `nix run .#release -- VERSION`
# starts one: a signed release commit and tag, pushed to GitLab with a merge request
# (release.sh). Master's pipeline finishes it with `nix run .#publish-releases`, which
# pushes the tag to GitHub and publishes the crates to crates.io (publish-releases.sh).
# The two scripts and their test are furymodel's, unchanged, and are to stay that way
# (copied by hand when furymodel's change); what is particular to saladfingers is set
# here. See docs/releasing.md.
{
  perSystem = {
    pkgs,
    rustToolchain,
    ...
  }: let
    # Signing goes through git, with the user's own gpg (gpg.program).
    release = pkgs.writeShellApplication {
      name = "release";
      runtimeInputs = [
        pkgs.coreutils
        pkgs.curl
        pkgs.gawk
        pkgs.git
        pkgs.glab
        pkgs.jq
        pkgs.yq-go
        rustToolchain
      ];
      text = builtins.readFile ./release.sh;
    };
    # cargo publish builds every package before it uploads it, here and in its --check
    # before a merge. aws-lc-sys and zstd-sys compile C (and aws-lc-sys GAS assembly)
    # with plain cc, so the builds' own C compiler and linker, stdenv.cc, are all they
    # need (see the aws-lc-sys note in package.nix). On macOS, Rust's standard library
    # also links libiconv, which the SDK gives the builds.
    publish-releases = pkgs.writeShellApplication {
      name = "publish-releases";
      runtimeInputs = [
        pkgs.stdenv.cc
        pkgs.coreutils
        pkgs.curl
        pkgs.gawk
        pkgs.git
        pkgs.gnupg
        pkgs.jq
        pkgs.openssh
        rustToolchain
      ];
      # Every dependency outside this workspace comes from crates.io, so Cargo.lock is
      # the one that users get, and --locked publishes exactly what CI has tested.
      runtimeEnv =
        {
          PUBLISH_FLAGS = "--locked";
        }
        // pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isDarwin {
          LIBRARY_PATH = "${pkgs.libiconv}/lib";
        };
      text = builtins.readFile ./publish-releases.sh;
    };
  in {
    apps = {
      release = {
        type = "app";
        program = pkgs.lib.getExe release;
        meta.description = "Start a release: a signed release commit and tag, and their merge request";
      };
      publish-releases = {
        type = "app";
        program = pkgs.lib.getExe publish-releases;
        meta.description = "Push the release tags merged into master to GitHub, and publish their crates (--check: all but the upload)";
      };
    };

    checks.release = pkgs.callPackage ./release-test.nix {
      inherit release publish-releases rustToolchain;
    };
  };
}
