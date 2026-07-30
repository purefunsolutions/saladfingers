# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
# mkCargoTestBinaries — compile a workspace's `cargo test` executables (without
# running them) and harvest them, plus a `run-all-tests` runner, into $out/bin.
#
# This is how GPU-gated tests reach rented hardware: a consumer (e.g. infurer)
# calls this with its own crane machinery, and the harvested test binaries' RPATHs
# drag the exact CUDA store paths into the image closure — so no cargo, nextest, or
# source ever ships in the image. Drop the result into an image's `contents`; the
# binaries land at /bin, and `sf-agent run` execs the JobSpec command
# (`/bin/run-all-tests --ignored`).
#
# Args: pkgs, craneLib, commonArgs, cargoArtifacts (the consumer's), plus
#   testPackages : list of `-p` package names whose tests to compile
#   pname        : derivation name
{
  pkgs,
  craneLib,
  commonArgs,
  cargoArtifacts,
  testPackages,
  pname ? "cargo-test-binaries",
}: let
  inherit (pkgs) lib;
  pkgFlags = lib.concatMapStringsSep " " (p: "-p ${lib.escapeShellArg p}") testPackages;
in
  craneLib.mkCargoDerivation (commonArgs
    // {
      inherit cargoArtifacts pname;
      version = commonArgs.version or "0.1.0";
      # We ship test binaries, not cargo state.
      doInstallCargoArtifacts = false;

      # Keep the build-only closure out of the image. Nix decides what an output depends
      # on by scanning it for store-path strings, and a Rust binary is full of them that
      # are not real dependencies: std's panic messages carry the toolchain's source
      # paths, and vendored crate sources appear the same way. Nothing references them at
      # run time, but the scanner cannot know that, so without scrubbing they drag the
      # whole Rust toolchain (~2 GB) and the vendored sources (~276 MB) into every image
      # built from these binaries — measured downstream as a 4.1 GB image that is 1.3 GB
      # once scrubbed.
      #
      # `craneLib.buildPackage` adds these two hooks itself; `mkCargoDerivation` does not,
      # which is exactly why harvesting test binaries this way needed them added by hand.
      # They install themselves into `postInstallHooks`, so they run after the install
      # phase below without it having to call anything.
      #
      # Only the toolchain and vendored sources are blanked. Genuine runtime deps — the
      # CUDA/ROCm userspace each binary's RUNPATH points at — are untouched, including
      # libraries reached by `dlopen` (cudarc opens libcublasLt by soname), which is why
      # this cannot be replaced with a blanket reference scrub.
      nativeBuildInputs =
        (commonArgs.nativeBuildInputs or [])
        ++ [
          craneLib.removeReferencesToRustToolchainHook
          craneLib.removeReferencesToVendoredSourcesHook
        ];

      # Compile the test harnesses (no execution — the GPU is on the rented node)
      # and record the compiler's JSON so we can find the executables.
      buildPhaseCargoCommand = ''
        cargo test --release --no-run --message-format json ${pkgFlags} \
          | tee "$TMPDIR/cargo-test.json" > /dev/null
      '';

      # Harvest every test executable (integration + unit-test harnesses), strip
      # cargo's metadata-hash suffix, and emit a runner that forwards args.
      installPhaseCommand = ''
        mkdir -p "$out/bin"
        names=""
        for exe in $(${pkgs.jq}/bin/jq -r \
          'select(.reason=="compiler-artifact" and .profile.test==true and .executable!=null) | .executable' \
          "$TMPDIR/cargo-test.json"); do
          base="$(basename "$exe" | sed 's/-[0-9a-f]\{16,\}$//')"
          install -Dm755 "$exe" "$out/bin/$base"
          names="$names $base"
        done
        if [ -z "$names" ]; then
          echo "mkCargoTestBinaries: no test executables harvested for ${pname}" >&2
          exit 1
        fi
        {
          echo '#!/bin/sh'
          echo '# Run every harvested test binary, forwarding args (e.g. --ignored).'
          echo 'set -u; rc=0'
          for n in $names; do
            printf 'echo "== %s =="; /bin/%s "$@" || rc=$?\n' "$n" "$n"
          done
          echo 'exit "$rc"'
        } > "$out/bin/run-all-tests"
        chmod +x "$out/bin/run-all-tests"
      '';
    })
