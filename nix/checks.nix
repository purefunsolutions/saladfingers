# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
{inputs, ...}: {
  perSystem = {
    pkgs,
    craneLib,
    commonArgs,
    cargoArtifacts,
    ...
  }: {
    checks = {
      # Enforce REUSE (SPDX) license compliance over the whole tree. Runs as a plain
      # `nix flake check` derivation (no git-hooks/pre-commit). `inputs.self` is the
      # git-tracked flake source, so target/ and result* are already excluded.
      saladfingers-reuse = pkgs.runCommand "saladfingers-reuse" {nativeBuildInputs = [pkgs.reuse];} ''
        reuse --root ${inputs.self} lint
        touch $out
      '';

      saladfingers-clippy = craneLib.cargoClippy (commonArgs
        // {
          inherit cargoArtifacts;
          cargoClippyExtraArgs = "--workspace --all-targets -- --deny warnings";
        });

      saladfingers-nextest = craneLib.cargoNextest (commonArgs
        // {
          inherit cargoArtifacts;
          partitions = 1;
          partitionType = "count";
          cargoNextestExtraArgs = "--workspace --no-tests=warn";
          # reqwest 0.13 trusts the platform CA store (rustls-platform-verifier),
          # so Client construction fails hard in the CA-less sandbox — even in
          # tests that only ever speak plain HTTP to a local wiremock. The
          # production images bake this same bundle (see image-lib.nix).
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
          # Boot ephemeral local Garage in the `presign_round_trip` test so the
          # previously-#[ignore]'d S3 presign round-trip actually runs in CI. The
          # binary is on PATH via nativeBuildInputs and pinned through this env
          # var. Linux-only: garage isn't guaranteed to build on the flake's darwin
          # systems and darwin CI is disabled; the test self-skips elsewhere.
          nativeBuildInputs =
            (commonArgs.nativeBuildInputs or [])
            ++ (pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [pkgs.garage]);
          SALADFINGERS_GARAGE_BIN =
            pkgs.lib.optionalString
            pkgs.stdenv.hostPlatform.isLinux
            "${pkgs.garage}/bin/garage";
        });

      saladfingers-doc = craneLib.cargoDoc (commonArgs
        // {
          inherit cargoArtifacts;
          cargoDocExtraArgs = "--workspace";
        });

      # `flake.lib.mkCargoTestBinaries` is consumer API (see flake.nix), and nothing in
      # this repo built it — which is how a missing-hooks bug reached a consumer's image
      # rather than CI. `nix/images.nix` exists so CI walks the consumer path for the
      # other half of that API; this does the same for the harvester, and pins the
      # property that was broken.
      saladfingers-test-binaries = let
        testBins = import ./lib.nix {
          inherit pkgs craneLib commonArgs cargoArtifacts;
          testPackages = ["saladfingers-protocol"];
          pname = "saladfingers-test-binaries-check";
        };
        closure = pkgs.closureInfo {rootPaths = [testBins];};
        # Measured on this workspace: 7 paths scrubbed, 323 unscrubbed. The cap leaves
        # generous room for real runtime deps while staying an order of magnitude below
        # a build-only leak. It bounds saladfingers' own instantiation only — a
        # consumer's CUDA userspace is legitimately far heavier.
        maxPaths = 40;
      in
        pkgs.runCommand "saladfingers-test-binaries" {} ''
          # The harvest must work at all: the runner, and the harness it was asked for.
          test -x ${testBins}/bin/run-all-tests
          grep -q saladfingers_protocol ${testBins}/bin/run-all-tests

          # The scrub must blank ONLY what nothing loads. A closure assertion alone
          # cannot see the difference between "reference removed" and "dependency
          # broken", so execute each harvested harness: `--list` exercises the ELF
          # interpreter, glibc and every DT_NEEDED library without running a test.
          # (run-all-tests is skipped — it calls /bin/<name>, the in-image layout.)
          for exe in ${testBins}/bin/*; do
            [ "$(basename "$exe")" = run-all-tests ] && continue
            "$exe" --list > /dev/null
          done

          # The named leak, so a failure says what leaked. Vendored sources are the half
          # this workspace exercises; the toolchain pattern guards the other.
          if grep -E 'vendor-cargo-deps|vendor-registry|cargo-package-|rust-default-' \
              ${closure}/store-paths; then
            echo "^ build-only closure leaked into mkCargoTestBinaries' output." >&2
            echo "  craneLib.removeReferencesTo* in nix/lib.nix is what keeps these" >&2
            echo "  out; mkCargoDerivation does not install those hooks itself." >&2
            exit 1
          fi

          # Naming-independent backstop: the patterns above are upstream conventions
          # that can be renamed, and a pattern which silently stops matching passes.
          n=$(wc -l < ${closure}/store-paths)
          if [ "$n" -gt ${toString maxPaths} ]; then
            echo "closure is $n store paths, expected at most ${toString maxPaths} —" >&2
            echo "something build-only is being dragged in." >&2
            exit 1
          fi

          touch $out
        '';
    };
  };
}
