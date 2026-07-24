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
        });

      saladfingers-doc = craneLib.cargoDoc (commonArgs
        // {
          inherit cargoArtifacts;
          cargoDocExtraArgs = "--workspace";
        });
    };
  };
}
