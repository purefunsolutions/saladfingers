# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
{inputs, ...}: {
  imports = [
    inputs.treefmt-nix.flakeModule
  ];

  perSystem = {
    treefmt.config = {
      projectRootFile = "flake.nix";

      settings.global.excludes = [
        "Cargo.lock"
        "flake.lock"
        "saladfingers-images.lock"
        "docs/reference/**"
        "LICENSES/**"
        "target/**"
        "result"
        "result-*"
      ];

      programs = {
        alejandra.enable = true;
        deadnix.enable = true;
        statix.enable = true;
        rustfmt.enable = true;
        shellcheck.enable = true;
        taplo.enable = true;
        yamlfmt.enable = true;
        mdformat.enable = true;
      };

      # The release scripts are writeShellApplication bodies: no shebang, and written
      # for the `set -o errexit` it prepends. writeShellApplication shellchecks them in
      # that context on every build (nix/release.nix), and they pass. Bare, without
      # it, shellcheck asks for a shell (SC2148) and for `cd … || exit` (SC2164); the
      # files are furymodel's and stay identical to it, so they skip this pass instead.
      settings.formatter.shellcheck.excludes = [
        "nix/release.sh"
        "nix/publish-releases.sh"
      ];
    };
  };
}
