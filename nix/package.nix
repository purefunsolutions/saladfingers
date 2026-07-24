# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
{inputs, ...}: {
  perSystem = {system, ...}: let
    pkgs = import inputs.nixpkgs {
      inherit system;
      overlays = [inputs.rust-overlay.overlays.default];
    };

    rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ../rust-toolchain.toml;
    craneLib = (inputs.crane.mkLib pkgs).overrideToolchain rustToolchain;

    src = craneLib.cleanCargoSource ../.;

    commonArgs = {
      inherit src;
      pname = "saladfingers";
      version = "0.1.0";
      strictDeps = true;
      # reqwest 0.13's rustls uses the aws-lc-rs crypto provider. aws-lc-sys
      # ships pregenerated bindings for every system in this flake and builds
      # its C + GAS-asm sources with plain cc; its cmake fallback is only for
      # FIPS/sanitizer/exotic targets. So the build env still needs no
      # cmake/openssl/pkg-config. If a future aws-lc-sys bump changes builder
      # selection it fails loudly ("Missing dependency: cmake") — add
      # pkgs.cmake to nativeBuildInputs (plus dontUseCmakeConfigure) then.
    };

    cargoArtifacts = craneLib.buildDepsOnly commonArgs;

    saladfingers = craneLib.buildPackage (commonArgs
      // {
        inherit cargoArtifacts;
        cargoExtraArgs = "--workspace";
        doCheck = false;
        meta.mainProgram = "saladfingers";
      });

    # sf-agent on its own, baked into every image. A normal dynamic build: its
    # runtime deps (glibc, etc.) ride along in the nix2container image closure.
    sf-agent = craneLib.buildPackage (commonArgs
      // {
        inherit cargoArtifacts;
        cargoExtraArgs = "-p saladfingers-agent";
        pname = "sf-agent";
        doCheck = false;
        meta.mainProgram = "sf-agent";
      });
  in {
    _module.args.pkgs = pkgs;
    _module.args.craneLib = craneLib;
    _module.args.commonArgs = commonArgs;
    _module.args.cargoArtifacts = cargoArtifacts;

    packages = {
      default = saladfingers;
      inherit saladfingers sf-agent;
    };
  };
}
