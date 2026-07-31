# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
{inputs, ...}: let
  # The one target images ship. Only aarch64-darwin builds cross-compile to it; every
  # other system builds sf-agent natively.
  linuxTarget = "x86_64-unknown-linux-gnu";
in {
  perSystem = {
    system,
    lib,
    ...
  }: let
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

    # The same agent, cross-compiled from this Mac to x86_64-linux.
    #
    # Images are linux/amd64 and sf-agent is baked into every one of them, so it is the
    # single piece of *compiled* content standing between a Mac and a complete image (see
    # docs/macos.md — everything else substitutes or assembles natively). Building it here
    # rather than on a Linux builder makes the derivation's `system` aarch64-darwin, which
    # is what lets `nix build .#packages.aarch64-darwin.<name>-image` finish unaided.
    #
    # `rust-toolchain.toml` is deliberately NOT edited to add the target: that file feeds
    # every system's toolchain, so adding a target there would change the x86_64-linux
    # derivation hash and cost a binary-cache hit on the Linux path. Overriding here keeps
    # the change contained to this derivation.
    sf-agent-linux = let
      # The C toolchain: aws-lc-sys (reqwest → rustls) compiles C and GAS assembly, so a
      # linker alone is not enough. Both this and the target glibc come from the binary
      # cache — nothing of the cross toolchain is compiled locally.
      crossPkgs = pkgs.pkgsCross.gnu64;
      # The real x86_64-linux package set the *image* ships. Only paths are read from it,
      # never executed, which is exactly why a Mac can do this at all.
      linuxPkgs = inputs.nixpkgs.legacyPackages.x86_64-linux;
      craneCross =
        (inputs.crane.mkLib pkgs).overrideToolchain
        (rustToolchain.override {targets = [linuxTarget];});
      crossArgs =
        commonArgs
        // {
          CARGO_BUILD_TARGET = linuxTarget;
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "${crossPkgs.stdenv.cc.targetPrefix}cc";
          # cc-rs reads this to find the target compiler for aws-lc-sys' C sources.
          CC_x86_64_unknown_linux_gnu = "${crossPkgs.stdenv.cc.targetPrefix}cc";
          # ...and this for anything it must compile for the build host instead.
          HOST_CC = "${pkgs.stdenv.cc.nativePrefix}cc";
          depsBuildBuild = [crossPkgs.stdenv.cc];
        };
    in
      craneCross.buildPackage (crossArgs
        // {
          cargoArtifacts = craneCross.buildDepsOnly crossArgs;
          cargoExtraArgs = "-p saladfingers-agent";
          pname = "sf-agent";
          doCheck = false;
          meta.mainProgram = "sf-agent";

          # The output is a foreign ELF, so darwin's fixups must not touch it: `strip` is
          # Mach-O-only (it silently no-ops), nixpkgs' patchELF hook is Linux-gated so it
          # is absent entirely, and crane's darwin re-signing pass would be run against a
          # file `codesign` cannot handle. `doNotSign` is crane's own escape hatch.
          dontStrip = true;
          dontPatchELF = true;
          doNotSign = true;

          nativeBuildInputs = [pkgs.patchelf];
          # Two fixes in one patchelf, both load-bearing:
          #
          # RUNPATH — a native build gets one from nixpkgs' ld-wrapper; this cross link
          # does not, and without it libgcc_s.so.1 does not resolve, so the agent cannot
          # start inside the container at all.
          #
          # Interpreter — the cross toolchain's glibc is a DIFFERENT store path from the
          # x86_64-linux glibc the image already carries for its /lib64 loader symlink.
          # Left alone it would drag a second, redundant glibc into every image closure.
          #
          # Pointing both at the native paths makes this binary's ELF headers byte-for-byte
          # what a Linux-built agent has (verified: same interpreter, same RUNPATH, same
          # 57 MB closure, no reference to the cross glibc).
          postInstall = ''
            patchelf \
              --set-interpreter ${linuxPkgs.glibc}/lib/ld-linux-x86-64.so.2 \
              --set-rpath ${linuxPkgs.glibc}/lib:${linuxPkgs.stdenv.cc.cc.lib}/lib \
              "$out/bin/sf-agent"
          '';
        });
  in {
    _module.args.pkgs = pkgs;
    _module.args.craneLib = craneLib;
    _module.args.commonArgs = commonArgs;
    _module.args.cargoArtifacts = cargoArtifacts;

    # Gated to aarch64-darwin: it is the only system that both needs a cross-compiled
    # agent and has the cached cross toolchain to produce one. Nothing about any other
    # system's evaluation or derivation hashes changes.
    packages =
      {
        default = saladfingers;
        inherit saladfingers sf-agent;
      }
      // lib.optionalAttrs (system == "aarch64-darwin") {inherit sf-agent-linux;};
  };
}
