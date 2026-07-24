# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
# saladfingers' own images, declared through the flakeModule this flake exports
# (flake.nix imports it alongside ./nix), so `nix build .#gpu-probe-image` in CI walks
# exactly the path a consumer walks: `saladfingers.images.<name>` → `packages.<name>-image`.
# Images are linux/amd64 only, so everything here is gated to x86_64-linux. The gate uses
# `inputs`-derived lib (not `pkgs.lib`) so it does not depend on `config` during option
# resolution.
{inputs, ...}: {
  perSystem = {
    system,
    pkgs,
    ...
  }:
    inputs.nixpkgs.lib.optionalAttrs (system == "x86_64-linux") (let
      hipMatmul = import ./hip-matmul.nix {
        inherit pkgs;
        inherit (pkgs) rocmPackages;
      };

      # SaladCloud's AMD/WSL2 host injects a ROCm into /opt/rocm-host/lib and puts it on
      # LD_LIBRARY_PATH. Every ROCm binary we ship (hipcc output included) carries
      # DT_RUNPATH — which LOSES to LD_LIBRARY_PATH — so any soname the host dir also
      # ships (libamdhip64, libamd_comgr, libhsa-runtime64) shadows ours, and the host's
      # copy then dies on ITS deps (libelf/libnuma, absent from both the host dir and our
      # minimal image) with exit 127. Whether that bites is PER-HOST: a self-contained
      # host dir loads fine (the Jul-22 hip-matmul pass), a leaner one 127s (caught live
      # on A2 of the release validation round). The one robust pattern (A1-proven for
      # rocminfo): wrap each binary to PREPEND our dirs for exactly those shadowable
      # sonames, so our closure (whose nix RUNPATHs resolve libelf & co.) wins; --prefix
      # (never --set) keeps the host's append so the librocdxg dispatch backend — which
      # only the host has — still loads.
      rocmPreferredLibPath =
        pkgs.lib.makeLibraryPath (with pkgs.rocmPackages; [clr rocm-comgr rocm-runtime]);

      rocmTools = pkgs.runCommand "rocm-tools-wrapped" {nativeBuildInputs = [pkgs.makeBinaryWrapper];} ''
        mkdir -p $out/bin
        for src in ${pkgs.rocmPackages.rocminfo}/bin/* ${pkgs.rocmPackages.rocm-smi}/bin/*; do
          [ -e "$src" ] || continue
          makeWrapper "$src" "$out/bin/$(basename "$src")" \
            --prefix LD_LIBRARY_PATH : "${rocmPreferredLibPath}"
        done
      '';

      # Same wrapper for the HIP smoke test: its RUNPATH does cover clr, but the host's
      # libamdhip64 preempts it via LD_LIBRARY_PATH on hosts that ship one (see above).
      hipMatmulWrapped = pkgs.runCommand "hip-matmul-wrapped" {nativeBuildInputs = [pkgs.makeBinaryWrapper];} ''
        mkdir -p $out/bin
        makeWrapper ${hipMatmul}/bin/hip-matmul $out/bin/hip-matmul \
          --prefix LD_LIBRARY_PATH : "${rocmPreferredLibPath}"
      '';
    in {
      saladfingers.images = {
        # The M3 probe image: base image only (no CUDA layer). Its job is to
        # empirically pin down Salad's driver-injection layout for pennies.
        gpu-probe = {
          gpu = "none";
          cmd = ["probe" "--emit" "http"];
          ports = [8000];
          env.SF_PROBE_BANDWIDTH_MB = "32";
        };

        # E13: the same probe with the ROCm userspace baked in (rocm-runtime flavor)
        # so it can run `rocminfo`/`rocm-smi` on an AMD node. The binaries are linked
        # onto PATH (extraContents) since the flavor only puts the libs on
        # LD_LIBRARY_PATH. Cached in the binary cache (no from-source ROCm/LLVM build).
        gpu-probe-rocm = {
          gpu = "rocm-runtime";
          inherit (pkgs) rocmPackages;
          cmd = ["probe" "--emit" "http"];
          ports = [8000];
          env.SF_PROBE_BANDWIDTH_MB = "8";
          extraContents = [rocmTools];
        };

        # A standalone HIP matmul smoke test on the rocm-runtime base — proves the AMD
        # GPU actually executes a compute kernel (not just enumerates). Run it with
        # `saladfingers run --image <this> --gpu-class <AMD> -- /bin/hip-matmul`.
        hip-matmul = {
          gpu = "rocm-runtime";
          inherit (pkgs) rocmPackages;
          cmd = ["run"];
          extraContents = [
            (pkgs.buildEnv {
              name = "hip-matmul-bin";
              paths = [hipMatmulWrapped];
              pathsToLink = ["/bin"];
            })
          ];
        };
      };
    });
}
