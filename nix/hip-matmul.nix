# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
# Ahead-of-time HIP compile of the standalone matmul smoke test
# (examples/hip-matmul/matmul.cpp), for every SaladCloud AMD class's GPU arch. No GPU is
# needed to build — hipcc cross-compiles and embeds the code objects.
#
# hipcc bakes a DT_RUNPATH (not RPATH!) covering clr's libamdhip64 and its whole
# comgr/LLVM/libz/... closure — but RUNPATH loses to LD_LIBRARY_PATH, and SaladCloud's
# WSL2 AMD hosts put their own /opt/rocm-host/lib (with its own libamdhip64) there at
# run time, so on some hosts the HOST's HIP stack preempts this closure and then dies
# on libelf/libnuma (exit 127; caught live 2026-07-23). images.nix therefore wraps the
# installed binary with `--prefix LD_LIBRARY_PATH` over our clr/comgr/rocm-runtime lib
# dirs. `--prefix` (never `--set`, which broke a run once by REPLACING the variable)
# keeps the host's append, so librocdxg.so — the /dev/dxg dispatch backend only the
# host has — still loads.
{
  pkgs,
  rocmPackages,
}:
pkgs.stdenv.mkDerivation {
  pname = "hip-matmul";
  version = "0.1.0";
  src = ../examples/hip-matmul;

  nativeBuildInputs = [rocmPackages.clr];
  dontConfigure = true;

  buildPhase = ''
    runHook preBuild
    hipcc -O3 \
      --rocm-device-lib-path=${rocmPackages.rocm-device-libs}/amdgcn/bitcode \
      --offload-arch=gfx1100 \
      --offload-arch=gfx1101 \
      --offload-arch=gfx1200 \
      --offload-arch=gfx1201 \
      matmul.cpp -o hip-matmul
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    install -Dm755 hip-matmul $out/bin/hip-matmul
    runHook postInstall
  '';

  meta.mainProgram = "hip-matmul";
}
