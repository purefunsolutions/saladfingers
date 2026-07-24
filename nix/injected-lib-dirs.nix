# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
# The runtime directories where SaladCloud's host injects GPU *driver* libraries
# (nvidia-container-toolkit / CDI) into a running container. These are NOT nix store
# paths and never exist at build time — the driver appears only at run time on a GPU
# node. Legacy injection lands libs in /usr/lib64 (images without /etc/debian_version,
# like ours) or /usr/lib/x86_64-linux-gnu (with it); CDI mirrors host paths;
# /usr/lib/wsl/lib covers WSL2 nodes (SaladCloud's AMD fleet); /opt/rocm/lib is the
# conventional ROCm prefix; /opt/rocm-host/lib + /opt/amdgpu/lib/x86_64-linux-gnu are
# where SaladCloud's AMD/WSL2 host injects the ROCm-on-WSL dispatch backend
# (librocdxg.so, the /dev/dxg thunk) — HIP compute needs it and it lives ONLY here.
#
# Only host-INJECTED libs (libcuda.so.1, libnvidia-ml.so.1, librocdxg.so) ever need these
# on LD_LIBRARY_PATH — nix's glibc ignores /etc/ld.so.cache, so there is no other way for
# a binary to find them. The image's *baked* GPU userspace (cudart, the ROCm libs and
# their comgr/LLVM/libz/... closure) must NOT go here: those resolve via the nix RPATHs
# the compiler already baked in, and forcing them onto LD_LIBRARY_PATH breaks the
# loader's RPATH walk for deep transitive deps.
#
# NOTE: a binary must INHERIT these (the host also appends /opt/rocm-host/lib at run
# time) — a makeBinaryWrapper that `--set`s LD_LIBRARY_PATH would discard the host's
# append and lose librocdxg. Inherit + rely on RPATH for baked libs instead.
[
  "/usr/lib/x86_64-linux-gnu"
  "/usr/lib64"
  "/usr/local/nvidia/lib"
  "/usr/local/nvidia/lib64"
  "/usr/lib/wsl/lib"
  "/opt/rocm/lib"
  "/opt/rocm-host/lib"
  "/opt/amdgpu/lib/x86_64-linux-gnu"
]
