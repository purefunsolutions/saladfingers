// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

// Self-contained HIP matrix-multiply smoke test for SaladCloud AMD nodes.
// Enumerates the device, runs a 512x512 float matmul on the GPU, verifies against a
// CPU reference, reports GFLOPS, and exits 0 (PASS) / 1 (FAIL) / 2 (HIP error).
#include <hip/hip_runtime.h>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <vector>

#define CHECK(x)                                                                      \
  do {                                                                                \
    hipError_t _e = (x);                                                              \
    if (_e != hipSuccess) {                                                           \
      printf("HIP error: %s at %s:%d\n", hipGetErrorString(_e), __FILE__, __LINE__);  \
      return 2;                                                                       \
    }                                                                                 \
  } while (0)

__global__ void matmul(const float *A, const float *B, float *C, int N) {
  int r = blockIdx.y * blockDim.y + threadIdx.y;
  int c = blockIdx.x * blockDim.x + threadIdx.x;
  if (r < N && c < N) {
    float acc = 0.0f;
    for (int k = 0; k < N; k++) acc += A[r * N + k] * B[k * N + c];
    C[r * N + c] = acc;
  }
}

int main() {
  hipDeviceProp_t p;
  CHECK(hipGetDeviceProperties(&p, 0));
  printf("HIP device: %s (%s), %d CUs, %zu MB VRAM, warp %d\n", p.name, p.gcnArchName,
         p.multiProcessorCount, (size_t)(p.totalGlobalMem >> 20), p.warpSize);

  const int N = 512;
  const size_t bytes = (size_t)N * N * sizeof(float);
  std::vector<float> hA(N * N), hB(N * N), hC(N * N), ref(N * N);
  srand(1234);
  for (int i = 0; i < N * N; i++) {
    hA[i] = (float)rand() / (float)RAND_MAX;
    hB[i] = (float)rand() / (float)RAND_MAX;
  }

  float *dA, *dB, *dC;
  CHECK(hipMalloc(&dA, bytes));
  CHECK(hipMalloc(&dB, bytes));
  CHECK(hipMalloc(&dC, bytes));
  CHECK(hipMemcpy(dA, hA.data(), bytes, hipMemcpyHostToDevice));
  CHECK(hipMemcpy(dB, hB.data(), bytes, hipMemcpyHostToDevice));

  dim3 block(16, 16), grid((N + 15) / 16, (N + 15) / 16);
  matmul<<<grid, block>>>(dA, dB, dC, N); // warmup
  CHECK(hipGetLastError());
  CHECK(hipDeviceSynchronize());

  hipEvent_t t0, t1;
  CHECK(hipEventCreate(&t0));
  CHECK(hipEventCreate(&t1));
  const int iters = 20;
  CHECK(hipEventRecord(t0));
  for (int it = 0; it < iters; it++) matmul<<<grid, block>>>(dA, dB, dC, N);
  CHECK(hipEventRecord(t1));
  CHECK(hipEventSynchronize(t1));
  float ms = 0;
  CHECK(hipEventElapsedTime(&ms, t0, t1));
  CHECK(hipMemcpy(hC.data(), dC, bytes, hipMemcpyDeviceToHost));

  for (int r = 0; r < N; r++)
    for (int c = 0; c < N; c++) {
      float a = 0;
      for (int k = 0; k < N; k++) a += hA[r * N + k] * hB[k * N + c];
      ref[r * N + c] = a;
    }

  double maxrel = 0;
  for (int i = 0; i < N * N; i++) {
    double d = fabs((double)hC[i] - (double)ref[i]);
    maxrel = fmax(maxrel, d / (fabs((double)ref[i]) + 1e-6));
  }
  double gflops = (2.0 * N * N * N * iters) / ((double)ms / 1e3) / 1e9;
  bool pass = maxrel < 1e-2;
  printf("matmul %dx%d: %.1f GFLOPS, max rel err %.2e -> %s\n", N, N, gflops, maxrel,
         pass ? "PASS" : "FAIL");
  hipFree(dA);
  hipFree(dB);
  hipFree(dC);
  return pass ? 0 : 1;
}
