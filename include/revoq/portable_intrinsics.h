// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) ||             \
    defined(_M_IX86)

#include <immintrin.h>

#define PORTABLE_PAUSE() _mm_pause()

#define PORTABLE_LFENCE() _mm_lfence()
#define PORTABLE_SFENCE() _mm_sfence()
#define PORTABLE_MFENCE() _mm_mfence()

#define PORTABLE_PREFETCH(addr) _mm_prefetch((const char *)(addr), _MM_HINT_T0)

#elif defined(__aarch64__) || defined(__arm64__) || defined(_M_ARM64)

static inline void PORTABLE_PAUSE() {
  __asm__ __volatile__("yield" ::: "memory");
}

static inline void PORTABLE_LFENCE() {
  __asm__ __volatile__("dmb ishld" ::: "memory");
}

static inline void PORTABLE_SFENCE() {
  __asm__ __volatile__("dmb ishst" ::: "memory");
}

static inline void PORTABLE_MFENCE() {
  __asm__ __volatile__("dmb ish" ::: "memory");
}

#define PORTABLE_PREFETCH(addr) __builtin_prefetch((addr), 0, 3)

#else

#warning "Unknown architecture - using compiler barriers"

#define PORTABLE_PAUSE()                                                       \
  do {                                                                         \
  } while (0)
#define PORTABLE_LFENCE() __asm__ __volatile__("" ::: "memory")
#define PORTABLE_SFENCE() __asm__ __volatile__("" ::: "memory")
#define PORTABLE_MFENCE() __asm__ __volatile__("" ::: "memory")
#define PORTABLE_PREFETCH(addr) __builtin_prefetch((addr), 0, 3)

#endif
