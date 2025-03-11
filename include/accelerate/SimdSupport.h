//
// Created by 杜建璋 on 2025/3/11.
//

#ifndef SIMDSUPPORT_H
#define SIMDSUPPORT_H
#include <iostream>
#include <vector>
#include <cstdint>

#ifdef __AVX512F__
    #include <immintrin.h>
    #define SIMD_AVX512
#elif defined(__AVX2__)
    #include <immintrin.h>
    #define SIMD_AVX2
#elif defined(__SSE2__)
    #include <emmintrin.h>
    #define SIMD_SSE2
#elif defined(__ARM_NEON)
    #include <arm_neon.h>
    #define SIMD_NEON
#else
    #define NO_SIMD
#endif

class SimdSupport {

};



#endif //SIMDSUPPORT_H
