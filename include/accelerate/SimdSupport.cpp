//
// Created by 杜建璋 on 2025/3/11.
//

#include "SimdSupport.h"

#include "comm/Comm.h"

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

std::vector<int64_t> SimdSupport::xorV(const std::vector<int64_t> &arr0,
                                       const std::vector<int64_t> &arr1) {
    int num = static_cast<int>(arr0.size());
    std::vector<int64_t> out(num);

#ifdef SIMD_AVX512
    int i = 0;
    for (; i + 8 <= num; i += 8) {
        __m512i vec1 = _mm512_loadu_si512(&arr0[i]);
        __m512i vec2 = _mm512_loadu_si512(&arr1[i]);
        __m512i result = _mm512_xor_si512(vec1, vec2);
        _mm512_storeu_si512(&out[i], result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] ^ arr1[i];
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    for (; i + 4 <= num; i += 4) {
        __m256i vec1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr0[i]));
        __m256i vec2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr1[i]));
        __m256i result = _mm256_xor_si256(vec1, vec2);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[i]), result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] ^ arr1[i];
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        __m128i vec1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr0[i]));
        __m128i vec2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr1[i]));
        __m128i result = _mm_xor_si128(vec1, vec2);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[i]), result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] ^ arr1[i];
    }
#elif defined(SIMD_NEON)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        int64x2_t vec1 = vld1q_s64(&arr0[i]);
        int64x2_t vec2 = vld1q_s64(&arr1[i]);
        int64x2_t result = veorq_s64(vec1, vec2);
        vst1q_s64(&out[i], result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] ^ arr1[i];
    }
#else
    for (int i = 0; i < num; i++) {
        out[i] = arr0[i] ^ arr1[i];
    }
#endif

    return out;
}

std::vector<int64_t> SimdSupport::andV(const std::vector<int64_t> &arr0,
                                       const std::vector<int64_t> &arr1) {
    int num = static_cast<int>(arr0.size());
    std::vector<int64_t> out(num);

#ifdef SIMD_AVX512
    int i = 0;
    for (; i + 8 <= num; i += 8) {
        __m512i vec1 = _mm512_loadu_si512(&arr0[i]);
        __m512i vec2 = _mm512_loadu_si512(&arr1[i]);
        __m512i result = _mm512_and_si512(vec1, vec2);
        _mm512_storeu_si512(&out[i], result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] & arr1[i];
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    for (; i + 4 <= num; i += 4) {
        __m256i vec1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr0[i]));
        __m256i vec2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr1[i]));
        __m256i result = _mm256_and_si256(vec1, vec2);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[i]), result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] & arr1[i];
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        __m128i vec1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr0[i]));
        __m128i vec2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr1[i]));
        __m128i result = _mm_and_si128(vec1, vec2);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[i]), result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] & arr1[i];
    }
#elif defined(SIMD_NEON)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        int64x2_t vec1 = vld1q_s64(&arr0[i]);
        int64x2_t vec2 = vld1q_s64(&arr1[i]);
        int64x2_t result = vandq_s64(vec1, vec2);
        vst1q_s64(&out[i], result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] & arr1[i];
    }
#else
    for (int i = 0; i < num; i++) {
        out[i] = arr0[i] & arr1[i];
    }
#endif

    return out;
}

std::vector<int64_t> SimdSupport::andVC(const std::vector<int64_t> &arr, int64_t constant) {
    int num = static_cast<int>(arr.size());
    std::vector<int64_t> output(num);

#ifdef SIMD_AVX512
    int i = 0;
    __m512i const_vec = _mm512_set1_epi64(constant);
    for (; i + 8 <= num; i += 8) {
        __m512i vec = _mm512_loadu_si512(&arr[i]);
        __m512i result = _mm512_and_si512(vec, const_vec);
        _mm512_storeu_si512(&output[i], result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] & constant;
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    __m256i const_vec = _mm256_set1_epi64x(constant);
    for (; i + 4 <= num; i += 4) {
        __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr[i]));
        __m256i result = _mm256_and_si256(vec, const_vec);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&output[i]), result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] & constant;
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    __m128i const_vec = _mm_set1_epi64x(constant);
    for (; i + 2 <= num; i += 2) {
        __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr[i]));
        __m128i result = _mm_and_si128(vec, const_vec);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&output[i]), result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] & constant;
    }
#elif defined(SIMD_NEON)
    int i = 0;
    int64x2_t const_vec = vdupq_n_s64(constant);
    for (; i + 2 <= num; i += 2) {
        int64x2_t vec = vld1q_s64(&arr[i]);
        int64x2_t result = vandq_s64(vec, const_vec);
        vst1q_s64(&output[i], result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] & constant;
    }
#else
    for (int i = 0; i < num; i++) {
        output[i] = arr[i] & constant;
    }
#endif

    return output;
}

std::vector<int64_t> SimdSupport::orV(const std::vector<int64_t> &arr0,
                                      const std::vector<int64_t> &arr1) {
    int num = static_cast<int>(arr0.size());
    std::vector<int64_t> out(num);

#ifdef SIMD_AVX512
    int i = 0;
    for (; i + 8 <= num; i += 8) {
        __m512i vec1 = _mm512_loadu_si512(&arr0[i]);
        __m512i vec2 = _mm512_loadu_si512(&arr1[i]);
        __m512i result = _mm512_or_si512(vec1, vec2);
        _mm512_storeu_si512(&out[i], result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] | arr1[i];
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    for (; i + 4 <= num; i += 4) {
        __m256i vec1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr0[i]));
        __m256i vec2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr1[i]));
        __m256i result = _mm256_or_si256(vec1, vec2);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[i]), result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] | arr1[i];
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        __m128i vec1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr0[i]));
        __m128i vec2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr1[i]));
        __m128i result = _mm_or_si128(vec1, vec2);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[i]), result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] | arr1[i];
    }
#elif defined(SIMD_NEON)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        int64x2_t vec1 = vld1q_s64(&arr0[i]);
        int64x2_t vec2 = vld1q_s64(&arr1[i]);
        int64x2_t result = vorrq_s64(vec1, vec2);
        vst1q_s64(&out[i], result);
    }
    for (; i < num; i++) {
        out[i] = arr0[i] | arr1[i];
    }
#else
    for (int i = 0; i < num; i++) {
        out[i] = arr0[i] | arr1[i];
    }
#endif

    return out;
}

std::vector<int64_t> SimdSupport::xorVC(const std::vector<int64_t> &arr, int64_t constant) {
    int num = static_cast<int>(arr.size());
    std::vector<int64_t> output(num);

#ifdef SIMD_AVX512
    int i = 0;
    __m512i const_vec = _mm512_set1_epi64(constant);
    for (; i + 8 <= num; i += 8) {
        __m512i vec = _mm512_loadu_si512(&input[i]);
        __m512i result = _mm512_xor_si512(vec, const_vec);
        _mm512_storeu_si512(&output[i], result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] ^ constant;
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    __m256i const_vec = _mm256_set1_epi64x(constant);
    for (; i + 4 <= num; i += 4) {
        __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&input[i]));
        __m256i result = _mm256_xor_si256(vec, const_vec);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&output[i]), result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] ^ constant;
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    __m128i const_vec = _mm_set1_epi64x(constant);
    for (; i + 2 <= num; i += 2) {
        __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&input[i]));
        __m128i result = _mm_xor_si128(vec, const_vec);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&output[i]), result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] ^ constant;
    }
#elif defined(SIMD_NEON)
    int i = 0;
    int64x2_t const_vec = vdupq_n_s64(constant);
    for (; i + 2 <= num; i += 2) {
        int64x2_t vec = vld1q_s64(&arr[i]);
        int64x2_t result = veorq_s64(vec, const_vec);
        vst1q_s64(&output[i], result);
    }
    for (; i < num; i++) {
        output[i] = arr[i] ^ constant;
    }
#else
    for (int i = 0; i < num; i++) {
        output[i] = arr[i] ^ constant;
    }
#endif

    return output;
}

std::vector<int64_t> SimdSupport::xor2VC(const std::vector<int64_t> &xis, const std::vector<int64_t> &yis,
                                         int64_t a, int64_t b) {
    int num = static_cast<int>(xis.size());
    std::vector<int64_t> out(num * 2);

#ifdef SIMD_AVX512
    int i = 0;
    __m512i a_vec = _mm512_set1_epi64(a);  // 512-bit broadcast a
    __m512i b_vec = _mm512_set1_epi64(b);  // 512-bit broadcast b
    for (; i + 8 <= num; i += 8) {
        __m512i xis_vec = _mm512_loadu_si512(&xis[i]);
        __m512i yis_vec = _mm512_loadu_si512(&yis[i]);
        __m512i out_x = _mm512_xor_si512(xis_vec, a_vec);
        __m512i out_y = _mm512_xor_si512(yis_vec, b_vec);
        _mm512_storeu_si512(&out[i], out_x);
        _mm512_storeu_si512(&out[num + i], out_y);
    }
    for (; i < num; i++) {
        out[i] = xis[i] ^ a;
        out[num + i] = yis[i] ^ b;
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    __m256i a_vec = _mm256_set1_epi64x(a);  // 256-bit broadcast a
    __m256i b_vec = _mm256_set1_epi64x(b);  // 256-bit broadcast b
    for (; i + 4 <= num; i += 4) {
        __m256i xis_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&xis[i]));
        __m256i yis_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&yis[i]));
        __m256i out_x = _mm256_xor_si256(xis_vec, a_vec);
        __m256i out_y = _mm256_xor_si256(yis_vec, b_vec);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[i]), out_x);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[num + i]), out_y);
    }
    for (; i < num; i++) {
        out[i] = xis[i] ^ a;
        out[num + i] = yis[i] ^ b;
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    __m128i a_vec = _mm_set1_epi64x(a);  // 128-bit broadcast a
    __m128i b_vec = _mm_set1_epi64x(b);  // 128-bit broadcast b
    for (; i + 2 <= num; i += 2) {
        __m128i xis_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&xis[i]));
        __m128i yis_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&yis[i]));
        __m128i out_x = _mm_xor_si128(xis_vec, a_vec);
        __m128i out_y = _mm_xor_si128(yis_vec, b_vec);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[i]), out_x);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[num + i]), out_y);
    }
    for (; i < num; i++) {
        out[i] = xis[i] ^ a;
        out[num + i] = yis[i] ^ b;
    }
#elif defined(SIMD_NEON)
    int i = 0;
    int64x2_t a_vec = vdupq_n_s64(a); // 128-bit broadcast a
    int64x2_t b_vec = vdupq_n_s64(b); // 128-bit broadcast b
    for (; i + 2 <= num; i += 2) {
        int64x2_t xis_vec = vld1q_s64(&xis[i]);
        int64x2_t yis_vec = vld1q_s64(&yis[i]);
        int64x2_t out_x = veorq_s64(xis_vec, a_vec);
        int64x2_t out_y = veorq_s64(yis_vec, b_vec);
        vst1q_s64(&out[i], out_x);
        vst1q_s64(&out[num + i], out_y);
    }
    for (; i < num; i++) {
        out[i] = xis[i] ^ a;
        out[num + i] = yis[i] ^ b;
    }
#else
    for (int i = 0; i < num; i++) {
        out[i] = xis[i] ^ a;
        out[num + i] = yis[i] ^ b;
    }
#endif

    return out;
}

std::vector<int64_t> SimdSupport::xor3(const int64_t *a, const int64_t *b, const int64_t *c, int num) {
    std::vector<int64_t> output(num);

#ifdef SIMD_AVX512
    int i = 0;
    for (; i + 8 <= num; i += 8) {
        __m512i va = _mm512_loadu_si512(a + i);
        __m512i vb = _mm512_loadu_si512(b + i);
        __m512i vc = _mm512_loadu_si512(c + i);
        __m512i res = _mm512_xor_si512(va, vb);
        res = _mm512_xor_si512(res, vc);
        _mm512_storeu_si512(output.data() + i, res);
    }
    for (; i < num; i++) {
        output[i] = a[i] ^ b[i] ^ c[i];
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    for (; i + 4 <= num; i += 4) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        __m256i vc = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(c + i));
        __m256i res = _mm256_xor_si256(va, vb);
        res = _mm256_xor_si256(res, vc);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(output.data() + i), res);
    }
    for (; i < num; i++) {
        output[i] = a[i] ^ b[i] ^ c[i];
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        __m128i va = _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i));
        __m128i vb = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i));
        __m128i vc = _mm_loadu_si128(reinterpret_cast<const __m128i*>(c + i));
        __m128i res = _mm_xor_si128(va, vb);
        res = _mm_xor_si128(res, vc);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(output.data() + i), res);
    }
    for (; i < num; i++) {
        output[i] = a[i] ^ b[i] ^ c[i];
    }
#elif defined(SIMD_NEON)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        int64x2_t va = vld1q_s64(a + i);
        int64x2_t vb = vld1q_s64(b + i);
        int64x2_t vc = vld1q_s64(c + i);
        int64x2_t res = veorq_s64(va, vb);
        res = veorq_s64(res, vc);
        vst1q_s64(output.data() + i, res);
    }
    for (; i < num; i++) {
        output[i] = a[i] ^ b[i] ^ c[i];
    }
#else
    for (int i = 0; i < num; i++) {
        output[i] = a[i] ^ b[i] ^ c[i];
    }
#endif

    return output;
}

std::vector<int64_t> SimdSupport::computeZ(const std::vector<int64_t> &efs, int64_t a, int64_t b,
                                           int64_t c) {
    int num = static_cast<int>(efs.size() / 2);
    std::vector<int64_t> zis(num);
    int64_t extendedRank = Comm::rank() ? -1ll : 0;

#ifdef SIMD_AVX512
    int i = 0;
    __m512i ext_vec = _mm512_set1_epi64(extendedRank);
    __m512i a_vec = _mm512_set1_epi64(a);
    __m512i b_vec = _mm512_set1_epi64(b);
    __m512i c_vec = _mm512_set1_epi64(c);
    for (; i + 8 <= num; i += 8) {
        __m512i e_vec = _mm512_loadu_si512(&efs[i]);
        __m512i f_vec = _mm512_loadu_si512(&efs[num + i]);

        __m512i ef = _mm512_and_si512(ext_vec, _mm512_and_si512(e_vec, f_vec));
        __m512i fa = _mm512_and_si512(f_vec, a_vec);
        __m512i eb = _mm512_and_si512(e_vec, b_vec);
        __m512i result = _mm512_xor_si512(_mm512_xor_si512(ef, fa), _mm512_xor_si512(eb, c_vec));

        _mm512_storeu_si512(&zis[i], result);
    }
    for (; i < num; i++) {
        zis[i] = (extendedRank & efs[i] & efs[num + i]) ^ (efs[num + i] & a) ^ (efs[i] & b) ^ c;
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    __m256i ext_vec = _mm256_set1_epi64x(extendedRank);
    __m256i a_vec = _mm256_set1_epi64x(a);
    __m256i b_vec = _mm256_set1_epi64x(b);
    __m256i c_vec = _mm256_set1_epi64x(c);
    for (; i + 4 <= num; i += 4) {
        __m256i e_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&efs[i]));
        __m256i f_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&efs[num + i]));

        __m256i ef = _mm256_and_si256(ext_vec, _mm256_and_si256(e_vec, f_vec));
        __m256i fa = _mm256_and_si256(f_vec, a_vec);
        __m256i eb = _mm256_and_si256(e_vec, b_vec);
        __m256i result = _mm256_xor_si256(_mm256_xor_si256(ef, fa), _mm256_xor_si256(eb, c_vec));

        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&zis[i]), result);
    }
    for (; i < num; i++) {
        zis[i] = (extendedRank & efs[i] & efs[num + i]) ^ (efs[num + i] & a) ^ (efs[i] & b) ^ c;
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    __m128i ext_vec = _mm_set1_epi64x(extendedRank);
    __m128i a_vec = _mm_set1_epi64x(a);
    __m128i b_vec = _mm_set1_epi64x(b);
    __m128i c_vec = _mm_set1_epi64x(c);
    for (; i + 2 <= num; i += 2) {
        __m128i e_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&efs[i]));
        __m128i f_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&efs[num + i]));

        __m128i ef = _mm_and_si128(ext_vec, _mm_and_si128(e_vec, f_vec));
        __m128i fa = _mm_and_si128(f_vec, a_vec);
        __m128i eb = _mm_and_si128(e_vec, b_vec);
        __m128i result = _mm_xor_si128(_mm_xor_si128(ef, fa), _mm_xor_si128(eb, c_vec));

        _mm_storeu_si128(reinterpret_cast<__m128i*>(&zis[i]), result);
    }
    for (; i < num; i++) {
        zis[i] = (extendedRank & efs[i] & efs[num + i]) ^ (efs[num + i] & a) ^ (efs[i] & b) ^ c;
    }
#elif defined(SIMD_NEON)
    int i = 0;
    int64x2_t ext_vec = vdupq_n_s64(extendedRank);
    int64x2_t a_vec = vdupq_n_s64(a);
    int64x2_t b_vec = vdupq_n_s64(b);
    int64x2_t c_vec = vdupq_n_s64(c);
    for (; i + 2 <= num; i += 2) {
        int64x2_t e_vec = vld1q_s64(&efs[i]);
        int64x2_t f_vec = vld1q_s64(&efs[num + i]);

        int64x2_t ef = vandq_s64(ext_vec, vandq_s64(e_vec, f_vec));
        int64x2_t fa = vandq_s64(f_vec, a_vec);
        int64x2_t eb = vandq_s64(e_vec, b_vec);
        int64x2_t result = veorq_s64(veorq_s64(ef, fa), veorq_s64(eb, c_vec));

        vst1q_s64(&zis[i], result);
    }
    for (; i < num; i++) {
        zis[i] = (extendedRank & efs[i] & efs[num + i]) ^ (efs[num + i] & a) ^ (efs[i] & b) ^ c;
    }
#else
    for (int i = 0; i < num; i++) {
        zis[i] = (extendedRank & efs[i] & efs[num + i]) ^ (efs[num + i] & a) ^ (efs[i] & b) ^ c;
    }
#endif

    return zis;
}
