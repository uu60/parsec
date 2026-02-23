
#include "accelerate/SimdSupport.h"

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
        __m512i vec = _mm512_loadu_si512(&arr[i]);
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
        __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr[i]));
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
        __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr[i]));
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
    __m512i a_vec = _mm512_set1_epi64(a);
    __m512i b_vec = _mm512_set1_epi64(b);
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
    __m256i a_vec = _mm256_set1_epi64x(a);
    __m256i b_vec = _mm256_set1_epi64x(b);
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
    __m128i a_vec = _mm_set1_epi64x(a);
    __m128i b_vec = _mm_set1_epi64x(b);
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
    int64x2_t a_vec = vdupq_n_s64(a);
    int64x2_t b_vec = vdupq_n_s64(b);
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

std::vector<int64_t> SimdSupport::xor3Concat(const int64_t *commonA, const int64_t *arrB, const int64_t *arrD,
                                             const int64_t *commonC, int num) {
    std::vector<int64_t> output(2 * num);

#ifdef SIMD_AVX512
    int i = 0;
    for (; i + 8 <= num; i += 8) {
        __m512i va = _mm512_loadu_si512(commonA + i);
        __m512i vb = _mm512_loadu_si512(arrB + i);
        __m512i vc = _mm512_loadu_si512(commonC + i);
        __m512i res = _mm512_xor_si512(va, vb);
        res = _mm512_xor_si512(res, vc);
        _mm512_storeu_si512(output.data() + i, res);
    }
    for (; i < num; i++) {
        output[i] = commonA[i] ^ arrB[i] ^ commonC[i];
    }
    i = 0;
    for (; i + 8 <= num; i += 8) {
        __m512i va = _mm512_loadu_si512(commonA + i);
        __m512i vd = _mm512_loadu_si512(arrD + i);
        __m512i vc = _mm512_loadu_si512(commonC + i);
        __m512i res = _mm512_xor_si512(va, vd);
        res = _mm512_xor_si512(res, vc);
        _mm512_storeu_si512(output.data() + num + i, res);
    }
    for (; i < num; i++) {
        output[num + i] = commonA[i] ^ arrD[i] ^ commonC[i];
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    for (; i + 4 <= num; i += 4) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(commonA + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(arrB + i));
        __m256i vc = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(commonC + i));
        __m256i res = _mm256_xor_si256(va, vb);
        res = _mm256_xor_si256(res, vc);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(output.data() + i), res);
    }
    for (; i < num; i++) {
        output[i] = commonA[i] ^ arrB[i] ^ commonC[i];
    }
    i = 0;
    for (; i + 4 <= num; i += 4) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(commonA + i));
        __m256i vd = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(arrD + i));
        __m256i vc = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(commonC + i));
        __m256i res = _mm256_xor_si256(va, vd);
        res = _mm256_xor_si256(res, vc);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(output.data() + num + i), res);
    }
    for (; i < num; i++) {
        output[num + i] = commonA[i] ^ arrD[i] ^ commonC[i];
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        __m128i va = _mm_loadu_si128(reinterpret_cast<const __m128i*>(commonA + i));
        __m128i vb = _mm_loadu_si128(reinterpret_cast<const __m128i*>(arrB + i));
        __m128i vc = _mm_loadu_si128(reinterpret_cast<const __m128i*>(commonC + i));
        __m128i res = _mm_xor_si128(va, vb);
        res = _mm_xor_si128(res, vc);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(output.data() + i), res);
    }
    for (; i < num; i++) {
        output[i] = commonA[i] ^ arrB[i] ^ commonC[i];
    }
    i = 0;
    for (; i + 2 <= num; i += 2) {
        __m128i va = _mm_loadu_si128(reinterpret_cast<const __m128i*>(commonA + i));
        __m128i vd = _mm_loadu_si128(reinterpret_cast<const __m128i*>(arrD + i));
        __m128i vc = _mm_loadu_si128(reinterpret_cast<const __m128i*>(commonC + i));
        __m128i res = _mm_xor_si128(va, vd);
        res = _mm_xor_si128(res, vc);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(output.data() + num + i), res);
    }
    for (; i < num; i++) {
        output[num + i] = commonA[i] ^ arrD[i] ^ commonC[i];
    }
#elif defined(SIMD_NEON)
    int i = 0;
    for (; i + 2 <= num; i += 2) {
        int64x2_t va = vld1q_s64(commonA + i);
        int64x2_t vb = vld1q_s64(arrB + i);
        int64x2_t vc = vld1q_s64(commonC + i);
        int64x2_t res = veorq_s64(va, vb);
        res = veorq_s64(res, vc);
        vst1q_s64(output.data() + i, res);
    }
    for (; i < num; i++) {
        output[i] = commonA[i] ^ arrB[i] ^ commonC[i];
    }
    i = 0;
    for (; i + 2 <= num; i += 2) {
        int64x2_t va = vld1q_s64(commonA + i);
        int64x2_t vd = vld1q_s64(arrD + i);
        int64x2_t vc = vld1q_s64(commonC + i);
        int64x2_t res = veorq_s64(va, vd);
        res = veorq_s64(res, vc);
        vst1q_s64(output.data() + num + i, res);
    }
    for (; i < num; i++) {
        output[num + i] = commonA[i] ^ arrD[i] ^ commonC[i];
    }
#else
    for (int i = 0; i < num; i++) {
        output[i] = commonA[i] ^ arrB[i] ^ commonC[i];
        output[num + i] = commonA[i] ^ arrD[i] ^ commonC[i];
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

std::vector<int64_t> SimdSupport::computeDiag(const std::vector<int64_t> &_yis,
                                              const std::vector<int64_t> &x_xor_y) {
    int n = static_cast<int>(_yis.size());
    std::vector<int64_t> diag(n);
    int64_t rank = Comm::rank();
#ifdef SIMD_AVX512
    int i = 0;
    __m512i one     = _mm512_set1_epi64(1);
    __m512i not_one = _mm512_set1_epi64(~1ll);
    __m512i rank_vec= _mm512_set1_epi64(rank);
    for (; i + 8 <= n; i += 8) {
        __m512i y_vec = _mm512_loadu_si512(&_yis[i]);
        __m512i x_vec = _mm512_loadu_si512(&x_xor_y[i]);
        __m512i yis_lsb = _mm512_and_si512(y_vec, one);
        __m512i xor_result = _mm512_xor_si512(yis_lsb, rank_vec);
        __m512i m = _mm512_and_si512(x_vec, not_one);
        __m512i res = _mm512_or_si512(m, xor_result);
        _mm512_storeu_si512(&diag[i], res);
    }
    for (; i < n; i++) {
        int64_t yis_lsb = _yis[i] & 1;
        int64_t xor_result = yis_lsb ^ rank;
        int64_t m = x_xor_y[i] & (~1ll);
        diag[i] = m | xor_result;
    }
#elif defined(SIMD_AVX2)
    int i = 0;
    __m256i one     = _mm256_set1_epi64x(1);
    __m256i not_one = _mm256_set1_epi64x(~1ll);
    __m256i rank_vec= _mm256_set1_epi64x(rank);
    for (; i + 4 <= n; i += 4) {
        __m256i y_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&_yis[i]));
        __m256i x_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&x_xor_y[i]));
        __m256i yis_lsb = _mm256_and_si256(y_vec, one);
        __m256i xor_result = _mm256_xor_si256(yis_lsb, rank_vec);
        __m256i m = _mm256_and_si256(x_vec, not_one);
        __m256i res = _mm256_or_si256(m, xor_result);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&diag[i]), res);
    }
    for (; i < n; i++) {
        int64_t yis_lsb = _yis[i] & 1;
        int64_t xor_result = yis_lsb ^ rank;
        int64_t m = x_xor_y[i] & (~1ll);
        diag[i] = m | xor_result;
    }
#elif defined(SIMD_SSE2)
    int i = 0;
    __m128i one     = _mm_set1_epi64x(1);
    __m128i not_one = _mm_set1_epi64x(~1ll);
    __m128i rank_vec= _mm_set1_epi64x(rank);
    for (; i + 2 <= n; i += 2) {
        __m128i y_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&_yis[i]));
        __m128i x_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&x_xor_y[i]));
        __m128i yis_lsb = _mm_and_si128(y_vec, one);
        __m128i xor_result = _mm_xor_si128(yis_lsb, rank_vec);
        __m128i m = _mm_and_si128(x_vec, not_one);
        __m128i res = _mm_or_si128(m, xor_result);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&diag[i]), res);
    }
    for (; i < n; i++) {
        int64_t yis_lsb = _yis[i] & 1;
        int64_t xor_result = yis_lsb ^ rank;
        int64_t m = x_xor_y[i] & (~1ll);
        diag[i] = m | xor_result;
    }
#elif defined(SIMD_NEON)
    int i = 0;
    int64x2_t one = vdupq_n_s64(1);
    int64x2_t not_one = vdupq_n_s64(~1ll);
    int64x2_t rank_vec = vdupq_n_s64(rank);
    for (; i + 2 <= n; i += 2) {
        int64x2_t y_vec = vld1q_s64(&_yis[i]);
        int64x2_t x_vec = vld1q_s64(&x_xor_y[i]);
        int64x2_t yis_lsb = vandq_s64(y_vec, one);
        int64x2_t xor_result = veorq_s64(yis_lsb, rank_vec);
        int64x2_t m = vandq_s64(x_vec, not_one);
        int64x2_t res = vorrq_s64(m, xor_result);
        vst1q_s64(&diag[i], res);
    }
    for (; i < n; i++) {
        int64_t yis_lsb = _yis[i] & 1;
        int64_t xor_result = yis_lsb ^ rank;
        int64_t m = x_xor_y[i] & (~1ll);
        diag[i] = m | xor_result;
    }
#else
    for (int i = 0; i < n; i++) {
        int64_t yis_lsb = _yis[i] & 1;
        int64_t xor_result = yis_lsb ^ rank;
        int64_t m = x_xor_y[i] & (~1ll);
        diag[i] = m | xor_result;
    }
#endif

    return diag;
}

// ============== U128 SIMD Operations for IKNP ==============

void SimdSupport::xorU128ArrayWithConstant(U128* out, const U128* arr, const U128& constant, size_t count) {
    // XOR each element of arr with constant, store in out
    // U128 is 16 bytes = 128 bits, perfect for SSE/NEON

#ifdef SIMD_AVX512
    // AVX-512: process 4 U128 (512 bits) at a time
    const __m512i const_vec = _mm512_set_epi64(
        constant.hi, constant.lo, constant.hi, constant.lo,
        constant.hi, constant.lo, constant.hi, constant.lo
    );
    size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m512i data = _mm512_loadu_si512(&arr[i]);
        __m512i result = _mm512_xor_si512(data, const_vec);
        _mm512_storeu_si512(&out[i], result);
    }
    for (; i < count; ++i) {
        out[i].lo = arr[i].lo ^ constant.lo;
        out[i].hi = arr[i].hi ^ constant.hi;
    }
#elif defined(SIMD_AVX2)
    // AVX2: process 2 U128 (256 bits) at a time
    const __m256i const_vec = _mm256_set_epi64x(constant.hi, constant.lo, constant.hi, constant.lo);
    size_t i = 0;
    for (; i + 2 <= count; i += 2) {
        __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr[i]));
        __m256i result = _mm256_xor_si256(data, const_vec);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[i]), result);
    }
    for (; i < count; ++i) {
        out[i].lo = arr[i].lo ^ constant.lo;
        out[i].hi = arr[i].hi ^ constant.hi;
    }
#elif defined(SIMD_SSE2)
    // SSE2: process 1 U128 (128 bits) at a time
    const __m128i const_vec = _mm_set_epi64x(constant.hi, constant.lo);
    for (size_t i = 0; i < count; ++i) {
        __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr[i]));
        __m128i result = _mm_xor_si128(data, const_vec);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[i]), result);
    }
#elif defined(SIMD_NEON)
    // NEON: process 1 U128 at a time
    const uint64x2_t const_vec = {constant.lo, constant.hi};
    for (size_t i = 0; i < count; ++i) {
        uint64x2_t data = vld1q_u64(reinterpret_cast<const uint64_t*>(&arr[i]));
        uint64x2_t result = veorq_u64(data, const_vec);
        vst1q_u64(reinterpret_cast<uint64_t*>(&out[i]), result);
    }
#else
    for (size_t i = 0; i < count; ++i) {
        out[i].lo = arr[i].lo ^ constant.lo;
        out[i].hi = arr[i].hi ^ constant.hi;
    }
#endif
}

void SimdSupport::xorU128Arrays(U128* out, const U128* arr0, const U128* arr1, size_t count) {
    // XOR two U128 arrays element-wise

#ifdef SIMD_AVX512
    size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m512i data0 = _mm512_loadu_si512(&arr0[i]);
        __m512i data1 = _mm512_loadu_si512(&arr1[i]);
        __m512i result = _mm512_xor_si512(data0, data1);
        _mm512_storeu_si512(&out[i], result);
    }
    for (; i < count; ++i) {
        out[i].lo = arr0[i].lo ^ arr1[i].lo;
        out[i].hi = arr0[i].hi ^ arr1[i].hi;
    }
#elif defined(SIMD_AVX2)
    size_t i = 0;
    for (; i + 2 <= count; i += 2) {
        __m256i data0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr0[i]));
        __m256i data1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr1[i]));
        __m256i result = _mm256_xor_si256(data0, data1);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[i]), result);
    }
    for (; i < count; ++i) {
        out[i].lo = arr0[i].lo ^ arr1[i].lo;
        out[i].hi = arr0[i].hi ^ arr1[i].hi;
    }
#elif defined(SIMD_SSE2)
    for (size_t i = 0; i < count; ++i) {
        __m128i data0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr0[i]));
        __m128i data1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr1[i]));
        __m128i result = _mm_xor_si128(data0, data1);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[i]), result);
    }
#elif defined(SIMD_NEON)
    for (size_t i = 0; i < count; ++i) {
        uint64x2_t data0 = vld1q_u64(reinterpret_cast<const uint64_t*>(&arr0[i]));
        uint64x2_t data1 = vld1q_u64(reinterpret_cast<const uint64_t*>(&arr1[i]));
        uint64x2_t result = veorq_u64(data0, data1);
        vst1q_u64(reinterpret_cast<uint64_t*>(&out[i]), result);
    }
#else
    for (size_t i = 0; i < count; ++i) {
        out[i].lo = arr0[i].lo ^ arr1[i].lo;
        out[i].hi = arr0[i].hi ^ arr1[i].hi;
    }
#endif
}

void SimdSupport::xor3U128Arrays(U128* out, const U128* arr0, const U128* arr1, const U128* arr2, size_t count) {
    // XOR three U128 arrays element-wise: out[i] = arr0[i] ^ arr1[i] ^ arr2[i]

#ifdef SIMD_AVX512
    size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m512i data0 = _mm512_loadu_si512(&arr0[i]);
        __m512i data1 = _mm512_loadu_si512(&arr1[i]);
        __m512i data2 = _mm512_loadu_si512(&arr2[i]);
        __m512i result = _mm512_xor_si512(_mm512_xor_si512(data0, data1), data2);
        _mm512_storeu_si512(&out[i], result);
    }
    for (; i < count; ++i) {
        out[i].lo = arr0[i].lo ^ arr1[i].lo ^ arr2[i].lo;
        out[i].hi = arr0[i].hi ^ arr1[i].hi ^ arr2[i].hi;
    }
#elif defined(SIMD_AVX2)
    size_t i = 0;
    for (; i + 2 <= count; i += 2) {
        __m256i data0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr0[i]));
        __m256i data1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr1[i]));
        __m256i data2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&arr2[i]));
        __m256i result = _mm256_xor_si256(_mm256_xor_si256(data0, data1), data2);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&out[i]), result);
    }
    for (; i < count; ++i) {
        out[i].lo = arr0[i].lo ^ arr1[i].lo ^ arr2[i].lo;
        out[i].hi = arr0[i].hi ^ arr1[i].hi ^ arr2[i].hi;
    }
#elif defined(SIMD_SSE2)
    for (size_t i = 0; i < count; ++i) {
        __m128i data0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr0[i]));
        __m128i data1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr1[i]));
        __m128i data2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&arr2[i]));
        __m128i result = _mm_xor_si128(_mm_xor_si128(data0, data1), data2);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(&out[i]), result);
    }
#elif defined(SIMD_NEON)
    for (size_t i = 0; i < count; ++i) {
        uint64x2_t data0 = vld1q_u64(reinterpret_cast<const uint64_t*>(&arr0[i]));
        uint64x2_t data1 = vld1q_u64(reinterpret_cast<const uint64_t*>(&arr1[i]));
        uint64x2_t data2 = vld1q_u64(reinterpret_cast<const uint64_t*>(&arr2[i]));
        uint64x2_t result = veorq_u64(veorq_u64(data0, data1), data2);
        vst1q_u64(reinterpret_cast<uint64_t*>(&out[i]), result);
    }
#else
    for (size_t i = 0; i < count; ++i) {
        out[i].lo = arr0[i].lo ^ arr1[i].lo ^ arr2[i].lo;
        out[i].hi = arr0[i].hi ^ arr1[i].hi ^ arr2[i].hi;
    }
#endif
}

void SimdSupport::conditionalXorU128(U128* out, const U128* src, const bool* mask, size_t count) {
    // Conditional XOR: if mask[i] is true, out[i] ^= src[i]
    // This is used for XOR with U based on sender's choice bits

    for (size_t i = 0; i < count; ++i) {
        if (mask[i]) {
            out[i].lo ^= src[i].lo;
            out[i].hi ^= src[i].hi;
        }
    }
}

void SimdSupport::selectBits(int64_t* result, const int64_t* y0, const int64_t* y1,
                             const int64_t* choice, const int64_t* hashValues, size_t count) {
    // Optimized bit selection for IKNP receiver:
    // result[i] = ((y0[i] & ~choice[i]) | (y1[i] & choice[i])) ^ hashValues[i]

#ifdef SIMD_AVX512
    size_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m512i v_y0 = _mm512_loadu_si512(&y0[i]);
        __m512i v_y1 = _mm512_loadu_si512(&y1[i]);
        __m512i v_choice = _mm512_loadu_si512(&choice[i]);
        __m512i v_hash = _mm512_loadu_si512(&hashValues[i]);

        // (y0 & ~choice) | (y1 & choice) = y0 ^ ((y0 ^ y1) & choice)
        __m512i xor_y = _mm512_xor_si512(v_y0, v_y1);
        __m512i masked = _mm512_and_si512(xor_y, v_choice);
        __m512i selected = _mm512_xor_si512(v_y0, masked);
        __m512i final = _mm512_xor_si512(selected, v_hash);

        _mm512_storeu_si512(&result[i], final);
    }
    for (; i < count; ++i) {
        uint64_t c = static_cast<uint64_t>(choice[i]);
        uint64_t selected = (static_cast<uint64_t>(y0[i]) & ~c) | (static_cast<uint64_t>(y1[i]) & c);
        result[i] = static_cast<int64_t>(selected ^ static_cast<uint64_t>(hashValues[i]));
    }
#elif defined(SIMD_AVX2)
    size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m256i v_y0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&y0[i]));
        __m256i v_y1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&y1[i]));
        __m256i v_choice = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&choice[i]));
        __m256i v_hash = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&hashValues[i]));

        // (y0 & ~choice) | (y1 & choice) = y0 ^ ((y0 ^ y1) & choice)
        __m256i xor_y = _mm256_xor_si256(v_y0, v_y1);
        __m256i masked = _mm256_and_si256(xor_y, v_choice);
        __m256i selected = _mm256_xor_si256(v_y0, masked);
        __m256i final = _mm256_xor_si256(selected, v_hash);

        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&result[i]), final);
    }
    for (; i < count; ++i) {
        uint64_t c = static_cast<uint64_t>(choice[i]);
        uint64_t selected = (static_cast<uint64_t>(y0[i]) & ~c) | (static_cast<uint64_t>(y1[i]) & c);
        result[i] = static_cast<int64_t>(selected ^ static_cast<uint64_t>(hashValues[i]));
    }
#elif defined(SIMD_SSE2)
    size_t i = 0;
    for (; i + 2 <= count; i += 2) {
        __m128i v_y0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&y0[i]));
        __m128i v_y1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&y1[i]));
        __m128i v_choice = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&choice[i]));
        __m128i v_hash = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&hashValues[i]));

        __m128i xor_y = _mm_xor_si128(v_y0, v_y1);
        __m128i masked = _mm_and_si128(xor_y, v_choice);
        __m128i selected = _mm_xor_si128(v_y0, masked);
        __m128i final = _mm_xor_si128(selected, v_hash);

        _mm_storeu_si128(reinterpret_cast<__m128i*>(&result[i]), final);
    }
    for (; i < count; ++i) {
        uint64_t c = static_cast<uint64_t>(choice[i]);
        uint64_t selected = (static_cast<uint64_t>(y0[i]) & ~c) | (static_cast<uint64_t>(y1[i]) & c);
        result[i] = static_cast<int64_t>(selected ^ static_cast<uint64_t>(hashValues[i]));
    }
#elif defined(SIMD_NEON)
    size_t i = 0;
    for (; i + 2 <= count; i += 2) {
        int64x2_t v_y0 = vld1q_s64(&y0[i]);
        int64x2_t v_y1 = vld1q_s64(&y1[i]);
        int64x2_t v_choice = vld1q_s64(&choice[i]);
        int64x2_t v_hash = vld1q_s64(&hashValues[i]);

        int64x2_t xor_y = veorq_s64(v_y0, v_y1);
        int64x2_t masked = vandq_s64(xor_y, v_choice);
        int64x2_t selected = veorq_s64(v_y0, masked);
        int64x2_t final = veorq_s64(selected, v_hash);

        vst1q_s64(&result[i], final);
    }
    for (; i < count; ++i) {
        uint64_t c = static_cast<uint64_t>(choice[i]);
        uint64_t selected = (static_cast<uint64_t>(y0[i]) & ~c) | (static_cast<uint64_t>(y1[i]) & c);
        result[i] = static_cast<int64_t>(selected ^ static_cast<uint64_t>(hashValues[i]));
    }
#else
    for (size_t i = 0; i < count; ++i) {
        uint64_t c = static_cast<uint64_t>(choice[i]);
        uint64_t selected = (static_cast<uint64_t>(y0[i]) & ~c) | (static_cast<uint64_t>(y1[i]) & c);
        result[i] = static_cast<int64_t>(selected ^ static_cast<uint64_t>(hashValues[i]));
    }
#endif
}

