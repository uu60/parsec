//
// Created by 杜建璋 on 2024/7/7.
//

#ifndef MPC_PACKAGE_SECUREEXECUTOR_H
#define MPC_PACKAGE_SECUREEXECUTOR_H

#include <cstdint>
#include <string>
#include "./utils/System.h"
#include "./utils/Log.h"

using namespace std;

template<typename T>
class SecureExecutor {
public:
    enum class BenchmarkLevel {
        NONE, GENERAL, DETAILED
    };
protected:
    // result
    T _result{};
    // unreconstructed share
    T _zi{};

    // _l
    const int _l = std::is_same_v<T, bool> ? 1 : sizeof(_result) * 8;

    // for benchmark
    BenchmarkLevel _benchmarkLevel = BenchmarkLevel::NONE;
    bool _isLogBenchmark = false;
    int64_t _mpiTime{};
    int64_t _entireComputationTime{};
public:
    // secret sharing process
    virtual SecureExecutor *execute(bool reconstruct);

    virtual SecureExecutor *reconstruct();

    // get calculated result
    [[nodiscard]] T result() const;

    // directly set unreconstructed share
    T zi();

    // for benchmark
    SecureExecutor *benchmark(BenchmarkLevel lv);

    SecureExecutor *logBenchmark(bool isLogBenchmark);

    [[nodiscard]] int64_t mpiTime() const;

    [[nodiscard]] int64_t entireComputationTime() const;

    virtual ~SecureExecutor() = default;

protected:
    [[nodiscard]] virtual std::string tag() const;

    virtual void finalize();
};

#endif //MPC_PACKAGE_SECUREEXECUTOR_H
