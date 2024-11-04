//
// Created by 杜建璋 on 2024/8/30.
//

#include "bit/and/RsaAndExecutor.h"

RsaAndExecutor::RsaAndExecutor(bool z, bool share) : AbstractAndExecutor(z, share) {}

RsaAndExecutor::RsaAndExecutor(bool x, bool y, bool share) : AbstractAndExecutor(x, y, share) {}

void RsaAndExecutor::obtainMultiplicationTriple() {
    RsaTripleGenerator<bool> e;
    e.benchmark(_benchmarkLevel);
    e.execute(false);
    if (_benchmarkLevel == BenchmarkLevel::DETAILED) {
        this->_mpiTime += e.mpiTime();
    }
    _ai = e._ai;
    _bi = e._bi;
    _ci = e._ci;

    if (_benchmarkLevel == BenchmarkLevel::DETAILED && _isLogBenchmark) {
        Log::i(tag() + " OT RSA keys generation time: " + std::to_string(e._otRsaGenerationTime) + " ms.");
        Log::i(tag() + " OT RSA encryption time: " + std::to_string(e._otRsaEncryptionTime) + " ms.");
        Log::i(tag() + " OT RSA decryption time: " + std::to_string(e._otRsaDecryptionTime) + " ms.");
        Log::i(tag() + " OT MPI transmission and synchronization time: " + std::to_string(e._otMpiTime) + " ms.");
        Log::i(tag() + " OT total computation time: " + std::to_string(e._otEntireComputationTime) + " ms.");
    }
}

std::string RsaAndExecutor::tag() const {
    return "[RSA OT And Share]";
}

