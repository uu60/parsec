//
// Created by 杜建璋 on 2024/8/30.
//

#include "bit/and/FixedAndExecutor.h"
#include "bmt/FixedTripleGenerator.h"

FixedAndExecutor::FixedAndExecutor(bool z, bool share) : AbstractAndExecutor(z, share) {}

FixedAndExecutor::FixedAndExecutor(bool x, bool y, bool share) : AbstractAndExecutor(x, y, share) {}

void FixedAndExecutor::obtainMultiplicationTriple() {
    FixedTripleGenerator<bool> e;
    e.benchmark(_benchmarkLevel);
    e.logBenchmark(false);
    e.execute(false);

    _ai = e._ai;
    _bi = e._bi;
    _ci = e._ci;
}

std::string FixedAndExecutor::tag() const {
    return "[Fixed And Boolean Share]";
}
