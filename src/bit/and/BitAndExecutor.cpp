//
// Created by 杜建璋 on 2024/8/29.
//

#include "bit/and/BitAndExecutor.h"
#include "utils/Comm.h"

BitAndExecutor::BitAndExecutor(bool z, bool share) : BitExecutor(z, share) {}

BitAndExecutor::BitAndExecutor(bool x, bool y, bool share) : BitExecutor(x, y, share) {}

BitAndExecutor* BitAndExecutor::execute(bool reconstruct) {
    // process
    if (Comm::isServer()) {
        bool ei = _ai ^ _xi;
        bool fi = _bi ^ _yi;
        bool eo, fo;
        Comm::sexch(&ei, &eo);
        Comm::sexch(&fi, &fo);
        bool e = ei ^ eo;
        bool f = fi ^ fo;
        _zi = Comm::rank() * e * f ^ f * _ai ^ e * _bi ^ _ci;
        _result = _zi;
    }
    if (reconstruct) {
        this->reconstruct();
    }

    return this;
}

BitAndExecutor *BitAndExecutor::obtainBmt(bool ai, bool bi, bool ci) {
    _ai = ai;
    _bi = bi;
    _ci = ci;
    return this;
}
