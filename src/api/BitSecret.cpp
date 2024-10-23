//
// Created by 杜建璋 on 2024/9/13.
//

#include "api/BitSecret.h"
#include "bit/BitExecutor.h"
#include "bit/and/RsaAndExecutor.h"

BitSecret::BitSecret(bool x) {
    _data = x;
}

BitSecret BitSecret::share() const {
    return BitSecret(BitExecutor(_data, true).zi());
}

BitSecret BitSecret::xor_(bool yi) const {
    return BitSecret(_data ^ yi);
}

BitSecret BitSecret::xor_(BitSecret yi) const {
    return xor_(yi.get());
}

BitSecret BitSecret::and_(bool yi) const {
    return BitSecret(RsaAndExecutor(_data, yi, false).execute(false)->result());
}

BitSecret BitSecret::and_(BitSecret yi) const {
    return and_(yi.get());
}

BitSecret BitSecret::reconstruct() const {
    return BitSecret(BitExecutor(_data, false).reconstruct()->result());
}

bool BitSecret::get() const {
    return _data;
}

BitSecret BitSecret::share(bool x) {
    return BitSecret(BitExecutor(x, true).zi());
}

BitSecret BitSecret::xor_(bool xi, bool yi) {
    return BitSecret(xi ^ yi);
}

BitSecret BitSecret::xor_(BitSecret xi, BitSecret yi) {
    return xor_(xi.get(), yi.get());
}

BitSecret BitSecret::and_(bool xi, bool yi) {
    return BitSecret(RsaAndExecutor(xi, yi, false).execute(false)->result());
}

BitSecret BitSecret::and_(BitSecret xi, BitSecret yi) {
    return and_(xi.get(), yi.get());
}


















