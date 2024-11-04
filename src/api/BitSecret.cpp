//
// Created by 杜建璋 on 2024/9/13.
//

#include "api/BitSecret.h"
#include "bit/BitExecutor.h"
#include "bit/and/RsaAndExecutor.h"
#include "int/comparison/MuxExecutor.h"

BitSecret::BitSecret(bool x) {
    _data = x;
}

BitSecret BitSecret::share() const {
    return BitSecret(BitExecutor(_data, true)._zi);
}

BitSecret BitSecret::not_() const {
    return BitSecret(_data ^ Comm::rank());
}

BitSecret BitSecret::xor_(bool yi) const {
    return BitSecret(_data ^ yi);
}

BitSecret BitSecret::xor_(BitSecret yi) const {
    return xor_(yi.get());
}

BitSecret BitSecret::and_(bool yi) const {
    return BitSecret(RsaAndExecutor(_data, yi, false).execute(false)->_result);
}

BitSecret BitSecret::and_(BitSecret yi) const {
    return and_(yi.get());
}

BitSecret BitSecret::or_(bool yi) const {
    return xor_(yi).xor_(and_(yi));
}

BitSecret BitSecret::or_(BitSecret yi) const {
    return or_(yi.get());
}

BitSecret BitSecret::mux(bool yi, bool ci) const {
    return BitSecret(MuxExecutor(_data, yi, ci, false).execute(false)->_zi);
}

BitSecret BitSecret::mux(BitSecret yi, BitSecret ci) const {
    return mux(yi.get(), ci.get());
}

BitSecret BitSecret::reconstruct() const {
    return BitSecret(BitExecutor(_data, false).reconstruct()->_result);
}

bool BitSecret::get() const {
    return _data;
}

BitSecret BitSecret::share(bool x) {
    return BitSecret(BitExecutor(x, true)._zi);
}

BitSecret BitSecret::xor_(bool xi, bool yi) {
    return BitSecret(xi ^ yi);
}

BitSecret BitSecret::xor_(BitSecret xi, BitSecret yi) {
    return xor_(xi.get(), yi.get());
}

BitSecret BitSecret::and_(bool xi, bool yi) {
    return BitSecret(xi).and_(yi);
}

BitSecret BitSecret::and_(BitSecret xi, BitSecret yi) {
    return and_(xi.get(), yi.get());
}

BitSecret BitSecret::or_(bool xi, bool yi) {
    return BitSecret(xi).or_(yi);
}

BitSecret BitSecret::or_(BitSecret xi, BitSecret yi) {
    return or_(xi.get(), yi.get());
}

BitSecret BitSecret::mux(bool xi, bool yi, bool ci) {
    return BitSecret(xi).mux(yi, ci);
}

BitSecret BitSecret::mux(BitSecret xi, BitSecret yi, BitSecret ci) {
    return mux(xi.get(), yi.get(), ci.get());
}

BitSecret BitSecret::reconstruct(bool xi) {
    return BitSecret(xi).reconstruct();
}

BitSecret BitSecret::reconstruct(BitSecret xi) {
    return reconstruct(xi.get());
}


















