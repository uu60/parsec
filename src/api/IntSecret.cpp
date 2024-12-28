//
// Created by 杜建璋 on 2024/9/12.
//

#include <utility>

#include "api/IntSecret.h"

#include "comm/IComm.h"
#include "compute/ArithExecutor.h"
#include "compute/BoolExecutor.h"
#include "compute/arith/ArithAddExecutor.h"
#include "compute/arith/ArithMulExecutor.h"
#include "compute/arith/ArithLessThanExecutor.h"
#include "compute/arith/ArithMuxExecutor.h"
#include "compute/arith/ArithToBoolExecutor.h"
#include "compute/bool/BoolAndExecutor.h"
#include "compute/bool/BoolToArithExecutor.h"
#include "compute/bool/BoolXorExecutor.h"
#include "utils/Log.h"

IntSecret::IntSecret(int64_t x, int l, int16_t objTag) : _data(x), _l(l), _objTag(objTag) {}

IntSecret IntSecret::arithShare(int clientRank) const {
    return {ArithExecutor(_data, _l, _objTag, 0, clientRank)._zi, _l, _objTag};
}

IntSecret IntSecret::boolShare(int clientRank) const {
    return {BoolExecutor(_data, _l, _objTag, 0, clientRank)._zi, _l, _objTag};
}

[[nodiscard]] IntSecret IntSecret::arithReconstruct(int clientRank) const {
    return {ArithExecutor(_data, _l, _objTag, 0, -1).reconstruct(clientRank)->_result, _l, _objTag};
}

[[nodiscard]] IntSecret IntSecret::boolReconstruct(int clientRank) const {
    return {BoolExecutor(_data, _l, _objTag, 0, -1).reconstruct(clientRank)->_result, _l, _objTag};
}

int64_t IntSecret::get() const {
    return _data;
}

IntSecret IntSecret::add(IntSecret yi) const {
    return {ArithAddExecutor(_data, yi.get(), _l, _objTag, 0, -1).execute()->_zi, _l, _objTag};
}

IntSecret IntSecret::mul(IntSecret yi) const {
    return {ArithMulExecutor(_data, yi.get(), _l, _objTag, 0, -1).execute()->_zi, _l, _objTag};
}

IntSecret IntSecret::xor_(IntSecret yi) const {
    return {BoolXorExecutor(_data, yi.get(), _l, _objTag, 0, -1).execute()->_zi, _l, _objTag};
}

IntSecret IntSecret::and_(IntSecret yi) const {
    return {BoolAndExecutor(_data, yi.get(), _l, _objTag, 0, -1).execute()->_zi, _l, _objTag};
}

IntSecret IntSecret::boolean() const {
    return {ArithToBoolExecutor(_data, _l, _objTag, 0, -1).execute()->_zi, _l, _objTag};
}

IntSecret IntSecret::arithmetic() const {
    return {BoolToArithExecutor(_data, _l, _objTag, 0, -1).execute()->_zi, _l, _objTag};
}

BitSecret IntSecret::arithLessThan(IntSecret yi) const {
    return BitSecret(ArithLessThanExecutor(_data, yi.get(), _l, _objTag, 0, -1).execute()->_sign, _objTag);
}

IntSecret IntSecret::mux(IntSecret yi, BitSecret cond_i) const {
    return {ArithMuxExecutor(_data, yi.get(), cond_i.get(), _l, _objTag, 0, -1).execute()->_zi, _l, _objTag};
}
