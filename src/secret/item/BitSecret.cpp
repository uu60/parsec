
#include "parsec/secret/item/BitSecret.h"

#include "parsec/comm/Comm.h"
#include "parsec/compute/single/arith/ArithMutexOperator.h"
#include "parsec/compute/single/arith/ArithOperator.h"
#include "parsec/compute/single/bool/BoolAndOperator.h"
#include "parsec/compute/single/bool/BoolLessOperator.h"
#include "parsec/compute/single/bool/BoolXorOperator.h"

namespace parsec {

BitSecret::BitSecret(bool x, int taskTag) : _data(x), _taskTag(taskTag) {}

BitSecret BitSecret::task(int taskTag) { return BitSecret(_data, taskTag); }

BitSecret BitSecret::share(int clientRank) const {
    return BitSecret(BoolOperator(_data, 1, _taskTag, 0, clientRank)._zi, _taskTag);
}

BitSecret BitSecret::lessThan(BitSecret yi) const {
    return BitSecret(
        BoolLessOperator(_data, yi.get(), 1, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi, _taskTag);
}

BitSecret BitSecret::not_() const { return BitSecret(_data ^ Comm::rank(), _taskTag); }

BitSecret BitSecret::xor_(BitSecret yi) const {
    return BitSecret(BoolXorOperator(_data, yi.get(), 1, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi,
                     _taskTag);
}

BitSecret BitSecret::and_(BitSecret yi) const {
    return BitSecret(BoolAndOperator(_data, yi.get(), 1, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi,
                     _taskTag);
}

BitSecret BitSecret::or_(BitSecret yi) const { return xor_(yi).xor_(and_(yi)); }

BitSecret BitSecret::mux(BitSecret yi, BitSecret cond_i) const {
    return BitSecret(
        ArithMutexOperator(_data, yi.get(), cond_i.get(), 1, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE)
            .execute()
            ->_zi,
        _taskTag);
}

BitSecret BitSecret::reconstruct(int clientRank) const {
    return BitSecret(
        ArithOperator(_data, 1, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).reconstruct(clientRank)->_result,
        _taskTag);
}

bool BitSecret::get() const { return _data; }

} // namespace parsec
