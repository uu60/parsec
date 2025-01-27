//
// Created by 杜建璋 on 2024/12/29.
//

#include <cmath>

#include "../../../include/compute/bool/BoolLessExecutor.h"
#include "../../../include/compute/bool/BoolAndExecutor.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolLessExecutor *BoolLessExecutor::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isServer()) {
        int64_t x_xor_y = _xi ^ _yi;
        int64_t lbs = Comm::rank() == 0 ? x_xor_y : x_xor_y ^ Math::ring(-1ll, _l);

        int64_t shifted_1 = shiftGreater(lbs, 1);

        std::vector<Bmt> temp;
        auto [needed, _] = BoolAndExecutor::needBmtsWithBits(_l);
        if (_bmts != nullptr) {
            temp.reserve(needed);
            for (int i = 0; i < needed; i++) {
                temp.emplace_back(_bmts->at(i));
            }
        }
        lbs = BoolAndExecutor(lbs, shifted_1, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
                .setBmts(_bmts == nullptr ? nullptr : &temp)->execute()->_zi;

        int64_t diag = Math::changeBit(x_xor_y, 0, Math::getBit(_yi, 0) ^ Comm::rank());
        // diag & x

        if (_bmts != nullptr) {
            for (int i = needed; i < needed * 2; i++) {
                temp[i - needed] = _bmts->at(i);
            }
        }
        diag = BoolAndExecutor(diag, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmts(
            _bmts == nullptr ? nullptr : &temp)->execute()->_zi;

        int rounds = (int) std::floor(std::log2(_l));
        for (int r = 2; r <= rounds; r++) {
            int64_t shifted_r = shiftGreater(lbs, r);

            if (_bmts != nullptr) {
                for (int i = needed * r; i < needed * (r + 1); i++) {
                    temp[i - needed * r] = _bmts->at(i);
                }
            }

            int64_t and_r = BoolAndExecutor(lbs, shifted_r, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
                    setBmts(_bmts == nullptr ? nullptr : &temp)->execute()->_zi;
            lbs = and_r;
        }

        int64_t shifted_accum = Math::changeBit(lbs >> 1, _l - 1, Comm::rank());

        if (_bmts != nullptr) {
            for (int i = needed * (rounds + 1); i < needed * (rounds + 2); i++) {
                temp[i - needed * (rounds + 1)] = _bmts->at(i);
            }
        }
        int64_t final_accum = BoolAndExecutor(shifted_accum, diag, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
                setBmts(_bmts == nullptr ? nullptr : &temp)->execute()->_zi;

        bool result = false;
        for (int i = 0; i < _l; i++) {
            result = result ^ Math::getBit(final_accum, i);
        }

        _zi = result;
    }

    return this;
}

std::string BoolLessExecutor::className() const {
    return "BoolLessThanExecutor";
}

int BoolLessExecutor::needMsgTags(int l) {
    return BoolAndExecutor::needMsgTags(l);
}

BoolLessExecutor *BoolLessExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}

std::pair<int, int> BoolLessExecutor::needBmtsWithBits(int l) {
    auto needed = BoolAndExecutor::needBmtsWithBits(l);
    return {
        (static_cast<int>(std::floor(std::log2(l))) + 2) * needed.first,
        needed.second
    };
}

int64_t BoolLessExecutor::shiftGreater(int64_t in, int r) const {
    int part_size = 1 << r;
    if (part_size > _l) {
        return in;
    }

    int offset = part_size >> 1;

    for (int i = 0; i < _l; i += part_size) {
        int start = i + offset;
        if (start >= _l) {
            break;
        }

        bool midBit = Math::getBit(in, start);

        for (int j = start - 1; j >= i; j--) {
            in = Math::changeBit(in, j, midBit);
        }
    }

    return in;
}
