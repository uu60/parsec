//
// Created by 杜建璋 on 25-5-25.
//

#include "../../../../include/compute/batch/bool/BoolEqualBatchOperator.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"

BoolEqualBatchOperator *BoolEqualBatchOperator::execute() {
    if (Comm::isClient()) {
        return this;
    }

    std::vector<BitwiseBmt> allBmts;
    bool gotBmt = prepareBmts(allBmts);

    std::vector<BitwiseBmt> bmts;
    if (gotBmt) {
        int bc = BoolLessBatchOperator::bmtCount(_xis->size() * 2, _width);
        bmts = std::vector(allBmts.end() - bc, allBmts.end());
        allBmts.resize(allBmts.size() - bc);
    }

    std::vector<int64_t> xy = *_xis;
    xy.insert(xy.end(), _yis->begin(), _yis->end());
    std::vector<int64_t> yx = *_yis;
    yx.insert(yx.end(), _xis->begin(), _xis->end());

    auto gtv_ltv = BoolLessBatchOperator(&xy, &yx, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;
    for (auto &v: gtv_ltv) {
        v = v ^ Comm::rank();
    }
    std::vector ltv(gtv_ltv.begin() + gtv_ltv.size() / 2, gtv_ltv.end());
    std::vector gtv = std::move(gtv_ltv);
    gtv.resize(gtv.size() / 2);

    _zis = BoolAndBatchOperator(&gtv, &ltv, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmts(
        gotBmt ? &allBmts : nullptr)->execute()->_zis;
    return this;

    /*
     * Following is an alternative implementation using bitwise operations
     * (Worse performance than the above method)
     */
    // Optimized bitwise comparison using secure operations
    // // Algorithm:
    // // 1. Compute XOR of x and y to get bit differences
    // // 2. For each bit position, check if difference is 0 (equal)
    // // 3. Use tree-based AND reduction to combine all bit equality results
    //
    // // Step 1: Compute XOR to get bit differences
    // auto xor_results = BoolXorBatchOperator(_xis, _yis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
    //     .execute()->_zis;
    //
    // // Step 2: Extract individual bits and check for equality (bit == 0)
    // // We need to check each bit position separately
    // std::vector<std::vector<int64_t>> bit_equality_results(_width);
    //
    // for (int bit_pos = 0; bit_pos < _width; bit_pos++) {
    //     bit_equality_results[bit_pos].resize(_xis->size());
    //
    //     // Extract bit at position bit_pos from each XOR result
    //     for (size_t i = 0; i < _xis->size(); i++) {
    //         int64_t bit = (xor_results[i] >> bit_pos) & 1;
    //         // For equality, we want bit to be 0, so we compute (1 - bit) = bit XOR 1
    //         bit_equality_results[bit_pos][i] = bit ^ Comm::rank();
    //     }
    // }
    //
    // // Step 3: Tree-based AND reduction across all bits (log(width) steps)
    // std::vector<std::vector<int64_t>> level = std::move(bit_equality_results);
    // if (level.empty()) { _zis.clear(); return this; }
    // if (level.size() == 1) { _zis = std::move(level[0]); return this; }
    //
    // const size_t batchSize = _xis->size();
    //
    // while (level.size() > 1) {
    //     const size_t pairs = level.size() / 2;
    //     const size_t total = pairs * batchSize;
    //
    //     // 1) 拼接本轮要做 AND 的所有输入到两个大数组
    //     std::vector<int64_t> bigLeft;
    //     std::vector<int64_t> bigRight;
    //     bigLeft.reserve(total);
    //     bigRight.reserve(total);
    //
    //     for (size_t p = 0; p < pairs; ++p) {
    //         auto& L = level[2 * p];
    //         auto& R = level[2 * p + 1];
    //         // 逐批量拼接
    //         bigLeft.insert(bigLeft.end(), L.begin(), L.end());
    //         bigRight.insert(bigRight.end(), R.begin(), R.end());
    //     }
    //
    //     // 2) 一次性做 AND（可选：如果预取了 BMT，确保数量足够）
    //     std::vector<int64_t> bigOut;
    //     {
    //         BoolAndBatchOperator andOp(&bigLeft, &bigRight,
    //                                    /*widthPerElem=*/1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    //         if (gotBmt && allBmts.size() >= total) {
    //             andOp.setBmts(&allBmts);
    //         }
    //         bigOut = andOp.execute()->_zis;  // size == total
    //     }
    //
    //     // 3) 将 bigOut 拆分回下一层对应位置
    //     std::vector<std::vector<int64_t>> next;
    //     next.reserve((level.size() + 1) / 2);
    //
    //     for (size_t p = 0; p < pairs; ++p) {
    //         const size_t off = p * batchSize;
    //         next.emplace_back(bigOut.begin() + off, bigOut.begin() + off + batchSize);
    //     }
    //
    //     // 奇数个时，最后一个直接晋级
    //     if (level.size() % 2 == 1) {
    //         next.emplace_back(std::move(level.back()));
    //     }
    //
    //     level.swap(next);
    // }
    //
    // _zis = std::move(level[0]);
    // return this;
}

BoolEqualBatchOperator *BoolEqualBatchOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    this->_bmts = bmts;
    return this;
}

int BoolEqualBatchOperator::tagStride() {
    return std::max(BoolLessBatchOperator::tagStride(), BoolAndBatchOperator::tagStride());
}

int BoolEqualBatchOperator::bmtCount(int num, int width) {
    return BoolLessBatchOperator::bmtCount(2 * num, width) + BoolAndBatchOperator::bmtCount(num, width);
}

bool BoolEqualBatchOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}
