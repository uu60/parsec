//
// Created by 杜建璋 on 2025/2/19.
//

#ifndef ABSTRACTBMTBATCHGENERATOR_H
#define ABSTRACTBMTBATCHGENERATOR_H
#include "../base/AbstractBatchExecutor.h"
#include "../conf/Conf.h"
#include "../ot/RandOtBatchExecutor.h"
#include "../ot/RandOtExecutor.h"

template <class BmtT>
class AbstractBmtBatchGenerator : public AbstractBatchExecutor {
public:
    std::vector<BmtT> _bmts{};
    std::vector<int64_t> _usi{};
    std::vector<int64_t> _vsi{};

protected:
    AbstractBmtBatchGenerator(int count, int l, int16_t taskTag, int16_t msgTagOffset) : AbstractBatchExecutor(l, taskTag, msgTagOffset) {
        if (Comm::isClient()) {
            return;
        }
        _bmts.reserve(count);
        for (int i = 0; i < count; i++) {
            _bmts.emplace_back();
        }
    }
};


#endif //ABSTRACTBMTBATCHGENERATOR_H
