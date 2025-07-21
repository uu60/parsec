//
// Created by 杜建璋 on 2025/2/19.
//

#ifndef ABSTRACTBMTBATCHGENERATOR_H
#define ABSTRACTBMTBATCHGENERATOR_H
#include "../base/AbstractBatchOperator.h"
#include "../conf/Conf.h"
#include "../ot/RandOtBatchOperator.h"
#include "../ot/RandOtOperator.h"

template <class BmtT>
class AbstractBmtBatchGenerator : public AbstractBatchOperator {
public:
    std::vector<BmtT> _bmts{};
    std::vector<int64_t> _usi{};
    std::vector<int64_t> _vsi{};

protected:
    AbstractBmtBatchGenerator(int count, int width, int taskTag, int msgTagOffset) : AbstractBatchOperator(width, taskTag, msgTagOffset) {
        if (Comm::isClient()) {
            return;
        }
        _bmts.resize(count);
    }
};


#endif //ABSTRACTBMTBATCHGENERATOR_H
