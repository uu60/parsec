//
// Created by 杜建璋 on 2025/2/19.
//

#ifndef ABSTRACTBMTGENERATOR_H
#define ABSTRACTBMTGENERATOR_H
#include <cstdint>

#include "../base/AbstractSingleOperator.h"
#include "../conf/Conf.h"


template<class BmtT>
class AbstractBmtSingleGenerator : public AbstractSingleOperator {
public:
    BmtT _bmt{};
    int64_t _ui{};
    int64_t _vi{};

protected:
    AbstractBmtSingleGenerator(int width, int taskTag, int msgTagOffset) : AbstractSingleOperator(
        width, taskTag, msgTagOffset) {
    }
};


#endif //ABSTRACTBMTGENERATOR_H
