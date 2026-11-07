
#ifndef ABSTRACTBMTBATCHGENERATOR_H
#define ABSTRACTBMTBATCHGENERATOR_H
#include "parsec/base/AbstractBatchOperator.h"
#include "parsec/conf/Conf.h"
#include "parsec/ot/RandOtBatchOperator.h"
#include "parsec/ot/RandOtOperator.h"

namespace parsec {

template <class BmtT> class AbstractBmtBatchGenerator : public AbstractBatchOperator {
  public:
    std::vector<BmtT> _bmts{};
    std::vector<int64_t> _usi{};
    std::vector<int64_t> _vsi{};

  protected:
    AbstractBmtBatchGenerator(int count, int width, int taskTag, int msgTagOffset)
        : AbstractBatchOperator(width, taskTag, msgTagOffset) {
        if (Comm::isClient()) {
            return;
        }
        _bmts.resize(count);
    }
};

} // namespace parsec

#endif
