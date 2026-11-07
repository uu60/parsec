
#ifndef ABSTRACTBMTBATCHGENERATOR_H
#define ABSTRACTBMTBATCHGENERATOR_H
#include "parsecdb/base/AbstractBatchOperator.h"
#include "parsecdb/conf/Conf.h"
#include "parsecdb/ot/RandOtBatchOperator.h"
#include "parsecdb/ot/RandOtOperator.h"

namespace parsecdb {

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

} // namespace parsecdb

#endif
