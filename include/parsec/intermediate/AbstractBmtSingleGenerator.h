
#ifndef ABSTRACTBMTGENERATOR_H
#define ABSTRACTBMTGENERATOR_H
#include <cstdint>

#include "parsec/base/AbstractSingleOperator.h"
#include "parsec/conf/Conf.h"

namespace parsec {

template <class BmtT> class AbstractBmtSingleGenerator : public AbstractSingleOperator {
  public:
    BmtT _bmt{};
    int64_t _ui{};
    int64_t _vi{};

  protected:
    AbstractBmtSingleGenerator(int width, int taskTag, int msgTagOffset)
        : AbstractSingleOperator(width, taskTag, msgTagOffset) {}
};

} // namespace parsec

#endif
