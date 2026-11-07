#ifndef TCPREQUESTWRAPPER_H
#define TCPREQUESTWRAPPER_H

#include "parsecdb/comm/item/AbstractRequest.h"

#include <future>

namespace parsecdb {

class TcpRequestWrapper : public AbstractRequest {
  private:
    std::future<void> _future;

  public:
    explicit TcpRequestWrapper(std::future<void> future);

    void wait() override;
};

} // namespace parsecdb

#endif
