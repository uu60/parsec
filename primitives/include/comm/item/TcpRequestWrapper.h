#ifndef TCPREQUESTWRAPPER_H
#define TCPREQUESTWRAPPER_H

#include "AbstractRequest.h"

#include <future>

class TcpRequestWrapper : public AbstractRequest {
private:
    std::future<void> _future;

public:
    explicit TcpRequestWrapper(std::future<void> future);

    void wait() override;
};

#endif
