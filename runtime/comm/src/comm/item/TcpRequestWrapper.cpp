#include "comm/item/TcpRequestWrapper.h"

TcpRequestWrapper::TcpRequestWrapper(std::future<void> future) : _future(std::move(future)) {}

void TcpRequestWrapper::wait() {
    _future.get();
}
