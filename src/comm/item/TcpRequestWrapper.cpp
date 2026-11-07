#include "parsec/comm/item/TcpRequestWrapper.h"

namespace parsec {

TcpRequestWrapper::TcpRequestWrapper(std::future<void> future) : _future(std::move(future)) {}

void TcpRequestWrapper::wait() { _future.get(); }

} // namespace parsec
