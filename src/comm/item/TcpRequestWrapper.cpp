#include "parsecdb/comm/item/TcpRequestWrapper.h"

namespace parsecdb {

TcpRequestWrapper::TcpRequestWrapper(std::future<void> future) : _future(std::move(future)) {}

void TcpRequestWrapper::wait() { _future.get(); }

} // namespace parsecdb
