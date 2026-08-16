
#include <utility>

#include "comm/Comm.h"

#include <vector>

#include "comm/MpiComm.h"
#include "comm/TcpComm.h"
#include "conf/Conf.h"

#include <chrono>
#include <string>

namespace {
int64_t currentTimeMillis() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}
}

#define MEASURE_EXECUTION_TIME(statement) \
int64_t start = 0; \
if (Conf::ENABLE_CLASS_WISE_TIMING) { \
start = currentTimeMillis(); \
} \
statement; \
if (Conf::ENABLE_CLASS_WISE_TIMING) { \
_totalTime += currentTimeMillis() - start; \
}

int Comm::rank() {
    return impl->rank_();
}

// Mirrors the narrowing ladder in MpiComm::send_(const std::vector<int64_t>&, ...):
// width==1 -> bool (1 B), <=8 -> int8, <=16 -> int16, <=32 -> int32, else int64.
// Without ENABLE_TRANSFER_COMPRESSION every value travels as int64 (8 B).
int Comm::wireBytesPerElem(int width) {
    if (!Conf::ENABLE_TRANSFER_COMPRESSION) {
        return 8;
    }
    if (width == 1) {
        return 1;
    }
    if (width <= 8) {
        return 1;
    }
    if (width <= 16) {
        return 2;
    }
    if (width <= 32) {
        return 4;
    }
    return 8;
}

void Comm::resetCounters() {
    _sentMessages = 0;
    _sentBytes = 0;
    _recvMessages = 0;
    _recvBytes = 0;
}

namespace {
inline void countSend(int64_t bytes) {
    Comm::_sentMessages.fetch_add(1, std::memory_order_relaxed);
    Comm::_sentBytes.fetch_add(bytes, std::memory_order_relaxed);
}

inline void countRecv(int64_t bytes) {
    Comm::_recvMessages.fetch_add(1, std::memory_order_relaxed);
    Comm::_recvBytes.fetch_add(bytes, std::memory_order_relaxed);
}
}

void Comm::init(int argc, char **argv) {
    if (Conf::COMM_TYPE == Conf::MPI) {
        impl = new MpiComm();
    } else if (Conf::COMM_TYPE == Conf::TCP) {
        impl = new TcpComm();
    }
    impl->init_(argc, argv);
}

void Comm::finalize() {
    impl->finalize_();
}

bool Comm::isServer() {
    return impl->isServer_();
}

bool Comm::isClient() {
    return impl->isClient_();
}

void Comm::serverSend(const int64_t &source, int width, int tag) {
    try {
        MEASURE_EXECUTION_TIME(send(source, width, 1 - rank(), tag));
    } catch (...) {}
}

void Comm::serverSend(const std::vector<int64_t> &source, int width, int tag) {
    try {
        MEASURE_EXECUTION_TIME(send(source, width, 1 - rank(), tag));
    } catch (...) {}
}

void Comm::serverSend(const std::string &source, int tag) {
    try {
        MEASURE_EXECUTION_TIME(send(source, 1 - rank(), tag));
    } catch (...) {}
}

void Comm::serverReceive(int64_t &source, int width, int tag) {
    try {
        MEASURE_EXECUTION_TIME(receive(source, width, 1 - rank(), tag));
    } catch (...) {}
}

void Comm::serverReceive(std::vector<int64_t> &source, int width, int tag) {
    try {
        MEASURE_EXECUTION_TIME(receive(source, width, 1 - rank(), tag));
    } catch (...) {}
}

void Comm::serverReceive(std::string &target, int tag) {
    try {
        MEASURE_EXECUTION_TIME(receive(target, 1 - rank(), tag));
    } catch (...) {}
}

void Comm::send(const int64_t &source, int width, int receiverRank, int tag) {
    countSend(wireBytesPerElem(width));
    try {
        MEASURE_EXECUTION_TIME(impl->send_(source, width, receiverRank, tag));
    } catch (...) {}
}

void Comm::send(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    countSend(static_cast<int64_t>(source.size()) * wireBytesPerElem(width));
    try {
        MEASURE_EXECUTION_TIME(impl->send_(source, width, receiverRank, tag));
    } catch (...) {}
}

void Comm::send(const std::string &source, int receiverRank, int tag) {
    countSend(static_cast<int64_t>(source.length()));
    try {
        MEASURE_EXECUTION_TIME(impl->send_(source, receiverRank, tag));
    } catch (...) {}
}

void Comm::receive(int64_t &source, int width, int senderRank, int tag) {
    try {
        MEASURE_EXECUTION_TIME(impl->receive_(source, width, senderRank, tag));
    } catch (...) {}
    countRecv(wireBytesPerElem(width));
}

void Comm::receive(std::vector<int64_t> &source, int width, int senderRank, int tag) {
    try {
        MEASURE_EXECUTION_TIME(impl->receive_(source, width, senderRank, tag));
    } catch (...) {}
    countRecv(static_cast<int64_t>(source.size()) * wireBytesPerElem(width));
}

void Comm::receive(std::string &target, int senderRank, int tag) {
    try {
        MEASURE_EXECUTION_TIME(impl->receive_(target, senderRank, tag));
    } catch (...) {}
    countRecv(static_cast<int64_t>(target.length()));
}

AbstractRequest *Comm::receiveAsync(int64_t &source, int width, int senderRank, int tag) {
    countRecv(wireBytesPerElem(width));
    try {
        return impl->receiveAsync_(source, width, senderRank, tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::receiveAsync(std::vector<int64_t> &source, int count, int width, int senderRank, int tag) {
    countRecv(static_cast<int64_t>(count) * wireBytesPerElem(width));
    try {
        return impl->receiveAsync_(source, count, width, senderRank, tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::receiveAsync(std::string &target, int length, int senderRank, int tag) {
    countRecv(static_cast<int64_t>(length));
    try {
        return impl->receiveAsync_(target, length, senderRank, tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::sendAsync(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    countSend(static_cast<int64_t>(source.size()) * wireBytesPerElem(width));
    try {
        return impl->sendAsync_(source, width, receiverRank, tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::sendAsync(const int64_t &source, int width, int receiverRank, int tag) {
    countSend(wireBytesPerElem(width));
    try {
        return impl->sendAsync_(source, width, receiverRank, tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::sendAsync(const std::string &source, int receiverRank, int tag) {
    countSend(static_cast<int64_t>(source.length()));
    try {
        return impl->sendAsync_(source, receiverRank, tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::serverSendAsync(const int64_t &source, int width, int tag) {
    try {
        return sendAsync(source, width, 1 - rank(), tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::serverSendAsync(const std::vector<int64_t> &source, int width, int tag) {
    try {
        return sendAsync(source, width, 1 - rank(), tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::serverSendAsync(const std::string &source, int tag) {
    try {
        return sendAsync(source, 1 - rank(), tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::serverReceiveAsync(int64_t &target, int width, int tag) {
    try {
        return receiveAsync(target, width, 1 - rank(), tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::serverReceiveAsync(std::vector<int64_t> &target, int count, int width, int tag) {
    try {
        return receiveAsync(target, count, width, 1 - rank(), tag);
    } catch (...) {
        return nullptr;
    }
}

AbstractRequest *Comm::serverReceiveAsync(std::string &target, int length, int tag) {
    try {
        return receiveAsync(target, length, 1 - rank(), tag);
    } catch (...) {
        return nullptr;
    }
}

void Comm::wait(AbstractRequest *request) {
    try {
        request->wait();
        delete request;
    } catch (...) {}
}
