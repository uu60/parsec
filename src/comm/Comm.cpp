//
// Created by 杜建璋 on 2024/12/8.
//

#include <utility>

#include "comm/Comm.h"

#include <vector>

#include "comm/MpiComm.h"
#include "conf/Conf.h"
#include "utils/System.h"

#define MEASURE_EXECUTION_TIME(statement) \
int64_t start = 0; \
if (Conf::ENABLE_CLASS_WISE_TIMING) { \
start = System::currentTimeMillis(); \
} \
statement; \
if (Conf::ENABLE_CLASS_WISE_TIMING) { \
_totalTime += System::currentTimeMillis() - start; \
}

int Comm::rank() {
    return impl->rank_();
}

void Comm::init(int argc, char **argv) {
    if (Conf::COMM_TYPE == Conf::MPI) {
        impl = new MpiComm();
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
    MEASURE_EXECUTION_TIME(send(source, width, 1 - rank(), tag));
}

void Comm::serverSend(const std::vector<int64_t> &source, int width, int tag) {
    MEASURE_EXECUTION_TIME(send(source, width, 1 - rank(), tag));
}

void Comm::serverSend(const std::string &source, int tag) {
    MEASURE_EXECUTION_TIME(send(source, 1 - rank(), tag));
}

void Comm::serverReceive(int64_t &source, int width, int tag) {
    MEASURE_EXECUTION_TIME(receive(source, width, 1 - rank(), tag));
}

void Comm::serverReceive(std::vector<int64_t> &source, int width, int tag) {
    MEASURE_EXECUTION_TIME(receive(source, width, 1 - rank(), tag));
}

void Comm::serverReceive(std::string &target, int tag) {
    MEASURE_EXECUTION_TIME(receive(target, 1 - rank(), tag));
}

void Comm::send(const int64_t &source, int width, int receiverRank, int tag) {
    MEASURE_EXECUTION_TIME(impl->send_(source, width, receiverRank, tag));
}

void Comm::send(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    MEASURE_EXECUTION_TIME(impl->send_(source, width, receiverRank, tag));
}

void Comm::send(const std::string &source, int receiverRank, int tag) {
    MEASURE_EXECUTION_TIME(impl->send_(source, receiverRank, tag));
}

void Comm::receive(int64_t &source, int width, int senderRank, int tag) {
    MEASURE_EXECUTION_TIME(impl->receive_(source, width, senderRank, tag));
}

void Comm::receive(std::vector<int64_t> &source, int width, int senderRank, int tag) {
    MEASURE_EXECUTION_TIME(impl->receive_(source, width, senderRank, tag));
}

void Comm::receive(std::string &target, int senderRank, int tag) {
    MEASURE_EXECUTION_TIME(impl->receive_(target, senderRank, tag));
}

AbstractRequest *Comm::receiveAsync(int64_t &source, int width, int senderRank, int tag) {
    return impl->receiveAsync_(source, width, senderRank, tag);
}

AbstractRequest *Comm::receiveAsync(std::vector<int64_t> &source, int count, int width, int senderRank, int tag) {
    return impl->receiveAsync_(source, count, width, senderRank, tag);
}

AbstractRequest *Comm::receiveAsync(std::string &target, int length, int senderRank, int tag) {
    return impl->receiveAsync_(target, length, senderRank, tag);
}

AbstractRequest *Comm::sendAsync(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    return impl->sendAsync_(source, width, receiverRank, tag);
}

AbstractRequest *Comm::sendAsync(const int64_t &source, int width, int receiverRank, int tag) {
    return impl->sendAsync_(source, width, receiverRank, tag);
}

AbstractRequest *Comm::sendAsync(const std::string &source, int receiverRank, int tag) {
    return impl->sendAsync_(source, receiverRank, tag);
}

AbstractRequest *Comm::serverSendAsync(const int64_t &source, int width, int tag) {
    return sendAsync(source, width, 1 - rank(), tag);
}

AbstractRequest *Comm::serverSendAsync(const std::vector<int64_t> &source, int width, int tag) {
    return sendAsync(source, width, 1 - rank(), tag);
}

AbstractRequest *Comm::serverSendAsync(const std::string &source, int tag) {
    return sendAsync(source, 1 - rank(), tag);
}

AbstractRequest *Comm::serverReceiveAsync(int64_t &target, int width, int tag) {
    return receiveAsync(target, width, 1 - rank(), tag);
}

AbstractRequest *Comm::serverReceiveAsync(std::vector<int64_t> &target, int count, int width, int tag) {
    return receiveAsync(target, count, width, 1 - rank(), tag);
}

AbstractRequest *Comm::serverReceiveAsync(std::string &target, int length, int tag) {
    return receiveAsync(target, length, 1 - rank(), tag);
}

void Comm::wait(AbstractRequest *request) {
    request->wait();
    delete request;
}
