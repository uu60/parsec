//
// Created by 杜建璋 on 2024/12/8.
//

#include <utility>

#include "comm/Comm.h"

#include <vector>

#include "conf/Conf.h"
#include "utils/System.h"

#define MEASURE_EXECUTION_TIME(statement) \
int64_t start = 0; \
if (Conf::CLASS_WISE_TIMING) { \
start = System::currentTimeMillis(); \
} \
statement; \
if (Conf::CLASS_WISE_TIMING) { \
_totalTime += System::currentTimeMillis() - start; \
}

void Comm::init(int argc, char **argv) {
    impl = Conf::COMM_IMPL;
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
    MEASURE_EXECUTION_TIME(impl->serverSend_(source, width, tag));
}

void Comm::serverSend(const std::vector<int64_t> &source, int width, int tag) {
    MEASURE_EXECUTION_TIME(impl->serverSend_(source, width, tag));
}

void Comm::serverSend(const std::string &source, int tag) {
    MEASURE_EXECUTION_TIME(impl->serverSend_(source, tag));
}

void Comm::serverReceive(int64_t &source, int width, int tag) {
    MEASURE_EXECUTION_TIME(impl->serverReceive_(source, width, tag));
}

void Comm::serverReceive(std::vector<int64_t> &source, int width, int tag) {
    MEASURE_EXECUTION_TIME(impl->serverReceive_(source, width, tag));
}

void Comm::serverReceive(std::string &target, int tag) {
    MEASURE_EXECUTION_TIME(impl->serverReceive_(target, tag));
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

void Comm::serverSend_(const int64_t &source, int width, int tag) {
    send_(source, width, 1 - rank_(), tag);
}

void Comm::serverSend_(const std::vector<int64_t> &source, int width, int tag) {
    send_(source, width, 1 - rank_(), tag);
}

void Comm::serverSend_(const std::string &source, int tag) {
    send_(source, 1 - rank_(), tag);
}

void Comm::serverReceive_(int64_t &source, int width, int tag) {
    receive_(source, width, 1 - rank_(), tag);
}

void Comm::serverReceive_(std::vector<int64_t> &source, int width, int tag) {
    receive_(source, width, 1 - rank_(), tag);
}

void Comm::serverReceive_(std::string &target, int tag) {
    receive_(target, 1 - rank_(), tag);
}
