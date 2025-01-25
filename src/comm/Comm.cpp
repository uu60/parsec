//
// Created by 杜建璋 on 2024/12/8.
//

#include <utility>

#include "comm/Comm.h"

#include <vector>

#include "conf/Conf.h"

void Comm::init(int argc, char **argv) {
    impl = Conf::COMM_IMPL;
    impl->init_(argc, argv);
}

void Comm::serverSend_(const std::vector<int64_t> &source, int tag) {
    send_(source, 1 - rank_(), tag);
}

void Comm::serverSend_(const std::string &source, int tag) {
    send_(source, 1 - rank_(), tag);
}

void Comm::serverReceive_(std::vector<int64_t> &source, int tag) {
    receive_(source, 1 - rank_(), tag);
}

void Comm::serverReceive_(std::string &target, int tag) {
    receive_(target, 1 - rank_(), tag);
}