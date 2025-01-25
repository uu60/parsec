//
// Created by 杜建璋 on 2024/11/20.
//

#ifndef ICOMM_H
#define ICOMM_H
#include <cstdint>
#include <string>


class Comm {
public:
    /**
     * 'impl' field is used to save implementation object of IComm.
     * This should be set value manually before using.
     */
    inline static Comm *impl = nullptr;

    virtual ~Comm() = default;

    /**
     * rank() should return the rank of the machine in this cluster.
     * Commonly, two computing servers should be 0 and 1. Other clients should be 2 or larger numbers.
     */
    virtual int rank_() = 0;

    static int rank() {
        return impl->rank_();
    }

    // init env
    virtual void init_(int argc, char **argv) = 0;

    static void init(int argc, char **argv);

    virtual void finalize_() = 0;

    static void finalize() {
        impl->finalize_();
    }

    // judge identity
    virtual bool isServer_() = 0;

    static bool isServer() {
        return impl->isServer_();
    }

    virtual bool isClient_() = 0;

    static bool isClient() {
        return impl->isClient_();
    }

    /**
     * Methods start with 'server' mean that the communication is between 2 servers. (3-Party server)
     */
    void serverSend_(const std::vector<int64_t> &source, int tag);

    static void serverSend(const std::vector<int64_t> &source, int tag) {
        impl->serverSend_(source, tag);
    }

    void serverSend_(const std::string &source, int tag);

    static void serverSend(const std::string &source, int tag) {
        impl->serverSend_(source, tag);
    }

    void serverReceive_(std::vector<int64_t> &source, int tag);

    static void serverReceive(std::vector<int64_t> &source, int tag) {
        impl->serverReceive_(source, tag);
    }

    void serverReceive_(std::string &target, int tag);

    static void serverReceive(std::string &target, int tag) {
        impl->serverReceive_(target, tag);
    }

    // send
    virtual void send_(const std::vector<int64_t> &source, int receiverRank, int tag) = 0;

    static void send(const std::vector<int64_t> &source, int receiverRank, int tag) {
        impl->send_(source, receiverRank, tag);
    }

    virtual void send_(const std::string &source, int receiverRank, int tag) = 0;

    static void send(const std::string &source, int receiverRank, int tag) {
        impl->send_(source, receiverRank, tag);
    }

    //receive
    virtual void receive_(std::vector<int64_t> &source, int senderRank, int tag) = 0;

    static void receive(std::vector<int64_t> &source, int senderRank, int tag) {
        impl->receive_(source, senderRank, tag);
    }

    virtual void receive_(std::string &target, int senderRank, int tag) = 0;

    static void receive(std::string &target, int senderRank, int tag) {
        impl->receive_(target, senderRank, tag);
    }
};


#endif //ICOMM_H
