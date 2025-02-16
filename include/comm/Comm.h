//
// Created by 杜建璋 on 2024/11/20.
//

#ifndef ICOMM_H
#define ICOMM_H
#include <cstdint>
#include <string>
#include <vector>

class Comm {
public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    /**
     * 'impl' field is used to save implementation object of IComm.
     * This should be set value manually before using.
     */
    inline static Comm *impl = nullptr;

    virtual ~Comm() = default;

    static int rank() {
        return impl->rank_();
    }

    static void init(int argc, char **argv);

    static void finalize();

    static bool isServer();

    static bool isClient();

    static void serverSend(const int64_t &source, int width, int tag);

    static void serverSend(const std::vector<int64_t> &source, int width, int tag);

    static void serverSend(const std::string &source, int tag);

    static void serverReceive(int64_t &source, int width, int tag);

    static void serverReceive(std::vector<int64_t> &source, int width, int tag);

    static void serverReceive(std::string &target, int tag);

    static void send(const int64_t &source, int width, int receiverRank, int tag);

    static void send(const std::vector<int64_t> &source, int width, int receiverRank, int tag);

    static void send(const std::string &source, int receiverRank, int tag);

    static void receive(int64_t &source, int width, int senderRank, int tag);

    static void receive(std::vector<int64_t> &source, int width, int senderRank, int tag);

    static void receive(std::string &target, int senderRank, int tag);

protected:
    /**
     * rank() should return the rank of the machine in this cluster.
     * Commonly, two computing servers should be 0 and 1. Other clients should be 2 or larger numbers.
     */
    virtual int rank_() = 0;

    // init env
    virtual void init_(int argc, char **argv) = 0;

    virtual void finalize_() = 0;

    // judge identity
    virtual bool isServer_() = 0;

    virtual bool isClient_() = 0;

    /**
     * Methods start with 'server' mean that the communication is between 2 servers. (3-Party server)
     */
    void serverSend_(const int64_t &source, int width, int tag);

    void serverSend_(const std::vector<int64_t> &source, int width, int tag);

    void serverSend_(const std::string &source, int tag);

    void serverReceive_(int64_t &source, int width, int tag);

    void serverReceive_(std::vector<int64_t> &source, int width, int tag);

    void serverReceive_(std::string &target, int tag);

    virtual void send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) = 0;

    // send
    virtual void send_(int64_t source, int width, int receiverRank, int tag) = 0;

    virtual void send_(const std::string &source, int receiverRank, int tag) = 0;

    //receive
    virtual void receive_(int64_t &source, int width, int senderRank, int tag) = 0;

    virtual void receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) = 0;

    virtual void receive_(std::string &target, int senderRank, int tag) = 0;
};


#endif //ICOMM_H
