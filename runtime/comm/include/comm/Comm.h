
#ifndef ICOMM_H
#define ICOMM_H
#include <cstdint>
#include <atomic>
#include <string>
#include <vector>

#include "item/AbstractRequest.h"

class Comm {
public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    inline static Comm *impl = nullptr;

    virtual ~Comm() = default;

    static int rank();

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

    static AbstractRequest *receiveAsync(int64_t &source, int width, int senderRank, int tag);

    static AbstractRequest *receiveAsync(std::vector<int64_t> &source, int count, int width, int senderRank, int tag);

    static AbstractRequest *receiveAsync(std::string &target, int length, int senderRank, int tag);

    static AbstractRequest *sendAsync(const std::vector<int64_t> &source, int width, int receiverRank, int tag);

    static AbstractRequest *sendAsync(const int64_t &source, int width, int receiverRank, int tag);

    static AbstractRequest *sendAsync(const std::string &source, int receiverRank, int tag);

    static AbstractRequest *serverSendAsync(const int64_t &source, int width, int tag);

    static AbstractRequest *serverSendAsync(const std::vector<int64_t> &source, int width, int tag);

    static AbstractRequest *serverSendAsync(const std::string &source, int tag);

    static AbstractRequest *serverReceiveAsync(int64_t &target, int width, int tag);

    static AbstractRequest *serverReceiveAsync(std::vector<int64_t> &target, int count, int width, int tag);

    static AbstractRequest *serverReceiveAsync(std::string &target, int length, int tag);

    static void wait(AbstractRequest *request);

protected:
    virtual int rank_() = 0;

    virtual void init_(int argc, char **argv) = 0;

    virtual void finalize_() = 0;

    virtual bool isServer_() = 0;

    virtual bool isClient_() = 0;

    virtual void send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) = 0;

    virtual void send_(int64_t source, int width, int receiverRank, int tag) = 0;

    virtual void send_(const std::string &source, int receiverRank, int tag) = 0;

    virtual void receive_(int64_t &source, int width, int senderRank, int tag) = 0;

    virtual void receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) = 0;

    virtual void receive_(std::string &target, int senderRank, int tag) = 0;

    virtual AbstractRequest *sendAsync_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) = 0;

    virtual AbstractRequest *sendAsync_(const int64_t &source, int width, int receiverRank, int tag) = 0;

    virtual AbstractRequest *sendAsync_(const std::string &source, int receiverRank, int tag) = 0;

    virtual AbstractRequest *receiveAsync_(int64_t &source, int width, int senderRank, int tag) = 0;

    virtual AbstractRequest *receiveAsync_(std::vector<int64_t> &source, int count, int width, int senderRank, int tag)
    = 0;

    virtual AbstractRequest *receiveAsync_(std::string &target, int length, int senderRank, int tag) = 0;
};


#endif
