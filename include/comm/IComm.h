//
// Created by 杜建璋 on 2024/11/20.
//

#ifndef ICOMM_H
#define ICOMM_H
#include <cstdint>
#include <string>


class IComm {
public:
    /**
     * 'impl' field is used to save implementation object of IComm.
     * This should be set value manually before using.
     */
    static IComm *impl;

    virtual ~IComm() = default;

    /**
     * rank() should return the rank of the machine in this cluster.
     * Commonly, two computing servers should be 0 and 1. Other clients should be 2 or larger numbers.
     */
    virtual int rank() = 0;

    // init env
    virtual void init(int argc, char **argv) = 0;

    virtual void finalize() = 0;

    // judge identity
    virtual bool isServer() = 0;

    virtual bool isClient() = 0;

    /**
     * Methods start with 'server' mean that the communication is between 2 servers. (3-Party server)
     */
    virtual void serverExchange(const int64_t *source, int64_t *target, int tag) = 0;

    virtual void serverExchange(const bool *source, bool *target, int tag) = 0;

    virtual void serverSend(const int64_t *source, int tag) = 0;

    virtual void serverSend(const bool *source, int tag) = 0;

    virtual void serverSend(const std::string *source, int tag) = 0;

    virtual void serverReceive(int64_t *target, int tag) = 0;

    virtual void serverReceive(bool *target, int tag) = 0;

    virtual void serverReceive(std::string *target, int tag) = 0;

    // send
    virtual void send(const int64_t *source, int receiverRank, int tag) = 0;

    virtual void send(const bool *source, int receiverRank, int tag) = 0;

    virtual void send(const std::string *source, int receiverRank, int tag) = 0;

    //receive
    virtual void receive(int64_t *target, int senderRank, int tag) = 0;

    virtual void receive(bool *target, int senderRank, int tag) = 0;

    virtual void receive(std::string *target, int senderRank, int tag) = 0;
};


#endif //ICOMM_H
