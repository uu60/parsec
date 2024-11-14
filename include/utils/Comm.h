//
// Created by 杜建璋 on 2024/7/15.
//

#ifndef MPC_PACKAGE_COMM_H
#define MPC_PACKAGE_COMM_H
#include <string>

/**
 * For sender, mpi rank must be 0 or 1.
 * The task publisher must be rank of 2.
 * Attention: Currently, there is no restriction in this util class.
 */
class Comm {
public:
    static const int CLIENT_RANK;

private:
    // mpi env init
    static bool _envInited;
    // joined party number
    static int _mpiSize;
    // _mpiRank of current device
    static int _mpiRank;

public:
    static bool inited();

    static int size();

    static int rank();

    // init env
    static void init(int argc, char **argv);

    static void finalize();

    // judge identity
    static bool isServer();

    static bool isClient();

    // exchange source (for rank of 0 and 1)
    static void sexch(const int64_t *source, int64_t *target);

    static void sexch(const bool *source, bool *target);

    // ssend
    static void ssend(const int64_t *source);

    static void ssend(const bool *source);

    static void ssend(const std::string *source);

    // srecv
    static void srecv(int64_t *target);

    static void srecv(bool *target);

    static void srecv(std::string *target);

    static void send(const int64_t *source, int receiverRank);

    static void send(const bool *source, int receiverRank);

    static void send(const std::string *source, int receiverRank);

    static void recv(int64_t *target, int senderRank);

    static void recv(bool *target, int senderRank);

    static void recv(std::string *target, int senderRank);

    // for int8
    // sexch functions for int8_t
    static void sexch(const int8_t *source, int8_t *target);

    static void ssend(const int8_t *source);

    static void srecv(int8_t *target);

    static void send(const int8_t *source, int receiverRank);

    static void recv(int8_t *target, int senderRank);

    // for int16
    static void sexch(const int16_t *source, int16_t *target);

    static void ssend(const int16_t *source);

    static void srecv(int16_t *target);

    static void send(const int16_t *source, int receiverRank);

    static void recv(int16_t *target, int senderRank);

    // for int32
    static void sexch(const int32_t *source, int32_t *target);

    static void ssend(const int32_t *source);

    static void srecv(int32_t *target);

    static void send(const int32_t *source, int receiverRank);

    static void recv(int32_t *target, int senderRank);
};


#endif //MPC_PACKAGE_COMM_H
