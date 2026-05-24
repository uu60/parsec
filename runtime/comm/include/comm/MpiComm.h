
#ifndef MPC_PACKAGE_COMM_H
#define MPC_PACKAGE_COMM_H
#include "./Comm.h"
#include "item/MpiRequestWrapper.h"

#include <string>

class MpiComm : public Comm {
public:
    static const int CLIENT_RANK;

private:
    int _mpiSize{};
    int _mpiRank{};

public:
    int rank_() override;

    void init_(int argc, char **argv) override;

    void finalize_() override;

    bool isServer_() override;

    bool isClient_() override;

    void send_(int64_t source, int width, int receiverRank, int tag) override;

    void send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) override;

    void send_(const std::string &source, int receiverRank, int tag) override;

    void receive_(int64_t &source, int width, int senderRank, int tag) override;

    void receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) override;

    void receive_(std::string &target, int senderRank, int tag) override;

    MpiRequestWrapper *sendAsync_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) override;

    MpiRequestWrapper *sendAsync_(const int64_t &source, int width, int receiverRank, int tag) override;

public:
    MpiRequestWrapper *sendAsync_(const std::string &source, int receiverRank, int tag) override;
    
    MpiRequestWrapper *receiveAsync_(int64_t &target, int width, int senderRank, int tag) override;
    
    MpiRequestWrapper *receiveAsync_(std::vector<int64_t> &target, int count, int width, int senderRank, int tag) override;
    
    MpiRequestWrapper *receiveAsync_(std::string &target, int length, int senderRank, int tag) override;
};


#endif
