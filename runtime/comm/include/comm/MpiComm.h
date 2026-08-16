
#ifndef MPC_PACKAGE_COMM_H
#define MPC_PACKAGE_COMM_H
#include "./Comm.h"
#include "item/MpiRequestWrapper.h"

#include <mpi.h>
#include <string>
#include <vector>

class MpiComm : public Comm {
public:
    static const int CLIENT_RANK;

private:
    int _mpiSize{};
    int _mpiRank{};

    // ---- packed-tag decoding (added 2026-08-13) --------------------------
    // Operators build tags as (taskTag << (32 - Conf::TASK_TAG_BITS)) | msgTag
    // (SecureOperator::buildTag). Handing that value to MPI as-is exceeds
    // MPI_TAG_UB on PMLs with a small tag field (OpenMPI ucx: 2^23-1) and, for
    // taskTag >= 2^(TASK_TAG_BITS-1), sets bit 31 (negative tag, invalid on
    // every PML). Streams that differ only in taskTag then silently collide.
    //
    // Fix: the packed value is a *logical* stream id. MpiComm decodes it into
    // (per-task communicator, msgTag) right before every MPI call, so task
    // isolation rides on communicators (an unbounded, standard matching
    // dimension) and only msgTag reaches MPI as the tag.
    int _tagUb{};                        // MPI_TAG_UB of MPI_COMM_WORLD
    std::vector<MPI_Comm> _taskComms;    // one MPI_Comm_dup per taskTag value

    // Splits `packedTag` in place into the raw MPI tag (written back to
    // `packedTag`) and returns the communicator for its taskTag. Aborts if the
    // resulting tag exceeds MPI_TAG_UB — a loud failure instead of silent
    // stream corruption.
    MPI_Comm resolveTag(int &packedTag);

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
