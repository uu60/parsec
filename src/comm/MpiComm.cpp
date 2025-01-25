//
// Created by 杜建璋 on 2024/7/15.
//

#include "comm/MpiComm.h"
#include <mpi.h>
#include <iostream>
#include <limits>
#include <vector>

#include "utils/Log.h"

void MpiComm::finalize_() {
    MPI_Finalize();
}

void MpiComm::init_(int argc, char **argv) {
    // init
    int provided;
    int required = MPI_THREAD_MULTIPLE;
    MPI_Init_thread(&argc, &argv, required, &provided);
    if (provided < required) {
        std::cerr << "MPI implementation does not support the required thread level!" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    // process _mpiRank and sum
    MPI_Comm_rank(MPI_COMM_WORLD, &_mpiRank);
    MPI_Comm_size(MPI_COMM_WORLD, &_mpiSize);
    if (_mpiSize != 3) {
        throw std::runtime_error("3 parties restricted.");
    }
}

int MpiComm::rank_() {
    return _mpiRank;
}

void MpiComm::send_(const std::vector<int64_t> &source, int receiverRank, int tag) {
    MPI_Send(source.data(), source.size(), MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD);
}

void MpiComm::send_(const std::string &source, int receiverRank, int tag) {
    MPI_Send(source.data(), static_cast<int>(source.length()), MPI_CHAR, receiverRank, tag, MPI_COMM_WORLD);
}

void MpiComm::receive_(std::vector<int64_t> &source, int senderRank, int tag) {
    MPI_Status status;
    MPI_Probe(senderRank, tag, MPI_COMM_WORLD, &status);

    int count;
    MPI_Get_count(&status, MPI_INT64_T, &count);

    source.resize(count);
    MPI_Recv(source.data(), count, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void MpiComm::receive_(std::string &target, int senderRank, int tag) {
    MPI_Status status;
    MPI_Probe(senderRank, tag, MPI_COMM_WORLD, &status);

    int count;
    MPI_Get_count(&status, MPI_CHAR, &count);

    std::vector<char> buffer(count);
    MPI_Recv( buffer.data(), count, MPI_CHAR, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    target = std::string(buffer.data(), count);
}

bool MpiComm::isServer_() {
    return _mpiRank == 0 or _mpiRank == 1;
}

bool MpiComm::isClient_() {
    return !isServer_();
}