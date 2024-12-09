//
// Created by 杜建璋 on 2024/7/15.
//

#include "comm/MpiComm.h"
#include <mpi.h>
#include <iostream>
#include <limits>
#include <vector>

void MpiComm::finalize() {
    MPI_Finalize();
}

void MpiComm::init(int argc, char **argv) {
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

void MpiComm::serverExchange(const int64_t *source, int64_t *target, int tag) {
    serverSend(source, tag);
    serverReceive(target, tag);
}

void MpiComm::serverSend(const int64_t *source, int tag) {
    send(source, 1 - _mpiRank, tag);
}

void MpiComm::serverSend(const std::string *source, int tag) {
    send(source, 1 - _mpiRank, tag);
}

void MpiComm::serverReceive(int64_t *target, int tag) {
    receive(target, 1 - _mpiRank, tag);
}

void MpiComm::serverReceive(std::string *target, int tag) {
    receive(target, 1 - _mpiRank, tag);
}

int MpiComm::rank() {
    return _mpiRank;
}

void MpiComm::send(const int64_t *source, int receiverRank, int tag) {
    MPI_Send(source, 1, MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD);
}

void MpiComm::send(const std::string *source, int receiverRank, int tag) {
    if (source->length() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        std::cerr << "String size exceeds MPI_Send limit." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    MPI_Send(source->data(), static_cast<int>(source->length()), MPI_CHAR, receiverRank, tag, MPI_COMM_WORLD);
}

void MpiComm::receive(int64_t *target, int senderRank, int tag) {
    MPI_Recv(target, 1, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void MpiComm::receive(std::string *target, int senderRank, int tag) {
    MPI_Status status;
    MPI_Probe(senderRank, tag, MPI_COMM_WORLD, &status);

    int count;
    MPI_Get_count(&status, MPI_CHAR, &count);

    std::vector<char> buffer(count);
    MPI_Recv( buffer.data(), count, MPI_CHAR, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    *target = std::string(buffer.data(), count);
}

bool MpiComm::isServer() {
    return _mpiRank == 0 or _mpiRank == 1;
}

bool MpiComm::isClient() {
    return !isServer();
}

void MpiComm::send(const bool *source, int receiverRank, int tag) {
    MPI_Send(source, 1, MPI_CXX_BOOL, receiverRank, tag, MPI_COMM_WORLD);
}

void MpiComm::receive(bool *target, int senderRank, int tag) {
    MPI_Recv(target, 1, MPI_CXX_BOOL, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void MpiComm::serverSend(const bool *source, int tag) {
    send(source, 1 - _mpiRank, tag);
}

void MpiComm::serverReceive(bool *target, int tag) {
    receive(target, 1 - _mpiRank, tag);
}

void MpiComm::serverExchange(const bool *source, bool *target, int tag) {
    serverSend(source, tag);
    serverReceive(target, tag);
}