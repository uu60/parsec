//
// Created by 杜建璋 on 2024/7/15.
//

#include "comm/MpiComm.h"
#include <mpi.h>
#include <iostream>
#include <limits>
#include <vector>

#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
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
        std::cerr << "MPI implementation does not support the required thread level." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    // process _mpiRank and sum
    MPI_Comm_rank(MPI_COMM_WORLD, &_mpiRank);
    MPI_Comm_size(MPI_COMM_WORLD, &_mpiSize);
    if (_mpiSize != 3) {
        throw std::runtime_error("3 parties restricted.");
    }

    IntermediateDataSupport::prepareRot();

    if (Conf::BMT_BACKGROUND) {
        IntermediateDataSupport::startGenerateBmtsAsync();
        IntermediateDataSupport::startGenerateBitwiseBmtsAsync();
    }
}

int MpiComm::rank_() {
    return _mpiRank;
}

void MpiComm::send_(int64_t source, int width, int receiverRank, int tag) {
    if (width <= 8) {
        auto s8 = static_cast<int8_t>(source);
        MPI_Send(&s8, 1, MPI_INT8_T, receiverRank, tag, MPI_COMM_WORLD);
    } else if (width <= 16) {
        auto s16 = static_cast<int16_t>(source);
        MPI_Send(&s16, 1, MPI_INT16_T, receiverRank, tag, MPI_COMM_WORLD);
    } else if (width <= 32) {
        auto s32 = static_cast<int32_t>(source);
        MPI_Send(&s32, 1, MPI_INT32_T, receiverRank, tag, MPI_COMM_WORLD);
    } else {
        MPI_Send(&source, 1, MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD);
    }
}

void MpiComm::send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    if (width <= 8) {
        std::vector<int8_t> s8;
        s8.reserve(source.size());
        for (auto i : source) {
            s8.push_back(static_cast<int8_t>(i));
        }
        MPI_Send(s8.data(), s8.size(), MPI_INT8_T, receiverRank, tag, MPI_COMM_WORLD);
    } else if (width <= 16) {
        std::vector<int16_t> s16;
        s16.reserve(source.size());
        for (auto i : source) {
            s16.push_back(static_cast<int16_t>(i));
        }
        MPI_Send(s16.data(), s16.size(), MPI_INT16_T, receiverRank, tag, MPI_COMM_WORLD);
    } else if (width <= 32) {
        std::vector<int32_t> s32;
        s32.reserve(source.size());
        for (auto i : source) {
            s32.push_back(static_cast<int32_t>(i));
        }
        MPI_Send(s32.data(), s32.size(), MPI_INT32_T, receiverRank, tag, MPI_COMM_WORLD);
    } else {
        MPI_Send(source.data(), source.size(), MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD);
    }
}

void MpiComm::send_(const std::string &source, int receiverRank, int tag) {
    MPI_Send(source.data(), static_cast<int>(source.length()), MPI_CHAR, receiverRank, tag, MPI_COMM_WORLD);
}

void MpiComm::receive_(int64_t &source, int width, int senderRank, int tag) {
    if (width <= 8) {
        int8_t temp;
        MPI_Recv(&temp, 1, MPI_INT8_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        source = temp;
    } else if (width <= 16) {
        int16_t temp;
        MPI_Recv(&temp, 1, MPI_INT16_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        source = temp;
    } else if (width <= 32) {
        int32_t temp;
        MPI_Recv(&temp, 1, MPI_INT32_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        source = temp;
    } else {
        MPI_Recv(&source, 1, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
}


void MpiComm::receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) {
    MPI_Status status;
    MPI_Probe(senderRank, tag, MPI_COMM_WORLD, &status);
    int count = 0;

    if (width <= 8) {
        MPI_Get_count(&status, MPI_INT8_T, &count);
        std::vector<int8_t> temp(count);
        MPI_Recv(temp.data(), count, MPI_INT8_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        source.resize(count);
        for (int i = 0; i < count; ++i) {
            source[i] = temp[i];
        }
    } else if (width <= 16) {
        MPI_Get_count(&status, MPI_INT16_T, &count);
        std::vector<int16_t> temp(count);
        MPI_Recv(temp.data(), count, MPI_INT16_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        source.resize(count);
        for (int i = 0; i < count; ++i) {
            source[i] = temp[i];
        }
    } else if (width <= 32) {
        MPI_Get_count(&status, MPI_INT32_T, &count);
        std::vector<int32_t> temp(count);
        MPI_Recv(temp.data(), count, MPI_INT32_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        source.resize(count);
        for (int i = 0; i < count; ++i) {
            source[i] = temp[i];
        }
    } else {
        MPI_Get_count(&status, MPI_INT64_T, &count);
        source.resize(count);
        MPI_Recv(source.data(), count, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
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
