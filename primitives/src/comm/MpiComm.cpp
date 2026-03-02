#include "comm/MpiComm.h"
#include <mpi.h>
#include <iostream>
#include <limits>
#include <vector>

#include "comm/item/MpiRequestWrapper.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

#include <string>
void MpiComm::finalize_() {
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}

void MpiComm::init_(int argc, char **argv) {
    if (Conf::DISABLE_MULTI_THREAD) {
        MPI_Init(&argc, &argv);
    } else {
        int provided;
        int required = MPI_THREAD_MULTIPLE;
        MPI_Init_thread(&argc, &argv, required, &provided);
        if (provided < required) {
            std::cerr << "MPI implementation does not support the required thread level." << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    MPI_Comm_set_errhandler(MPI_COMM_SELF,  MPI_ERRORS_RETURN);
    MPI_Comm_rank(MPI_COMM_WORLD, &_mpiRank);
    MPI_Comm_size(MPI_COMM_WORLD, &_mpiSize);
    if (_mpiSize != 3) {
        throw std::runtime_error("3 parties restricted.");
    }
}

int MpiComm::rank_() {
    return _mpiRank;
}

void MpiComm::send_(int64_t source, int width, int receiverRank, int tag) {
    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 1) {
            auto s1 = static_cast<bool>(source);
            MPI_Send(&s1, 1, MPI_CXX_BOOL, receiverRank, tag, MPI_COMM_WORLD);
        } else if (width <= 8) {
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
    } else {
        MPI_Send(&source, 1, MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD);
    }
}

void MpiComm::send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 1) {
            int size = static_cast<int>(source.size());
            bool s1[size];
            for (int i = 0; i < size; i++) {
                s1[i] = source[i];
            }
            MPI_Send(s1, size, MPI_CXX_BOOL, receiverRank, tag, MPI_COMM_WORLD);
        } else if (width <= 8) {
            std::vector<int8_t> s8;
            s8.reserve(source.size());
            for (auto i: source) {
                s8.push_back(static_cast<int8_t>(i));
            }
            MPI_Send(s8.data(), s8.size(), MPI_INT8_T, receiverRank, tag, MPI_COMM_WORLD);
        } else if (width <= 16) {
            std::vector<int16_t> s16;
            s16.reserve(source.size());
            for (auto i: source) {
                s16.push_back(static_cast<int16_t>(i));
            }
            MPI_Send(s16.data(), static_cast<int>(s16.size()), MPI_INT16_T, receiverRank, tag, MPI_COMM_WORLD);
        } else if (width <= 32) {
            std::vector<int32_t> s32;
            s32.reserve(source.size());
            for (auto i: source) {
                s32.push_back(static_cast<int32_t>(i));
            }
            MPI_Send(s32.data(), s32.size(), MPI_INT32_T, receiverRank, tag, MPI_COMM_WORLD);
        } else {
            MPI_Send(source.data(), source.size(), MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD);
        }
    } else {
        MPI_Send(source.data(), source.size(), MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD);
    }
}

void MpiComm::send_(const std::string &source, int receiverRank, int tag) {
    MPI_Send(source.data(), static_cast<int>(source.length()), MPI_CHAR, receiverRank, tag, MPI_COMM_WORLD);
}

void MpiComm::receive_(int64_t &source, int width, int senderRank, int tag) {
    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 1) {
            bool temp;
            MPI_Recv(&temp, 1, MPI_CXX_BOOL, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            source = temp;
        } else if (width <= 8) {
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
    } else {
        MPI_Recv(&source, 1, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
}


void MpiComm::receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) {
    MPI_Status status;
    MPI_Probe(senderRank, tag, MPI_COMM_WORLD, &status);
    int count = 0;

    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 1) {
            MPI_Get_count(&status, MPI_CXX_BOOL, &count);
            bool temp[count];
            MPI_Recv(temp, count, MPI_CXX_BOOL, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            source.resize(count);
            for (int i = 0; i < count; ++i) {
                source[i] = temp[i];
            }
        } else if (width <= 8) {
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

    target.resize(count);
    MPI_Recv(&target[0], count, MPI_CHAR, senderRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

MpiRequestWrapper *MpiComm::sendAsync_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    auto *request = new MpiRequestWrapper(false);

    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 1) {
            int size = static_cast<int>(source.size());
            bool *s1 = new bool[size];
            for (int i = 0; i < size; i++) {
                s1[i] = source[i];
            }
            request->_mode = MpiRequestWrapper::VEC1;
            request->_sv1 = s1;
            MPI_Isend(s1, size, MPI_CXX_BOOL, receiverRank, tag, MPI_COMM_WORLD, request->_r);
        } else if (width <= 8) {
            auto *s8 = new std::vector<int8_t>;
            s8->resize(source.size());
            for (int i = 0; i < source.size(); i++) {
                (*s8)[i] = static_cast<int8_t>(source[i]);
            }
            request->_mode = MpiRequestWrapper::VEC8;
            request->_sv8 = s8;
            MPI_Isend(s8->data(), static_cast<int>(s8->size()), MPI_INT8_T, receiverRank, tag, MPI_COMM_WORLD,
                      request->_r);
        } else if (width <= 16) {
            auto *s16 = new std::vector<int16_t>;
            s16->resize(source.size());
            for (int i = 0; i < source.size(); i++) {
                (*s16)[i] = static_cast<int16_t>(source[i]);
            }
            request->_mode = MpiRequestWrapper::VEC16;
            request->_sv16 = s16;
            MPI_Isend(s16->data(), static_cast<int>(s16->size()), MPI_INT16_T, receiverRank, tag, MPI_COMM_WORLD,
                      request->_r);
        } else if (width <= 32) {
            auto *s32 = new std::vector<int32_t>;
            s32->resize(source.size());
            for (int i = 0; i < source.size(); i++) {
                (*s32)[i] = static_cast<int32_t>(source[i]);
            }
            request->_mode = MpiRequestWrapper::VEC32;
            request->_sv32 = s32;
            MPI_Isend(s32->data(), static_cast<int>(s32->size()), MPI_INT32_T, receiverRank, tag, MPI_COMM_WORLD,
                      request->_r);
        } else {
            MPI_Isend(source.data(), static_cast<int>(source.size()), MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD,
                      request->_r);
        }
    } else {
        MPI_Isend(source.data(), static_cast<int>(source.size()), MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD,
                  request->_r);
    }

    return request;
}

MpiRequestWrapper *MpiComm::sendAsync_(const int64_t &source, int width, int receiverRank, int tag) {
    auto *request = new MpiRequestWrapper(false);

    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 1) {
            auto s1 = new bool(source);
            request->_mode = MpiRequestWrapper::INT1;
            request->_si1 = s1;
            // send the allocated scalar buffer, not the pointer variable
            MPI_Isend(s1, 1, MPI_CXX_BOOL, receiverRank, tag, MPI_COMM_WORLD, request->_r);
        } else if (width <= 8) {
            auto s8 = new int8_t(static_cast<int8_t>(source));
            request->_mode = MpiRequestWrapper::INT8;
            request->_si8 = s8;
            MPI_Isend(s8, 1, MPI_INT8_T, receiverRank, tag, MPI_COMM_WORLD, request->_r);
        } else if (width <= 16) {
            auto s16 = new int16_t(static_cast<int16_t>(source));
            request->_mode = MpiRequestWrapper::INT16;
            request->_si16 = s16;
            MPI_Isend(s16, 1, MPI_INT16_T, receiverRank, tag, MPI_COMM_WORLD, request->_r);
        } else if (width <= 32) {
            auto s32 = new int32_t(static_cast<int32_t>(source));
            request->_mode = MpiRequestWrapper::INT32;
            request->_si32 = s32;
            MPI_Isend(s32, 1, MPI_INT32_T, receiverRank, tag, MPI_COMM_WORLD, request->_r);
        } else {
            MPI_Isend(&source, 1, MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD, request->_r);
        }
    } else {
        MPI_Isend(&source, 1, MPI_INT64_T, receiverRank, tag, MPI_COMM_WORLD, request->_r);
    }

    return request;
}

MpiRequestWrapper *MpiComm::sendAsync_(const std::string &source, int receiverRank, int tag) {
    auto *request = new MpiRequestWrapper(false);
    MPI_Isend(source.data(), static_cast<int>(source.length()), MPI_CHAR, receiverRank, tag, MPI_COMM_WORLD,
              request->_r);
    return request;
}

MpiRequestWrapper *MpiComm::receiveAsync_(int64_t &target, int width, int senderRank, int tag) {
    auto *request = new MpiRequestWrapper(true);
    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 64) {
            MPI_Irecv(&target, 1, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
        } else {
            request->_targetInt = &target;
            if (width == 1) {
                request->_mode = MpiRequestWrapper::INT1;
                MPI_Irecv(&request->_int1, 1, MPI_CXX_BOOL, senderRank, tag, MPI_COMM_WORLD, request->_r);
            } else if (width <= 8) {
                request->_mode = MpiRequestWrapper::INT8;
                MPI_Irecv(&request->_int8, 1, MPI_INT8_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
            } else if (width <= 16) {
                request->_mode = MpiRequestWrapper::INT16;
                MPI_Irecv(&request->_int16, 1, MPI_INT16_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
            } else if (width <= 32) {
                request->_mode = MpiRequestWrapper::INT32;
                MPI_Irecv(&request->_int32, 1, MPI_INT32_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
            }
        }
    } else {
        MPI_Irecv(&target, 1, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
    }
    return request;
}

MpiRequestWrapper *MpiComm::receiveAsync_(std::vector<int64_t> &target, int count, int width, int senderRank, int tag) {
    auto *request = new MpiRequestWrapper(true);
    if (Conf::ENABLE_TRANSFER_COMPRESSION) {
        if (width == 64) {
            target.resize(count);
            MPI_Irecv(target.data(), count, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
        } else {
            request->_targetIntVec = &target;
            if (width == 1) {
                request->_mode = MpiRequestWrapper::VEC1;
                request->_vec1 = new bool[count];
                request->_vec1Size = count;
                MPI_Irecv(request->_vec1, count, MPI_CXX_BOOL, senderRank, tag, MPI_COMM_WORLD, request->_r);
            } else if (width <= 8) {
                request->_mode = MpiRequestWrapper::VEC8;
                request->_vec8.resize(count);
                MPI_Irecv(request->_vec8.data(), count, MPI_INT8_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
            } else if (width <= 16) {
                request->_mode = MpiRequestWrapper::VEC16;
                request->_vec16.resize(count);
                MPI_Irecv(request->_vec16.data(), count, MPI_INT16_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
            } else if (width <= 32) {
                request->_mode = MpiRequestWrapper::VEC32;
                request->_vec32.resize(count);
                MPI_Irecv(request->_vec32.data(), count, MPI_INT32_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
            }
        }
    } else {
        target.resize(count);
        MPI_Irecv(target.data(), count, MPI_INT64_T, senderRank, tag, MPI_COMM_WORLD, request->_r);
    }
    return request;
}

MpiRequestWrapper *MpiComm::receiveAsync_(std::string &target, int length, int senderRank, int tag) {
    auto *request = new MpiRequestWrapper(true);
    target.resize(length);
    MPI_Irecv(&target[0], length, MPI_CHAR, senderRank, tag, MPI_COMM_WORLD, request->_r);
    return request;
}

bool MpiComm::isServer_() {
    return _mpiRank == 0 || _mpiRank == 1;
}

bool MpiComm::isClient_() {
    return !isServer_();
}

