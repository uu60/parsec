//
// Created by 杜建璋 on 2025/3/22.
//

#ifndef MPIREQUEST_H
#define MPIREQUEST_H
#include <cstdint>
#include <mpi.h>
#include <vector>

#include "AbstractRequest.h"


class MpiRequestWrapper : public AbstractRequest {
public:
    bool _recv{};

    // target pointer
    int64_t *_targetInt{};
    std::vector<int64_t> *_targetIntVec{};

    enum Mode {
        INT1, INT8, INT16, INT32, VEC1, VEC8, VEC16, VEC32, NO_CALLBACK
    };

    Mode _mode;

    // temp storage
    // int
    bool _int1;
    int8_t _int8;
    int16_t _int16;
    int32_t _int32;

    // vec<int>
    bool *_vec1;
    int _vec1Size;
    std::vector<int8_t> _vec8;
    std::vector<int16_t> _vec16;
    std::vector<int32_t> _vec32;

    MPI_Request *_r = new MPI_Request();

public:
    explicit MpiRequestWrapper(bool recv);

    void wait() override;
};



#endif //MPIREQUEST_H
