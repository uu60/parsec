
#ifndef MPIREQUEST_H
#define MPIREQUEST_H
#include <cstdint>
#include <mpi.h>
#include <vector>

#include "AbstractRequest.h"


class MpiRequestWrapper : public AbstractRequest {
public:
    bool _recv{};

    int64_t *_targetInt{};
    std::vector<int64_t> *_targetIntVec{};

    enum Mode {
        INT1, INT8, INT16, INT32, VEC1, VEC8, VEC16, VEC32, NO_CALLBACK
    };

    Mode _mode = NO_CALLBACK;

    bool *_si1{};
    int8_t *_si8{};
    int16_t *_si16{};
    int32_t *_si32{};
    bool *_sv1{};
    std::vector<int8_t> *_sv8{};
    std::vector<int16_t> *_sv16{};
    std::vector<int32_t> *_sv32{};

    bool _int1{};
    int8_t _int8{};
    int16_t _int16{};
    int32_t _int32{};

    bool *_vec1{};
    int _vec1Size{};
    std::vector<int8_t> _vec8;
    std::vector<int16_t> _vec16;
    std::vector<int32_t> _vec32;

    MPI_Request *_r = new MPI_Request();

public:
    explicit MpiRequestWrapper(bool recv);
    
    ~MpiRequestWrapper() {
        delete _r;
        
        if (_si1) delete _si1;
        if (_si8) delete _si8;
        if (_si16) delete _si16;
        if (_si32) delete _si32;
        if (_sv1) delete[] _sv1;
        if (_sv8) delete _sv8;
        if (_sv16) delete _sv16;
        if (_sv32) delete _sv32;
        if (_vec1) delete[] _vec1;
    }

    void wait() override;
};



#endif
