//
// Created by 杜建璋 on 2025/3/22.
//

#include "comm/item/MpiRequestWrapper.h"

#include "conf/Conf.h"
#include "utils/Log.h"

MpiRequestWrapper::MpiRequestWrapper(bool recv) {
    _recv = recv;
}

void MpiRequestWrapper::wait() {
    MPI_Status status;
    MPI_Wait(_r, &status);

    if (Conf::ENABLE_TRANSFER_COMPRESSION && _recv && _mode != NO_CALLBACK) {
        switch (_mode) {
            case INT1:
                *_targetInt = _int1;
                break;
            case INT8:
                *_targetInt = _int8;
                break;
            case INT16:
                *_targetInt = _int16;
                break;
            case INT32:
                *_targetInt = _int32;
                break;
            case VEC1:
                _targetIntVec->resize(_vec1Size);
                for (int i = 0; i < _vec1Size; i++) {
                    (*_targetIntVec)[i] = _vec1[i];
                }
                delete[] _vec1;
                break;
            case VEC8:
                _targetIntVec->resize(_vec8.size());
                for (int i = 0; i < _vec8.size(); i++) {
                    (*_targetIntVec)[i] = _vec8[i];
                }
                break;
            case VEC16:
                _targetIntVec->resize(_vec16.size());
                for (int i = 0; i < _vec16.size(); i++) {
                    (*_targetIntVec)[i] = _vec16[i];
                }
                break;
            case VEC32:
                _targetIntVec->resize(_vec32.size());
                for (int i = 0; i < _vec32.size(); i++) {
                    (*_targetIntVec)[i] = _vec32[i];
                }
                break;
            default:
                break;
        }
    }
}
