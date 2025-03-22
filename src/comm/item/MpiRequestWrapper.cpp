//
// Created by 杜建璋 on 2025/3/22.
//

#include "comm/item/MpiRequestWrapper.h"

void MpiRequestWrapper::wait() {
    MPI_Status status;
    MPI_Wait(r, &status);
}
