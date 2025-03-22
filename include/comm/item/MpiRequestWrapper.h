//
// Created by 杜建璋 on 2025/3/22.
//

#ifndef MPIREQUEST_H
#define MPIREQUEST_H
#include <mpi.h>

#include "AbstractRequest.h"


class MpiRequestWrapper : public AbstractRequest {
public:
    MPI_Request *r = new MPI_Request();

    void wait() override;
};



#endif //MPIREQUEST_H
