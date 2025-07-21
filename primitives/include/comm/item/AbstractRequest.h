//
// Created by 杜建璋 on 2025/3/22.
//

#ifndef ABSTRACTREQUEST_H
#define ABSTRACTREQUEST_H



class AbstractRequest {
public:
    virtual ~AbstractRequest() = default;

    virtual void wait() = 0;
};



#endif //ABSTRACTREQUEST_H
