
#ifndef ABSTRACTREQUEST_H
#define ABSTRACTREQUEST_H



class AbstractRequest {
public:
    virtual ~AbstractRequest() = default;

    virtual void wait() = 0;
};



#endif
