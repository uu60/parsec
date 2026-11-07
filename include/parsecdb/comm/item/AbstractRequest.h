
#ifndef ABSTRACTREQUEST_H
#define ABSTRACTREQUEST_H

namespace parsecdb {

class AbstractRequest {
  public:
    virtual ~AbstractRequest() = default;

    virtual void wait() = 0;
};

} // namespace parsecdb

#endif
