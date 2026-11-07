
#ifndef ABSTRACTREQUEST_H
#define ABSTRACTREQUEST_H

namespace parsec {

class AbstractRequest {
  public:
    virtual ~AbstractRequest() = default;

    virtual void wait() = 0;
};

} // namespace parsec

#endif
