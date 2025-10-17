
#ifndef MPC_PACKAGE_SECUREEXECUTOR_H
#define MPC_PACKAGE_SECUREEXECUTOR_H

#include <cstdint>
#include <atomic>
#include <string>
#include <cmath>
#include "utils/System.h"
#include "conf/Conf.h"

class SecureOperator {
public:
    static constexpr int NO_CLIENT_COMPUTE = -1;
    int _width{};

protected:
    
    int _taskTag{};
    int _startMsgTag{};
    std::atomic_int _currentMsgTag{};

public:
    
    virtual ~SecureOperator() = default;

    explicit SecureOperator(int width, int taskTag, int msgTagOffset) : _width(width), _taskTag(taskTag),
        _startMsgTag(msgTagOffset), _currentMsgTag(msgTagOffset) {
    }

    virtual SecureOperator *execute() = 0;

    virtual SecureOperator *reconstruct(int clientRank) = 0;

protected:
    [[nodiscard]] int64_t ring(int64_t raw) const;

    [[nodiscard]] int buildTag(int msgTag) const;
};

#endif
