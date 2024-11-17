//
// Created by 杜建璋 on 2024/7/7.
//

#ifndef MPC_PACKAGE_SECUREEXECUTOR_H
#define MPC_PACKAGE_SECUREEXECUTOR_H

#include <cstdint>
#include <string>
#include "./utils/System.h"
#include "./utils/Log.h"

using namespace std;

class SecureExecutor {
public:
    virtual ~SecureExecutor() = default;

    // result
    int64_t _result{};
    // unreconstructed share
    int64_t _zi{};

    // _l
    int _l{};

    explicit SecureExecutor(int l);

    // secret sharing process
    [[deprecated("This function should not be called.")]]
    virtual SecureExecutor *execute();

    [[deprecated("This function should not be called.")]]
    virtual SecureExecutor *reconstruct();

protected:
    [[nodiscard]] int64_t ring(int64_t raw) const;

    [[deprecated("This function should not be called.")]]
    [[nodiscard]] virtual std::string tag() const;
};

#endif //MPC_PACKAGE_SECUREEXECUTOR_H
