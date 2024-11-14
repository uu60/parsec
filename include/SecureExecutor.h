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

template<typename T>
class SecureExecutor {
public:
    // result
    T _result{};
    // unreconstructed share
    T _zi{};

    // _l
    const int _l = std::is_same_v<T, bool> ? 1 : sizeof(_result) * 8;

    // secret sharing process
    [[deprecated("This function should not be called.")]]
    virtual SecureExecutor *execute(bool reconstruct);

    [[deprecated("This function should not be called.")]]
    virtual SecureExecutor *reconstruct();

protected:
    [[deprecated("This function should not be called.")]]
    [[nodiscard]] virtual std::string tag() const;

    virtual void finalize();
};

#endif //MPC_PACKAGE_SECUREEXECUTOR_H
