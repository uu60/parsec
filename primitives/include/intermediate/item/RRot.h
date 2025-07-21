//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef RROT_H
#define RROT_H
#include <cstdint>

/*
 * Used by receiver in a Random OT process.
 */
class RRot {
public:
    int64_t _rb{};
    int _b{}; // true = 1, false = 0
};



#endif //RROT_H
