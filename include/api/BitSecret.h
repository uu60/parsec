//
// Created by 杜建璋 on 2024/9/13.
//

#ifndef MPC_PACKAGE_BITSECRET_H
#define MPC_PACKAGE_BITSECRET_H

#include "./Secret.h"

class BitSecret : public Secret {
private:
    bool _data{};
public:
    explicit BitSecret(bool x);

    BitSecret share() const;

    [[nodiscard]] BitSecret xor_(bool yi) const;

    [[nodiscard]] BitSecret xor_(BitSecret yi) const;

    [[nodiscard]] BitSecret and_(bool yi) const;

    [[nodiscard]] BitSecret and_(BitSecret yi) const;

    BitSecret reconstruct() const;

    [[nodiscard]] bool get() const;

    // static methods for multiple usage
    static BitSecret share(bool x);

    static BitSecret xor_(bool xi, bool yi);

    static BitSecret xor_(BitSecret xi, BitSecret yi);

    static BitSecret and_(bool xi, bool yi);

    static BitSecret and_(BitSecret xi, BitSecret yi);
};


#endif //MPC_PACKAGE_BITSECRET_H
