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

    [[nodiscard]] BitSecret not_() const;

    [[nodiscard]] BitSecret xor_(bool yi) const;

    [[nodiscard]] BitSecret xor_(BitSecret yi) const;

    [[nodiscard]] BitSecret and_(bool yi) const;

    [[nodiscard]] BitSecret and_(BitSecret yi) const;

    [[nodiscard]] BitSecret or_(bool yi) const;

    [[nodiscard]] BitSecret or_(BitSecret yi) const;

    [[nodiscard]] BitSecret mux(bool yi, bool ci) const;

    [[nodiscard]] BitSecret mux(BitSecret yi, BitSecret ci) const;

    BitSecret reconstruct() const;

    [[nodiscard]] bool get() const;

    // static methods for multiple usage
    static BitSecret share(bool x);

    static BitSecret xor_(bool xi, bool yi);

    static BitSecret xor_(BitSecret xi, BitSecret yi);

    static BitSecret and_(bool xi, bool yi);

    static BitSecret and_(BitSecret xi, BitSecret yi);

    static BitSecret or_(bool xi, bool yi);

    static BitSecret or_(BitSecret xi, BitSecret yi);

    static BitSecret mux(bool xi, bool yi, bool ci);

    static BitSecret mux(BitSecret xi, BitSecret yi, BitSecret ci);

    static BitSecret reconstruct(bool xi);

    static BitSecret reconstruct(BitSecret xi);
};


#endif //MPC_PACKAGE_BITSECRET_H
