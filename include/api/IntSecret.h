//
// Created by 杜建璋 on 2024/9/12.
//

#ifndef MPC_PACKAGE_INTSECRET_H
#define MPC_PACKAGE_INTSECRET_H

#include "./Secret.h"
#include "../utils/Comm.h"
#include "./BitSecret.h"
#include <vector>

template<typename T>
class IntSecret : public Secret {
private:
    T _data{};
public:
    explicit IntSecret(T x);

    [[nodiscard]] IntSecret share() const;

    [[nodiscard]] IntSecret add(T yi) const;

    [[nodiscard]] IntSecret add(IntSecret yi) const;

    [[nodiscard]] IntSecret mul(T yi) const;

    [[nodiscard]] IntSecret mul(IntSecret yi) const;

    [[nodiscard]] IntSecret reconstruct() const;

    [[nodiscard]] IntSecret mux(T yi, T ci) const;

    [[nodiscard]] IntSecret mux(IntSecret yi, IntSecret ci) const;

    [[nodiscard]] IntSecret boolean() const;

    [[nodiscard]] IntSecret arithmetic() const;

    [[nodiscard]] BitSecret compare(T yi) const;

    [[nodiscard]] BitSecret compare(IntSecret yi) const;

    [[nodiscard]] T get() const;

    // static methods for multiple usage
    static IntSecret share(T x);

    static IntSecret share(IntSecret x);

    static IntSecret add(T xi, T yi);

    static IntSecret add(IntSecret xi, IntSecret yi);

    static IntSecret mul(T xi, T yi);

    static IntSecret mul(IntSecret xi, IntSecret yi);

    static IntSecret sum(const std::vector<T> &xis);

    static IntSecret sum(const std::vector<IntSecret> &xis);

    static IntSecret sum(const std::vector<T> &xis, const std::vector<T> &yis);

    static IntSecret sum(const std::vector<IntSecret> &xis, const std::vector<IntSecret> &yis);

    static IntSecret product(const std::vector<T> &xis);

    static IntSecret product(const std::vector<IntSecret> &xis);

    static IntSecret dot(const std::vector<T> &xis, const std::vector<T> &yis);

    static IntSecret dot(const std::vector<IntSecret> &xis, const std::vector<IntSecret> &yis);

    static BitSecret compare(T xi, T yi);

    static BitSecret compare(IntSecret xi, IntSecret yi);

    static IntSecret mux(T xi, T yi, T ci);

    static IntSecret mux(IntSecret xi, IntSecret yi, IntSecret ci);
};


#endif //MPC_PACKAGE_INTSECRET_H
