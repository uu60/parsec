//
// Created by 杜建璋 on 2024/8/30.
//

#include "bmt/OtBmtGenerator.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Math.h"
#include "utils/Comm.h"

template<typename T>
OtBmtGenerator<T>::OtBmtGenerator() = default;

template<typename T>
void OtBmtGenerator<T>::generateRandomAB() {
    this->_ai = Math::randInt() & this->_mask;
    this->_bi = Math::randInt() & this->_mask;
}

template<typename T>
void OtBmtGenerator<T>::computeU() {
    computeMix(0, this->_ui);
}

template<typename T>
void OtBmtGenerator<T>::computeV() {
    computeMix(1, this->_vi);
}

template<typename T>
T OtBmtGenerator<T>::corr(int i, T x) const {
    return ((this->_ai << i) - x) & this->_mask;
}

template<typename T>
void OtBmtGenerator<T>::computeMix(int sender, T &mix) {
    bool isSender = Comm::rank() == sender;
    T sum = 0;
    for (int i = 0; i < this->_l; i++) {
        T s0 = 0, s1 = 0;
        int choice = 0;
        if (isSender) {
            s0 = Math::randInt(0, 1) & this->_mask;
            s1 = corr(i, s0);
        } else {
            choice = (int) ((this->_bi >> i) & 1);
        }
        RsaOtExecutor<T> r(sender, s0, s1, choice);
        r.execute(false);

        if (isSender) {
            sum = sum + s0;
        } else {
            T temp = r._result;
            if (choice == 0) {
                temp = (-temp) & this->_mask;
            }
            sum = sum + temp;
        }
    }
    mix = sum;
}

template<typename T>
void OtBmtGenerator<T>::computeC() {
    if (this->_l == 1) {
        this->_ci = this->_ai & this->_bi ^ this->_ui ^ this->_vi;
        return;
    }
    this->_ci = this->_ai * this->_bi + this->_ui + this->_vi;
}

template<typename T>
OtBmtGenerator<T> *OtBmtGenerator<T>::execute(bool dummy) {
    generateRandomAB();

    computeU();
    computeV();
    computeC();
    return this;
}

template<typename T>
std::string OtBmtGenerator<T>::tag() const {
    return "[BMT Generator]";
}

template class OtBmtGenerator<bool>;
template class OtBmtGenerator<int8_t>;
template class OtBmtGenerator<int16_t>;
template class OtBmtGenerator<int32_t>;
template class OtBmtGenerator<int64_t>;