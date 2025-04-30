//
// Created by 杜建璋 on 2024/10/25.
//

#include "basis/AbstractRecord.h"
#include <iostream>
#include <iomanip>
#include "basis/Table.h"

// Template helper function to print a field value
template<typename T>
void printT(std::ostringstream &oss,
            const std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<
                int64_t> > &field) {
    oss << std::setw(10) << static_cast<int64_t>(std::get<T>(field).get());
}

// Optimized print function
void AbstractRecord::print(std::ostringstream &oss) const {
    for (int i = 0; i < _fieldValues.size(); i++) {
        int type = getType(i);
        switch (type) {
            case 1:
                printT<BitSecret>(oss, _fieldValues[i]);
                break;
            case 8:
                printT<IntSecret<int8_t> >(oss, _fieldValues[i]);
                break;
            case 16:
                printT<IntSecret<int16_t> >(oss, _fieldValues[i]);
                break;
            case 32:
                printT<IntSecret<int32_t> >(oss, _fieldValues[i]);
                break;
            default:
                printT<IntSecret<int64_t> >(oss, _fieldValues[i]);
                break;
        }
    }
    oss << std::endl;
}

template<typename T>
void addFieldT(
    std::vector<std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<
        int64_t> > > &fieldValues,
    const std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t> > &
    secret) {
    fieldValues.emplace_back(std::get<T>(secret));
}

void AbstractRecord::addField(
    std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t> > secret,
    int type) {
    addType(type);

    switch (type) {
        case 1:
            addFieldT<BitSecret>(this->_fieldValues, secret);
            break;
        case 8:
            addFieldT<IntSecret<int8_t> >(this->_fieldValues, secret);
            break;
        case 16:
            addFieldT<IntSecret<int16_t> >(this->_fieldValues, secret);
            break;
        case 32:
            addFieldT<IntSecret<int32_t> >(this->_fieldValues, secret);
            break;
        default:
            addFieldT<IntSecret<int64_t> >(this->_fieldValues, secret);
            break;
    }
}