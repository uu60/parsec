//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_RECORD_H
#define SMPC_DATABASE_RECORD_H

#include <iostream>
#include <sstream>
#include <vector>
#include <variant>

class AbstractRecord {
public:
    std::vector<int64_t> _fieldValues;

    AbstractRecord() = default;

    void addField(int64_t value, int type);

    void print(std::ostringstream& oss) const;

    [[nodiscard]] virtual int getType(int idx) const = 0;

    virtual void addType(int type) = 0;

    [[nodiscard]] virtual int getIdx(const std::string& fieldName) const = 0;
};


#endif //SMPC_DATABASE_RECORD_H
