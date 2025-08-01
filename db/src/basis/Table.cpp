//
// Created by 杜建璋 on 2024/10/25.
//

#include <iostream>
#include <iomanip>
#include <string>
#include <utility>

#include "../../include/basis/Table.h"

#include "utils/Log.h"
#include "utils/StringUtils.h"


Table::Table(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths, std::string keyField) {
    this->_tableName = tableName;
    this->_fieldNames = fieldNames;
    this->_fieldWidths = fieldWidths;
    this->_keyField = std::move(keyField);

    if (!this->_keyField.empty()) {
        this->_fieldNames.push_back(BUCKET_TAG_PREFIX + _keyField);
        this->_fieldWidths.push_back(32);
    }

    for (auto w: this->_fieldWidths) {
        _maxWidth = std::max(w, _maxWidth);
    }

    _dataCols.resize(_fieldNames.size(), {});
}

bool Table::insert(const std::vector<int64_t> &r) {
    if (r.size() != colNum()) {
        return false;
    }
    for (int i = 0; i < r.size(); i++) {
        _dataCols[i].push_back(r[i]);
    }
    return true;
}

int Table::colIndex(const std::string &colName) const {
    for (int i = 0; i < _dataCols.size(); i++) {
        if (_fieldNames[i] == colName) {
            return i;
        }
    }
    return -1;
}

size_t Table::colNum() const {
    return _dataCols.size();
}

size_t Table::rowNum() const {
    if (_dataCols.empty()) {
        return 0;
    }
    return _dataCols[0].size();
}
