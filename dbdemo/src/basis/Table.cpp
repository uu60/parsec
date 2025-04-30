//
// Created by 杜建璋 on 2024/10/25.
//

#include <utility>

#include <iostream>
#include <iomanip>

#include "../../include/basis/Table.h"


Table::Table(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths) {
    this->_tableName = tableName;
    this->_fieldNames = fieldNames;
    this->_fieldWidths = fieldWidths;
    for (auto w : this->_fieldWidths) {
        _maxColWidth = std::max(w, _maxColWidth);
    }
    this->_cols = fieldNames.size();
    for (int i = 0; i < this->_cols; i++) {
        this->_dataCols.push_back(std::vector<int64_t>());
    }
}

bool Table::insert(const std::vector<int64_t> &r) {
    if (r.size() != _fieldNames.size()) {
        return false;
    }
    for (int i = 0; i < r.size(); i++) {
        _dataCols[i].push_back(r[i]);
    }
    return true;
}

const std::vector<int64_t> &Table::getColData(std::string colName) {
    for (int i = 0; i < _dataCols.size(); i++) {
        if (_fieldNames[i] == colName) {
            return _dataCols[i];
        }
    }
    return {};
}

int Table::getColWidth(const std::string &colName) const {
    for (int i = 0; i < _dataCols.size(); i++) {
        if (_fieldNames[i] == colName) {
            return _fieldWidths[i];
        }
    }
    return -1;
}
