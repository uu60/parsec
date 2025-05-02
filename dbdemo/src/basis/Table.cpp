//
// Created by 杜建璋 on 2024/10/25.
//

#include <iostream>
#include <iomanip>

#include "../../include/basis/Table.h"

#include "utils/System.h"


Table::Table(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths) {
    this->_tableName = tableName;
    this->_fieldNames = fieldNames;
    this->_fieldWidths = fieldWidths;
    for (auto w: this->_fieldWidths) {
        _maxColWidth = std::max(w, _maxColWidth);
    }
    this->_cols = fieldNames.size();
    this->_taskTag = System::nextTask();
}

bool Table::insert(const std::vector<int64_t> &r) {
    if (_dataCols.size() == 0) {
        _dataCols.resize(_fieldNames.size(), {});
    }
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
    return EMPTY_COL;
}

int Table::getColWidth(const std::string &colName) const {
    for (int i = 0; i < _dataCols.size(); i++) {
        if (_fieldNames[i] == colName) {
            return _fieldWidths[i];
        }
    }
    return -1;
}
