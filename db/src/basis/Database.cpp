//
// Created by 杜建璋 on 2024/10/25.
//

#include <string>
#include <utility>

#include "../../include/basis/Database.h"

Database::Database(std::string databaseName) {
    this->_databaseName = std::move(databaseName);
}

std::string Database::name() {
    return this->_databaseName;
}

bool
Database::createTable(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths,
                      std::string &keyField, std::string &msg) {
    if (getTable(tableName)) {
        msg = "Table already exists.";
        return false;
    }
    _tables[tableName] = Table(tableName, fieldNames, fieldWidths, keyField);
    return true;
}

bool Database::dropTable(const std::string &tableName, std::string &msg) {
    if (!getTable(tableName)) {
        msg = "Table not existed.";
        return false;
    }
    return true;
}

Table *Database::getTable(const std::string &tableName) {
    if (_tables.find(tableName) == _tables.end()) {
        return nullptr;
    }
    return &_tables[tableName];
}

Database::Database() = default;
