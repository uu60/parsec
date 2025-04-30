//
// Created by 杜建璋 on 2024/10/25.
//

#include <utility>

#include "../../include/basis/Database.h"

Database::Database(std::string databaseName) {
    this->_databaseName = databaseName;
}

std::string Database::name() {
    return this->_databaseName;
}

bool
Database::createTable(const std::string &tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes,
                      std::string &msg) {
    if (getTable(tableName)) {
        msg = "Table already exists.";
        return false;
    }
    _tables[tableName] = Table(tableName, std::move(fieldNames), std::move(fieldTypes));
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
