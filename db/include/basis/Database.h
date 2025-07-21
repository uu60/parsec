//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_DATABASE_H
#define SMPC_DATABASE_DATABASE_H
#include <vector>
#include <map>
#include "./Table.h"

class Database {
private:
    std::string _databaseName;
    std::map<std::string, Table> _tables;

public:
    explicit Database(std::string databaseName);

    Database();

    std::string name();

    bool createTable(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths, std::string &keyField, std::string &msg);

    bool dropTable(const std::string& tableName, std::string &msg);

    Table* getTable(const std::string& tableName);
};


#endif //SMPC_DATABASE_DATABASE_H
