//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_TABLE_H
#define SMPC_DATABASE_TABLE_H
#include <vector>

class Table {
public:
    virtual ~Table() = default;

    std::string _tableName;
    std::vector<std::string> _fieldNames;
    std::vector<int> _fieldWidths;
    std::vector<std::vector<int64_t> > _dataCols;
    int _maxColWidth{};
    int _cols{};
    int _taskTag;

public:
    Table() = default;

    explicit Table(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths);

    bool insert(const std::vector<int64_t> &r);

    const std::vector<int64_t> &getColData(std::string colName);

    int getColWidth(const std::string &colName) const;
};


#endif //SMPC_DATABASE_TABLE_H
