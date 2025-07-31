//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_TABLE_H
#define SMPC_DATABASE_TABLE_H
#include <vector>

#include <string>
class Table {
public:
    inline static const std::string BUCKET_TAG_PREFIX = "$tag:";

    virtual ~Table() = default;

    std::string _tableName;
    std::vector<std::string> _fieldNames;
    std::string _keyField;
    std::vector<int> _fieldWidths;
    std::vector<std::vector<int64_t> > _dataCols;
    int _maxWidth{};

    inline static std::vector<int64_t> EMPTY_COL{};

public:
    Table() = default;

    explicit Table(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths, std::string keyField);

    bool insert(const std::vector<int64_t> &r);

    [[nodiscard]] int colIndex(const std::string &colName) const;

    [[nodiscard]] size_t colNum() const;

    [[nodiscard]] size_t rowNum() const;
};


#endif //SMPC_DATABASE_TABLE_H
