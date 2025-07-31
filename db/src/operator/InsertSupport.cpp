//
// Created by 杜建璋 on 2024/11/3.
//

#include "operator/InsertSupport.h"

#include <sstream>
#include "../third_party/hsql/sql/InsertStatement.h"

#include "basis/Table.h"
#include "basis/View.h"
#include "basis/Views.h"
#include "comm/Comm.h"
#include "dbms/SystemManager.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>
bool InsertSupport::clientInsert(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    int64_t done;
    const auto *insertStmt = dynamic_cast<const hsql::InsertStatement *>(stmt);

    std::string tableName = insertStmt->tableName;
    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
    if (!table) {
        resp << "Failed. Table `" + tableName + "` does not exist." << std::endl;
        return false;
    }
    const auto fieldNames = table->_fieldNames;

    // get inserted values
    const auto columns = insertStmt->columns;
    const auto values = insertStmt->values;
    std::vector<std::string> cols;

    // insert values all columns
    std::string keyField = table->_keyField;
    // if table has no key field, pass this check
    bool containsKey = !keyField.empty();
    if (!insertStmt->columns) {
        for (auto n : table->_fieldNames) {
            if (n.find(View::BUCKET_TAG_PREFIX) == 0) {
                continue;
            }
            cols.emplace_back(n);
        }
    } else {
        for (auto c: *columns) {
            if (!containsKey && c == keyField) {
                containsKey = true;
            }
            cols.emplace_back(c);
        }
    }

    if (!containsKey) {
        resp << "Failed. Key field value needed." << std::endl;
        return false;
    }
    if (cols.size() != values->size()) {
        resp << "Failed. Unmatched parameter numbers." << std::endl;
        return false;
    }
    for (auto &column : cols) {
        if (std::find(fieldNames.begin(), fieldNames.end(), column) == fieldNames.end()) {
            resp << "Failed. Unknown field name `" + column + "`." << std::endl;
            return false;
        }
    }

    json j;
    j["type"] = SystemManager::getCommandPrefix(SystemManager::INSERT);
    j["name"] = tableName;
    std::string m = j.dump();
    Comm::send(m, 0, 0);
    Comm::send(m, 1, 0);

    std::vector<int64_t> parsedValues;
    int64_t keyValue = 0;
    for (size_t i = 0; i < values->size(); ++i) {
        const auto expr = (*values)[i];
        int64_t v;

        if (expr->type == hsql::kExprLiteralInt) {
            v = expr->ival;
        } else if (expr->type == hsql::kExprOperator && expr->expr->type == hsql::kExprLiteralInt) {
            if (expr->opType == hsql::kOpUnaryMinus) {
                v = -expr->expr->ival;
            }
        } else {
            resp << "Failed. Unsupported value type." << std::endl;
            return false;
        }

        // column idx in the table
        int64_t idx = std::distance(fieldNames.begin(),
                                    std::find(fieldNames.begin(), fieldNames.end(), cols[i]));
        int type = table->_fieldWidths[idx];
        int64_t masked = v & ((1LL << type) - 1);
        if (type < 64 && masked != v) {
            resp << "Failed. Inserted parameters out of range." << std::endl;
            return false;
        }
        parsedValues.emplace_back(v);
        if (cols[i] == keyField) {
            keyValue = v;
        }
    }

    cols.push_back(View::BUCKET_TAG_PREFIX + keyField);
    parsedValues.emplace_back(Views::hash(keyValue));

    // secret share
    std::vector<int64_t> shareValues(table->colNum());
    for (size_t i = 0; i < table->colNum(); ++i) {
        // table field idx in the inserted columns
        const auto &find = std::find(cols.begin(), cols.end(), fieldNames[i]);
        int64_t idx = std::distance(cols.begin(), find);

        int64_t v = find != cols.end() ? parsedValues[idx] : 0;
        shareValues[i] = v;
    }
    Secrets::boolShare(shareValues, 2, table->_maxWidth, 0);

    Comm::receive(done, 1, 0, 0);
    Comm::receive(done, 1, 1, 0);
    resp << "OK. Record inserted into `" + tableName + "`." << std::endl;

    return true;
}

void InsertSupport::serverInsert(nlohmann::basic_json<> j) {
    std::string tbName = j.at("name").get<std::string>();
    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tbName);
    auto record = Secrets::boolShare(Table::EMPTY_COL, 2, table->_maxWidth, 0);
    table->insert(record);
}