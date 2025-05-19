//
// Created by 杜建璋 on 25-5-1.
//

#include <sstream>
#include "../../include/operator/SelectSupport.h"

#include "dbms/SystemManager.h"
#include "../third_party/hsql/sql/SQLStatement.h"
#include "../third_party/hsql/sql/SelectStatement.h"
#include "comm/Comm.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/System.h"

bool SelectSupport::clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    int64_t done;

    const auto *selectStmt = dynamic_cast<const hsql::SelectStatement *>(stmt);

    std::string tableName = selectStmt->fromTable->getName();

    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
    if (!table) {
        resp << "Failed. Table `" << tableName << "` does not exist." << std::endl;
        return false;
    }
    const auto fieldNames = table->_fieldNames;

    // select content
    std::vector<std::string> selectedFieldNames;
    const auto list = selectStmt->selectList;
    for (const auto c: *list) {
        // select *
        if (c->type == hsql::kExprStar) {
            selectedFieldNames = table->_fieldNames;
            break;
        }
        if (c->type == hsql::kExprColumnRef) {
            selectedFieldNames.emplace_back(c->getName());
        }
    }

    // filter
    if (selectStmt->whereClause) {

    }

    // order
    std::vector<std::string> orderFields;
    std::vector<bool> ascendings;
    if (selectStmt->order) {
        const auto *order = selectStmt->order;
        if (order->size() > 1) {
            resp << "Failed. Only one order column allowed." << std::endl;
            return false;
        }
        for (auto desc: *order) {
            std::string name = desc->expr->getName();
            if (std::find(fieldNames.begin(), fieldNames.end(), name) == fieldNames.end()) {
                resp << "Failed. Table does not have field `" << name << "`." << std::endl;
                return false;
            }
            orderFields.emplace_back(name);
            bool isAscending = desc->type == hsql::kOrderAsc;
            ascendings.emplace_back(isAscending);
        }
    }

    // notify servers
    json js;
    js["type"] = SystemManager::getCommandPrefix(SystemManager::SELECT);
    js["name"] = tableName;
    js["fieldNames"] = selectedFieldNames;
    if (selectStmt->whereClause) {

    }
    if (selectStmt->order) {
        js["orderFields"] = orderFields;
        js["ascendings"] = ascendings;
    }
    std::string m = js.dump();
    Comm::send(m, 0, 0);
    Comm::send(m, 1, 0);

    auto reconstructed = Secrets::boolReconstruct(Table::EMPTY_COL, 2, table->_maxWidth, 0);
    size_t cols = selectedFieldNames.size();
    size_t rows = reconstructed.size() / cols;
    for (int i = 0; i < cols; ++i) {
        resp << std::setw(10) << selectedFieldNames[i];
    }

    resp << std::endl;
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            resp << std::setw(10) << reconstructed[i * cols + j];
        }
        resp << std::endl;
    }

    Comm::receive(done, 1, 0, 0);
    Comm::receive(done, 1, 1, 0);
    return true;
}

void SelectSupport::serverSelect(json js) {
    std::string tableName = js.at("name").get<std::string>();
    std::vector<std::string> selectedFields = js.at("fieldNames").get<std::vector<std::string> >();

    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
    // v's task tag is assigned by System::nextTag()
    auto v = View::selectColumns(*table, selectedFields);

    if (v._dataCols.empty()) {
        Secrets::boolReconstruct(Table::EMPTY_COL, 2, v._maxWidth, 0);
        return;
    }

    // order
    if (js.contains("orderFields")) {
        std::vector<std::string> orderFields = js.at("orderFields").get<std::vector<std::string> >();
        std::vector<bool> ascendings = js.at("ascendings").get<std::vector<bool> >();

        std::vector<bool> ascs;
        ascs.reserve(ascendings.size());

        for (bool a: ascendings) {
            ascs.emplace_back(a & Comm::rank());
        }
        v.sort(orderFields[0], ascendings[0], 0);
    }

    std::vector<int64_t> toReconstruct(v._dataCols[0].size() * (v._colNum - 1));
    for (int i = 0; i < v._dataCols[0].size(); ++i) {
        for (int j = 0; j < v._colNum - 1; ++j) {
            toReconstruct[i] = v._dataCols[j][i];
        }
    }
    Secrets::boolReconstruct(toReconstruct, 2, v._maxWidth, 0);
}
