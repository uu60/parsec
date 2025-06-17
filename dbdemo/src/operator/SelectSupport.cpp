//
// Created by 杜建璋 on 25-5-1.
//

#include <sstream>
#include "../../include/operator/SelectSupport.h"

#include "dbms/SystemManager.h"
#include "../third_party/hsql/sql/SQLStatement.h"
#include "../third_party/hsql/sql/SelectStatement.h"
#include "../third_party/hsql/sql/Table.h"
#include "basis/Views.h"
#include "comm/Comm.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/StringUtils.h"
#include "utils/System.h"

bool SelectSupport::clientHandleOrder(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                      std::vector<std::string> &fieldNames, std::vector<std::string> &orderFields,
                                      std::vector<bool> &ascendings) {
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
    return true;
}

void SelectSupport::clientHandleNotify(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                       const std::string& tableName, Table *table, std::vector<std::string> selectedFieldNames,
                                       const std::vector<std::string>& filterCols,
                                       const std::vector<View::ComparatorType>& filterCmps, std::vector<int64_t> filterVals,
                                       const std::vector<std::string>& orderFields, std::vector<bool> ascendings) {
    json js;
    js["type"] = SystemManager::getCommandPrefix(SystemManager::SELECT);
    js["name"] = tableName;
    js["fieldNames"] = selectedFieldNames;
    std::vector<int64_t> fv0, fv1;
    if (selectStmt->whereClause) {
        fv0.resize(filterVals.size());
        fv1.resize(filterVals.size());
        js["filterFields"] = filterCols;
        js["filterCmps"] = filterCmps;
        for (int i = 0; i < filterVals.size(); ++i) {
            fv0[i] = Math::randInt();
            fv1[i] = fv0[i] ^ filterVals[i];
        }
        js["filterVals"] = fv0;
    }
    if (selectStmt->order) {
        js["orderFields"] = orderFields;
        js["ascendings"] = ascendings;
    }
    std::string m = js.dump();
    Comm::send(m, 0, 0);

    if (selectStmt->whereClause) {
        js["filterVals"] = fv1;
    }
    m = js.dump();
    Comm::send(m, 1, 0);

    auto reconstructed = Secrets::boolReconstruct(Table::EMPTY_COL, 2, table->_maxWidth, 0);
    size_t cols = selectedFieldNames.size();
    size_t rows = reconstructed.size() / cols;
    for (int i = 0; i < cols; ++i) {
        resp << std::setw(10) << selectedFieldNames[i];
    }

    resp << std::endl;
    for (int i = 0; i < rows; i++) {
        if (!reconstructed[i * (cols + 1) + cols]) {
            continue;
        }
        for (int j = 0; j < cols; j++) {
            resp << std::setw(10) << reconstructed[i * (cols + 1) + j];
        }
        resp << std::endl;
    }
}

bool SelectSupport::clientHandleFilter(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                       const std::string &tableName, Table *table, std::vector<std::string> &filterCols,
                                       std::vector<View::ComparatorType> &filterCmps,
                                       std::vector<int64_t> &filterVals) {
    if (selectStmt->whereClause) {
        std::vector<const hsql::Expr *> stk;
        stk.push_back(selectStmt->whereClause);
        bool ok = true;

        while (!stk.empty() && ok) {
            const auto *e = stk.back();
            stk.pop_back();

            if (e->type == hsql::kExprOperator && e->opType == hsql::kOpAnd) {
                stk.push_back(e->expr2);
                stk.push_back(e->expr);
                continue;
            }

            if (e->type != hsql::kExprOperator) {
                ok = false;
                break;
            }

            if (e->expr->type != hsql::kExprColumnRef ||
                e->expr2->type != hsql::kExprLiteralInt) {
                ok = false;
                break;
            }

            View::ComparatorType cmp;
            switch (e->opType) {
                case hsql::kOpEquals:
                    cmp = View::ComparatorType::EQUALS;
                    break;
                case hsql::kOpNotEquals:
                    cmp = View::ComparatorType::NOT_EQUALS;
                    break;
                case hsql::kOpGreater:
                    cmp = View::ComparatorType::GREATER;
                    break;
                case hsql::kOpGreaterEq:
                    cmp = View::ComparatorType::GREATER_EQ;
                    break;
                case hsql::kOpLess:
                    cmp = View::ComparatorType::LESS;
                    break;
                case hsql::kOpLessEq:
                    cmp = View::ComparatorType::LESS_EQ;
                    break;
                default:
                    ok = false;
                    break;
            }

            if (!ok) {
                break;
            }

            if (std::find(table->_fieldNames.begin(), table->_fieldNames.end(), e->expr->getName()) == table->
                _fieldNames.end()) {
                resp << "Failed. Table `" << tableName << "` does not has field `" << e->expr->getName() << "`." <<
                        std::endl;
                return false;
            }
            filterCols.emplace_back(e->expr->getName());
            filterCmps.push_back(cmp);
            filterVals.push_back(e->expr2->ival);
        }

        if (!ok) {
            resp << "Failed. Only simple AND-combined column-to-integer comparisons are supported." << std::endl;
            return false;
        }
    }
    return true;
}

bool SelectSupport::clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    int64_t done;

    auto *selectStmt = dynamic_cast<const hsql::SelectStatement *>(stmt);

    // Check if this is a JOIN query
    std::vector<JoinInfo> joinInfos;
    std::vector<std::string> allFieldNames;
    bool isJoin = clientHandleJoin(resp, selectStmt, joinInfos, allFieldNames);

    if (isJoin) {
        // Handle JOIN query
        std::vector<std::string> selectedFieldNames;
        const auto list = selectStmt->selectList;
        
        for (const auto c: *list) {
            // select *
            if (c->type == hsql::kExprStar) {
                selectedFieldNames = allFieldNames;
                break;
            }
            if (c->type == hsql::kExprColumnRef) {
                std::string fieldName = c->getName();
                if (std::find(allFieldNames.begin(), allFieldNames.end(), fieldName) == allFieldNames.end()) {
                    resp << "Failed. Field `" << fieldName << "` does not exist in joined tables." << std::endl;
                    return false;
                }
                selectedFieldNames.emplace_back(fieldName);
            }
        }

        // For JOIN, we don't support complex filters and ordering for now
        std::vector<std::string> filterCols;
        std::vector<View::ComparatorType> filterCmps;
        std::vector<int64_t> filterVals;
        std::vector<std::string> orderFields;
        std::vector<bool> ascendings;

        // notify servers for JOIN
        clientHandleNotifyJoin(resp, selectStmt, joinInfos, selectedFieldNames, filterCols, filterCmps, filterVals,
                               orderFields, ascendings);

        auto r0 = Comm::receiveAsync(done, 1, 0, 0);
        auto r1 = Comm::receiveAsync(done, 1, 1, 0);
        Comm::wait(r0);
        Comm::wait(r1);
        return true;
    } else {
        // Handle single table query (original logic)
        std::string tableName = selectStmt->fromTable->getName();

        Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
        if (!table) {
            resp << "Failed. Table `" << tableName << "` does not exist." << std::endl;
            return false;
        }
        auto fieldNames = table->_fieldNames;

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
                if (std::find(table->_fieldNames.begin(), table->_fieldNames.end(), c->getName()) == table->_fieldNames.
                    end()) {
                    resp << "Failed. Table `" << tableName << "` does not has field `" << c->getName() << "`." << std::endl;
                    return false;
                }
                selectedFieldNames.emplace_back(c->getName());
            }
        }

        // filter
        std::vector<std::string> filterCols;
        std::vector<View::ComparatorType> filterCmps;
        std::vector<int64_t> filterVals;
        if (!clientHandleFilter(resp, selectStmt, tableName, table, filterCols, filterCmps, filterVals)) {
            return false;
        }

        // order
        std::vector<std::string> orderFields;
        std::vector<bool> ascendings;
        if (!clientHandleOrder(resp, selectStmt, fieldNames, orderFields, ascendings)) {
            return false;
        }

        // notify servers
        clientHandleNotify(resp, selectStmt, tableName, table, selectedFieldNames, filterCols, filterCmps, filterVals,
                           orderFields, ascendings);

        auto r0 = Comm::receiveAsync(done, 1, 0, 0);
        auto r1 = Comm::receiveAsync(done, 1, 1, 0);
        Comm::wait(r0);
        Comm::wait(r1);
        return true;
    }
}

void SelectSupport::serverSelect(json js) {
    // Check if this is a JOIN operation
    if (js.contains("isJoin") && js.at("isJoin").get<bool>()) {
        // Handle JOIN operation
        std::vector<std::string> selectedFields = js.at("fieldNames").get<std::vector<std::string> >();
        auto joinArray = js.at("joins");
        
        if (joinArray.empty()) {
            Secrets::boolReconstruct(Table::EMPTY_COL, 2, 64, 0);
            return;
        }

        // For simplicity, only handle the first join (two tables)
        auto joinObj = joinArray[0];
        std::string leftTableName = joinObj.at("leftTable").get<std::string>();
        std::string rightTableName = joinObj.at("rightTable").get<std::string>();
        std::string leftField = joinObj.at("leftField").get<std::string>();
        std::string rightField = joinObj.at("rightField").get<std::string>();

        Table *leftTable = SystemManager::getInstance()._currentDatabase->getTable(leftTableName);
        Table *rightTable = SystemManager::getInstance()._currentDatabase->getTable(rightTableName);

        if (!leftTable || !rightTable) {
            Secrets::boolReconstruct(Table::EMPTY_COL, 2, 64, 0);
            return;
        }

        // Create views for both tables with prefixed field names
        View leftView = Views::selectAllWithFieldPrefix(*leftTable);
        View rightView = Views::selectAllWithFieldPrefix(*rightTable);

        // Perform the join (using nestedLoopJoin for cartesian product with condition)
        View joinResult = Views::nestedLoopJoin(leftView, rightView, leftField, rightField);

        if (joinResult._dataCols.empty()) {
            Secrets::boolReconstruct(Table::EMPTY_COL, 2, joinResult._maxWidth, 0);
            return;
        }

        // Select only the requested fields from the join result
        std::vector<std::string> availableFields = joinResult._fieldNames;
        std::vector<std::string> finalSelectedFields;
        
        for (const auto& field : selectedFields) {
            if (std::find(availableFields.begin(), availableFields.end(), field) != availableFields.end()) {
                finalSelectedFields.push_back(field);
            }
        }

        if (finalSelectedFields.empty()) {
            finalSelectedFields = availableFields; // Select all if none specified
        }

        View finalView = Views::selectColumns(joinResult, finalSelectedFields);

        // Reconstruct the result
        std::vector<int64_t> toReconstruct;
        if (!finalView._dataCols.empty()) {
            toReconstruct.resize(finalView._dataCols[0].size() * (finalView.colNum() - 1));
            int idx = 0;
            for (int i = 0; i < finalView._dataCols[0].size(); ++i) {
                for (int j = 0; j < finalView.colNum() - 1; ++j) {
                    toReconstruct[idx++] = finalView._dataCols[j][i];
                }
            }
        }
        Secrets::boolReconstruct(toReconstruct, 2, finalView._maxWidth, 0);
    } else {
        // Handle single table operation (original logic)
        std::string tableName = js.at("name").get<std::string>();
        std::vector<std::string> selectedFields = js.at("fieldNames").get<std::vector<std::string> >();

        Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
        // v's task tag is assigned by System::nextTag()
        auto v = Views::selectColumns(*table, selectedFields);

        if (v._dataCols.empty()) {
            Secrets::boolReconstruct(Table::EMPTY_COL, 2, v._maxWidth, 0);
            return;
        }

        // filter
        if (js.contains("filterFields")) {
            std::vector<std::string> filterFields = js.at("filterFields").get<std::vector<std::string> >();
            std::vector<View::ComparatorType> filterCmps = js.at("filterCmps").get<std::vector<View::ComparatorType> >();
            std::vector<int64_t> filterVals = js.at("filterVals").get<std::vector<int64_t> >();
            v.filterAndConditions(filterFields, filterCmps, filterVals);
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

        std::vector<int64_t> toReconstruct(v._dataCols[0].size() * (v.colNum() - 1));
        int idx = 0;
        for (int i = 0; i < v._dataCols[0].size(); ++i) {
            for (int j = 0; j < v.colNum() - 1; ++j) {
                toReconstruct[idx++] = v._dataCols[j][i];
            }
        }
        Secrets::boolReconstruct(toReconstruct, 2, v._maxWidth, 0);
    }
}

std::string SelectSupport::getJoinTypeString(hsql::JoinType joinType) {
    return "CROSS"; // Only support cartesian product
}

bool SelectSupport::parseJoinCondition(std::ostringstream &resp, const hsql::Expr *condition,
                                       const std::string &leftTable, const std::string &rightTable,
                                       std::string &leftField, std::string &rightField) {
    if (!condition || condition->type != hsql::kExprOperator || condition->opType != hsql::kOpEquals) {
        resp << "Failed. Only equality join conditions are supported." << std::endl;
        return false;
    }

    if (condition->expr->type != hsql::kExprColumnRef || condition->expr2->type != hsql::kExprColumnRef) {
        resp << "Failed. Join condition must be between two columns." << std::endl;
        return false;
    }

    std::string col1 = condition->expr->getName();
    std::string col2 = condition->expr2->getName();

    // For cartesian product, we still need to identify the join fields
    Table *leftTableObj = SystemManager::getInstance()._currentDatabase->getTable(leftTable);
    Table *rightTableObj = SystemManager::getInstance()._currentDatabase->getTable(rightTable);

    bool col1InLeft = std::find(leftTableObj->_fieldNames.begin(), leftTableObj->_fieldNames.end(), col1) != leftTableObj->_fieldNames.end();
    bool col1InRight = std::find(rightTableObj->_fieldNames.begin(), rightTableObj->_fieldNames.end(), col1) != rightTableObj->_fieldNames.end();
    bool col2InLeft = std::find(leftTableObj->_fieldNames.begin(), leftTableObj->_fieldNames.end(), col2) != leftTableObj->_fieldNames.end();
    bool col2InRight = std::find(rightTableObj->_fieldNames.begin(), rightTableObj->_fieldNames.end(), col2) != rightTableObj->_fieldNames.end();

    if (col1InLeft && col2InRight) {
        leftField = col1;
        rightField = col2;
    } else if (col1InRight && col2InLeft) {
        leftField = col2;
        rightField = col1;
    } else {
        resp << "Failed. Join condition columns must belong to the respective tables." << std::endl;
        return false;
    }

    return true;
}

bool SelectSupport::clientHandleJoin(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                     std::vector<JoinInfo> &joinInfos, std::vector<std::string> &allFieldNames) {
    if (selectStmt->fromTable->type != hsql::kTableJoin) {
        return false; // No join
    }

    // For simplicity, only support two-table joins for now
    if (selectStmt->fromTable->join) {
        JoinInfo joinInfo;
        joinInfo.leftTable = selectStmt->fromTable->join->left->getName();
        joinInfo.rightTable = selectStmt->fromTable->join->right->getName();
        joinInfo.joinType = "CROSS"; // Only cartesian product

        // Validate tables exist
        Table *leftTable = SystemManager::getInstance()._currentDatabase->getTable(joinInfo.leftTable);
        Table *rightTable = SystemManager::getInstance()._currentDatabase->getTable(joinInfo.rightTable);
        
        if (!leftTable) {
            resp << "Failed. Table `" << joinInfo.leftTable << "` does not exist." << std::endl;
            return false;
        }
        if (!rightTable) {
            resp << "Failed. Table `" << joinInfo.rightTable << "` does not exist." << std::endl;
            return false;
        }

        // Parse join condition if exists
        if (selectStmt->fromTable->join->condition) {
            if (!parseJoinCondition(resp, selectStmt->fromTable->join->condition, 
                                    joinInfo.leftTable, joinInfo.rightTable,
                                    joinInfo.leftField, joinInfo.rightField)) {
                return false;
            }
        } else {
            // For cartesian product without condition, use first field of each table
            joinInfo.leftField = leftTable->_fieldNames[0];
            joinInfo.rightField = rightTable->_fieldNames[0];
        }

        // Collect all field names with table prefixes
        for (const auto& fieldName : leftTable->_fieldNames) {
            allFieldNames.push_back(Views::getAliasColName(joinInfo.leftTable, const_cast<std::string&>(fieldName)));
        }
        for (const auto& fieldName : rightTable->_fieldNames) {
            allFieldNames.push_back(Views::getAliasColName(joinInfo.rightTable, const_cast<std::string&>(fieldName)));
        }

        joinInfos.push_back(joinInfo);
        return true;
    }

    return false;
}

void SelectSupport::clientHandleNotifyJoin(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                           const std::vector<JoinInfo>& joinInfos,
                                           std::vector<std::string> selectedFieldNames,
                                           const std::vector<std::string>& filterCols, const std::vector<View::ComparatorType>& filterCmps,
                                           std::vector<int64_t> filterVals, const std::vector<std::string>& orderFields,
                                           std::vector<bool> ascendings) {
    json js;
    js["type"] = SystemManager::getCommandPrefix(SystemManager::SELECT);
    js["isJoin"] = true;
    js["fieldNames"] = selectedFieldNames;

    // Serialize join information
    json joinArray = json::array();
    for (const auto& joinInfo : joinInfos) {
        json joinObj;
        joinObj["leftTable"] = joinInfo.leftTable;
        joinObj["rightTable"] = joinInfo.rightTable;
        joinObj["leftField"] = joinInfo.leftField;
        joinObj["rightField"] = joinInfo.rightField;
        joinObj["joinType"] = joinInfo.joinType;
        joinArray.push_back(joinObj);
    }
    js["joins"] = joinArray;

    std::vector<int64_t> fv0, fv1;
    if (selectStmt->whereClause) {
        fv0.resize(filterVals.size());
        fv1.resize(filterVals.size());
        js["filterFields"] = filterCols;
        js["filterCmps"] = filterCmps;
        for (int i = 0; i < filterVals.size(); ++i) {
            fv0[i] = Math::randInt();
            fv1[i] = fv0[i] ^ filterVals[i];
        }
        js["filterVals"] = fv0;
    }
    if (selectStmt->order) {
        js["orderFields"] = orderFields;
        js["ascendings"] = ascendings;
    }

    std::string m = js.dump();
    Comm::send(m, 0, 0);

    if (selectStmt->whereClause) {
        js["filterVals"] = fv1;
    }
    m = js.dump();
    Comm::send(m, 1, 0);

    // Calculate expected width for reconstruction
    int maxWidth = 64; // Default width
    for (const auto& joinInfo : joinInfos) {
        Table *leftTable = SystemManager::getInstance()._currentDatabase->getTable(joinInfo.leftTable);
        Table *rightTable = SystemManager::getInstance()._currentDatabase->getTable(joinInfo.rightTable);
        maxWidth = std::max(maxWidth, std::max(leftTable->_maxWidth, rightTable->_maxWidth));
    }

    auto reconstructed = Secrets::boolReconstruct(Table::EMPTY_COL, 2, maxWidth, 0);
    size_t cols = selectedFieldNames.size();
    if (cols == 0 || reconstructed.empty()) {
        resp << "No results." << std::endl;
        return;
    }
    
    size_t rows = reconstructed.size() / (cols + 1); // +1 for valid column
    
    for (int i = 0; i < cols; ++i) {
        resp << std::setw(10) << selectedFieldNames[i];
    }

    resp << std::endl;
    for (int i = 0; i < rows; i++) {
        if (!reconstructed[i * (cols + 1) + cols]) {
            continue;
        }
        for (int j = 0; j < cols; j++) {
            resp << std::setw(10) << reconstructed[i * (cols + 1) + j];
        }
        resp << std::endl;
    }
}
