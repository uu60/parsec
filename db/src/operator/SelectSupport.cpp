//
// Created by 杜建璋 on 25-5-1.
//

#include <sstream>
#include <set>
#include <map>
#include <functional>
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

bool SelectSupport::clientHandleFilter(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                       std::vector<JoinInfo> joinInfos,
                                       std::vector<std::string> &filterCols,
                                       std::vector<View::ComparatorType> &filterCmps,
                                       std::vector<int64_t> &filterVals) {
    if (selectStmt->whereClause) {
        std::vector<const hsql::Expr *> stk;
        stk.push_back(selectStmt->whereClause);
        bool ok = true;
        bool isJoin = selectStmt->fromTable->type == hsql::kTableJoin;
        std::set<std::string> joinTables;
        if (isJoin) {
            for (auto info : joinInfos) {
                joinTables.insert(info.leftTable);
                joinTables.insert(info.rightTable);
            }
        }

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

            std::string tableName = isJoin ? e->expr->table : selectStmt->fromTable->getName();
            if (isJoin && joinTables.count(tableName) == 0) {
                resp << "Failed. Table `" << tableName << "` used in WHERE clause is not present in FROM clause." << std::endl;
                return false;
            }

            auto table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
            std::string colName = e->expr->getName();
            if (std::find(table->_fieldNames.begin(), table->_fieldNames.end(), colName) == table->
                _fieldNames.end()) {
                resp << "Failed. Table `" << tableName << "` does not has field `" << colName << "`." <<
                        std::endl;
                return false;
            }
            filterCols.emplace_back(isJoin ? Views::getAliasColName(tableName, colName) : colName);
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
    bool isJoin = selectStmt->fromTable->type == hsql::kTableJoin;

    std::vector<std::string> selectedFieldNames;
    std::string tableName; // For single table queries
    Table *table = nullptr; // For single table queries

    if (isJoin) {
        // Handle JOIN query - get table info and field names
        if (!clientHandleJoin(resp, selectStmt, joinInfos, allFieldNames)) {
            return false;
        }

        // Parse selected fields for JOIN
        const auto list = selectStmt->selectList;
        for (const auto c: *list) {
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
    } else {
        // Handle single table query - get table info and field names
        tableName = selectStmt->fromTable->getName();
        table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
        if (!table) {
            resp << "Failed. Table `" << tableName << "` does not exist." << std::endl;
            return false;
        }

        // Parse selected fields for single table
        const auto list = selectStmt->selectList;
        for (const auto c: *list) {
            if (c->type == hsql::kExprStar) {
                selectedFieldNames = table->_fieldNames;
                break;
            }
            if (c->type == hsql::kExprColumnRef) {
                if (std::find(table->_fieldNames.begin(), table->_fieldNames.end(), c->getName()) == table->_fieldNames.
                    end()) {
                    resp << "Failed. Table `" << tableName << "` does not has field `" << c->getName() << "`." <<
                            std::endl;
                    return false;
                }
                selectedFieldNames.emplace_back(c->getName());
            }
        }
    }

    // Common processing for both JOIN and single table: filter and order
    std::vector<std::string> filterCols;
    std::vector<View::ComparatorType> filterCmps;
    std::vector<int64_t> filterVals;
    std::vector<std::string> orderFields;
    std::vector<bool> ascendings;

    // Parse filter conditions (simplified for now - could be enhanced for JOIN)
    if (selectStmt->whereClause) {
        if (!clientHandleFilter(resp, selectStmt, joinInfos, filterCols, filterCmps, filterVals)) {
            return false;
        }
    }

    // Parse order conditions
    if (selectStmt->order) {
        auto fieldNames = table->_fieldNames;
        if (!clientHandleOrder(resp, selectStmt, fieldNames, orderFields, ascendings)) {
            return false;
        }
    }

    // Send unified JSON message to servers
    json js;
    js["type"] = SystemManager::getCommandPrefix(SystemManager::SELECT);
    js["fieldNames"] = selectedFieldNames;

    if (isJoin) {
        js["isJoin"] = true;
        json joinArray = json::array();
        for (const auto &joinInfo: joinInfos) {
            json joinObj;
            joinObj["leftTable"] = joinInfo.leftTable;
            joinObj["rightTable"] = joinInfo.rightTable;
            joinObj["leftField"] = joinInfo.leftField;
            joinObj["rightField"] = joinInfo.rightField;
            joinArray.push_back(joinObj);
        }
        js["joins"] = joinArray;
    } else {
        js["isJoin"] = false;
        js["name"] = tableName;
    }

    // Add filter and order info to JSON
    std::vector<int64_t> fv0, fv1;
    if (selectStmt->whereClause && !filterCols.empty()) {
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
    if (selectStmt->order && !orderFields.empty()) {
        js["orderFields"] = orderFields;
        js["ascendings"] = ascendings;
    }

    // Send to servers
    std::string m = js.dump();
    Comm::send(m, 0, 0);

    if (selectStmt->whereClause && !filterCols.empty()) {
        js["filterVals"] = fv1;
    }
    m = js.dump();
    Comm::send(m, 1, 0);

    // Reconstruct results
    int maxWidth = isJoin ? 64 : table->_maxWidth;
    auto reconstructed = Secrets::boolReconstruct(Table::EMPTY_COL, 2, maxWidth, 0);
    size_t cols = selectedFieldNames.size();
    if (cols == 0 || reconstructed.empty()) {
        resp << "No results." << std::endl;
    } else {
        size_t rows = reconstructed.size() / (cols + 1);

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

    auto r0 = Comm::receiveAsync(done, 1, 0, 0);
    auto r1 = Comm::receiveAsync(done, 1, 1, 0);
    Comm::wait(r0);
    Comm::wait(r1);
    return true;
}

void SelectSupport::serverSelect(json js) {
    // Check if this is a JOIN operation
    View v;
    if (js.contains("isJoin") && js.at("isJoin").get<bool>()) {
        // Handle JOIN operation
        std::vector<std::string> selectedFields = js.at("fieldNames").get<std::vector<std::string> >();
        auto joinArray = js.at("joins");

        if (joinArray.empty()) {
            Secrets::boolReconstruct(Table::EMPTY_COL, 2, 64, 0);
            return;
        }

        // Collect all unique table names from joins
        std::set<std::string> tableNamesSet;
        for (const auto &joinObj: joinArray) {
            tableNamesSet.insert(joinObj.at("leftTable").get<std::string>());
            tableNamesSet.insert(joinObj.at("rightTable").get<std::string>());
        }

        std::vector<std::string> tableNames(tableNamesSet.begin(), tableNamesSet.end());

        // Create views for all tables with prefixed field names and apply single-table filters
        std::map<std::string, View> tableViews;
        
        // Classify filters by table if filters exist
        std::map<std::string, std::vector<std::string>> tableFilterCols;
        std::map<std::string, std::vector<View::ComparatorType>> tableFilterCmps;
        std::map<std::string, std::vector<int64_t>> tableFilterVals;
        
        if (js.contains("filterFields")) {
            std::vector<std::string> filterFields = js.at("filterFields").get<std::vector<std::string>>();
            std::vector<View::ComparatorType> filterCmps = js.at("filterCmps").get<std::vector<View::ComparatorType>>();
            std::vector<int64_t> filterVals = js.at("filterVals").get<std::vector<int64_t>>();
            
            // Classify filters by table name (extract from "tableName.columnName" format)
            for (int i = 0; i < filterFields.size(); ++i) {
                std::string filterField = filterFields[i];
                size_t dotPos = filterField.find('.');
                if (dotPos != std::string::npos) {
                    std::string tableName = filterField.substr(0, dotPos);
                    if (tableNamesSet.count(tableName) > 0) {
                        tableFilterCols[tableName].push_back(filterField);
                        tableFilterCmps[tableName].push_back(filterCmps[i]);
                        tableFilterVals[tableName].push_back(filterVals[i]);
                    }
                }
            }
        }
        
        // Create views and apply single-table filters before joins
        for (const auto &tableName: tableNames) {
            Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
            View tableView = Views::selectAllWithFieldPrefix(*table);
            
            // Apply filters specific to this table before joining
            if (tableFilterCols.count(tableName) > 0) {
                tableView.filterAndConditions(tableFilterCols[tableName], 
                                            tableFilterCmps[tableName], 
                                            tableFilterVals[tableName]);
            }
            
            tableViews[tableName] = tableView;
        }

        // Perform joins sequentially - start with first two tables
        View currentResult;
        bool firstJoin = true;
        std::set<std::string> tablesInResult; // Track which tables are already in the result

        for (const auto &joinObj: joinArray) {
            std::string leftTableName = joinObj.at("leftTable").get<std::string>();
            std::string rightTableName = joinObj.at("rightTable").get<std::string>();
            std::string leftField = joinObj.at("leftField").get<std::string>();
            std::string rightField = joinObj.at("rightField").get<std::string>();

            if (firstJoin) {
                // First join: join two tables directly
                View leftView = tableViews[leftTableName];
                View rightView = tableViews[rightTableName];
                currentResult = Views::hashJoin(leftView, rightView, leftField, rightField);
                tablesInResult.insert(leftTableName);
                tablesInResult.insert(rightTableName);
                firstJoin = false;
            } else {
                // Subsequent joins: only join if at least one table is not in result yet
                bool leftInResult = tablesInResult.count(leftTableName) > 0;
                bool rightInResult = tablesInResult.count(rightTableName) > 0;
                
                // Skip this join if both tables are already in the result
                if (leftInResult && rightInResult) {
                    continue;
                }
                
                // Determine which table to join with current result
                std::string nextTableName;
                std::string nextField;
                std::string currentField;

                if (leftInResult) {
                    // Left table is in result, join with right table
                    nextTableName = rightTableName;
                    nextField = rightField;
                    currentField = leftField;
                } else if (rightInResult) {
                    // Right table is in result, join with left table
                    nextTableName = leftTableName;
                    nextField = leftField;
                    currentField = rightField;
                } else {
                    // Neither table is in result - this shouldn't happen in a proper join sequence
                    // but handle it by joining the two tables and then joining with current result
                    View leftView = tableViews[leftTableName];
                    View rightView = tableViews[rightTableName];
                    View tempResult = Views::hashJoin(leftView, rightView, leftField, rightField);
                    
                    // This is a complex case - for now, just continue with the temp result
                    // In a real implementation, you'd need to find a common field to join with currentResult
                    continue;
                }

                View nextView = tableViews[nextTableName];
                currentResult = Views::hashJoin(currentResult, nextView, currentField, nextField);
                tablesInResult.insert(nextTableName);
            }
        }

        if (currentResult._dataCols.empty()) {
            Secrets::boolReconstruct(Table::EMPTY_COL, 2, currentResult._maxWidth, 0);
            return;
        }

        // Select only the requested fields from the join result
        std::vector<std::string> availableFields = currentResult._fieldNames;
        std::vector<std::string> finalSelectedFields;

        for (const auto &field: selectedFields) {
            if (std::find(availableFields.begin(), availableFields.end(), field) != availableFields.end()) {
                finalSelectedFields.push_back(field);
            }
        }

        if (finalSelectedFields.empty()) {
            finalSelectedFields = availableFields; // Select all if none specified
        }

        v = Views::selectColumns(currentResult, finalSelectedFields);
    } else {
        // Handle single table operation (original logic)
        std::string tableName = js.at("name").get<std::string>();
        std::vector<std::string> selectedFields = js.at("fieldNames").get<std::vector<std::string> >();

        Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
        // v's task tag is assigned by System::nextTag()
        v = Views::selectColumns(*table, selectedFields);
    }

    if (v._dataCols.empty()) {
        Secrets::boolReconstruct(Table::EMPTY_COL, 2, v._maxWidth, 0);
        return;
    }

    // Apply remaining filters only for single table queries or cross-table filters
    if (js.contains("filterFields") && !js.contains("isJoin")) {
        // For single table queries, apply all filters here
        std::vector<std::string> filterFields = js.at("filterFields").get<std::vector<std::string> >();
        std::vector<View::ComparatorType> filterCmps = js.at("filterCmps").get<std::vector<
            View::ComparatorType> >();
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

    Table *t1 = SystemManager::getInstance()._currentDatabase->getTable(leftTable);
    Table *t2 = SystemManager::getInstance()._currentDatabase->getTable(rightTable);

    std::string tableName1 = condition->expr->table;
    std::string colName1 = condition->expr->name;
    std::string col1 = Views::getAliasColName(tableName1, colName1);
    if (tableName1 != leftTable || std::find(t1->_fieldNames.begin(), t1->_fieldNames.end(), colName1) == t1->
        _fieldNames.end()) {
        resp << "Failed. Table `" << leftTable << "` does not has field `" << col1 << "`." << std::endl;
        return false;
    }

    std::string tableName2 = condition->expr2->table;
    std::string colName2 = condition->expr2->name;
    std::string col2 = Views::getAliasColName(tableName2, colName2);
    if (tableName2 != rightTable || std::find(t2->_fieldNames.begin(), t2->_fieldNames.end(), colName2) == t2->
        _fieldNames.end()) {
        resp << "Failed. Table `" << rightTable << "` does not has field `" << col2 << "`." << std::endl;
        return false;
    }

    bool col1InLeft = std::find(t1->_fieldNames.begin(), t1->_fieldNames.end(), colName1) !=
                      t1->_fieldNames.end();
    bool col1InRight = std::find(t2->_fieldNames.begin(), t2->_fieldNames.end(), colName1) !=
                       t2->_fieldNames.end();
    bool col2InLeft = std::find(t1->_fieldNames.begin(), t1->_fieldNames.end(), colName2) !=
                      t1->_fieldNames.end();
    bool col2InRight = std::find(t2->_fieldNames.begin(), t2->_fieldNames.end(), colName2) !=
                       t2->_fieldNames.end();

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
    // Collect all tables and joins recursively
    std::vector<std::string> tableNames;
    std::vector<hsql::JoinDefinition *> joins;

    // Recursive function to parse join structure
    std::function<void(hsql::TableRef *)> parseTableRef = [&](hsql::TableRef *tableRef) {
        if (tableRef->type == hsql::kTableJoin) {
            parseTableRef(tableRef->join->left);
            parseTableRef(tableRef->join->right);
            joins.push_back(tableRef->join);
        } else if (tableRef->type == hsql::kTableName) {
            tableNames.push_back(tableRef->getName());
        }
    };

    parseTableRef(selectStmt->fromTable);

    // Validate all tables exist and collect field names
    for (const auto &tableName: tableNames) {
        Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
        if (!table) {
            resp << "Failed. Table `" << tableName << "` does not exist." << std::endl;
            return false;
        }

        // Add field names with table prefix to avoid conflicts
        for (const auto &fieldName: table->_fieldNames) {
            allFieldNames.push_back(Views::getAliasColName(const_cast<std::string &>(tableName),
                                                           const_cast<std::string &>(fieldName)));
        }
    }

    // Process joins - for cartesian product, we need to create join pairs
    for (auto joinDef: joins) {
        JoinInfo joinInfo;
        joinInfo.leftTable = joinDef->left->getName();
        joinInfo.rightTable = joinDef->right->getName();

        // Validate tables exist
        Table *leftTable = SystemManager::getInstance()._currentDatabase->getTable(joinInfo.leftTable);
        Table *rightTable = SystemManager::getInstance()._currentDatabase->getTable(joinInfo.rightTable);

        if (!leftTable || !rightTable) {
            resp << "Failed. One of the join tables does not exist." << std::endl;
            return false;
        }

        // Parse join condition if exists
        if (joinDef->condition) {
            if (!parseJoinCondition(resp, joinDef->condition,
                                    joinInfo.leftTable, joinInfo.rightTable,
                                    joinInfo.leftField, joinInfo.rightField)) {
                return false;
            }
        } else {
            // For cartesian product without condition, use first field of each table
            joinInfo.leftField = leftTable->_fieldNames[0];
            joinInfo.rightField = rightTable->_fieldNames[0];
        }

        joinInfos.push_back(joinInfo);
    }

    if (joinInfos.empty()) {
        resp << "Failed. No joins found." << std::endl;
    }

    return true;
}
