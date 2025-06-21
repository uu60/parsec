//
// Created by 杜建璋 on 25-5-1.
//

#ifndef SELECTSUPPORT_H
#define SELECTSUPPORT_H
#include "../basis/Table.h"
#include "../basis/View.h"
#include "../third_party/json.hpp"
#include "../third_party/hsql/sql/SQLStatement.h"
#include "../third_party/hsql/sql/Table.h"

struct JoinInfo {
    std::string leftTable;
    std::string rightTable;
    std::string leftField;
    std::string rightField;
};

class SelectSupport {
public:
    static bool clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverSelect(nlohmann::basic_json<> js);

private:
    static bool clientHandleOrder(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                  std::vector<std::string> &fieldNames, std::vector<std::string> &orderFields,
                                  std::vector<bool> &ascendings);

    static void clientHandleNotify(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                   const std::string &tableName,
                                   Table *table, std::vector<std::string> selectedFieldNames,
                                   const std::vector<std::string> &filterCols,
                                   const std::vector<View::ComparatorType> &filterCmps,
                                   std::vector<int64_t> filterVals, const std::vector<std::string> &orderFields,
                                   std::vector<bool> ascendings);


    static bool clientHandleFilter(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                   std::vector<JoinInfo> joinInfos,
                                   std::vector<std::string> &filterCols,
                                   std::vector<View::ComparatorType> &filterCmps, std::vector<int64_t> &filterVals);

    static bool clientHandleJoin(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                 std::vector<JoinInfo> &joinInfos, std::vector<std::string> &allFieldNames);

    static bool parseJoinCondition(std::ostringstream &resp, const hsql::Expr *condition,
                                   const std::string &leftTable, const std::string &rightTable,
                                   std::string &leftField, std::string &rightField);
};


#endif //SELECTSUPPORT_H
