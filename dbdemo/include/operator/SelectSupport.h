//
// Created by 杜建璋 on 25-5-1.
//

#ifndef SELECTSUPPORT_H
#define SELECTSUPPORT_H
#include "../basis/Table.h"
#include "../basis/View.h"
#include "../third_party/json.hpp"
#include "../third_party/hsql/sql/SQLStatement.h"

class SelectSupport {
public:
    static bool clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverSelect(nlohmann::basic_json<> js);

private:
    static bool clientHandleOrder(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                  std::vector<std::string> &fieldNames, std::vector<std::string> &orderFields,
                                  std::vector<bool> &ascendings);

    static void clientHandleNotify(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                   const std::string& tableName,
                                   Table *table, std::vector<std::string> selectedFieldNames,
                                   const std::vector<std::string>& filterCols, const std::vector<View::ComparatorType>& filterCmps,
                                   std::vector<int64_t> filterVals, const std::vector<std::string>& orderFields,
                                   std::vector<bool> ascendings);

    static bool clientHandleFilter(std::ostringstream &resp, const hsql::SelectStatement *selectStmt,
                                   const std::string &tableName,
                                   Table *table, std::vector<std::string> &filterCols,
                                   std::vector<View::ComparatorType> &filterCmps, std::vector<int64_t> &filterVals);
};


#endif //SELECTSUPPORT_H
