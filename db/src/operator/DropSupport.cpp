//
// Created by 杜建璋 on 2024/11/3.
//

#include "operator/DropSupport.h"

#include <sstream>
#include <string>
#include "../third_party/hsql/SQLParser.h"

#include "dbms/SystemManager.h"


bool DropSupport::clientDropTable(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    const auto *dropStmt = dynamic_cast<const hsql::DropStatement *>(stmt);

    if (dropStmt->type == hsql::kDropTable) {
        std::string tableName = dropStmt->name;

        std::string msg;
        if (!SystemManager::getInstance()._currentDatabase->dropTable(tableName, msg)) {
            resp << "Failed " << msg << std::endl;
            return false;
        }

        // notify servers
        json j;
        j["type"] = SystemManager::getCommandPrefix(SystemManager::DROP_TABLE);
        j["name"] = tableName;
        SystemManager::notifyServersSync(j);

        resp << "OK. Table " + tableName + " dropped successfully." << std::endl;
        return true;
    }
    resp << "Failed. Unsupported DROP statement type." << std::endl;
    return false;
}

void DropSupport::serverDropTable(json j) {
    std::string tbName = j.at("name").get<std::string>();
    std::string msg;
    SystemManager::getInstance()._currentDatabase->dropTable(tbName, msg);
}
