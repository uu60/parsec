//
// Created by 杜建璋 on 2024/10/25.
//

#include "dbms/SystemManager.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <limits>
#include <iomanip>
#include <SQLParser.h>
#include <mpc_package/utils/Log.h>
#include <sql/SQLStatement.h>
#include <nlohmann/json.hpp>

#include "basis/TableRecord.h"
#include "basis/TempRecord.h"
#include "socket/LocalServer.h"
#include <mpc_package/utils/System.h>

#include "operator/Select.h"
#include "operator/Insert.h"
#include "operator/Create.h"
#include "operator/Drop.h"

using json = nlohmann::json;

SystemManager::CommandType SystemManager::getCommandType(const std::string &prefix) {
    static const std::unordered_map<std::string, SystemManager::CommandType> typeMap = {
        {"exit", EXIT},
        {"cdb", CREATE_DB},
        {"ddb", DROP_DB},
        {"udb", USE_DB},
        {"ctb", CREATE_TABLE},
        {"dtb", DROP_TABLE},
        {"ins", INSERT},
        {"sel", SELECT}
    };
    auto it = typeMap.find(prefix);
    return (it != typeMap.end()) ? it->second : SystemManager::UNKNOWN;
}

std::string SystemManager::getCommandPrefix(CommandType type) {
    static const std::unordered_map<CommandType, std::string> typeMap = {
        {EXIT, "exit"},
        {CREATE_DB, "cdb"},
        {DROP_DB, "ddb"},
        {USE_DB, "udb"},
        {CREATE_TABLE, "ctb"},
        {DROP_TABLE, "dtb"},
        {INSERT, "ins"},
        {SELECT, "sel"}
    };
    auto it = typeMap.find(type);
    return (it != typeMap.end()) ? it->second : "exit";
}

SystemManager &SystemManager::getInstance() {
    static SystemManager instance;
    return instance;
}

bool SystemManager::createDatabase(const std::string &dbName, std::string &msg) {
    if (_databases.find(dbName) != _databases.end()) {
        msg = "Database " + dbName + " already exists.";
        return false;
    }
    _databases[dbName] = Database(dbName);
    return true;
}

bool SystemManager::dropDatabase(const std::string &dbName, std::string &msg) {
    if (_currentDatabase && _currentDatabase->name() == dbName) {
        _currentDatabase = nullptr;
    }
    if (_databases.erase(dbName) > 0) {
        return true;
    }
    msg = "Database " + dbName + " does not exist.";
    return false;
}

bool SystemManager::useDatabase(const std::string &dbName, std::string &msg) {
    auto it = _databases.find(dbName);
    if (it != _databases.end()) {
        _currentDatabase = &(it->second);
        return true;
    }
    msg = "Database " + dbName + " does not exist.";
    return false;
}

std::string handleEndingDatabaseName(std::istringstream &iss, std::ostringstream &resp) {
    std::string dbName;
    iss >> dbName;
    // missing db name
    if (dbName.empty()) {
        resp << "Failed. Syntax error: Missing database name." << std::endl;
        return "";
    }

    // remove extra chars
    std::string remaining;
    std::getline(iss, remaining);
    remaining.erase(std::remove_if(remaining.begin(), remaining.end(), ::isspace), remaining.end());

    // handle `;`
    if (!remaining.empty() && remaining != ";") {
        resp << "Failed. Syntax error: Invalid characters after database name." << std::endl;
        return "";
    }
    if (dbName.back() == ';') {
        dbName.pop_back();
    }

    return dbName;
}

void SystemManager::notifyServersSync(json &j) {
    int done;
    std::string m = j.dump();
    Comm::send(&m, 0);
    Comm::send(&m, 1);

    // sync
    Comm::recv(&done, 0);
    Comm::recv(&done, 1);
}

// return if is create table
bool SystemManager::clientCreateDeleteDb(std::istringstream &iss, std::ostringstream &resp, std::string &word,
                                         bool create) {
    iss >> word;
    if (strcasecmp(word.c_str(), "database") == 0) {
        std::string dbName = handleEndingDatabaseName(iss, resp);

        if (dbName.empty()) {
            return false;
        }

        if (create) {
            // execute
            std::string msg;
            if (!createDatabase(dbName, msg)) {
                resp << "Failed. " << msg << std::endl;
                return false;
            }
            // notify servers
            json j;
            j["type"] = getCommandPrefix(CREATE_DB);
            j["name"] = dbName;
            notifyServersSync(j);

            resp << "OK. Database `" + dbName + "` created." << std::endl;
        } else {
            std::string msg;
            if (!dropDatabase(dbName, msg)) {
                resp << "Failed. " << msg << std::endl;
                return false;
            }

            // notify servers
            json j;
            j["type"] = getCommandPrefix(DROP_DB);
            j["name"] = dbName;
            notifyServersSync(j);

            resp << "OK. Database " + dbName + " dropped." << std::endl;
        }
        return false;
    }
    return true;
}

void SystemManager::clientUseDb(std::istringstream &iss, std::ostringstream &resp) {
    std::string dbName = handleEndingDatabaseName(iss, resp);
    std::string msg;
    if (!useDatabase(dbName, msg)) {
        resp << "Failed. " << msg << std::endl;
        return;
    }
    // notify servers
    json j;
    j["type"] = getCommandPrefix(USE_DB);
    j["name"] = dbName;
    notifyServersSync(j);

    resp << "OK. Database `" + dbName + "` selected." << std::endl;
}

void SystemManager::clientExecute(const std::string &command) {
    int64_t start = System::currentTimeMillis();
    std::istringstream iss(command);
    std::string word;
    iss >> word;
    std::ostringstream resp;
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(command, &result);
    // handle `create db` and `use db`
    bool create = strcasecmp(word.c_str(), "create") == 0;
    bool drop = strcasecmp(word.c_str(), "drop") == 0;
    if (create || drop) {
        // return if create table
        if (!clientCreateDeleteDb(iss, resp, word, create)) goto over;
    }

    if (strcasecmp(word.c_str(), "use") == 0) {
        clientUseDb(iss, resp);
        goto over;
    }

    if (!_currentDatabase) {
        resp << "Failed. No database selected." << std::endl;
        goto over;
    }

    if (!result.isValid()) {
        resp << "Failed. " << result.errorMsg() << std::endl;
        goto over;
    }

    for (int si = 0; si < result.getStatements().size(); si++) {
        auto stmt = result.getStatement(si);
        switch (stmt->type()) {
            case hsql::kStmtCreate: {
                if (!Create::clientCreateTable(resp, stmt)) goto over;
                break;
            }
            case hsql::kStmtDrop: {
                if (!Drop::clientDropTable(resp, stmt)) goto over;
                break;
            }
            case hsql::kStmtInsert: {
                if (!Insert::clientInsert(resp, stmt)) goto over;
                break;
            }
            case hsql::kStmtSelect: {
                if (!Select::clientSelect(resp, stmt)) goto over;
                break;
            }
            default: {
                resp << "Syntax error." << std::endl;
                goto over;
            }
        }
    }
over:
    resp << "(" << System::currentTimeMillis() - start << " ms)" << std::endl;
    LocalServer::getInstance().send_(resp.str());
}

void SystemManager::log(const std::string &msg, bool success) {
    LocalServer::getInstance().send_((success ? "OK. " : "Failed. ") + msg + "\n");
}

void SystemManager::serverExecute() {
    while (true) {
        std::string jstr;
        Comm::recv(&jstr, Comm::CLIENT_RANK);
        auto j = json::parse(jstr);
        std::string type = j.at("type").get<std::string>();
        auto commandType = getCommandType(type);

        switch (commandType) {
            case EXIT: {
                Comm::send(&done, Comm::CLIENT_RANK);
                break;
            }
            case CREATE_DB: {
                // create database
                std::string dbName = j.at("name").get<std::string>();
                std::string msg;
                createDatabase(dbName, msg);
                break;
            }
            case DROP_DB: {
                std::string dbName = j.at("name").get<std::string>();
                std::string msg;
                dropDatabase(dbName, msg);
                break;
            }
            case USE_DB: {
                std::string dbName = j.at("name").get<std::string>();
                std::string msg;
                useDatabase(dbName, msg);
                break;
            }
            case CREATE_TABLE: {
                Create::serverCreateTable(j);
                break;
            }
            case DROP_TABLE: {
                Drop::serverDropTable(j);
                break;
            }
            case INSERT: {
                Insert::serverInsert(j);
                break;
            }
            case SELECT: {
                Select::serverSelect(j);
                break;
            }
            case UNKNOWN: {
                std::cerr << "Unknown command type: " << type << std::endl;
                break;
            }
        }
        // sync
        Comm::send(&done, Comm::CLIENT_RANK);
    }
}
