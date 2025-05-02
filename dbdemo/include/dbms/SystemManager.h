// //
// // Created by 杜建璋 on 2024/10/25.
// //
//
// #ifndef SMPC_DATABASE_DBMS_H
// #define SMPC_DATABASE_DBMS_H
//
// #include <iostream>
// #include <vector>
// #include <map>
// #include "../basis/Database.h"
// #include <nlohmann/json.hpp>
// #include <sql/SQLStatement.h>
//
// using json = nlohmann::json;
//
// class SystemManager {
// public:
//     enum CommandType {
//         EXIT,
//         CREATE_DB,
//         DROP_DB,
//         USE_DB,
//         CREATE_TABLE,
//         DROP_TABLE,
//         INSERT,
//         SELECT,
//         UNKNOWN
//     };
//
//     std::map<std::string, Database> _databases;
//     Database *_currentDatabase = nullptr;
//
//     // temp
//     int done{};
//
// private:
//     // private constructor
//     SystemManager() = default;
//
// public:
//     // forbid clone
//     SystemManager(const SystemManager &) = delete;
//
//     SystemManager &operator=(const SystemManager &) = delete;
//
//     static SystemManager &getInstance();
//
//     bool createDatabase(const std::string &dbName, std::string &msg);
//
//     bool dropDatabase(const std::string &dbName, std::string &msg);
//
//     bool useDatabase(const std::string &dbName, std::string &msg);
//
//     static void log(const std::string &msg, bool success);
//
//     void clientExecute(const std::string &command);
//
//     static void notifyServersSync(json &j);
//
//     void serverExecute();
//
//     static CommandType getCommandType(const std::string &prefix);
//
//     static std::string getCommandPrefix(CommandType type);
//
// private:
//     bool clientCreateDeleteDb(std::istringstream &iss, std::ostringstream &resp, std::string &word, bool create);
//
//     void clientUseDb(std::istringstream &iss, std::ostringstream &resp);
// };
//
// #endif //SMPC_DATABASE_DBMS_H
