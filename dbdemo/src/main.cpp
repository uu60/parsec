#include <SQLParser.h>
#include <sstream>
#include "../utils/Comm.h"
#include "../third_party/json.hpp"
using json = nlohmann::json;
#include "socket/LocalServer.h"
#include "dbms/SystemManager.h"

int main(int argc, char **argv) {
    Comm::init(argc, argv);

    if (Comm::rank() == Comm::CLIENT_RANK) {
        LocalServer &server = LocalServer::getInstance();
        server.run();
    } else {
        SystemManager::getInstance().serverExecute();
    }

    Comm::finalize();
    return 0;
    // hsql::SQLParserResult result;
    // hsql::SQLParser::parse("create table t (a int(8));", &result);
}
