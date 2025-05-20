#include <iostream>
#include <sstream>
#include "../third_party/json.hpp"
#include "basis/Table.h"
#include "operator/SelectSupport.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"
#include "utils/System.h"
using json = nlohmann::json;
#include "socket/LocalServer.h"
#include "dbms/SystemManager.h"
#include "../third_party/hsql/SQLParserResult.h"
#include "../third_party/hsql/SQLParser.h"

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        LocalServer &server = LocalServer::getInstance();
        server.run();
    } else {
        SystemManager::getInstance().serverExecute();
    }

    System::finalize();
    // hsql::SQLParserResult result;
    // hsql::SQLParser::parse("select * from t where a > 9223372036854775808 and 20 <= b and 10 = c", &result);
    return 0;
}
