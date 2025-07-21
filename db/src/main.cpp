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

void runDb(int argc, char **argv);

int main(int argc, char **argv) {
    runDb(argc, argv);
    return 0;
}

void runDb(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        LocalServer &server = LocalServer::getInstance();
        server.run();
    } else {
        SystemManager::getInstance().serverExecute();
    }

    System::finalize();
}
