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

int main(int argc, char **argv) {
    // Comm::init(argc, argv);
    //
    // if (Comm::rank() == Comm::CLIENT_RANK) {
    //     LocalServer &server = LocalServer::getInstance();
    //     server.run();
    // } else {
    //     SystemManager::getInstance().serverExecute();
    // }
    //
    // Comm::finalize();
    // return 0;
    // hsql::SQLParserResult result;
    // hsql::SQLParser::parse("create table t (a int(8));", &result);

    System::init(argc, argv);

    int num = 100;
    std::vector<int64_t> shares(num);
    if (Comm::rank() == 2) {
        for (int i = num; i >= 0; i--) {
            shares[num - i] = i;
        }
    }

    shares = Secrets::boolShare(shares, 2, 64, System::nextTask());

    // View v;
    // if (Comm::isServer()) {
    //     std::string name = "demo";
    //     std::vector<std::string> fn = {"a"};
    //     std::vector<int> ws = {64};
    //     Table t(name, fn, ws);
    //     for (int i = 0; i < shares.size(); i++) {
    //         std::vector<int64_t> r = {shares[i]};
    //         t.insert(r);
    //     }
    //
    //     v = SelectSupport::selectAll(t);
    //     v.sort("a", true, 0);
    // }
    //
    //
    // auto secrets = Comm::isServer() ? v._dataCols[0] : std::vector<int64_t>();
    // shares = Secrets::boolReconstruct(secrets, 2, 64, System::nextTask());
    //
    //
    // if (Comm::rank() == 2) {
    //     Log::i(StringUtils::toString(shares));
    // }
    View v;
    if (Comm::isServer()) {
        std::string name = "demo";
        std::vector<std::string> fn = {"a", "b"};
        std::vector<int> ws = {64, 64};
        Table t(name, fn, ws);
        for (int i = 0; i < shares.size(); i++) {
            std::vector<int64_t> r = {shares[i], shares[shares.size() - 1 - i]};
            t.insert(r);
        }

        v = SelectSupport::selectAll(t);
        v.sort("b", false, 0);
    }


    auto secrets = Comm::isServer() ? v._dataCols[0] : std::vector<int64_t>();
    shares = Secrets::boolReconstruct(secrets, 2, 64, System::nextTask());

    auto secrets1 = Comm::isServer() ? v._dataCols[1] : std::vector<int64_t>();
    Log::i("s0: {} s1: {}", secrets.size(), secrets1.size());
    auto shares1 = Secrets::boolReconstruct(secrets1, 2, 64, System::nextTask());

    if (Comm::rank() == 2) {
        Log::i(StringUtils::toString(shares));
        Log::i(StringUtils::toString(shares1));
    }

    System::finalize();
}
