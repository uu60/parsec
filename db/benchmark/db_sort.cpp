
#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "basis/Views.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>

#include "ot/IknpOtBatchOperator.h"
#include "utils/Math.h"
#include "artifact/Artifact.h"
#include "conf/DbConf.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();

    int rows = 100;
    int cols = 2;

    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }

    if (Conf::_userParams.count("cols")) {
        cols = std::stoi(Conf::_userParams["cols"]);
    }

    Log::ir(2, "Sorting benchmark: rows={}, cols={}", rows, cols);

    std::vector<int64_t> shares(rows);
    if (Comm::rank() == 2) {
        for (int i = 0; i < rows; i++) {
            shares[i] = Artifact::workloadRandInt(0, 100);
        }
    }

    shares = Secrets::boolShare(shares, 2, 64, System::nextTask());

    View v;
    std::vector<std::string> sortColumns;
    std::vector<bool> ascendingOrders;
    if (Comm::isServer()) {
        std::string name = "demo";
        std::vector<std::string> fn(cols);
        for (int i = 0; i < cols; i++) {
            fn[i] = "a" + std::to_string(i);
        }
        std::vector<int> ws(cols, 64);

        Table t(name, fn, ws, "");
        for (int i = 0; i < shares.size(); i++) {
            std::vector<int64_t> r(cols);
            for (int j = 0; j < cols; j++) {
                r[j] = shares[i];
            }
            t.insert(r);
        }

        v = Views::selectAll(t);

        for (int i = 0; i < cols; i++) {
            sortColumns.push_back(v._fieldNames[i]);
            ascendingOrders.push_back(true);
        }
    }

    Artifact::Timer artifact_timer("sort");
    if (Comm::isServer()) {
        v.sort(sortColumns, ascendingOrders, 0);
        // Views::revealAndPrint(v);
    }
    artifact_timer.finish(Comm::isServer() ? static_cast<int64_t>(v.rowNum()) : -1);

    System::finalize();
}
