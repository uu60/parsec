
#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "basis/Views.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>

#include "utils/Math.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

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
            shares[i] = Math::randInt(0, 100);
        }
    }

    shares = Secrets::boolShare(shares, 2, 64, System::nextTask());

    View v;
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

        std::vector<std::string> sortColumns;
        std::vector<bool> ascendingOrders;
        for (int i = 0; i < cols; i++) {
            sortColumns.push_back(v._fieldNames[i]);
            ascendingOrders.push_back(true);
        }

        auto start = System::currentTimeMillis();
        v.sort(sortColumns, ascendingOrders, 0);
        Log::i("Multi-column sort time (all {} columns): {}ms", cols, System::currentTimeMillis() - start);
        // Views::revealAndPrint(v);
    }

    System::finalize();
}
