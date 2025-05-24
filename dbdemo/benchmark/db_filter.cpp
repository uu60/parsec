//
// Created by 杜建璋 on 25-5-2.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    int num = 100;
    int col = 2;

    if (Conf::_userParams.count("num")) {
        num = std::stoi(Conf::_userParams["num"]);
    }

    if (Conf::_userParams.count("col")) {
        col = std::stoi(Conf::_userParams["col"]);
    }

    std::vector<int64_t> shares(num);
    if (Comm::rank() == 2) {
        for (int i = num; i > 0; i--) {
            shares[num - i] = i;
        }
    }

    shares = Secrets::boolShare(shares, 2, 64, System::nextTask());

    View v;
    if (Comm::isServer()) {
        std::string name = "demo";
        std::vector<std::string> fn(col);
        for (int i = 0; i < col; i++) {
            fn[i] = "a" + std::to_string(i);
        }
        std::vector<int> ws(col, 64);

        Table t(name, fn, ws, "a0");
        for (int i = 0; i < shares.size(); i++) {
            std::vector<int64_t> r(col);
            for (int j = 0; j < col; j++) {
                r[j] = shares[i];
            }
            t.insert(r);
        }

        v = View::selectAll(t);

        auto start = System::currentTimeMillis();
        std::vector<std::string> a0 = {"a0", "a1"};
        std::vector a1 = {View::ComparatorType::GREATER, View::ComparatorType::GREATER};
        std::vector a2 = {Comm::rank() == 1 ? 0ll : 10ll, Comm::rank() == 1 ? 0ll : 20ll};
        v.filterAnd(a0, a1, a2);
        Log::i("Filter time: {}ms", System::currentTimeMillis() - start);
    }


    for (int i = 0; i < col + 2; i++) {
        auto secrets = Comm::isServer() ? v._dataCols[i] : std::vector<int64_t>();
        shares = Secrets::boolReconstruct(secrets, 2, 64, System::nextTask());

        // if (Comm::rank() == 2) {
        //     for (int j = 0; j < shares.size(); j++) {
        //         if (shares[j] <= 10) {
        //             Log::i("Wrong. share: {} cor: {}", shares[j], num - j - 1);
        //         }
        //     }
        // }
        if (Comm::rank() == 2) {
            for (auto s: shares) {
                std::cout << s << " ";
            }
            std::cout << std::endl;
        }
    }

    System::finalize();
}
