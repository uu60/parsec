//
// Created by 杜建璋 on 25-7-16.
//

#include "compute/single/arith/ArithLessOperator.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    int num = 10000;
    int width = 64;

    if (Conf::_userParams.count("num")) {
        num = std::stoi(Conf::_userParams["num"]);
    }

    if (Conf::_userParams.count("width")) {
        width = std::stoi(Conf::_userParams["width"]);
    }

    std::vector<int64_t> origins;
    if (Comm::isClient()) {
        origins.resize(num);
        Log::i("=====test arith share comparator=====");
        for (int i = 0; i < num; i++) {
            origins[i] = Math::randInt(0, num);
        }
    }
    auto secrets = Secrets::arithShare(origins, Comm::rank(), width, 0);

    // arith compare less than



    System::finalize();
}
