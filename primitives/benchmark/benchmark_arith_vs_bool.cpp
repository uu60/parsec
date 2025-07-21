//
// Created by 杜建璋 on 25-7-16.
//

#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
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

    std::vector<int64_t> originsA, originsB;
    // arith compare less than
    if (Comm::isClient()) {
        originsA.resize(num);
        originsB.resize(num);
        for (int i = 0; i < num; i++) {
            originsA[i] = Math::randInt(0, num);
            originsB[i] = Math::randInt(0, num);
        }
    }
    auto secretsA = Secrets::arithShare(originsA, 2, width, 0);
    auto secretsB = Secrets::arithShare(originsB, 2, width, 0);

    int64_t start = System::currentTimeMillis();
    ArithLessBatchOperator(&secretsA, &secretsB, width, System::nextTask(), 0, -1).execute();
    if (Comm::isServer()) {
        Log::i("ArithLessBatchOperator total time: {}ms", System::currentTimeMillis() - start);
    }

    // bool compare less than
    secretsA = Secrets::boolShare(originsA, 2, width, 0);
    secretsB = Secrets::boolShare(originsB, 2, width, 0);
    start = System::currentTimeMillis();
    BoolLessBatchOperator(&secretsA, &secretsB, width, System::nextTask(), 0, -1).execute();
    if (Comm::isServer()) {
        Log::i("BoolLessBatchOperator total time: {}ms", System::currentTimeMillis() - start);
    }

    System::finalize();
}
