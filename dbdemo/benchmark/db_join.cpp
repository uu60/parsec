//
// Created by 杜建璋 on 25-6-3.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"
#include "conf/DbConf.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    int num0 = 5; // Number of records in table 0
    int num1 = 5; // Number of records in table 1
    bool testShuffle = false; // Whether to test shuffle bucket join

    if (Conf::_userParams.count("num0")) {
        num0 = std::stoi(Conf::_userParams["num0"]);
    }

    if (Conf::_userParams.count("num1")) {
        num1 = std::stoi(Conf::_userParams["num1"]);
    }

    if (Conf::_userParams.count("shuffle")) {
        testShuffle = (Conf::_userParams["shuffle"] == "1");
    }

    Log::i("Testing join with {} records in table0, {} records in table1", num0, num1);
    Log::i("Shuffle bucket join: {}", testShuffle ? "enabled" : "disabled");

    // Create test data for table 0
    std::vector<int64_t> shares0(num0);
    std::vector<int64_t> tagShares0(num0);
    if (Comm::rank() == 2) {
        for (int i = 0; i < num0; i++) {
            shares0[i] = i % 10; // Join key values 0-9
            // Compute bucket tag using hash of the key value
            int64_t keyValue = shares0[i];
            // Simple hash function: (key * 31 + 17) % numBuckets
            tagShares0[i] = (keyValue * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM;
        }
    }


    if (Comm::isClient()) {
        Log::i("shares0: {} tagShares0: {}", StringUtils::vecString(shares0), StringUtils::vecString(tagShares0));
    }
    shares0 = Secrets::boolShare(shares0, 2, 64, 0);
    tagShares0 = Secrets::boolShare(tagShares0, 2, 64, 0);

    // Create test data for table 1
    std::vector<int64_t> shares1(num1);
    std::vector<int64_t> tagShares1(num1);
    if (Comm::rank() == 2) {
        for (int i = 0; i < num1; i++) {
            shares1[i] = i % 10; // Join key values 0-9
            // Compute bucket tag using the same hash function as table 0
            int64_t keyValue = shares1[i];
            // Same hash function: (key * 31 + 17) % numBuckets
            tagShares1[i] = (keyValue * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM;
        }
    }

    if (Comm::isClient()) {
        Log::i("shares1: {} tagShares1: {}", StringUtils::vecString(shares1), StringUtils::vecString(tagShares1));
    }
    shares1 = Secrets::boolShare(shares1, 2, 64, 0);
    tagShares1 = Secrets::boolShare(tagShares1, 2, 64, 0);


    View joinResult;

    if (Comm::isServer()) {
        // Create table 0
        std::vector<std::string> fieldNames0 = {"id", "value"};
        std::vector<int> fieldWidths0 = {64, 64};

        std::string tableName0 = "table0";
        std::string keyField0 = "id";
        Table table0(tableName0, fieldNames0, fieldWidths0, keyField0);
        for (int i = 0; i < shares0.size(); i++) {
            std::vector<int64_t> record = {shares0[i], shares0[i], tagShares0[i]};
            if (testShuffle) {
                record.push_back(tagShares0[i]);
            }
            table0.insert(record);
        }

        // Create table 1
        std::vector<std::string> fieldNames1 = {"id", "data"};
        std::vector<int> fieldWidths1 = {64, 64};

        std::string tableName1 = "table1";
        std::string keyField1 = "id";
        Table table1(tableName1, fieldNames1, fieldWidths1, keyField1);
        for (int i = 0; i < shares1.size(); i++) {
            std::vector<int64_t> record = {shares1[i], shares1[i], tagShares1[i]};
            if (testShuffle) {
                record.push_back(tagShares1[i]);
            }
            table1.insert(record);
        }

        // Create views
        View view0 = Views::selectAll(table0);
        View view1 = Views::selectAll(table1);

        std::string joinField0 = "id";
        std::string joinField1 = "id";

        auto start = System::currentTimeMillis();

        if (testShuffle) {
            Log::i("Starting shuffle bucket join...");
            joinResult = Views::shuffleBucketJoin(view0, view1, joinField0, joinField1);
        } else {
            Log::i("Starting nested loop join...");
            joinResult = Views::nestedLoopJoin(view0, view1, joinField0, joinField1);
        }

        auto elapsed = System::currentTimeMillis() - start;
        Log::i("Join completed in {}ms", elapsed);
        Log::i("Result has {} records", joinResult._dataCols.empty() ? 0 : joinResult._dataCols[0].size());
    }

    // Reconstruct all data at once - combine all columns into one vector
    std::vector<int64_t> allSecrets;
    int numCols = 8;
    int numRows = num0 * num1;

    if (Comm::isServer() && !joinResult._dataCols.empty()) {
        numCols = joinResult._dataCols.size();
        numRows = joinResult._dataCols[0].size();

        // Flatten all columns into one array: col0[0], col0[1], ..., col0[n], col1[0], col1[1], ..., col1[n], ...
        allSecrets.reserve(numCols * numRows);
        for (int col = 0; col < numCols; col++) {
            for (int row = 0; row < numRows; row++) {
                allSecrets.push_back(joinResult._dataCols[col][row]);
            }
        }
    }
    Log::i("numcols: {} fields: {}", numCols, StringUtils::vecString(joinResult._fieldNames));
    // Single reconstruct call for all data
    auto allReconstructed = Secrets::boolReconstruct(allSecrets, 2, 64, System::nextTask());

    if (Comm::rank() == 2) {
        for (int col = 0; col < numCols; col++) {
            std::vector<int64_t> tempCol;
            for (int row = 0; row < numRows; row++) {
                tempCol.push_back(allReconstructed[col * numRows + row]);
            }
            Log::i("col[{}]: {}", col, StringUtils::vecString(tempCol));
        }
    }


    System::finalize();
    return 0;
}
