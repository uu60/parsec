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

#include <string>

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();

    int rows = 5; // Number of records in each table
    int table_num = 3; // Number of tables to join (default 3)
    bool hash = false; // Whether to test shuffle bucket join

    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }

    if (Conf::_userParams.count("table_num")) {
        table_num = std::stoi(Conf::_userParams["table_num"]);
        if (table_num < 2) {
            Log::e("table_num must be at least 2");
            return -1;
        }
    }

    if (Conf::_userParams.count("hash")) {
        hash = (Conf::_userParams["hash"] == "true");
    }

    Log::ir(2, "Join benchmark - Rows per table: {}, Table count: {}, Hash join: {}", rows, table_num, hash);

    // Create test data for multiple tables
    std::vector<std::vector<int64_t>> allShares(table_num);
    std::vector<std::vector<int64_t>> allTagShares(table_num);
    
    // Use rows for all tables for simplicity, but could be extended to support different row counts per table
    int rows_per_table = rows;
    
    for (int table_idx = 0; table_idx < table_num; table_idx++) {
        allShares[table_idx].resize(rows_per_table);
        allTagShares[table_idx].resize(rows_per_table);
        
        if (Comm::rank() == 2) {
            for (int i = 0; i < rows_per_table; i++) {
                allShares[table_idx][i] = i; // Join key values 0-9
                // Compute bucket tag using hash of the key value
                int64_t keyValue = allShares[table_idx][i];
                // Simple hash function: (key * 31 + 17) % numBuckets
                allTagShares[table_idx][i] = (keyValue * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM;
            }
        }
        
        allShares[table_idx] = Secrets::boolShare(allShares[table_idx], 2, 64, 0);
        allTagShares[table_idx] = Secrets::boolShare(allTagShares[table_idx], 2, 64, 0);
    }

    View joinResult;

    if (Comm::isServer()) {
        // Create multiple tables dynamically
        std::vector<Table> tables;
        std::vector<View> views;
        
        for (int table_idx = 0; table_idx < table_num; table_idx++) {
            std::vector<std::string> fieldNames = {"id"};
            std::vector<int> fieldWidths = {64};
            
            std::string tableName = "table" + std::to_string(table_idx);
            std::string keyField = "id";
            
            Table table(tableName, fieldNames, fieldWidths, keyField);
            for (int i = 0; i < allShares[table_idx].size(); i++) {
                std::vector<int64_t> record = {allShares[table_idx][i], allTagShares[table_idx][i]};
                table.insert(record);
            }
            
            tables.push_back(std::move(table));
            views.push_back(Views::selectAll(tables.back()));
        }

        // Perform sequential joins: result = table0 JOIN table1 JOIN table2 JOIN ...
        joinResult = views[0];
        std::string joinField = "id";
        auto start = System::currentTimeMillis();

        for (int i = 1; i < table_num; i++) {
            // Log::i("Joining table {} with previous result", i);
            if (hash) {
                joinResult = Views::hashJoin(joinResult, views[i], joinField, joinField, false);
            } else {
                joinResult = Views::nestedLoopJoin(joinResult, views[i], joinField, joinField, false);
            }
            
            // After join, rename the first column (join key) back to "id" for next iteration
            // Also rename the corresponding tag column
            if (!joinResult._fieldNames.empty()) {
                // Find and rename the tag column for the first field
                std::string oldTagName = Table::BUCKET_TAG_PREFIX + joinResult._fieldNames[0];
                std::string newTagName = Table::BUCKET_TAG_PREFIX + "id";
                
                for (size_t j = 0; j < joinResult._fieldNames.size(); j++) {
                    if (joinResult._fieldNames[j] == oldTagName) {
                        joinResult._fieldNames[j] = newTagName;
                        break;
                    }
                }
                
                // Rename the first column (join key) to "id"
                joinResult._fieldNames[0] = "id";
            }
            
            // Log::i("Intermediate result has {} records", joinResult._dataCols.empty() ? 0 : joinResult._dataCols[0].size());
        }

        // joinResult.clearInvalidEntries(0);
        // Views::revealAndPrint(joinResult);
        auto elapsed = System::currentTimeMillis() - start;
        Log::i("Multi-table join completed in {}ms", elapsed);
        Log::i("Final result has {} records", joinResult._dataCols.empty() ? 0 : joinResult._dataCols[0].size());
    }

    // Reconstruct all data at once - combine all columns into one vector
    std::vector<int64_t> allSecrets;
    int numCols = 8;
    int numRows;

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


    System::finalize();
    return 0;
}
