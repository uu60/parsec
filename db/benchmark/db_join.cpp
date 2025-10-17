
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

#include "utils/Math.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();

    int rows = 5;
    int table_num = 3;
    bool hash = false;

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

    std::vector<std::vector<int64_t> > allShares(table_num);
    std::vector<std::vector<int64_t> > allTagShares(table_num);

    int rows_per_table = rows;

    for (int table_idx = 0; table_idx < table_num; table_idx++) {
        allShares[table_idx].resize(rows_per_table);
        allTagShares[table_idx].resize(rows_per_table);

        if (Comm::rank() == 2) {
            for (int i = 0; i < rows_per_table; i++) {
                allShares[table_idx][i] = Math::randInt();
                int64_t keyValue = allShares[table_idx][i];
                allTagShares[table_idx][i] = (keyValue * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM;
            }
        }

        allShares[table_idx] = Secrets::boolShare(allShares[table_idx], 2, 64, 0);
        allTagShares[table_idx] = Secrets::boolShare(allTagShares[table_idx], 2, 64, 0);
    }

    View joinResult;

    if (Comm::isServer()) {
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

        joinResult = views[0];
        std::string joinField = "id";
        auto start = System::currentTimeMillis();

        for (int i = 1; i < table_num; i++) {
            if (hash) {
                joinResult = Views::hashJoin(joinResult, views[i], joinField, joinField, false);
            } else {
                joinResult = Views::nestedLoopJoin(joinResult, views[i], joinField, joinField, false);
            }

            if (!joinResult._fieldNames.empty()) {
                std::string oldTagName = Table::BUCKET_TAG_PREFIX + joinResult._fieldNames[0];
                std::string newTagName = Table::BUCKET_TAG_PREFIX + "id";

                for (size_t j = 0; j < joinResult._fieldNames.size(); j++) {
                    if (joinResult._fieldNames[j] == oldTagName) {
                        joinResult._fieldNames[j] = newTagName;
                        break;
                    }
                }

                joinResult._fieldNames[0] = "id";

                std::vector<std::string> fieldNames = {"id", "$tag:id"};
                joinResult.select(fieldNames);
            }
        }

        auto elapsed = System::currentTimeMillis() - start;
        Log::i("Multi-table join completed in {}ms", elapsed);
        Log::i("Final result has {} records", joinResult._dataCols.empty() ? 0 : joinResult._dataCols[0].size());
    }

    std::vector<int64_t> allSecrets;
    int numCols = 8;
    int numRows;

    if (Comm::isServer() && !joinResult._dataCols.empty()) {
        numCols = joinResult._dataCols.size();
        numRows = joinResult._dataCols[0].size();

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
