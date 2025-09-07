//
// Created by 杜建璋 on 25-8-18.
//

/*
 * SELECT S.ID
 * FROM (
 *     SELECT ID, MIN(CS) as cs1, MAX(CS) as cs2
 *     FROM R WHERE R.year=2019 GROUP BY ID
 *     ) as S
 * WHERE S.cs2 - S.cs1 > c
 */

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/basis/Views.h"
#include "../include/basis/Table.h"
#include "../include/operator/SelectSupport.h"
#include "conf/DbConf.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"
#include "utils/Math.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "compute/batch/arith/ArithAddBatchOperator.h"
#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <map>

// Forward declarations
void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &cs_data,
                      std::vector<int64_t> &year_data);

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &cs_shares,
                  std::vector<int64_t> &year_shares);

View filterByYear(View &r_view, int64_t target_year, int tid);

View groupByAndAggregate(View &filtered_view, int tid);

View filterByDifference(View &aggregated_view, int64_t threshold_c, int tid);

void displayResults(View &result_view, int tid);

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    DbConf::init();
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int rows = 1000;

    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }

    if (Comm::isClient()) {
        Log::i("Data size: R: {}", rows);
    }

    // Generate test data
    std::vector<int64_t> id_data, cs_data, year_data;
    generateTestData(rows, id_data, cs_data, year_data);

    // Convert to secret shares for 2PC
    auto id_shares = Secrets::boolShare(id_data, 2, 64, tid);
    auto cs_shares = Secrets::boolShare(cs_data, 2, 64, tid);
    auto year_shares = Secrets::boolShare(year_data, 2, 64, tid);

    int64_t target_year, threshold_c;
    if (Comm::isClient()) {
        int64_t target_year0 = Math::randInt();
        int64_t target_year1 = target_year0 ^ 2019;
        int64_t threshold_c0 = Math::randInt();
        int64_t threshold_c1 = Math::randInt();
        Comm::send(target_year0, 64, 0, tid);
        Comm::send(threshold_c0, 64, 0, tid);
        Comm::send(target_year1, 64, 1, tid);
        Comm::send(threshold_c1, 64, 1, tid);
    } else {
        Comm::receive(target_year, 64, 2, tid);
        Comm::receive(threshold_c, 64, 2, tid);
    }

    View result_view;
    if (Comm::isServer()) {
        auto query_start = System::currentTimeMillis();

        // Create table R
        auto r_view = createRTable(id_shares, cs_shares, year_shares);

        // Step 1: Filter by year = 2019
        auto filtered_view = filterByYear(r_view, target_year, tid);

        // Step 2: GROUP BY ID and compute MIN(CS), MAX(CS)
        auto aggregated_view = groupByAndAggregate(filtered_view, tid);

        // Step 3: Filter by cs2 - cs1 > c
        result_view = filterByDifference(aggregated_view, threshold_c, tid);

        auto query_end = System::currentTimeMillis();
        Log::i("Total query execution time: {}ms", query_end - query_start);
    }

    System::finalize();
    return 0;
}

// Function implementations

void generateTestData(int rows,
                      std::vector<int64_t> &id_data,
                      std::vector<int64_t> &cs_data,
                      std::vector<int64_t> &year_data) {
    if (Comm::rank() == 2) {
        id_data.reserve(rows);
        cs_data.reserve(rows);
        year_data.reserve(rows);

        for (int i = 0; i < rows; i++) {
            int64_t id = Math::randInt(); // Groups 1-10
            int64_t year = Math::randInt(); // Random year
            int64_t cs = Math::randInt(); // Random CS value 0-999

            id_data.push_back(id);
            year_data.push_back(year);
            cs_data.push_back(cs);
        }
    }
}

View createRTable(std::vector<int64_t> &id_shares,
                  std::vector<int64_t> &cs_shares,
                  std::vector<int64_t> &year_shares) {
    std::string table_name = "R";
    std::vector<std::string> fields = {"ID", "CS", "year"};
    std::vector<int> widths = {64, 64, 64};

    Table r_table(table_name, fields, widths, "");

    for (size_t i = 0; i < id_shares.size(); i++) {
        std::vector<int64_t> row = {
            id_shares[i],
            cs_shares[i],
            year_shares[i]
        };
        r_table.insert(row);
    }

    auto r_view = Views::selectAll(r_table);
    return r_view;
}

View filterByYear(View &r_view, int64_t target_year, int tid) {
    std::vector<std::string> fieldNames = {"year"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {target_year};

    View filtered_view = r_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    return filtered_view;
}

View groupByAndAggregate(View &filtered_view, int tid) {
    if (filtered_view.rowNum() == 0) {
        // Create empty result
        std::vector<std::string> result_fields = {"ID", "cs1", "cs2"};
        std::vector<int> result_widths = {64, 64, 64};
        View result_view(result_fields, result_widths);
        return result_view;
    }

    // Perform GROUP BY on ID field
    auto heads = filtered_view.groupBy("ID", tid);

    // Use the new minAndMax function to compute both min and max in one operation
    filtered_view.minAndMax(heads, "CS", "cs1", "cs2", tid);

    return filtered_view;
}

View filterByDifference(View &aggregated_view, int64_t threshold_c, int tid) {
    if (aggregated_view.rowNum() == 0) {
        return aggregated_view;
    }

    auto &cs1_data = aggregated_view._dataCols[aggregated_view.colIndex("cs1")]; // cs1 (min)
    auto &cs2_data = aggregated_view._dataCols[aggregated_view.colIndex("cs2")]; // cs2 (max)

    std::vector<int64_t> comparison_result_bool;

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        // Single batch processing
        
        // Step 1: Convert bool shares to arith shares
        auto cs1_arith = BoolToArithBatchOperator(&cs1_data, 64, 0, tid,
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
        auto cs2_arith = BoolToArithBatchOperator(&cs2_data, 64, 0, tid + BoolToArithBatchOperator::tagStride(),
                                                  SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Step 2: Compute cs2 - cs1 using addition: cs2 + (-cs1)
        std::vector<int64_t> neg_cs1_arith;
        neg_cs1_arith.reserve(cs1_arith.size());
        for (const auto &val : cs1_arith) {
            neg_cs1_arith.push_back(-val);
        }

        auto differences = ArithAddBatchOperator(&cs2_arith, &neg_cs1_arith, 64, 0, 
                                                 tid + 2 * BoolToArithBatchOperator::tagStride(),
                                                 SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Step 3: Compare with threshold: diff > threshold_c (threshold < diff)
        std::vector<int64_t> threshold_arith(differences.size(), threshold_c);
        auto comparison_result_arith = ArithLessBatchOperator(&threshold_arith, &differences, 64, 0, 
                                                              tid + 2 * BoolToArithBatchOperator::tagStride() + ArithAddBatchOperator::tagStride(),
                                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Step 4: Convert comparison result back to bool shares
        comparison_result_bool = ArithToBoolBatchOperator(&comparison_result_arith, 64, 0, 
                                                          tid + 2 * BoolToArithBatchOperator::tagStride() + ArithAddBatchOperator::tagStride() + ArithLessBatchOperator::tagStride(64),
                                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        // Multi-batch processing
        
        size_t data_size = cs1_data.size();
        int batchSize = Conf::BATCH_SIZE;
        int batchNum = (data_size + batchSize - 1) / batchSize;

        std::vector<std::future<std::vector<int64_t>>> batch_futures(batchNum);

        for (int b = 0; b < batchNum; ++b) {
            batch_futures[b] = ThreadPoolSupport::submit([&, b]() -> std::vector<int64_t> {
                int start = b * batchSize;
                int end = std::min(start + batchSize, static_cast<int>(data_size));

                std::vector<int64_t> batch_cs1(cs1_data.begin() + start, cs1_data.begin() + end);
                std::vector<int64_t> batch_cs2(cs2_data.begin() + start, cs2_data.begin() + end);

                int batch_tid = tid + b * (2 * BoolToArithBatchOperator::tagStride() + ArithAddBatchOperator::tagStride() + ArithLessBatchOperator::tagStride(64) + ArithToBoolBatchOperator::tagStride(64));

                // Step 1: Convert bool shares to arith shares
                auto cs1_arith = BoolToArithBatchOperator(&batch_cs1, 64, 0, batch_tid,
                                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
                auto cs2_arith = BoolToArithBatchOperator(&batch_cs2, 64, 0, batch_tid + BoolToArithBatchOperator::tagStride(),
                                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                // Step 2: Compute cs2 - cs1 using addition: cs2 + (-cs1)
                std::vector<int64_t> neg_cs1_arith;
                neg_cs1_arith.reserve(cs1_arith.size());
                for (const auto &val : cs1_arith) {
                    neg_cs1_arith.push_back(-val);
                }

                auto differences = ArithAddBatchOperator(&cs2_arith, &neg_cs1_arith, 64, 0, 
                                                         batch_tid + 2 * BoolToArithBatchOperator::tagStride(),
                                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                // Step 3: Compare with threshold: diff > threshold_c (threshold < diff)
                std::vector<int64_t> threshold_arith(differences.size(), threshold_c);
                auto comparison_result_arith = ArithLessBatchOperator(&threshold_arith, &differences, 64, 0, 
                                                                      batch_tid + 2 * BoolToArithBatchOperator::tagStride() + ArithAddBatchOperator::tagStride(),
                                                                      SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

                // Step 4: Convert comparison result back to bool shares
                return ArithToBoolBatchOperator(&comparison_result_arith, 64, 0, 
                                                batch_tid + 2 * BoolToArithBatchOperator::tagStride() + ArithAddBatchOperator::tagStride() + ArithLessBatchOperator::tagStride(64),
                                                SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
            });
        }

        comparison_result_bool.reserve(data_size);
        for (auto &f : batch_futures) {
            auto batch_res = f.get();
            comparison_result_bool.insert(comparison_result_bool.end(), batch_res.begin(), batch_res.end());
        }
    }

    // Apply the filter condition
    aggregated_view._dataCols[aggregated_view.colNum() + View::VALID_COL_OFFSET] = comparison_result_bool;
    
    aggregated_view.clearInvalidEntries(0);

    // Create final result view with only ID column
    std::vector<std::string> result_fields = {"ID"};
    std::vector<int> result_widths = {64};
    View result_view(result_fields, result_widths);

    if (aggregated_view.rowNum() > 0) {
        result_view._dataCols[0] = aggregated_view._dataCols[0]; // ID column
        result_view._dataCols[result_view.colNum() + View::VALID_COL_OFFSET] =
                std::vector<int64_t>(result_view._dataCols[0].size(), Comm::rank());
        result_view._dataCols[result_view.colNum() + View::PADDING_COL_OFFSET] =
                std::vector<int64_t>(result_view._dataCols[0].size(), 0);
    }

    return result_view;
}

void displayResults(View &result_view, int tid) {
    std::vector<int64_t> id_col;
    if (Comm::isServer() && !result_view._dataCols.empty()) {
        id_col = result_view._dataCols[0];
    }

    auto id_plain = Secrets::boolReconstruct(id_col, 2, 64, tid);

    if (Comm::rank() == 2) {
        Log::i("Number of IDs satisfying condition: {}", id_plain.size());
    }
}
