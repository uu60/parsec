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
    auto tid = System::nextTask() << (32 - Conf::TASK_TAG_BITS);

    // Read parameters from command line
    int rows = 1000;
    int64_t target_year = 2019;
    int64_t threshold_c = 100;

    if (Conf::_userParams.count("rows")) {
        rows = std::stoi(Conf::_userParams["rows"]);
    }
    if (Conf::_userParams.count("year")) {
        target_year = std::stoll(Conf::_userParams["year"]);
    }
    if (Conf::_userParams.count("threshold")) {
        threshold_c = std::stoll(Conf::_userParams["threshold"]);
    }

    // Generate test data
    std::vector<int64_t> id_data, cs_data, year_data;
    generateTestData(rows, id_data, cs_data, year_data);

    // Convert to secret shares for 2PC
    auto id_shares = Secrets::boolShare(id_data, 2, 64, tid);
    auto cs_shares = Secrets::boolShare(cs_data, 2, 64, tid);
    auto year_shares = Secrets::boolShare(year_data, 2, 64, tid);

    View result_view;
    if (Comm::isServer()) {
        Log::i("Starting complex query execution...");
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

    // Display results
    displayResults(result_view, tid);

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

        // Create test data with known groups and values
        // Group 1: ID=1, year=2019, CS values=[10, 30, 20] -> min=10, max=30, diff=20
        // Group 2: ID=2, year=2019, CS values=[50, 200, 100] -> min=50, max=200, diff=150
        // Group 3: ID=3, year=2018, CS values=[40, 80] -> should be filtered out by year
        // Group 4: ID=4, year=2019, CS values=[60, 70] -> min=60, max=70, diff=10

        std::vector<std::tuple<int64_t, int64_t, int64_t> > test_data = {
            {1, 2019, 10}, {1, 2019, 30}, {1, 2019, 20}, // Group 1: diff=20
            {2, 2019, 50}, {2, 2019, 200}, {2, 2019, 100}, // Group 2: diff=150
            {3, 2018, 40}, {3, 2018, 80}, // Group 3: filtered out
            {4, 2019, 60}, {4, 2019, 70} // Group 4: diff=10
        };

        // Fill with test data first
        for (const auto &tuple: test_data) {
            id_data.push_back(std::get<0>(tuple));
            year_data.push_back(std::get<1>(tuple));
            cs_data.push_back(std::get<2>(tuple));
        }

        // Fill remaining with random data if needed
        // for (int i = test_data.size(); i < rows; i++) {
        //     int64_t id = (i % 10) + 1; // Groups 1-10
        //     int64_t year = (Math::randInt() % 2 == 0) ? 2019 : 2018; // Random year
        //     int64_t cs = Math::randInt() % 1000; // Random CS value 0-999
        //
        //     id_data.push_back(id);
        //     year_data.push_back(year);
        //     cs_data.push_back(cs);
        //     tag_data.push_back((id * 31 + 17) % DbConf::SHUFFLE_BUCKET_NUM);
        // }

        Log::i("Generated {} records", rows);
        Log::i("Test data includes known groups:");
        Log::i("  Group 1 (ID=1, year=2019): CS=[10,30,20] -> min=10, max=30, diff=20");
        Log::i("  Group 2 (ID=2, year=2019): CS=[50,200,100] -> min=50, max=200, diff=150");
        Log::i("  Group 3 (ID=3, year=2018): CS=[40,80] -> filtered out by year");
        Log::i("  Group 4 (ID=4, year=2019): CS=[60,70] -> min=60, max=70, diff=10");
        Log::i("Filter values - Alice: year=0, threshold=0; Bob: year=2019, threshold=100");
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
    Log::i("Created table R with {} rows", r_table.rowNum());
    return r_view;
}

View filterByYear(View &r_view, int64_t target_year, int tid) {
    Log::i("Step 1: Filtering by year = {}...", target_year);
    auto step1_start = System::currentTimeMillis();

    std::vector<std::string> fieldNames = {"year"};
    std::vector<View::ComparatorType> comparatorTypes = {View::EQUALS};
    std::vector<int64_t> constShares = {Comm::rank() * target_year};

    View filtered_view = r_view;
    filtered_view.filterAndConditions(fieldNames, comparatorTypes, constShares, tid);

    auto step1_end = System::currentTimeMillis();
    Log::i("Step 1 completed in {}ms", step1_end - step1_start);
    Log::i("Filtered table has {} rows", filtered_view.rowNum());
    return filtered_view;
}

View groupByAndAggregate(View &filtered_view, int tid) {
    Log::i("Step 2: GROUP BY ID and computing MIN(CS), MAX(CS)...");
    auto step2_start = System::currentTimeMillis();

    if (filtered_view.rowNum() == 0) {
        // Create empty result
        std::vector<std::string> result_fields = {"ID", "cs1", "cs2"};
        std::vector<int> result_widths = {64, 64, 64};
        View result_view(result_fields, result_widths);

        auto step2_end = System::currentTimeMillis();
        Log::i("Step 2 completed in {}ms (empty input)", step2_end - step2_start);
        return result_view;
    }

    // Perform GROUP BY on ID field
    auto heads = filtered_view.groupBy("ID", tid);

    // Use the new minAndMax function to compute both min and max in one operation
    filtered_view.minAndMax(heads, "CS", "cs1", "cs2", tid);

    auto step2_end = System::currentTimeMillis();
    Log::i("Step 2 completed in {}ms", step2_end - step2_start);
    Log::i("Aggregated table has {} rows", filtered_view.rowNum());
    return filtered_view;
}

View filterByDifference(View &aggregated_view, int64_t threshold_c, int tid) {
    Log::i("Step 3: Filtering by cs2 - cs1 > {}...", threshold_c);
    auto step3_start = System::currentTimeMillis();

    if (aggregated_view.rowNum() == 0) {
        auto step3_end = System::currentTimeMillis();
        Log::i("Step 3 completed in {}ms (empty input)", step3_end - step3_start);
        return aggregated_view;
    }

    auto &cs1_data = aggregated_view._dataCols[aggregated_view.colIndex("cs1")]; // cs1 (min)
    auto &cs2_data = aggregated_view._dataCols[aggregated_view.colIndex("cs2")]; // cs2 (max)

    std::vector<int64_t> comparison_result_bool;

    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        // Single batch processing
        Log::i("Using single batch processing...");
        
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
        std::vector<int64_t> threshold_arith(differences.size(), Comm::rank() * threshold_c);
        auto comparison_result_arith = ArithLessBatchOperator(&threshold_arith, &differences, 64, 0, 
                                                              tid + 2 * BoolToArithBatchOperator::tagStride() + ArithAddBatchOperator::tagStride(),
                                                              SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

        // Step 4: Convert comparison result back to bool shares
        comparison_result_bool = ArithToBoolBatchOperator(&comparison_result_arith, 64, 0, 
                                                          tid + 2 * BoolToArithBatchOperator::tagStride() + ArithAddBatchOperator::tagStride() + ArithLessBatchOperator::tagStride(64),
                                                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        // Multi-batch processing
        Log::i("Using multi-batch processing...");
        
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

    auto step3_end = System::currentTimeMillis();
    Log::i("Step 3 completed in {}ms", step3_end - step3_start);
    Log::i("Final result has {} rows", result_view.rowNum());
    return result_view;
}

void displayResults(View &result_view, int tid) {
    Log::i("Reconstructing results for verification...");
    std::vector<int64_t> id_col;
    if (Comm::isServer() && !result_view._dataCols.empty()) {
        id_col = result_view._dataCols[0];
    }

    auto id_plain = Secrets::boolReconstruct(id_col, 2, 64, tid);

    if (Comm::rank() == 2) {
        Log::i("Query Results:");
        Log::i("Number of IDs satisfying condition: {}", id_plain.size());
        if (!id_plain.empty()) {
            Log::i("IDs where cs2 - cs1 > threshold:");
            for (size_t i = 0; i < std::min(id_plain.size(), size_t(10)); i++) {
                Log::i("  ID: {}", id_plain[i]);
            }
            if (id_plain.size() > 10) {
                Log::i("  ... and {} more", id_plain.size() - 10);
            }
        }
        Log::i("Expected results based on test data:");
        Log::i("  If threshold <= 20: should include ID=1 (diff=20) and ID=2 (diff=150)");
        Log::i("  If threshold > 20 and <= 150: should include only ID=2 (diff=150)");
        Log::i("  If threshold > 150: should include no IDs");
        Log::i("  ID=4 (diff=10) should never be included if threshold >= 10");
    }
}
