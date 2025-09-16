//
// Created by 杜建璋 on 25-6-2.
//

#ifndef VIEWS_H
#define VIEWS_H
#include "View.h"


#include <string>

class Views {
public:
    static View selectAll(Table &t);

    static View selectAllWithFieldPrefix(Table &t);

    static View selectColumns(Table &t, std::vector<std::string> &fieldNames);

    /**
     *  Make sure all records of v0 and v1 are valid (call View::clearInvalidEntriesObliviously() if necessary).
     */
    static View nestedLoopJoin(View &v0, View &v1, std::string &field0, std::string &field1, bool compress);

    static View nestedLoopJoin(View &v0, View &v1, std::string &field0, std::string &field1);

    static View hashJoin(View &v0, View &v1, std::string &field0, std::string &field1);

    static View hashJoin(View &v0, View &v1, std::string &field0, std::string &field1, bool compress);

    static View leftOuterJoin(View &v0, View &v1, std::string &field0, std::string &field1);

    static View leftOuterJoin(View &v0, View &v1, std::string &field0, std::string &field1, bool doHashJoin, bool compress);

    static std::string getAliasColName(std::string &tableName, std::string &fieldName);

    static int64_t hash(int64_t keyValue);

    static std::vector<int64_t> in(std::vector<int64_t> &col1,
                                   std::vector<int64_t> &col2,
                                   std::vector<int64_t> &left_valid,
                                   std::vector<int64_t> &right_valid);

    static void revealAndPrint(View &v);

private:
    static std::vector<int64_t> inSingleBatch(std::vector<int64_t> &col1,
                                              std::vector<int64_t> &col2,
                                              std::vector<int64_t> &left_valid,
                                              std::vector<int64_t> &right_valid);

    static std::vector<int64_t> inMultiBatches(std::vector<int64_t> &col1,
                                               std::vector<int64_t> &col2,
                                               std::vector<int64_t> &left_valid,
                                               std::vector<int64_t> &right_valid);

    static void addRedundantCols(View &v);

    static std::vector<View> butterflyPermutation(
        View &view,
        int tagColIndex,
        int msgTagBase
    );

    static View performBucketJoins(
        std::vector<View> &buckets0,
        std::vector<View> &buckets1,
        View &v0,
        View &v1,
        std::string &field0,
        std::string &field1,
        bool compress
    );

    static int butterflyPermutationTagStride(View &v);
};


#endif //VIEWS_H
