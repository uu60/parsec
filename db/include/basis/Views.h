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
    static View nestedLoopJoin(View &v0, View &v1, std::string &field0, std::string &field1);

    static View hashJoin(View &v0, View &v1, std::string &field0, std::string &field1);

    static std::string getAliasColName(std::string& tableName, std::string& fieldName);

    static int64_t hash(int64_t keyValue);

private:
    static void addRedundantCols(View &v);
    
    static std::vector<std::vector<std::vector<int64_t>>> butterflyPermutation(
        View& view,
        int tagColIndex,
        int msgTagBase
    );
    
    static View performBucketJoins(
        std::vector<std::vector<std::vector<int64_t>>>& buckets0,
        std::vector<std::vector<std::vector<int64_t>>>& buckets1,
        View& v0,
        View& v1,
        std::string& field0,
        std::string& field1
    );

    static int butterflyPermutationTagStride(View& v);
};



#endif //VIEWS_H
