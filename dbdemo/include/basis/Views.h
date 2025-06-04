//
// Created by 杜建璋 on 25-6-2.
//

#ifndef VIEWS_H
#define VIEWS_H
#include "View.h"


class Views {
public:
    static View selectAll(Table &t);

    static View selectColumns(Table &t, std::vector<std::string> &fieldNames);

    /**
     *  Make sure all records of v0 and v1 are valid (call View::clearInvalidEntriesObliviously() if necessary).
     */
    static View nestedLoopJoin(View &v0, View &v1, std::string &field0, std::string &field1);

    static View shuffleBucketJoin(View &v0, View &v1, std::string &field0, std::string &field1);

    static std::string getAliasColName(std::string& tableName, std::string& fieldName);

private:
    static void addRedundantCols(View &v);
    
    static std::vector<std::vector<size_t>> distributeToBuckets(const View &v, int tagColIdx, int numBuckets);
    
    static View joinBuckets(View &v0, View &v1,
                           const std::vector<std::vector<size_t>> &buckets0,
                           const std::vector<std::vector<size_t>> &buckets1,
                           const std::string &field0, const std::string &field1);
};



#endif //VIEWS_H
