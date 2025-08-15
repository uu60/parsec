//
// Created by 杜建璋 on 25-4-25.
//

#ifndef VIEW_H
#define VIEW_H
#include "Table.h"


#include <string>
class View : public Table {
public:
    static const int VALID_COL_OFFSET = -2;
    static const int PADDING_COL_OFFSET = -1;
    inline static const std::string VALID_COL_NAME = "$valid";
    inline static const std::string PADDING_COL_NAME = "$padding";
    inline static const std::string COUNT_COL_NAME = "$cnt";

    inline static std::string EMPTY_KEY_FIELD;
    inline static std::string EMPTY_VIEW_NAME;

    enum ComparatorType {
        LESS,
        GREATER,
        GREATER_EQ,
        LESS_EQ,
        EQUALS,
        NOT_EQUALS,
    };

    View() = default;

    View(std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths);

    View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths);

    // without auto add redundant columns
    View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths, bool dummy);

    void sort(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    // Multi-column sort support
    void sort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);

    int sortTagStride();

    int sortTagStride(const std::vector<std::string> &orderFields);

    void filterAndConditions(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes, std::vector<int64_t> &constShares);

    void clearInvalidEntries(int msgTagBase);

    int clearInvalidEntriesTagStride();

    void addRedundantCols();

    // Group by functionality for 2PC secret sharing
    // Returns: pair<groupIds for each row, total number of groups>
    std::vector<int64_t> groupBy(const std::string &groupField, int msgTagBase);
    
    // Multi-column group by functionality for 2PC secret sharing
    // Returns: group boundary indicators for each row (1 if starts new group, 0 otherwise)
    std::vector<int64_t> groupBy(const std::vector<std::string> &groupFields, int msgTagBase);

    // View must have run group by before calling this
    void count(std::vector<int64_t> &heads, std::string alias, int msgTagBase);

    void distinct(int msgTagBase);

    int groupByTagStride();
    
    int distinctTagStride();

private:
    void bitonicSortSingleBatch(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void bitonicSortMultiBatches(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    // Multi-column sort private methods
    void bitonicSortSingleBatch(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);

    void bitonicSortMultiBatches(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);

    void filterSingleBatch(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
               std::vector<int64_t> &constShares);

    void filterMultiBatches(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
              std::vector<int64_t> &constShares);

    void bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void bitonicSort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);
};


#endif //VIEW_H
