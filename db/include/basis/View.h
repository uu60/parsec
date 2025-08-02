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

private:
    void bitonicSortSingleBatch(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void bitonicSortSplittedBatches(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    // Multi-column sort private methods
    void bitonicSortSingleBatch(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);

    void bitonicSortSplittedBatches(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);

    void filterSingleBatch(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
               std::vector<int64_t> &constShares);

    void filterSplittedBatches(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
              std::vector<int64_t> &constShares);

    void bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void bitonicSort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);

    // Helper method for multi-column comparison
    std::vector<int64_t> compareMultiColumn(const std::vector<std::string> &orderFields, 
                                           const std::vector<bool> &ascendingOrders,
                                           int row1, int row2, int msgTagBase);
    
};


#endif //VIEW_H
