
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
    inline static const std::string OUTER_MATCH_PREFIX = "$match:";

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

    View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths, bool dummy);

    void select(std::vector<std::string> &fieldNames);

    void sort(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void sort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders, int msgTagBase);

    int sortTagStride();

    int sortTagStride(const std::vector<std::string> &orderFields);

    void filterAndConditions(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                             std::vector<int64_t> &constShares, bool clear, int msgTagBase);

    void filterAndConditions(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                             std::vector<int64_t> &constShares, int msgTagBase);

    void clearInvalidEntries(int msgTagBase);

    void clearInvalidEntries(bool doSort, int msgTagBase);

    int clearInvalidEntriesTagStride();

    void addRedundantCols();

    std::vector<int64_t> groupBy(const std::string &groupField, int msgTagBase);

    std::vector<int64_t> groupBy(const std::string &groupField, bool doSort, int msgTagBase);

    std::vector<int64_t> groupBy(const std::vector<std::string> &groupFields, int msgTagBase);

    void count(std::vector<std::string> &groupFields, std::vector<int64_t> &heads, std::string alias, int msgTagBase);

    void count(std::vector<std::string> &groupFields, std::vector<int64_t> &heads, std::string alias, bool compress,
               int msgTagBase);

    void max(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase);

    void min(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase);

    void minAndMax(std::vector<int64_t> &heads, const std::string &fieldName, std::string minAlias,
                   std::string maxAlias, int msgTagBase);

    void minAndMax(std::vector<int64_t> &heads,
                   const std::string &minFieldName,
                   const std::string &maxFieldName,
                   std::string minAlias,
                   std::string maxAlias,
                   int msgTagBase);

    void distinct(int msgTagBase);

    int groupByTagStride();

    int distinctTagStride();

private:
    void bitonicSortSingleBatch(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void bitonicSortMultiBatches(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void bitonicSortSingleBatch(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders,
                                int msgTagBase);

    void bitonicSortMultiBatches(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders,
                                 int msgTagBase);

    void filterSingleBatch(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                           std::vector<int64_t> &constShares, bool clear, int msgTagBase);

    void filterMultiBatches(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes,
                            std::vector<int64_t> &constShares, bool clear, int msgTagBase);

    void bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagBase);

    void bitonicSort(const std::vector<std::string> &orderFields, const std::vector<bool> &ascendingOrders,
                     int msgTagBase);

    std::vector<int64_t> groupBySingleBatch(const std::string &groupField, int msgTagBase);

    std::vector<int64_t> groupByMultiBatches(const std::string &groupField, int msgTagBase);

    std::vector<int64_t> groupBySingleBatch(const std::vector<std::string> &groupFields, int msgTagBase);

    std::vector<int64_t> groupByMultiBatches(const std::vector<std::string> &groupFields, int msgTagBase);

    void countSingleBatch(std::vector<int64_t> &heads, std::string alias, int msgTagBase, std::string matchedTable,
                          bool compress);

    void countMultiBatches(std::vector<int64_t> &heads, std::string alias, int msgTagBase, std::string matchedTable,
                           bool compress);

    void maxSingleBatch(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase);

    void maxMultiBatches(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase);

    void minSingleBatch(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase);

    void minMultiBatches(std::vector<int64_t> &heads, const std::string &fieldName, std::string alias, int msgTagBase);

    void minAndMaxSingleBatch(std::vector<int64_t> &heads,
                              const std::string &minFieldName,
                              const std::string &maxFieldName,
                              std::string minAlias,
                              std::string maxAlias,
                              int msgTagBase);

    void minAndMaxMultiBatches(std::vector<int64_t> &heads,
                               const std::string &minFieldName,
                               const std::string &maxFieldName,
                               std::string minAlias,
                               std::string maxAlias,
                               int msgTagBase);
};


#endif
