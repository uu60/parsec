//
// Created by 杜建璋 on 25-4-25.
//

#ifndef VIEW_H
#define VIEW_H
#include "Table.h"


class View : public Table {
public:
    static const int PADDING_COL_OFFSET = -2;
    static const int VALID_COL_OFFSET = -1;

    enum ComparatorType {
        LESS_THAN,
        GREATER_THAN,
        NO_LESS_THAN,
        NO_GREATER_THAN,
        EQUAL,
        UNEQUAL,
    };

    View() = default;

    View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths);

    void sort(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

    void fa1B(std::vector<std::string> &fieldNames, std::vector<View::ComparatorType> &comparatorTypes,
              std::vector<int64_t> &constShares);

    void faNB(std::vector<std::string> &fieldNames, std::vector<View::ComparatorType> &comparatorTypes,
              std::vector<int64_t> &constShares);

    void filterAnd(std::vector<std::string> &fieldNames, std::vector<ComparatorType> &comparatorTypes, std::vector<int64_t> &constShares);

    static View selectAll(Table &t);

    static View selectColumns(Table &t, std::vector<std::string> &fieldNames);

private:
    void bs1B(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

    void bsNB(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

    void bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagOffset);
};


#endif //VIEW_H
