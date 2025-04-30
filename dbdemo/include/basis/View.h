//
// Created by 杜建璋 on 25-4-25.
//

#ifndef VIEW_H
#define VIEW_H
#include "Table.h"


class View : public Table {
public:
    View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths);

    void sort(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

    void collect(bool ascendingOrder, int n, const std::vector<int64_t> &col, int k, int j, std::vector<int64_t> &xs,
                 std::vector<int64_t> &ys, std::vector<int64_t> &xIdx, std::vector<int64_t> &yIdx,
                 std::vector<bool> &ascs);

    void compareAndSwap(const std::string &orderField, int msgTagOffset, std::vector<int64_t> xs,
                        std::vector<int64_t> ys,
                        std::vector<int64_t> xIdx, std::vector<int64_t> yIdx, std::vector<bool> ascs,
                        std::vector<int64_t> zs, int mw);

private:
    void doSort(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

};


#endif //VIEW_H
