//
// Created by 杜建璋 on 25-4-25.
//

#ifndef VIEW_H
#define VIEW_H
#include "Table.h"


class View : public Table {
public:
    View() = default;

    View(std::string &tableName, std::vector<std::string> &fieldNames, std::vector<int> &fieldWidths);

    void sort(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

    void bs1B(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

    void bsNB(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

    static View selectAll(Table &t);

    static View selectColumns(Table &t, std::vector<std::string> &fieldNames);

private:
    void bitonicSort(const std::string &orderField, bool ascendingOrder, int msgTagOffset);

};


#endif //VIEW_H
