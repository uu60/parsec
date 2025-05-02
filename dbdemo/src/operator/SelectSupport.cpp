//
// Created by 杜建璋 on 25-5-1.
//

#include "../../include/operator/SelectSupport.h"

View SelectSupport::selectAll(Table &t) {
    View v(t._tableName, t._fieldNames, t._fieldWidths);
    v._dataCols = t._dataCols;
    v._fieldNames.push_back("$padding");
    v._dataCols.push_back(std::vector<int64_t>(v._dataCols[0].size()));
    return v;
}
