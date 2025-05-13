//
// Created by 杜建璋 on 25-5-1.
//

#ifndef SELECTSUPPORT_H
#define SELECTSUPPORT_H
#include "../basis/Table.h"
#include "../basis/View.h"
#include "../third_party/json.hpp"
#include "../third_party/hsql/sql/SQLStatement.h"

class SelectSupport {
public:
    static bool clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverSelect(nlohmann::basic_json<> js);

};



#endif //SELECTSUPPORT_H
