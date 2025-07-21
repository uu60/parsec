//
// Created by 杜建璋 on 2024/11/3.
//

#ifndef CREATE_H
#define CREATE_H
#include <sstream>
#include "../../third_party/hsql/sql/SQLStatement.h"

#include "../dbms/SystemManager.h"


class CreateSupport {
public:
    static bool clientCreateTable(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverCreateTable(json &j);
};



#endif //CREATE_H
