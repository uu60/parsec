
#ifndef DROP_H
#define DROP_H
#include <sstream>
#include "../third_party/hsql/sql/SQLStatement.h"

#include "dbms/SystemManager.h"


class DropSupport {
public:
    static bool clientDropTable(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverDropTable(json j);
};



#endif
