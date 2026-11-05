
#ifndef INSERT_H
#define INSERT_H
#include <sstream>
#include "../third_party/json.hpp"
#include "../third_party/hsql/sql/SQLStatement.h"


class InsertSupport {
public:
    static bool clientInsert(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverInsert(nlohmann::basic_json<> j);
};



#endif
