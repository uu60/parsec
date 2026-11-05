
#include <iostream>

#include "../third_party/hsql/SQLParserResult.h"
#include "../third_party/hsql/SQLParser.h"

int main(int argc, char *argv[]) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parse("select * from t0 join t1 on t0.a = t1.a where t1.a < 0;", &result);
    const auto *stmt = dynamic_cast<const hsql::SelectStatement *>(result.getStatement(0));
    std::cout << stmt;
}
