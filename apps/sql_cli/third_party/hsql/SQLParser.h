#ifndef SQLPARSER_SQLPARSER_H
#define SQLPARSER_SQLPARSER_H

#include "SQLParserResult.h"
#include "sql/statements.h"

namespace hsql {

class SQLParser {
 public:
  static bool parse(const std::string& sql, SQLParserResult* result);

  static bool tokenize(const std::string& sql, std::vector<int16_t>* tokens);

  static bool parseSQLString(const char* sql, SQLParserResult* result);

  static bool parseSQLString(const std::string& sql, SQLParserResult* result);

 private:
  SQLParser();
};

}

#endif
