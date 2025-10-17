#ifndef SQLPARSER_SHOW_STATEMENT_H
#define SQLPARSER_SHOW_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {

enum ShowType { kShowColumns, kShowTables };

struct ShowStatement : SQLStatement {
  ShowStatement(ShowType type);
  ~ShowStatement() override;

  ShowType type;
  char* schema;
  char* name;
};

}
#endif
