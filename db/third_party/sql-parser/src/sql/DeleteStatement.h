#ifndef SQLPARSER_DELETE_STATEMENT_H
#define SQLPARSER_DELETE_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {

struct DeleteStatement : SQLStatement {
  DeleteStatement();
  ~DeleteStatement() override;

  char* schema;
  char* tableName;
  Expr* expr;
};

}

#endif
