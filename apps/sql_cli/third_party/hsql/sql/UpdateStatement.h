#ifndef SQLPARSER_UPDATE_STATEMENT_H
#define SQLPARSER_UPDATE_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {

struct UpdateClause {
  char* column;
  Expr* value;
};

struct UpdateStatement : SQLStatement {
  UpdateStatement();
  ~UpdateStatement() override;

  TableRef* table;
  std::vector<UpdateClause*>* updates;
  Expr* where;
};

}

#endif
