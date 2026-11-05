#ifndef SQLPARSER_EXECUTE_STATEMENT_H
#define SQLPARSER_EXECUTE_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {

struct ExecuteStatement : SQLStatement {
  ExecuteStatement();
  ~ExecuteStatement() override;

  char* name;
  std::vector<Expr*>* parameters;
};

}

#endif
