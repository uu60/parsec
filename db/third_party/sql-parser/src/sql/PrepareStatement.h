#ifndef SQLPARSER_PREPARE_STATEMENT_H
#define SQLPARSER_PREPARE_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {

struct PrepareStatement : SQLStatement {
  PrepareStatement();
  ~PrepareStatement() override;

  char* name;

  char* query;
};

}

#endif
