#ifndef SQLPARSER_DROP_STATEMENT_H
#define SQLPARSER_DROP_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {

enum DropType { kDropTable, kDropSchema, kDropIndex, kDropView, kDropPreparedStatement };

struct DropStatement : SQLStatement {
  DropStatement(DropType type);
  ~DropStatement() override;

  DropType type;
  bool ifExists;
  char* schema;
  char* name;
  char* indexName;
};

}
#endif
