#ifndef SQLPARSER_IMPORT_STATEMENT_H
#define SQLPARSER_IMPORT_STATEMENT_H

#include "ImportExportOptions.h"
#include "SQLStatement.h"

namespace hsql {

struct ImportStatement : SQLStatement {
  ImportStatement(ImportType type);
  ~ImportStatement() override;

  ImportType type;
  char* filePath;
  char* schema;
  char* tableName;
  Expr* whereClause;
  char* encoding;
};

}

#endif
