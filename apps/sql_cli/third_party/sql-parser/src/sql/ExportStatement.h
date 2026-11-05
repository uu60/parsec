#ifndef SQLPARSER_EXPORT_STATEMENT_H
#define SQLPARSER_EXPORT_STATEMENT_H

#include "ImportExportOptions.h"
#include "SQLStatement.h"
#include "SelectStatement.h"

namespace hsql {
struct ExportStatement : SQLStatement {
  ExportStatement(ImportType type);
  ~ExportStatement() override;

  ImportType type;
  char* filePath;
  char* schema;
  char* tableName;
  SelectStatement* select;
  char* encoding;
};

}

#endif
