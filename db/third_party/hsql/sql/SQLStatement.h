#ifndef SQLPARSER_SQLSTATEMENT_H
#define SQLPARSER_SQLSTATEMENT_H

#include <vector>

#include "Expr.h"

namespace hsql {
enum StatementType {
  kStmtError,
  kStmtSelect,
  kStmtImport,
  kStmtInsert,
  kStmtUpdate,
  kStmtDelete,
  kStmtCreate,
  kStmtDrop,
  kStmtPrepare,
  kStmtExecute,
  kStmtExport,
  kStmtRename,
  kStmtAlter,
  kStmtShow,
  kStmtTransaction
};

struct SQLStatement {
  SQLStatement(StatementType type);

  virtual ~SQLStatement();

  StatementType type() const;

  bool isType(StatementType type) const;

  bool is(StatementType type) const;

  size_t stringLength;

  std::vector<Expr*>* hints;

 private:
  StatementType type_;
};

}

#endif
