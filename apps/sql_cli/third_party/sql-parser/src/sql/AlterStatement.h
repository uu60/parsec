#ifndef SQLPARSER_ALTER_STATEMENT_H
#define SQLPARSER_ALTER_STATEMENT_H

#include "SQLStatement.h"

namespace hsql {

enum ActionType {
  DropColumn,
};

struct AlterAction {
  AlterAction(ActionType type);
  ActionType type;
  virtual ~AlterAction();
};

struct DropColumnAction : AlterAction {
  DropColumnAction(char* column_name);
  char* columnName;
  bool ifExists;

  ~DropColumnAction() override;
};

struct AlterStatement : SQLStatement {
  AlterStatement(char* name, AlterAction* action);
  ~AlterStatement() override;

  char* schema;
  bool ifTableExists;
  char* name;
  AlterAction* action;
};
}

#endif
