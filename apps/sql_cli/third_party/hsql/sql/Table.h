#ifndef SQLPARSER_TABLEREF_H
#define SQLPARSER_TABLEREF_H

#include <stdio.h>
#include <vector>
#include "Expr.h"

namespace hsql {

struct SelectStatement;
struct JoinDefinition;
struct TableRef;

enum TableRefType { kTableName, kTableSelect, kTableJoin, kTableCrossProduct };

struct TableName {
  char* schema;
  char* name;
};

struct Alias {
  Alias(char* name, std::vector<char*>* columns = nullptr);
  ~Alias();

  char* name;
  std::vector<char*>* columns;
};

struct TableRef {
  TableRef(TableRefType type);
  virtual ~TableRef();

  TableRefType type;

  char* schema;
  char* name;
  Alias* alias;

  SelectStatement* select;
  std::vector<TableRef*>* list;
  JoinDefinition* join;

  bool hasSchema() const;

  const char* getName() const;
};

enum JoinType { kJoinInner, kJoinFull, kJoinLeft, kJoinRight, kJoinCross, kJoinNatural };

struct JoinDefinition {
  JoinDefinition();
  virtual ~JoinDefinition();

  TableRef* left;
  TableRef* right;
  Expr* condition;
  std::vector<char*>* namedColumns;

  JoinType type;
};

}
#endif
