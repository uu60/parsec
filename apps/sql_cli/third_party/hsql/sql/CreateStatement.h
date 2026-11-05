#ifndef SQLPARSER_CREATE_STATEMENT_H
#define SQLPARSER_CREATE_STATEMENT_H

#include "ColumnType.h"
#include "SQLStatement.h"

#include <ostream>
#include <unordered_set>

namespace hsql {
struct SelectStatement;

enum struct ConstraintType { None, NotNull, Null, PrimaryKey, Unique };

struct TableElement {
  virtual ~TableElement() {}
};

struct TableConstraint : TableElement {
  TableConstraint(ConstraintType keyType, std::vector<char*>* columnNames);

  ~TableConstraint() override;

  ConstraintType type;
  std::vector<char*>* columnNames;
};

struct ColumnDefinition : TableElement {
  ColumnDefinition(char* name, ColumnType type, std::unordered_set<ConstraintType>* column_constraints);

  ~ColumnDefinition() override;

  bool trySetNullableExplicit() {
    if (column_constraints->count(ConstraintType::NotNull) || column_constraints->count(ConstraintType::PrimaryKey)) {
      if (column_constraints->count(ConstraintType::Null)) {
        return false;
      }
      nullable = false;
    }

    return true;
  }

  std::unordered_set<ConstraintType>* column_constraints;
  char* name;
  ColumnType type;
  bool nullable;
};

enum CreateType {
  kCreateTable,
  kCreateTableFromTbl,
  kCreateView,
  kCreateIndex
};

struct CreateStatement : SQLStatement {
  CreateStatement(CreateType type);
  ~CreateStatement() override;

  void setColumnDefsAndConstraints(std::vector<TableElement*>* tableElements);

  CreateType type;
  bool ifNotExists;
  char* filePath;
  char* schema;
  char* tableName;
  char* indexName;
  std::vector<char*>* indexColumns;
  std::vector<ColumnDefinition*>* columns;
  std::vector<TableConstraint*>* tableConstraints;
  std::vector<char*>* viewColumns;
  SelectStatement* select;
};

}

#endif
