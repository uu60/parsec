#ifndef SQLPARSER_SELECT_STATEMENT_H
#define SQLPARSER_SELECT_STATEMENT_H

#include "Expr.h"
#include "SQLStatement.h"
#include "Table.h"

namespace hsql {
enum OrderType { kOrderAsc, kOrderDesc };

enum SetType { kSetUnion, kSetIntersect, kSetExcept };

enum RowLockMode { ForUpdate, ForNoKeyUpdate, ForShare, ForKeyShare };
enum RowLockWaitPolicy { NoWait, SkipLocked, None };

struct OrderDescription {
  OrderDescription(OrderType type, Expr* expr);
  virtual ~OrderDescription();

  OrderType type;
  Expr* expr;
};

struct LimitDescription {
  LimitDescription(Expr* limit, Expr* offset);
  virtual ~LimitDescription();

  Expr* limit;
  Expr* offset;
};

struct GroupByDescription {
  GroupByDescription();
  virtual ~GroupByDescription();

  std::vector<Expr*>* columns;
  Expr* having;
};

struct WithDescription {
  ~WithDescription();

  char* alias;
  SelectStatement* select;
};

struct SetOperation {
  SetOperation();
  virtual ~SetOperation();

  SetType setType;
  bool isAll;

  SelectStatement* nestedSelectStatement;
  std::vector<OrderDescription*>* resultOrder;
  LimitDescription* resultLimit;
};

struct LockingClause {
  RowLockMode rowLockMode;
  RowLockWaitPolicy rowLockWaitPolicy;
  std::vector<char*>* tables;
};

struct SelectStatement : SQLStatement {
  SelectStatement();
  ~SelectStatement() override;

  TableRef* fromTable;
  bool selectDistinct;
  std::vector<Expr*>* selectList;
  Expr* whereClause;
  GroupByDescription* groupBy;

  std::vector<SetOperation*>* setOperations;

  std::vector<OrderDescription*>* order;
  std::vector<WithDescription*>* withDescriptions;
  LimitDescription* limit;
  std::vector<LockingClause*>* lockings;
};

}

#endif
