#ifndef SQLPARSER_EXPR_H
#define SQLPARSER_EXPR_H

#include <stdlib.h>
#include <memory>
#include <vector>
#include "ColumnType.h"

namespace hsql {
struct SelectStatement;
struct OrderDescription;

char* substr(const char* source, int from, int to);

enum ExprType {
  kExprLiteralFloat,
  kExprLiteralString,
  kExprLiteralInt,
  kExprLiteralNull,
  kExprLiteralDate,
  kExprLiteralInterval,
  kExprStar,
  kExprParameter,
  kExprColumnRef,
  kExprFunctionRef,
  kExprOperator,
  kExprSelect,
  kExprHint,
  kExprArray,
  kExprArrayIndex,
  kExprExtract,
  kExprCast
};

enum OperatorType {
  kOpNone,

  kOpBetween,

  kOpCase,
  kOpCaseListElement,

  kOpPlus,
  kOpMinus,
  kOpAsterisk,
  kOpSlash,
  kOpPercentage,
  kOpCaret,

  kOpEquals,
  kOpNotEquals,
  kOpLess,
  kOpLessEq,
  kOpGreater,
  kOpGreaterEq,
  kOpLike,
  kOpNotLike,
  kOpILike,
  kOpAnd,
  kOpOr,
  kOpIn,
  kOpConcat,

  kOpNot,
  kOpUnaryMinus,
  kOpIsNull,
  kOpExists
};

enum DatetimeField {
  kDatetimeNone,
  kDatetimeSecond,
  kDatetimeMinute,
  kDatetimeHour,
  kDatetimeDay,
  kDatetimeMonth,
  kDatetimeYear,
};

enum FrameBoundType { kFollowing, kPreceding, kCurrentRow };
struct FrameBound {
  FrameBound(int64_t offset, FrameBoundType type, bool unbounded);

  int64_t offset;
  FrameBoundType type;
  bool unbounded;
};

enum FrameType { kRange, kRows, kGroups };
struct FrameDescription {
  FrameDescription(FrameType type, FrameBound* start, FrameBound* end);
  virtual ~FrameDescription();

  FrameType type;
  FrameBound* start;
  FrameBound* end;
};

typedef struct Expr Expr;

struct WindowDescription {
  WindowDescription(std::vector<Expr*>* partitionList, std::vector<OrderDescription*>* orderList,
                    FrameDescription* frameDescription);
  virtual ~WindowDescription();

  std::vector<Expr*>* partitionList;
  std::vector<OrderDescription*>* orderList;
  FrameDescription* frameDescription;
};

struct Expr {
  Expr(ExprType type);
  virtual ~Expr();

  ExprType type;

  Expr* expr;
  Expr* expr2;
  std::vector<Expr*>* exprList;
  SelectStatement* select;
  char* name;
  char* table;
  char* alias;
  double fval;
  int64_t ival;
  int64_t ival2;
  DatetimeField datetimeField;
  ColumnType columnType;
  bool isBoolLiteral;

  OperatorType opType;
  bool distinct;

  WindowDescription* windowDescription;


  bool isType(ExprType exprType) const;

  bool isLiteral() const;

  bool hasAlias() const;

  bool hasTable() const;

  const char* getName() const;


  static Expr* make(ExprType type);

  static Expr* makeOpUnary(OperatorType op, Expr* expr);

  static Expr* makeOpBinary(Expr* expr1, OperatorType op, Expr* expr2);

  static Expr* makeBetween(Expr* expr, Expr* left, Expr* right);

  static Expr* makeCaseList(Expr* caseListElement);

  static Expr* makeCaseListElement(Expr* when, Expr* then);

  static Expr* caseListAppend(Expr* caseList, Expr* caseListElement);

  static Expr* makeCase(Expr* expr, Expr* when, Expr* elseExpr);

  static Expr* makeLiteral(int64_t val);

  static Expr* makeLiteral(double val);

  static Expr* makeLiteral(char* val);

  static Expr* makeLiteral(bool val);

  static Expr* makeNullLiteral();

  static Expr* makeDateLiteral(char* val);

  static Expr* makeIntervalLiteral(int64_t duration, DatetimeField unit);

  static Expr* makeColumnRef(char* name);

  static Expr* makeColumnRef(char* table, char* name);

  static Expr* makeStar(void);

  static Expr* makeStar(char* table);

  static Expr* makeFunctionRef(char* func_name, std::vector<Expr*>* exprList, bool distinct, WindowDescription* window);

  static Expr* makeArray(std::vector<Expr*>* exprList);

  static Expr* makeArrayIndex(Expr* expr, int64_t index);

  static Expr* makeParameter(int id);

  static Expr* makeSelect(SelectStatement* select);

  static Expr* makeExists(SelectStatement* select);

  static Expr* makeInOperator(Expr* expr, std::vector<Expr*>* exprList);

  static Expr* makeInOperator(Expr* expr, SelectStatement* select);

  static Expr* makeExtract(DatetimeField datetimeField1, Expr* expr);

  static Expr* makeCast(Expr* expr, ColumnType columnType);
};

#define ALLOC_EXPR(var, type)         \
  Expr* var;                          \
  do {                                \
    Expr zero = {type};               \
    var = (Expr*)malloc(sizeof *var); \
    *var = zero;                      \
  } while (0);
#undef ALLOC_EXPR

}

#endif
