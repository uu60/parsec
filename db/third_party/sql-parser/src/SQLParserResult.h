#ifndef SQLPARSER_SQLPARSER_RESULT_H
#define SQLPARSER_SQLPARSER_RESULT_H

#include "sql/SQLStatement.h"

namespace hsql {
class SQLParserResult {
 public:
  SQLParserResult();

  SQLParserResult(SQLStatement* stmt);

  SQLParserResult(SQLParserResult&& moved);
  SQLParserResult& operator=(SQLParserResult&& moved);

  virtual ~SQLParserResult();

  void setIsValid(bool isValid);

  bool isValid() const;

  size_t size() const;

  void setErrorDetails(char* errorMsg, int errorLine, int errorColumn);

  const char* errorMsg() const;

  int errorLine() const;

  int errorColumn() const;

  void addStatement(SQLStatement* stmt);

  const SQLStatement* getStatement(size_t index) const;

  SQLStatement* getMutableStatement(size_t index);

  const std::vector<SQLStatement*>& getStatements() const;

  std::vector<SQLStatement*> releaseStatements();

  void reset();

  void addParameter(Expr* parameter);

  const std::vector<Expr*>& parameters();

 private:
  std::vector<SQLStatement*> statements_;

  bool isValid_;

  char* errorMsg_;

  int errorLine_;

  int errorColumn_;

  std::vector<Expr*> parameters_;
};

}

#endif
