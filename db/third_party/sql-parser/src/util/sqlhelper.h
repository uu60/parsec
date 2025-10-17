#ifndef SQLPARSER_SQLHELPER_H
#define SQLPARSER_SQLHELPER_H

#include "../sql/statements.h"

namespace hsql {

void printStatementInfo(const SQLStatement* stmt);

void printSelectStatementInfo(const SelectStatement* stmt, uintmax_t num_indent);

void printImportStatementInfo(const ImportStatement* stmt, uintmax_t num_indent);

void printExportStatementInfo(const ExportStatement* stmt, uintmax_t num_indent);

void printInsertStatementInfo(const InsertStatement* stmt, uintmax_t num_indent);

void printCreateStatementInfo(const CreateStatement* stmt, uintmax_t num_indent);

void printTransactionStatementInfo(const TransactionStatement* stmt, uintmax_t num_indent);

void printExpression(Expr* expr, uintmax_t num_indent);

void printOrderBy(const std::vector<OrderDescription*>* expr, uintmax_t num_indent);

void printWindowDescription(WindowDescription* window_description, uintmax_t num_indent);

}

#endif
