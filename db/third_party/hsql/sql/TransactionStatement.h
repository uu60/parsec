#ifndef HYRISE_TRANSACTIONSTATEMENT_H
#define HYRISE_TRANSACTIONSTATEMENT_H

#include "SQLStatement.h"

namespace hsql {

enum TransactionCommand { kBeginTransaction, kCommitTransaction, kRollbackTransaction };

struct TransactionStatement : SQLStatement {
  TransactionStatement(TransactionCommand command);
  ~TransactionStatement() override;

  TransactionCommand command;
};

}

#endif
