
#include "PrepareStatement.h"

namespace hsql {
PrepareStatement::PrepareStatement() : SQLStatement(kStmtPrepare), name(nullptr), query(nullptr) {}

PrepareStatement::~PrepareStatement() {
  free(name);
  free(query);
}
}
