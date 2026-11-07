
#ifndef SECRETS_H
#define SECRETS_H
#include <vector>

#include "parsecdb/secret/item/ArithSecret.h"
#include "parsecdb/secret/item/BoolSecret.h"

namespace parsecdb {

class Secrets {
  public:
    static void sort(std::vector<BoolSecret>& secrets, bool asc, int taskTag);

    static void sort(std::vector<ArithSecret>& secrets, bool asc, int taskTag);

    static std::vector<int64_t> boolShare(std::vector<int64_t>& origins, int clientRank, int width, int taskTag);

    static std::vector<int64_t> boolReconstruct(std::vector<int64_t>& secrets, int clientRank, int width, int taskTag);

    static std::vector<int64_t> arithShare(std::vector<int64_t>& origins, int clientRank, int width, int taskTag);

    static std::vector<int64_t> arithReconstruct(std::vector<int64_t>& secrets, int clientRank, int width, int taskTag);
};

} // namespace parsecdb

#endif
