
#ifndef BASEOTBATCHOPERATOR_H
#define BASEOTBATCHOPERATOR_H

#include "parsecdb/ot/base/AbstractOtBatchOperator.h"
#include <string>

namespace parsecdb {

class BaseOtBatchOperator : public AbstractOtBatchOperator {
  private:
    std::vector<std::string> _rand0s{};
    std::vector<std::string> _rand1s{};
    std::vector<std::string> _randKs{};

    // 128-bit message mode (see withHighWords).
    bool _wide = false;
    std::vector<int64_t>* _ms0Hi{};
    std::vector<int64_t>* _ms1Hi{};

  public:
    inline static std::atomic_int64_t _totalTime = 0;

    // High 64-bit words of the received messages in 128-bit mode (receiver only).
    std::vector<int64_t> _resultsHi{};

    explicit BaseOtBatchOperator(int sender, std::vector<int64_t>* ms0, std::vector<int64_t>* ms1,
                                 std::vector<int>* choices, int width, int taskTag, int msgTagOffset);

    // Switches to 128-bit messages: each OT also transfers ms0Hi[i] / ms1Hi[i] under the same RSA exchange, masked by
    // the next 8 bytes of the same key derivation. Both parties must call it before execute(); the receiver passes
    // nullptrs and reads the high words from _resultsHi.
    BaseOtBatchOperator* withHighWords(std::vector<int64_t>* ms0Hi, std::vector<int64_t>* ms1Hi);

    BaseOtBatchOperator* execute() override;

    [[nodiscard]] static int tagStride();

  private:
    void generateAndShareRandoms();

    void process();
};

} // namespace parsecdb

#endif
