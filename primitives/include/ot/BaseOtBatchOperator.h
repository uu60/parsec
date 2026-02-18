
#ifndef BASEOTBATCHOPERATOR_H
#define BASEOTBATCHOPERATOR_H

#include "./base/AbstractOtBatchOperator.h"
#include <string>

class BaseOtBatchOperator : public AbstractOtBatchOperator {
private:
    std::vector<std::string> _rand0s{};
    std::vector<std::string> _rand1s{};
    std::vector<std::string> _randKs{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

    explicit BaseOtBatchOperator(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1,
                                  std::vector<int> *choices, int width, int taskTag, int msgTagOffset);

    BaseOtBatchOperator *execute() override;

    [[nodiscard]] static int tagStride();

private:
    void generateAndShareRandoms();

    void process();
};


#endif


