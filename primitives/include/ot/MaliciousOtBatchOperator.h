#ifndef MALICIOUSOTBATCHOPERATOR_H
#define MALICIOUSOTBATCHOPERATOR_H

#include "./base/AbstractOtBatchOperator.h"
#include <random>

class MaliciousOtBatchOperator : public AbstractOtBatchOperator {
public:
    inline static std::atomic_int64_t _totalTime = 0;
    
    bool _doBits{};
    std::vector<int64_t> *_choiceBits{};
    
    int64_t _macKey{};
    static constexpr int CONSISTENCY_CHECK_COUNT = 40;

public:
    MaliciousOtBatchOperator(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1, 
                             std::vector<int> *choices, int width, int taskTag, int msgTagOffset);

    MaliciousOtBatchOperator(int sender, std::vector<int64_t> *bits0, std::vector<int64_t> *bits1,
                             std::vector<int64_t> *choiceBits, int taskTag, int msgTagOffset);

    MaliciousOtBatchOperator *execute() override;

    static int tagStride();

private:
    void execute0();
    void executeForBits();
    int64_t generateMac(int64_t message, int64_t key);
    bool verifyMac(int64_t message, int64_t mac, int64_t key);
    bool performConsistencyCheck();
    std::vector<int64_t> generateChallenges(int count);
    int64_t computeLinearCombination(const std::vector<int64_t>& values, 
                                      const std::vector<int64_t>& coefficients);
    void executeCutAndChoose();
    int64_t generateCommitment(int64_t value, int64_t randomness);
    bool verifyCommitment(int64_t commitment, int64_t value, int64_t randomness);
};

#endif