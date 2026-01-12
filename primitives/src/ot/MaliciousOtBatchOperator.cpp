#include "../../include/ot/MaliciousOtBatchOperator.h"

#include "../../include/intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include <functional>

MaliciousOtBatchOperator::MaliciousOtBatchOperator(int sender, std::vector<int64_t> *ms0, 
                                                   std::vector<int64_t> *ms1,
                                                   std::vector<int> *choices, int width, 
                                                   int taskTag, int msgTagOffset)
    : AbstractOtBatchOperator(sender, ms0, ms1, choices, width, taskTag, msgTagOffset) {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    _macKey = static_cast<int64_t>(gen());
}

MaliciousOtBatchOperator::MaliciousOtBatchOperator(int sender, std::vector<int64_t> *bits0, 
                                                   std::vector<int64_t> *bits1,
                                                   std::vector<int64_t> *choiceBits, 
                                                   int taskTag, int msgTagOffset)
    : AbstractOtBatchOperator(64, taskTag, msgTagOffset) {
    if (Comm::isClient()) {
        return;
    }

    _doBits = true;
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _ms0 = bits0;
        _ms1 = bits1;
    } else {
        _choiceBits = choiceBits;
    }
    
    std::random_device rd;
    std::mt19937_64 gen(rd());
    _macKey = static_cast<int64_t>(gen());
}

int64_t MaliciousOtBatchOperator::generateMac(int64_t message, int64_t key) {
    std::hash<int64_t> hasher;
    return static_cast<int64_t>(hasher(message ^ key) ^ hasher(key));
}

bool MaliciousOtBatchOperator::verifyMac(int64_t message, int64_t mac, int64_t key) {
    return generateMac(message, key) == mac;
}

std::vector<int64_t> MaliciousOtBatchOperator::generateChallenges(int count) {
    std::vector<int64_t> challenges(count);
    std::random_device rd;
    std::mt19937_64 gen(rd());
    for (int i = 0; i < count; ++i) {
        challenges[i] = static_cast<int64_t>(gen());
    }
    return challenges;
}

int64_t MaliciousOtBatchOperator::computeLinearCombination(
    const std::vector<int64_t>& values,
    const std::vector<int64_t>& coefficients) {
    int64_t result = 0;
    for (size_t i = 0; i < values.size() && i < coefficients.size(); ++i) {
        result ^= (values[i] & coefficients[i]);
    }
    return result;
}

int64_t MaliciousOtBatchOperator::generateCommitment(int64_t value, int64_t randomness) {
    std::hash<int64_t> hasher;
    return static_cast<int64_t>(hasher(value) ^ hasher(randomness) ^ hasher(value ^ randomness));
}

bool MaliciousOtBatchOperator::verifyCommitment(int64_t commitment, int64_t value, int64_t randomness) {
    return generateCommitment(value, randomness) == commitment;
}

bool MaliciousOtBatchOperator::performConsistencyCheck() {
    Log::i("[Rank {}] performConsistencyCheck start, _isSender={}", Comm::rank(), _isSender);

    if (_isSender) {
        int size = static_cast<int>(_ms0->size());
        Log::i("[Rank {}] Sender: size={}", Comm::rank(), size);

        Log::i("[Rank {}] Sender: waiting for challenges...", Comm::rank());
        std::vector<int64_t> challenges;
        auto r0 = Comm::serverReceiveAsync(challenges, CONSISTENCY_CHECK_COUNT, _width, 
                                           buildTag(_currentMsgTag + 1));
        Comm::wait(r0);
        Log::i("[Rank {}] Sender: challenges received", Comm::rank());

        std::vector<int64_t> response0(CONSISTENCY_CHECK_COUNT);
        std::vector<int64_t> response1(CONSISTENCY_CHECK_COUNT);
        
        for (int c = 0; c < CONSISTENCY_CHECK_COUNT; ++c) {
            int64_t sum0 = 0, sum1 = 0;
            int64_t challenge = challenges[c];
            
            for (int i = 0; i < size; ++i) {
                int64_t coef = (challenge >> (i % 64)) & 1;
                if (coef) {
                    sum0 ^= (*_ms0)[i];
                    sum1 ^= (*_ms1)[i];
                }
            }
            response0[c] = sum0;
            response1[c] = sum1;
        }
        
        std::vector<int64_t> toSend(CONSISTENCY_CHECK_COUNT * 4);
        for (int c = 0; c < CONSISTENCY_CHECK_COUNT; ++c) {
            toSend[c * 4] = response0[c];
            toSend[c * 4 + 1] = generateMac(response0[c], _macKey);
            toSend[c * 4 + 2] = response1[c];
            toSend[c * 4 + 3] = generateMac(response1[c], _macKey);
        }
        
        auto r1 = Comm::serverSendAsync(toSend, _width, buildTag(_currentMsgTag + 2));
        Comm::wait(r1);
        
        std::vector<int64_t> keyVec = {_macKey};
        auto r2 = Comm::serverSendAsync(keyVec, _width, buildTag(_currentMsgTag + 3));
        Comm::wait(r2);
        
        return true;
    } else {
        Log::i("[Rank {}] Receiver: starting consistency check", Comm::rank());
        Log::i("[Rank {}] Receiver: generating challenges", Comm::rank());
        std::vector<int64_t> challenges = generateChallenges(CONSISTENCY_CHECK_COUNT);
        Log::i("[Rank {}] Receiver: sending challenges...", Comm::rank());
        auto r0 = Comm::serverSendAsync(challenges, _width, buildTag(_currentMsgTag + 1));
        Comm::wait(r0);
        Log::i("[Rank {}] Receiver: challenges sent", Comm::rank());

        Log::i("[Rank {}] Receiver: waiting for responses...", Comm::rank());
        std::vector<int64_t> responses;
        auto r1 = Comm::serverReceiveAsync(responses, CONSISTENCY_CHECK_COUNT * 4, _width, 
                                           buildTag(_currentMsgTag + 2));
        Comm::wait(r1);
        Log::i("[Rank {}] Receiver: responses received", Comm::rank());

        Log::i("[Rank {}] Receiver: waiting for MAC key...", Comm::rank());
        std::vector<int64_t> keyVec;
        auto r2 = Comm::serverReceiveAsync(keyVec, 1, _width, buildTag(_currentMsgTag + 3));
        Comm::wait(r2);
        Log::i("[Rank {}] Receiver: MAC key received", Comm::rank());

        int64_t senderMacKey = keyVec[0];
        
        for (int c = 0; c < CONSISTENCY_CHECK_COUNT; ++c) {
            int64_t resp0 = responses[c * 4];
            int64_t mac0 = responses[c * 4 + 1];
            int64_t resp1 = responses[c * 4 + 2];
            int64_t mac1 = responses[c * 4 + 3];
            
            if (!verifyMac(resp0, mac0, senderMacKey) || 
                !verifyMac(resp1, mac1, senderMacKey)) {
                Log::e("MaliciousOtBatchOperator: MAC verification failed in consistency check");
                return false;
            }
        }
        
        return true;
    }
}

void MaliciousOtBatchOperator::execute0() {
    if (_isSender) {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_ms0->size());
        auto r0 = Comm::serverReceiveAsync(ks, size, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toSend;
    toSend.resize(size * 4);
        
        Comm::wait(r0);
        for (int i = 0; i < size; ++i) {
            int64_t masked_m0 = (*_ms0)[i] ^ (ks[i] == 0
                                              ? IntermediateDataSupport::_sRot0->_r0
                                              : IntermediateDataSupport::_sRot0->_r1);
            int64_t masked_m1 = (*_ms1)[i] ^ (ks[i] == 0
                                                  ? IntermediateDataSupport::_sRot0->_r1
                                                  : IntermediateDataSupport::_sRot0->_r0);
            
            toSend[i * 4] = masked_m0;
            toSend[i * 4 + 1] = generateMac(masked_m0, _macKey);
            toSend[i * 4 + 2] = masked_m1;
            toSend[i * 4 + 3] = generateMac(masked_m1, _macKey);
        }
        
        auto r1 = Comm::serverSendAsync(toSend, _width, buildTag(_currentMsgTag));
        Comm::wait(r1);

        std::vector<int64_t> keyVec = {_macKey};
        auto r2 = Comm::serverSendAsync(keyVec, _width, buildTag(_currentMsgTag + 4));
        Comm::wait(r2);
    } else {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_choices->size());
        ks.resize(size);
        for (int i = 0; i < size; ++i) {
            ks[i] = IntermediateDataSupport::_rRot0->_b ^ (*_choices)[i];
        }
        auto r0 = Comm::serverSendAsync(ks, 1, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        auto r1 = Comm::serverReceiveAsync(toRecv, size * 4, _width, buildTag(_currentMsgTag));

        _results.resize(size);
        Comm::wait(r1);

        std::vector<int64_t> keyVec;
        auto r2 = Comm::serverReceiveAsync(keyVec, 1, _width, buildTag(_currentMsgTag + 4));
        Comm::wait(r2);
        int64_t senderMacKey = keyVec[0];
        
        for (int i = 0; i < size; ++i) {
            int choice = (*_choices)[i];
            int64_t selected_msg = toRecv[i * 4 + choice * 2];
            int64_t selected_mac = toRecv[i * 4 + choice * 2 + 1];

            if (!verifyMac(selected_msg, selected_mac, senderMacKey)) {
                Log::e("MaliciousOtBatchOperator: MAC verification failed for OT " + std::to_string(i));
            }
            
            _results[i] = ring(selected_msg ^ IntermediateDataSupport::_rRot0->_rb);
        }
        Comm::wait(r0);
    }
}

void MaliciousOtBatchOperator::executeForBits() {
    if (_isSender) {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_ms0->size());
        auto r0 = Comm::serverReceiveAsync(ks, size, _width, buildTag(_currentMsgTag));

    std::vector<int64_t> toSend(size * 4);
        Comm::wait(r0);

        int64_t ir0 = IntermediateDataSupport::_sRot0->_r0;
        int64_t ir1 = IntermediateDataSupport::_sRot0->_r1;
        
        for (int i = 0; i < size; ++i) {
            int64_t ks_i = ks[i];

            int64_t mask0 = (ir0 & ~ks_i) | (ir1 & ks_i);
            int64_t mask1 = (ir1 & ~ks_i) | (ir0 & ks_i);

            int64_t masked_m0 = (*_ms0)[i] ^ mask0;
            int64_t masked_m1 = (*_ms1)[i] ^ mask1;
            
            toSend[i * 4] = masked_m0;
            toSend[i * 4 + 1] = generateMac(masked_m0, _macKey);
            toSend[i * 4 + 2] = masked_m1;
            toSend[i * 4 + 3] = generateMac(masked_m1, _macKey);
        }
        
        auto r1 = Comm::serverSendAsync(toSend, _width, buildTag(_currentMsgTag));
        Comm::wait(r1);

        std::vector<int64_t> keyVec = {_macKey};
        auto r2 = Comm::serverSendAsync(keyVec, _width, buildTag(_currentMsgTag + 5));
        Comm::wait(r2);
    } else {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_choiceBits->size());
        ks.resize(size);

        int64_t ib = IntermediateDataSupport::_rRot0->_b;
        if (ib == 1) {
            ib = -1;
        }

        for (int i = 0; i < size; ++i) {
            ks[i] = ib ^ (*_choiceBits)[i];
        }

        auto r0 = Comm::serverSendAsync(ks, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        auto r1 = Comm::serverReceiveAsync(toRecv, size * 4, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> keyVec;
        auto r2 = Comm::serverReceiveAsync(keyVec, 1, _width, buildTag(_currentMsgTag + 5));

        int64_t rb = IntermediateDataSupport::_rRot0->_rb;
        _results.resize(size);
        
        Comm::wait(r1);
        Comm::wait(r2);
        int64_t senderMacKey = keyVec[0];
        
        for (int i = 0; i < size; ++i) {
            int64_t choice = (*_choiceBits)[i];

            int64_t block0 = toRecv[i * 4];
            int64_t mac0 = toRecv[i * 4 + 1];
            int64_t block1 = toRecv[i * 4 + 2];
            int64_t mac1 = toRecv[i * 4 + 3];

            int64_t selected = (block0 & ~choice) | (block1 & choice);
            int64_t selected_mac = (mac0 & ~choice) | (mac1 & choice);

            _results[i] = selected ^ rb;
        }
        Comm::wait(r0);
    }
}

void MaliciousOtBatchOperator::executeCutAndChoose() {
    constexpr int BUCKET_SIZE = 3;
    constexpr int CHECK_RATIO = 2;

    if (_isSender) {
        int size = static_cast<int>(_ms0->size());
        int totalOTs = size * BUCKET_SIZE;
        
        std::vector<int64_t> allM0(totalOTs);
        std::vector<int64_t> allM1(totalOTs);
        std::random_device rd;
        std::mt19937_64 gen(rd());
        
        for (int i = 0; i < size; ++i) {
            for (int j = 0; j < BUCKET_SIZE; ++j) {
                int64_t r = static_cast<int64_t>(gen());
                allM0[i * BUCKET_SIZE + j] = (*_ms0)[i] ^ r;
                allM1[i * BUCKET_SIZE + j] = (*_ms1)[i] ^ r;
            }
        }
        

    }
}

MaliciousOtBatchOperator *MaliciousOtBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    Log::i("[Rank {}] MaliciousOtBatchOperator::execute() start, isClient={}",
           Comm::rank(), Comm::isClient());

    if (Comm::isClient()) {
        Log::i("[Client] MaliciousOtBatchOperator::execute() returning early");
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }


    Log::i("[Rank {}] Starting consistency check...", Comm::rank());
    if (!performConsistencyCheck()) {
        Log::e("MaliciousOtBatchOperator: Consistency check failed, aborting");
        return this;
    }
    Log::i("[Rank {}] Consistency check passed", Comm::rank());

    _currentMsgTag += 4;

    Log::i("[Rank {}] Executing main OT, _doBits={}", Comm::rank(), _doBits);
    if (_doBits) {
        executeForBits();
    } else {
        execute0();
    }
    Log::i("[Rank {}] Main OT execution done", Comm::rank());

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int MaliciousOtBatchOperator::tagStride() {
    return 6;
}