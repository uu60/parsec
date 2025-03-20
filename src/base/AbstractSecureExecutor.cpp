//
// Created by 杜建璋 on 2024/7/7.
//

#include "base/AbstractSecureExecutor.h"

#include "conf/Conf.h"
#include "ot/RandOtBatchExecutor.h"
#include "ot/RandOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

int AbstractSecureExecutor::buildTag(int msgTag) const {
    int bits = 32 - TASK_TAG_BITS;
    return (static_cast<unsigned int>(_taskTag) << bits) | static_cast<unsigned int>(msgTag & ((1 << bits) - 1));
}

std::vector<int64_t> AbstractSecureExecutor::handleOt(int sender, std::vector<int64_t> &ss0, std::vector<int64_t> &ss1,
                                                      std::vector<int> &choices) {
    std::vector<int64_t> results;
    size_t all = sender == Comm::rank() ? ss0.size() : choices.size();
    if constexpr (Conf::TASK_BATCHING) {
        RandOtBatchExecutor r(sender, &ss0, &ss1, &choices, _width, _taskTag, static_cast<int>(
                                  _currentMsgTag + sender * RandOtBatchExecutor::msgTagCount()));
        results = r.execute()->_results;
    } else if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
        std::vector<std::future<int64_t> > futures;
        futures.reserve(all);
        bool isSender = sender == Comm::rank();
        for (int i = 0; i < all - 1; i++) {
            futures.push_back(ThreadPoolSupport::submit([&, i] {
                auto s0 = isSender ? ss0[i] : 0;
                auto s1 = isSender ? ss1[i] : 0;
                auto choice = isSender ? 0 : choices[i];
                auto baseOffset = static_cast<int>(sender * all * RandOtExecutor::msgTagCount(_width));
                auto offset = static_cast<int>(
                    _currentMsgTag + RandOtExecutor::msgTagCount(_width) * i + baseOffset);
                RandOtExecutor r(sender, s0, s1, choice, _width, _taskTag, offset);
                return r.execute()->_result;
            }));
        }
        auto s0 = isSender ? ss0[all - 1] : 0;
        auto s1 = isSender ? ss1[all - 1] : 0;
        auto choice = isSender ? 0 : choices[all - 1];
        RandOtExecutor r(sender, s0, s1, choice, _width, _taskTag,
                         static_cast<int>(
                             _currentMsgTag + RandOtExecutor::msgTagCount(_width) * (all - 1) + sender * all *
                             RandOtExecutor::msgTagCount(_width)));
        r.execute();
        results.reserve(all);
        for (auto &f: futures) {
            results.push_back(f.get());
        }
        results.push_back(r._result);
    } else {
        results.reserve(all);
        for (int i = 0; i < all; i++) {
            auto s0 = sender == Comm::rank() ? ss0[i] : 0;
            auto s1 = sender == Comm::rank() ? ss1[i] : 0;
            auto choice = sender == Comm::rank() ? 0 : choices[i];
            RandOtExecutor r(sender, s0, s1, choice, _width, _taskTag, _currentMsgTag);
            results.push_back(r.execute()->_result);
        }
    }
    return results;
}

int64_t AbstractSecureExecutor::ring(int64_t raw) const {
    return Math::ring(raw, _width);
}
