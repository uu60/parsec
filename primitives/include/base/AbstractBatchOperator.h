
#ifndef ABSTRACTBATCHEXECUTOR_H
#define ABSTRACTBATCHEXECUTOR_H
#include "./SecureOperator.h"


class AbstractBatchOperator : public SecureOperator {
public:
    std::vector<int64_t> _results{};
    std::vector<int64_t> *_xis{};
    std::vector<int64_t> *_yis{};
    std::vector<int64_t> _zis{};

    bool _dx{};
    bool _dy{};

    AbstractBatchOperator(int width, int taskTag, int msgTagOffset)
        : SecureOperator(width, taskTag, msgTagOffset) {
    }

    ~AbstractBatchOperator() override {
        if (_dx) {
            delete _xis;
        }
        if (_dy) {
            delete _yis;
        }
    }
};



#endif
