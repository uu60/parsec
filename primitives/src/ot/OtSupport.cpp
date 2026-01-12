#include "ot/OtSupport.h"

#include "comm/Comm.h"
#include "ot/BaseOtOperator.h"
#include "ot/IknpOtBatchOperator.h"
#include "ot/RandOtBatchOperator.h"
#include "utils/Log.h"

#include <stdexcept>

namespace {
// Expand packed 64-bit choice masks to per-bit choices (0/1).
// bits = number of choice bits to expand; if bits < 0, expand all (packed.size()*64).
static void expandChoicesFromPacked64(const std::vector<int64_t> &packed,
                                     int bits,
                                     std::vector<int> &outChoices) {
    const int totalBits = bits >= 0 ? bits : static_cast<int>(packed.size()) * 64;
    outChoices.resize(totalBits);
    for (int i = 0; i < totalBits; ++i) {
        const uint64_t limb = static_cast<uint64_t>(packed[static_cast<size_t>(i >> 6)]);
        outChoices[i] = static_cast<int>((limb >> (i & 63)) & 1ULL);
    }
}

// Expand packed 64-bit sender masks to per-bit messages.
// For each bit: message is 0 or -1 (mask) depending on the bit value.
// bits = number of message bits to expand; if bits < 0, expand all (packed.size()*64).
static void expandMasksFromPacked64(const std::vector<int64_t> &packed,
                                   int bits,
                                   std::vector<int64_t> &outMsgs) {
    const int totalBits = bits >= 0 ? bits : static_cast<int>(packed.size()) * 64;
    outMsgs.resize(totalBits);
    for (int i = 0; i < totalBits; ++i) {
        const uint64_t limb = static_cast<uint64_t>(packed[static_cast<size_t>(i >> 6)]);
        const int b = static_cast<int>((limb >> (i & 63)) & 1ULL);
        outMsgs[i] = (b == 0) ? 0LL : -1LL;
    }
}
}

OtSupport::Backend OtSupport::backend() {
    switch (Conf::OT_METHOD) {
        case Conf::OT_BASE:
            return Backend::BASE;
        case Conf::OT_RAND:
            return Backend::RAND;
        case Conf::OT_IKNP:
            return Backend::IKNP;
        default:
            return Backend::RAND;
    }
}

const char *OtSupport::backendName() {
    switch (backend()) {
        case Backend::BASE:
            return "base";
        case Backend::RAND:
            return "rand";
        case Backend::IKNP:
            return "iknp";
        default:
            return "unknown";
    }
}

int OtSupport::batchTagStride(int width) {
    // Keep this value stable and backend-specific so callers can partition tag ranges safely.
    // For BASE backend, tagStride is per-single-OT, so callers that do a loop must still
    // account for the per-item stride themselves.
    switch (backend()) {
        case Backend::RAND:
            return RandOtBatchOperator::tagStride();
        case Backend::IKNP:
            return IknpOtBatchOperator::tagStride();
        case Backend::BASE:
            return BaseOtOperator::tagStride();
        default:
            return RandOtBatchOperator::tagStride();
    }
}

void OtSupport::otBatch(int sender,
                        std::vector<int64_t> *ms0,
                        std::vector<int64_t> *ms1,
                        std::vector<int> *choices,
                        int width,
                        int taskTag,
                        int msgTagOffset,
                        std::vector<int64_t> *outResults) {
    if (Comm::isClient()) {
        return;
    }

    if (!outResults) {
        throw std::invalid_argument("OtSupport::otBatch outResults is null");
    }

    switch (backend()) {
        case Backend::RAND: {
            RandOtBatchOperator op(sender, ms0, ms1, choices, width, taskTag, msgTagOffset);
            op.execute();
            *outResults = op._results;
            return;
        }
        case Backend::BASE: {
            const bool isSender = Comm::rank() == sender;
            const int n = isSender ? static_cast<int>(ms0 ? ms0->size() : 0)
                                   : static_cast<int>(choices ? choices->size() : 0);
            outResults->assign(n, 0);

            for (int i = 0; i < n; ++i) {
                const int64_t m0i = (isSender && ms0) ? (*ms0)[i] : -1;
                const int64_t m1i = (isSender && ms1) ? (*ms1)[i] : -1;
                const int ci = (!isSender && choices) ? (*choices)[i] : -1;
                BaseOtOperator e(sender, m0i, m1i, ci, 64, taskTag, msgTagOffset + i * BaseOtOperator::tagStride());
                e.execute();
                if (!isSender) {
                    (*outResults)[i] = e._result;
                }
            }
            return;
        }
        case Backend::IKNP: {
            IknpOtBatchOperator op(sender, ms0, ms1, choices, width, taskTag, msgTagOffset);
            op.execute();
            *outResults = op._results;
            return;
        }
        default:
            throw std::runtime_error("OtSupport::otBatch unknown backend");
    }
}

void OtSupport::otBatchPackedChoices64(int sender,
                                      std::vector<int64_t> *ms0,
                                      std::vector<int64_t> *ms1,
                                      std::vector<int64_t> *choicesPacked,
                                      int width,
                                      int taskTag,
                                      int msgTagOffset,
                                      std::vector<int64_t> *outResults) {
    if (Comm::isClient()) {
        return;
    }

    if (!outResults) {
        throw std::invalid_argument("OtSupport::otBatchPackedChoices64 outResults is null");
    }

    if (backend() == Backend::RAND) {
        RandOtBatchOperator op(sender, ms0, ms1, choicesPacked, taskTag, msgTagOffset);
        op.execute();
        *outResults = op._results;
        return;
    }

    if (backend() == Backend::IKNP) {
        (void) width;
        IknpOtBatchOperator op(sender, ms0, ms1, choicesPacked, taskTag, msgTagOffset);
        op.execute();
        *outResults = op._results;
        return;
    }

    // BASE backend: compatibility path by expanding packed choices/messages to per-bit OTs.
    const bool isSender = Comm::rank() == sender;
    const int nPacked = isSender ? static_cast<int>(ms0 ? ms0->size() : 0)
                                 : static_cast<int>(choicesPacked ? choicesPacked->size() : 0);
    const int totalBits = nPacked * 64;

    std::vector<int> choicesBits;
    std::vector<int64_t> ms0Bits;
    std::vector<int64_t> ms1Bits;

    if (isSender) {
        if (!ms0 || !ms1) {
            throw std::invalid_argument("OtSupport::otBatchPackedChoices64 sender ms0/ms1 is null");
        }
        if (static_cast<int>(ms0->size()) != nPacked || static_cast<int>(ms1->size()) != nPacked) {
            throw std::invalid_argument("OtSupport::otBatchPackedChoices64 sender ms0/ms1 size mismatch");
        }
        expandMasksFromPacked64(*ms0, totalBits, ms0Bits);
        expandMasksFromPacked64(*ms1, totalBits, ms1Bits);
    } else {
        if (!choicesPacked) {
            throw std::invalid_argument("OtSupport::otBatchPackedChoices64 receiver choicesPacked is null");
        }
        expandChoicesFromPacked64(*choicesPacked, totalBits, choicesBits);
    }

    std::vector<int64_t> outBitResults;
    otBatch(sender,
            isSender ? &ms0Bits : nullptr,
            isSender ? &ms1Bits : nullptr,
            isSender ? nullptr : &choicesBits,
            width,
            taskTag,
            msgTagOffset,
            &outBitResults);

    // Pack per-bit results back into 64-bit limbs (each bit result is expected to be 0/-1).
    outResults->assign(nPacked, 0);
    if (!isSender) {
        for (int i = 0; i < totalBits; ++i) {
            const int64_t v = outBitResults[static_cast<size_t>(i)];
            const uint64_t bit = (v == 0) ? 0ULL : 1ULL;
            const int idx = i >> 6;
            const int off = i & 63;
            (*outResults)[static_cast<size_t>(idx)] |= static_cast<int64_t>(bit << off);
        }
    }
}
