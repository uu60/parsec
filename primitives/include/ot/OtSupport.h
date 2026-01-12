#ifndef OT_SUPPORT_H
#define OT_SUPPORT_H

#include "conf/Conf.h"

#include <vector>

class OtSupport {
public:
    enum class Backend {
        BASE,
        RAND,
        IKNP,
    };

    static Backend backend();
    static const char *backendName();

    // Returns the tag stride consumed by a single otBatch(...) call.
    // Note: width is the same width parameter passed to otBatch/otBatchPackedChoices64.
    static int batchTagStride(int width);

    static void otBatch(int sender,
                        std::vector<int64_t> *ms0,
                        std::vector<int64_t> *ms1,
                        std::vector<int> *choices,
                        int width,
                        int taskTag,
                        int msgTagOffset,
                        std::vector<int64_t> *outResults);

    // Batch OT where choices are packed in 64-bit limbs (each int64_t contains 64 choice bits).
    // outResults has the same length as choicesPacked.
    static void otBatchPackedChoices64(int sender,
                                      std::vector<int64_t> *ms0,
                                      std::vector<int64_t> *ms1,
                                      std::vector<int64_t> *choicesPacked,
                                      int width,
                                      int taskTag,
                                      int msgTagOffset,
                                      std::vector<int64_t> *outResults);
};

#endif
