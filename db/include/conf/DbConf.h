
#ifndef DBCONF_H
#define DBCONF_H

#include "conf/AppConfig.h"
#include "conf/Conf.h"
#include "utils/Log.h"

class DbConf {
public:
    inline static bool ENABLE_HASH_JOIN = true;
    inline static int SHUFFLE_BUCKET_NUM = 32;
    inline static bool DISABLE_PRECISE_COMPACTION = true;
    inline static bool BASELINE_MODE = false;
    inline static bool NO_COMPACTION = false;

    static void init() {
        ENABLE_HASH_JOIN = AppConfig::getBool("enable_hash_join", ENABLE_HASH_JOIN);
        SHUFFLE_BUCKET_NUM = AppConfig::getInt("shuffle_bucket_num", SHUFFLE_BUCKET_NUM);
        BASELINE_MODE = AppConfig::getBool("baseline_mode", BASELINE_MODE);
        NO_COMPACTION = AppConfig::getBool("no_compaction", NO_COMPACTION);
        DISABLE_PRECISE_COMPACTION =
                AppConfig::getBool("disable_precise_compaction", DISABLE_PRECISE_COMPACTION);

        if (BASELINE_MODE) {
            NO_COMPACTION = true;
            Conf::DISABLE_MULTI_THREAD = true;
            Conf::ENABLE_INTRA_OPERATOR_PARALLELISM = false;
            Conf::ENABLE_SIMD = false;
            Conf::BATCH_SIZE = 0;
        }
    };
};


#endif
