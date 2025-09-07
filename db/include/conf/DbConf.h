//
// Created by 杜建璋 on 25-5-30.
//

#ifndef DBCONF_H
#define DBCONF_H

#include "conf/Conf.h"
#include "utils/Log.h"

class DbConf {
public:
    inline static bool ENABLE_HASH_JOIN = true;
    inline static int SHUFFLE_BUCKET_NUM = 32;
    inline static bool BASELINE_MODE = false;

    static void init() {
        if (Conf::_userParams.count("enable_hash_join")) {
            ENABLE_HASH_JOIN = Conf::_userParams["enable_hash_join"] == "true";
        }
        if (Conf::_userParams.count("shuffle_bucket_num")) {
            SHUFFLE_BUCKET_NUM = std::stoi(Conf::_userParams["shuffle_bucket_num"]);
        }
        if (Conf::_userParams.count("baseline_mode")) {
            BASELINE_MODE = Conf::_userParams["baseline_mode"] == "true";
        }

        if (BASELINE_MODE) {
            Conf::DISABLE_MULTI_THREAD = true;
            Conf::ENABLE_INTRA_OPERATOR_PARALLELISM = false;
            Conf::ENABLE_SIMD = false;
            Conf::BATCH_SIZE = 0;
        }
    };
};


#endif //DBCONF_H
