//
// Created by 杜建璋 on 25-5-30.
//

#ifndef DBCONF_H
#define DBCONF_H

#include "conf/Conf.h"

class DbConf {
public:
    inline static bool ENABLE_HASH_JOIN = true;
    inline static int SHUFFLE_BUCKET_NUM = 32;
    inline static bool DISABLE_HASH_JOIN = false;
    // Only use when not separating batches
    inline static bool DISABLE_COMPACTION = false;

    static void init() {
        if (Conf::_userParams.count("enable_hash_join")) {
            ENABLE_HASH_JOIN = Conf::_userParams["enable_hash_join"] == "true";
        }
        if (Conf::_userParams.count("shuffle_bucket_num")) {
            SHUFFLE_BUCKET_NUM = std::stoi(Conf::_userParams["shuffle_bucket_num"]);
        }
        if (Conf::_userParams.count("disable_hash_join")) {
            DISABLE_HASH_JOIN = Conf::_userParams["disable_hash_join"] == "true";
        }
        if (Conf::_userParams.count("disable_compaction")) {
            DISABLE_COMPACTION = Conf::_userParams["disable_compaction"] == "true";
        }
    };
};


#endif //DBCONF_H
