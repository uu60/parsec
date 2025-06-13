//
// Created by 杜建璋 on 25-5-30.
//

#ifndef DBCONF_H
#define DBCONF_H

class DbConf {
public:
    inline static bool ENABLE_SHUFFLE_BUCKET_JOIN = true;
    inline static int SHUFFLE_BUCKET_NUM = 4;
};



#endif //DBCONF_H
