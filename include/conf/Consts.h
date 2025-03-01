//
// Created by 杜建璋 on 2025/2/24.
//

#ifndef CONSTS_H
#define CONSTS_H



class Consts {
public:
    // pool type
    static constexpr int CTPL_POOL = 0;
    static constexpr int TBB_POOL = 1;
    // blocking queue type
    static constexpr int CAS_QUEUE = 0;
    static constexpr int LOCK_QUEUE = 1;
    static constexpr int MPI = 0;
    // bmt generation
    static constexpr int BMT_BACKGROUND = 0;
    static constexpr int BMT_JIT = 1;
    static constexpr int BMT_FIXED = 2; // ONLY FOR TESTING!
    static constexpr int BMT_BATCH_BACKGROUND = 3;
};



#endif //CONSTS_H
