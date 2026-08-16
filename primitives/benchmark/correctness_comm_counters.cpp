// Regression test for the Comm traffic counters (feat/comm-counters).
// Runs under any transport: mpirun -np 3 ./correctness_comm_counters
// Checks, on the two servers, that after known exchanges the counters read
// exactly the expected messages/bytes for both ENABLE_TRANSFER_COMPRESSION
// settings and that a BoolAndBatchOperator (bmt_fixed) accounts for exactly one
// exchange of 2N int64 words per server (16N B uncompressed, 8N B at width 32
// compressed).
#include "comm/Comm.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "conf/Conf.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/System.h"

#include <cstdint>
#include <iostream>
#include <vector>

static int fails = 0;
static void expect(const char *what, int64_t got, int64_t want) {
    if (got != want) {
        std::cout << "[FAIL] " << what << ": got " << got << " want " << want << std::endl;
        fails++;
    } else {
        std::cout << "[ ok ] " << what << " = " << got << std::endl;
    }
}

int main(int argc, char **argv) {
    System::init(argc, argv);
    if (Comm::isServer()) {
        const int peer = 1 - Comm::rank();
        // 1) scalar + vector + string, uncompressed (default)
        Conf::ENABLE_TRANSFER_COMPRESSION = false;
        Comm::resetCounters();
        std::vector<int64_t> v(1000, 7), r;
        int64_t s = 5, sr = 0;
        std::string str(123, 'x'), strr;
        if (Comm::rank() == 0) {
            Comm::send(v, 32, peer, 11);
            Comm::send(s, 32, peer, 12);
            Comm::send(str, peer, 13);
            Comm::receive(r, 32, peer, 21);
            Comm::receive(sr, 32, peer, 22);
            Comm::receive(strr, peer, 23);
        } else {
            Comm::receive(r, 32, peer, 11);
            Comm::receive(sr, 32, peer, 12);
            Comm::receive(strr, peer, 13);
            Comm::send(v, 32, peer, 21);
            Comm::send(s, 32, peer, 22);
            Comm::send(str, peer, 23);
        }
        expect("uncompressed sentMessages", Comm::_sentMessages, 3);
        expect("uncompressed sentBytes", Comm::_sentBytes, 1000 * 8 + 8 + 123);
        expect("uncompressed recvMessages", Comm::_recvMessages, 3);
        expect("uncompressed recvBytes", Comm::_recvBytes, 1000 * 8 + 8 + 123);
        expect("payload intact", (int64_t)r.size() + sr + (int64_t)strr.size(), 1000 + 5 + 123);

        // 2) compressed at width 32 -> 4 B/elem
        Conf::ENABLE_TRANSFER_COMPRESSION = true;
        Comm::resetCounters();
        if (Comm::rank() == 0) {
            Comm::send(v, 32, peer, 31);
            Comm::receive(r, 32, peer, 41);
        } else {
            Comm::receive(r, 32, peer, 31);
            Comm::send(v, 32, peer, 41);
        }
        expect("compressed sentBytes", Comm::_sentBytes, 1000 * 4);
        expect("compressed recvBytes", Comm::_recvBytes, 1000 * 4);
        Conf::ENABLE_TRANSFER_COMPRESSION = false;

        // 3) async path
        Comm::resetCounters();
        auto *rq = Comm::serverReceiveAsync(r, 1000, 32, 51);
        auto *sq = Comm::serverSendAsync(v, 32, 51);
        Comm::wait(sq);
        Comm::wait(rq);
        expect("async sentMessages", Comm::_sentMessages, 1);
        expect("async sentBytes", Comm::_sentBytes, 8000);
        expect("async recvMessages", Comm::_recvMessages, 1);
        expect("async recvBytes", Comm::_recvBytes, 8000);

        // 4) one BoolAndBatchOperator under bmt_fixed: exactly one exchange of
        //    (e,f) = 2N int64 words per server, no BMT-generation traffic.
        Conf::BMT_METHOD = Conf::BMT_FIXED;
        const int N = 4096;
        std::vector<int64_t> xs(N, Comm::rank()), ys(N, 3);
        Comm::resetCounters();
        BoolAndBatchOperator(&xs, &ys, 32, 0, 0, SecureOperator::NO_CLIENT_COMPUTE).execute();
        expect("AND sentMessages", Comm::_sentMessages, 1);
        expect("AND sentBytes", Comm::_sentBytes, 2LL * N * 8);
        expect("AND recvMessages", Comm::_recvMessages, 1);
        expect("AND recvBytes", Comm::_recvBytes, 2LL * N * 8);

        std::cout << (fails ? "[RESULT] FAIL " : "[RESULT] PASS ") << "rank " << Comm::rank()
                  << " fails=" << fails << std::endl;
    }
    System::finalize();
    return fails ? 1 : 0;
}
