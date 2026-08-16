// Regression test for the packed-tag / MPI_TAG_UB defect (2026-08-13).
//
// parsec packs (taskTag << (32 - TASK_TAG_BITS)) | msgTag into one int and,
// before the fix, handed it straight to MPI as the message tag. Under PMLs
// with a small MPI_TAG_UB (OpenMPI ucx: 2^23-1) the taskTag bits were silently
// truncated, so streams that differ only in taskTag collided. This test
// exercises exactly that failure class and passes only if streams with equal
// msgTag but different taskTag are kept apart, and only if the raw MPI tag
// handed to the runtime is within MPI_TAG_UB.
//
// Run:  mpirun -np 3 --mca pml ucx ./build/primitives/benchmark/correctness_comm_tags
//       mpirun -np 3 --mca pml ob1 --mca btl tcp,vader,self ./build/...  (regression)
// Exit code 0 = PASS, 1 = FAIL. Rank 2 (client) is idle by design.

#include <cstdlib>
#include <string>
#include <vector>

#include "comm/Comm.h"
#include "conf/Conf.h"
#include "utils/Log.h"
#include "utils/System.h"

namespace {
int packTag(int taskTag, int msgTag) {
    // Mirror of SecureOperator::buildTag (kept private there).
    int bits = 32 - Conf::TASK_TAG_BITS;
    return static_cast<int>((static_cast<unsigned>(taskTag) << bits) |
                            (static_cast<unsigned>(msgTag) & ((1u << bits) - 1)));
}

int failures = 0;
void check(bool ok, const std::string &what) {
    if (!ok) {
        ++failures;
        Log::e("FAIL: {}", what);
    } else {
        Log::i("ok:   {}", what);
    }
}
} // namespace

int main(int argc, char *argv[]) {
    System::init(argc, argv);
    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }
    const int me = Comm::rank();
    const int peer = 1 - me;

    // ---- Case 1: same msgTag, different taskTag, sent in the "wrong" order.
    // Rank 0 sends the task-1 stream first, then task-0. Rank 1 receives task-0
    // first. With correct isolation task-0's receive must get task-0's payload;
    // with tag truncation the two streams share a tag and rank 1 gets the
    // task-1 payload (FIFO on the collided stream).
    {
        const int msg = 5;
        if (me == 0) {
            std::string a = "TASK1-PAYLOAD-AAAA";
            std::string b = "TASK0-PAYLOAD-BB";
            Comm::send(a, peer, packTag(1, msg));
            Comm::send(b, peer, packTag(0, msg));
        } else {
            std::string got0, got1;
            Comm::receive(got0, peer, packTag(0, msg));
            Comm::receive(got1, peer, packTag(1, msg));
            check(got0 == "TASK0-PAYLOAD-BB", "case1 task0 stream isolated (got '" + got0 + "')");
            check(got1 == "TASK1-PAYLOAD-AAAA", "case1 task1 stream isolated (got '" + got1 + "')");
        }
    }

    // ---- Case 2: the startup collision seen in the field — a raw small tag
    // (RSA pubkey exchange uses tag 1) vs a packed tag whose low bits are 1
    // (ROT base-OT pair 2 = taskTag 2, msgTag 1). Payload lengths differ, so a
    // collision manifests exactly like BaseOtOperator.cpp:36 did.
    {
        if (me == 0) {
            std::string big(490, 'R');            // 2 x 245, the base-OT comb size
            std::string small = "PUBKEY";
            Comm::send(big, peer, packTag(2, 1));  // ROT-like stream
            Comm::send(small, peer, 1);            // raw tag 1, RSA-like stream
        } else {
            std::string small, big;
            Comm::receive(small, peer, 1);
            Comm::receive(big, peer, packTag(2, 1));
            check(small == "PUBKEY", "case2 raw tag 1 not polluted by taskTag-2 stream");
            check(big.size() == 490 && big[0] == 'R', "case2 taskTag-2 stream received intact (size " + std::to_string(big.size()) + ")");
        }
    }

    // ---- Case 3: taskTag near the top of the 6-bit range (bit 31 hazard):
    // packTag(63, 7) has bit 31 set -> negative int. Before the fix that is an
    // invalid tag under EVERY pml. After the fix it decodes to comm[63], tag 7.
    {
        const int t = packTag((1 << Conf::TASK_TAG_BITS) - 1, 7);
        if (me == 0) {
            std::vector<int64_t> v = {11, 22, 33};
            Comm::send(v, 64, peer, t);
        } else {
            std::vector<int64_t> v;
            Comm::receive(v, 64, peer, t);
            check(v.size() == 3 && v[0] == 11 && v[2] == 33, "case3 max taskTag (bit-31) round-trips");
        }
    }

    // ---- Case 4: async path uses the same decode.
    {
        const int t = packTag(3, 9);
        if (me == 0) {
            std::vector<int64_t> v = {7, 8};
            auto *r = Comm::sendAsync(v, 64, peer, t);
            Comm::wait(r);
        } else {
            std::vector<int64_t> v(2);
            auto *r = Comm::receiveAsync(v, 2, 64, peer, t);
            Comm::wait(r);
            check(v[0] == 7 && v[1] == 8, "case4 async send/recv decode");
        }
    }

    // Aggregate verdict on rank 1 (the receiver in every case). Rank 0 mirrors it
    // via one final message so both exit codes agree.
    int verdict = 0;
    if (me == 1) {
        verdict = failures == 0 ? 0 : 1;
        Comm::send(static_cast<int64_t>(verdict), 64, peer, packTag(0, 4242));
        Log::i(verdict == 0 ? "correctness_comm_tags: PASS" : "correctness_comm_tags: FAIL ({} failures)", failures);
    } else {
        int64_t v = 0;
        Comm::receive(v, 64, peer, packTag(0, 4242));
        verdict = static_cast<int>(v);
    }
    System::finalize();
    return verdict;
}
