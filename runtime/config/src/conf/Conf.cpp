#include "conf/Conf.h"

#include "conf/AppConfig.h"

#include <iostream>
#include <stdexcept>

namespace {
Conf::BmtT parseBmtMethod(const std::string &value) {
    if (value == "bmt_background") {
        return Conf::BMT_BACKGROUND;
    }
    if (value == "bmt_jit") {
        return Conf::BMT_JIT;
    }
    if (value == "bmt_fixed") {
        return Conf::BMT_FIXED;
    }
    if (value == "bmt_pipeline") {
        return Conf::BMT_PIPELINE;
    }
    throw std::runtime_error("Unknown bmt_method value.");
}

Conf::QueueT parseQueueType(const std::string &value) {
    if (value == "cas_queue" || value == "lock_free_queue") {
        return Conf::LOCK_FREE_QUEUE;
    }
    if (value == "lock_queue") {
        return Conf::LOCK_QUEUE;
    }
    if (value == "spsc_queue") {
        return Conf::SPSC_QUEUE;
    }
    throw std::runtime_error("Unknown bmt_queue_type value.");
}

int parseThreadPool(const std::string &value) {
    if (value == "ctpl_pool") {
        return Conf::CTPL_POOL;
    }
    if (value == "async") {
        return Conf::ASYNC;
    }
    if (value == "tbb_pool") {
#ifdef PARSEC_HAS_TBB
        return Conf::TBB_POOL;
#else
        std::cerr << "Warning: TBB is not available, falling back to ctpl_pool" << std::endl;
        return Conf::CTPL_POOL;
#endif
    }
    throw std::runtime_error("Unknown thread_pool value.");
}

Conf::CommT parseCommType(const std::string &value) {
    if (value == "mpi") {
        return Conf::MPI;
    }
    if (value == "tcp") {
        return Conf::TCP;
    }
    throw std::runtime_error("Unknown comm_type value.");
}

void printHelp() {
    std::cout
            << "Allowed options:\n"
            << "  --help\n"
            << "  --bmt_method=bmt_background|bmt_jit|bmt_fixed|bmt_pipeline\n"
            << "  --bmt_pre_gen_seconds=N\n"
            << "  --max_bmts=N\n"
            << "  --bmt_usage_limit=N\n"
            << "  --bmt_queue_type=cas_queue|lock_free_queue|lock_queue|spsc_queue\n"
            << "  --bmt_queue_num=N\n"
            << "  --disable_arith=true|false\n"
            << "  --bmt_gen_batch_size=N\n"
            << "  --task_tag_bits=N\n"
            << "  --disable_multi_thread=true|false\n"
            << "  --enable_intra_operator_parallelism=true|false\n"
            << "  --local_threads=N\n"
            << "  --thread_pool=async|ctpl_pool|tbb_pool\n"
            << "  --comm_type=mpi|tcp\n"
            << "  --tcp_rank=N --tcp_size=N --tcp_host=IP --tcp_base_port=PORT\n"
            << "  --batch_size=N\n"
            << "  --enable_transfer_compression=true|false\n"
            << "  --enable_redundant_ot=true|false\n"
            << "  --enable_class_wise_timing=true|false\n"
            << "  --enable_simd=true|false\n"
            << "  --enable_iknp_multithread=true|false\n"
            << "DB options:\n"
            << "  --enable_hash_join=true|false\n"
            << "  --shuffle_bucket_num=N\n"
            << "  --baseline_mode=true|false\n"
            << "  --no_compaction=true|false\n"
            << "  --disable_precise_compaction=true|false\n";
}
}

void Conf::init(int argc, char **argv) {
    try {
        AppConfig::init(argc, argv);
        _userParams = AppConfig::params();

        if (AppConfig::has("help")) {
            printHelp();
            std::exit(0);
        }

        if (AppConfig::has("bmt_method")) {
            BMT_METHOD = parseBmtMethod(AppConfig::getString("bmt_method", ""));
        }
        BMT_PRE_GEN_SECONDS = AppConfig::getInt("bmt_pre_gen_seconds", BMT_PRE_GEN_SECONDS);
        MAX_BMTS = AppConfig::getInt("max_bmts", MAX_BMTS);
        BMT_USAGE_LIMIT = AppConfig::getInt("bmt_usage_limit", BMT_USAGE_LIMIT);
        if (AppConfig::has("bmt_queue_type")) {
            BMT_QUEUE_TYPE = parseQueueType(AppConfig::getString("bmt_queue_type", ""));
        }
        BMT_QUEUE_NUM = AppConfig::getInt("bmt_queue_num", BMT_QUEUE_NUM);
        DISABLE_ARITH = AppConfig::getBool("disable_arith", DISABLE_ARITH);
        BMT_GEN_BATCH_SIZE = AppConfig::getInt("bmt_gen_batch_size", BMT_GEN_BATCH_SIZE);
        TASK_TAG_BITS = AppConfig::getInt("task_tag_bits", TASK_TAG_BITS);
        DISABLE_MULTI_THREAD = AppConfig::getBool("disable_multi_thread", DISABLE_MULTI_THREAD);
        ENABLE_INTRA_OPERATOR_PARALLELISM =
                AppConfig::getBool("enable_intra_operator_parallelism", ENABLE_INTRA_OPERATOR_PARALLELISM);
        LOCAL_THREADS = AppConfig::getInt("local_threads", LOCAL_THREADS);
        if (AppConfig::has("thread_pool")) {
            THREAD_POOL_TYPE = parseThreadPool(AppConfig::getString("thread_pool", ""));
        }
        if (AppConfig::has("comm_type")) {
            COMM_TYPE = parseCommType(AppConfig::getString("comm_type", ""));
        }
        BATCH_SIZE = AppConfig::getInt("batch_size", BATCH_SIZE);
        ENABLE_TRANSFER_COMPRESSION =
                AppConfig::getBool("enable_transfer_compression", ENABLE_TRANSFER_COMPRESSION);
        ENABLE_REDUNDANT_OT = AppConfig::getBool("enable_redundant_ot", ENABLE_REDUNDANT_OT);
        ENABLE_CLASS_WISE_TIMING = AppConfig::getBool("enable_class_wise_timing", ENABLE_CLASS_WISE_TIMING);
        ENABLE_SIMD = AppConfig::getBool("enable_simd", ENABLE_SIMD);
        ENABLE_IKNP_MULTITHREAD = AppConfig::getBool("enable_iknp_multithread", ENABLE_IKNP_MULTITHREAD);
    } catch (const std::exception &ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
    }
}
