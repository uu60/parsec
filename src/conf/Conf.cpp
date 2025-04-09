//
// Created by 杜建璋 on 2025/3/26.
//

#include "conf/Conf.h"
#include <boost/program_options.hpp>
#include <iostream>
namespace po = boost::program_options;

void Conf::init(int argc, char **argv) {
    try {
        po::options_description desc("Allowed options");
        std::string bmt_method;
        std::string bmt_queue_type;
        std::string thread_pool;
        std::string comm_type;

        desc.add_options()
                ("help", "Display help message")
                ("bmt_method", po::value<std::string>(&bmt_method)->default_value("bmt_background"),
                 "Set bmt_method (bmt_background, bmt_jit, bmt_fixed, bmt_batch_background)")
                ("max_bmts", po::value<int>(&MAX_BMTS)->default_value(5),
                 "Set max_bmts")
                ("bmt_usage_limit", po::value<int>(&BMT_USAGE_LIMIT)->default_value(10),
                 "Set bmt_usage_limit")
                ("bmt_queue_type", po::value<std::string>(&bmt_queue_type)->default_value("spsc_queue"),
                 "Set bmt_queue_type (cas_queue, lock_queue, spsc_queue)")
                ("bmt_queue_num", po::value<int>(&BMT_QUEUE_NUM)->default_value(1), "Set bmt_queue_num")
                ("disable_arith", po::value<bool>(&DISABLE_ARITH)->default_value(true), "Set disable_arith")
                ("bmt_gen_batch_size", po::value<int>(&BMT_GEN_BATCH_SIZE)->default_value(100000),
                 "Set bmt_gen_batch_size")
                ("task_tag_bits", po::value<int>(&TASK_TAG_BITS)->default_value(32),
                 "Set task_tag_bits")
                ("disable_multi_thread", po::value<bool>(&DISABLE_MULTI_THREAD)->default_value(false),
                 "Set disable_multi_thread (true/false)")
                ("enable_intra_operator_parallelism",
                 po::value<bool>(&ENABLE_INTRA_OPERATOR_PARALLELISM)->default_value(false),
                 "Set intra_operator_parallelism (true/false)")
                ("local_threads", po::value<int>(&LOCAL_THREADS)->default_value(4),
                 "Set local_threads")
                ("thread_pool", po::value<std::string>(&thread_pool)->default_value("async_pool"),
                 "Set thread_pool (ctpl_pool, tbb_pool)")
                ("comm_type", po::value<std::string>(&comm_type)->default_value("mpi"),
                 "Set comm_type (mpi)")
                ("batch_size", po::value<int>(&BATCH_SIZE)->default_value(64),
                 "Set batch_size")
                ("enable_transfer_compression", po::value<bool>(&ENABLE_TRANSFER_COMPRESSION)->default_value(false),
                 "Set enable_transfer_compression (true/false)")
                ("enable_class_wise_timing", po::value<bool>(&ENABLE_CLASS_WISE_TIMING)->default_value(false),
                 "Set enable_class_wise_timing (true/false)")
                ("enable_simd", po::value<bool>(&ENABLE_SIMD)->default_value(true),
                 "Set enable_simd (true/false)");

        po::parsed_options parsed = po::command_line_parser(argc, argv)
                .options(desc)
                .allow_unregistered()
                .run();
        po::variables_map vm;
        po::store(parsed, vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << desc << "\n";
            std::exit(0);
        }

        if (vm.count("bmt_method")) {
            if (bmt_method == "bmt_background") {
                BMT_METHOD = BMT_BACKGROUND;
            } else if (bmt_method == "bmt_jit") {
                BMT_METHOD = BMT_JIT;
            } else if (bmt_method == "bmt_fixed") {
                BMT_METHOD = BMT_FIXED;
            } else {
                throw std::runtime_error("Unknown bmt_method value.");
            }
        }

        if (vm.count("bmt_queue_type")) {
            if (bmt_queue_type == "cas_queue") {
                BMT_QUEUE_TYPE = LOCK_FREE_QUEUE;
            } else if (bmt_queue_type == "lock_queue") {
                BMT_QUEUE_TYPE = LOCK_QUEUE;
            } else if (bmt_queue_type == "spsc_queue") {
                BMT_QUEUE_TYPE = SPSC_QUEUE;
            } else {
                throw std::runtime_error("Unknown bmt_queue_type value.");
            }
        }

        if (vm.count("thread_pool")) {
            if (thread_pool == "ctpl_pool") {
                THREAD_POOL_TYPE = CTPL_POOL;
            } else if (thread_pool == "tbb_pool") {
                THREAD_POOL_TYPE = TBB_POOL;
            } else if (thread_pool == "async_pool") {
                THREAD_POOL_TYPE = ASYNC_POOL;
            } else {
                throw std::runtime_error("Unknown thread_pool value.");
            }
        }

        if (vm.count("comm_type")) {
            if (comm_type == "mpi") {
                COMM_TYPE = MPI;
            } else {
                throw std::runtime_error("Unknown comm_type value.");
            }
        }

        std::vector<std::string> extra_args = po::collect_unrecognized(parsed.options, po::include_positional);
        if (!extra_args.empty()) {
            std::cout << "Warning: unrecognized params:";
            for (const auto &arg: extra_args) {
                std::cout << " " << arg;
            }
            std::cout << std::endl;
        }
    } catch (const std::exception &ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
    }
}
