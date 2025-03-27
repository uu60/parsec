//
// Created by 杜建璋 on 2025/3/26.
//

#include "conf/Conf.h"

#include <iostream>

void Conf::init(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-bmt_method" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "bmt_background") {
                BMT_METHOD = BMT_BACKGROUND;
            } else if (value == "bmt_jit") {
                BMT_METHOD = BMT_JIT;
            } else if (value == "bmt_fixed") {
                BMT_METHOD = BMT_FIXED;
            } else if (value == "bmt_batch_background") {
                BMT_METHOD = BMT_BATCH_BACKGROUND;
            } else {
                throw std::runtime_error("Unknown bmt_method value.");
            }
        } else if (arg == "-max_bmts" && i + 1 < argc) {
            MAX_BMTS = std::stoi(argv[++i]);
        } else if (arg == "-bmt_usage_limit" && i + 1 < argc) {
            BMT_USAGE_LIMIT = std::stoi(argv[++i]);
        } else if (arg == "-bmt_queue_type" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "cas_queue") {
                BMT_QUEUE_TYPE = CAS_QUEUE;
            } else if (value == "lock_queue") {
                BMT_QUEUE_TYPE = LOCK_QUEUE;
            } else {
                throw std::runtime_error("Unknown queue_type value.");
            }
        } else if (arg == "-task_tag_bits" && i + 1 < argc) {
            TASK_TAG_BITS = std::stoi(argv[++i]);
        } else if (arg == "-disable_multi_thread" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "true") {
                DISABLE_MULTI_THREAD = true;
            } else if (value == "false") {
                DISABLE_MULTI_THREAD = false;
            } else {
                throw std::runtime_error("Unknown disable_multi_thread value.");
            }
        } else if (arg == "-intra_operator_parallelism" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "true") {
                INTRA_OPERATOR_PARALLELISM = true;
            } else if (value == "false") {
                INTRA_OPERATOR_PARALLELISM = false;
            } else {
                throw std::runtime_error("Unknown intra_operator_parallelism value.");
            }
        } else if (arg == "-local_threads" && i + 1 < argc) {
            LOCAL_THREADS = std::stoi(argv[++i]);
        } else if (arg == "-thread_pool" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "ctpl_pool") {
                THREAD_POOL_TYPE = CTPL_POOL;
            } else if (value == "tbb_pool") {
                THREAD_POOL_TYPE = TBB_POOL;
            } else {
                throw std::runtime_error("Unknown thread_pool value.");
            }
        } else if (arg == "-comm_type" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "mpi") {
                COMM_TYPE = MPI;
            } else {
                throw std::runtime_error("Unknown comm_type value.");
            }
        } else if (arg == "-enable_task_batching" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "true") {
                ENABLE_TASK_BATCHING = true;
            } else if (value == "false") {
                ENABLE_TASK_BATCHING = false;
            } else {
                throw std::runtime_error("Unknown enable_task_batching value.");
            }
        } else if (arg == "-batch_size" && i + 1 < argc) {
            BATCH_SIZE = std::stoi(argv[++i]);
        } else if (arg == "-enable_transfer_compression" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "true") {
                ENABLE_TRANSFER_COMPRESSION = true;
            } else if (value == "false") {
                ENABLE_TRANSFER_COMPRESSION = false;
            } else {
                throw std::runtime_error("Unknown enable_transfer_compression value.");
            }
        } else if (arg == "-enable_class_wise_timing" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "true") {
                ENABLE_CLASS_WISE_TIMING = true;
            } else if (value == "false") {
                ENABLE_CLASS_WISE_TIMING = false;
            } else {
                throw std::runtime_error("Unknown enable_class_wise_timing value.");
            }
        } else if (arg == "-sort_in_parallel" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "true") {
                SORT_IN_PARALLEL = true;
            } else if (value == "false") {
                SORT_IN_PARALLEL = false;
            } else {
                throw std::runtime_error("Unknown sort_in_parallel value.");
            }
        } else if (arg == "-max_sorting_threads" && i + 1 < argc) {
            MAX_SORTING_THREADS = std::stoi(argv[++i]);
        } else if (arg == "-enable_simd" && i + 1 < argc) {
            std::string value = argv[++i];
            if (value == "true") {
                ENABLE_SIMD = true;
            } else if (value == "false") {
                ENABLE_SIMD = false;
            } else {
                throw std::runtime_error("Unknown enable_simd value.");
            }
        }
    }
}
