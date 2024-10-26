//
// Created by 杜建璋 on 2024/7/12.
//

#include <iostream>
#include "utils/Log.h"
#include "utils/Comm.h"
#include <iomanip>
#include <sstream>
#include <unistd.h>
// required
#include <chrono>

const std::string Log::INFO = "INFO";
const std::string Log::DEBUG = "DEBUG";
const std::string Log::WARN = "WARN";
const std::string Log::ERROR = "ERROR";
std::string Log::HOSTNAME;

void Log::i(const std::string& msg) {
    print(INFO, msg);
}

void Log::d(const std::string& msg) {
    print(DEBUG, msg);
}

void Log::w(const std::string& msg) {
    print(WARN, msg);
}

void Log::e(const std::string& msg) {
    print(ERROR, msg);
}

std::string Log::getCurrentTimestampStr() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm now_tm = *std::localtime(&now_time_t);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream oss;
    oss << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S");
    oss << '.' << std::setfill('0') << std::setw(3) << now_ms.count();
    return oss.str();
}

void Log::print(const std::string& level, const std::string& msg) {
    if (HOSTNAME.empty()) {
        char hostName[128];
        gethostname(hostName, sizeof(hostName));
        HOSTNAME.append(std::string(hostName));
    }

    std::cout << "[" << HOSTNAME + ":" + std::to_string(getpid()) << "] "
              << "[" << getCurrentTimestampStr() << "] "
              << "[" + std::to_string(Comm::rank()) + "] "
              << "[" << std::setw(5) << level << "] "
              << msg << std::endl;
}

void Log::print(const std::string& level, const std::string& tag, const std::string& msg) {
    print(level, tag + " " + msg);
}

void Log::i(const std::string &tag, const std::string &msg) {
    print(INFO, tag, msg);
}

void Log::d(const std::string &tag, const std::string &msg) {
    print(DEBUG, tag, msg);
}

void Log::w(const std::string &tag, const std::string &msg) {
    print(WARN, tag, msg);
}

void Log::e(const std::string &tag, const std::string &msg) {
    print(ERROR, tag, msg);
}
