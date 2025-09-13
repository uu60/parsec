#ifndef MPC_PACKAGE_LOG_H
#define MPC_PACKAGE_LOG_H

#include <string>
#include <sstream>
#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <unistd.h>
#include <mutex>
#include "../comm/Comm.h"

class Log {
private:
    static inline const std::string INFO = "INFO";
    static inline const std::string DEBUG = "DEBUG";
    static inline const std::string WARN = "WARN";
    static inline const std::string ERROR = "ERROR";
    static inline std::string HOSTNAME = "";
    static inline std::mutex logMutex;

public:
    // Variadic template functions for logging
    template<typename... Args>
    static void i(const std::string &msg, Args &&... args) {
        log(INFO, msg, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void d(const std::string &msg, Args &&... args) {
        log(DEBUG, msg, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void w(const std::string &msg, Args &&... args) {
        log(WARN, msg, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void e(const std::string &msg, Args &&... args) {
        log(ERROR, msg, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void ir(int rank, const std::string &msg, Args &&... args) {
        if (Comm::rank() != rank) return;
        log(INFO, msg, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void dr(int rank, const std::string &msg, Args &&... args) {
        if (Comm::rank() != rank) return;
        log(DEBUG, msg, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void wr(int rank, const std::string &msg, Args &&... args) {
        if (Comm::rank() != rank) return;
        log(WARN, msg, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void er(int rank, const std::string &msg, Args &&... args) {
        if (Comm::rank() != rank) return;
        log(ERROR, msg, std::forward<Args>(args)...);
    }

private:
    template<typename... Args>
    static void
    log(const std::string &level, const std::string &msg, Args &&... args) {
        std::lock_guard<std::mutex> lock(logMutex);
        if (HOSTNAME.empty()) {
            char hostName[128];
            gethostname(hostName, sizeof(hostName));
            HOSTNAME = hostName;
        }
        std::string formatted_msg = format(msg, std::forward<Args>(args)...);
        std::cout << "[" << HOSTNAME + ":" + std::to_string(getpid()) << "] "
                << "[" << getCurrentTimestampStr() << "] "
                << "[" << Comm::rank() << "] "
                << "[" << level << "] "
                << formatted_msg << std::endl;
    }

    template<typename T>
    static std::string to_string_any(T &&value) {
        std::ostringstream oss;
        oss << value;
        return oss.str();
    }

    template<typename... Args>
    static std::string format(const std::string &msg, Args &&... args) {
        std::string formatted_msg = msg;
        std::vector<std::string> arg_strings = {to_string_any(std::forward<Args>(args))...};
        for (const auto &arg: arg_strings) {
            replaceFirstPlaceholder(formatted_msg, arg);
        }
        return formatted_msg;
    }

    static void replaceFirstPlaceholder(std::string &msg, const std::string &value) {
        size_t pos = msg.find("{}");
        if (pos != std::string::npos) {
            msg.replace(pos, 2, value);
        }
    }

    static std::string getCurrentTimestampStr() {
        auto now = std::chrono::system_clock::now();
        std::time_t now_time_t = std::chrono::system_clock::to_time_t(now);
        std::tm now_tm = *std::localtime(&now_time_t);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        std::ostringstream oss;
        oss << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S");
        oss << '.' << std::setfill('0') << std::setw(3) << now_ms.count();
        return oss.str();
    }
};

#endif // MPC_PACKAGE_LOG_H
