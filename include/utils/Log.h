//
// Created by 杜建璋 on 2024/7/12.
//

#ifndef MPC_PACKAGE_LOG_H
#define MPC_PACKAGE_LOG_H
#include <string>

class Log {
private:
    static const std::string INFO;
    static const std::string DEBUG;
    static const std::string WARN;
    static const std::string ERROR;
    static std::string HOSTNAME;
public:
    // info
    static void i(const std::string& msg);
    static void i(const std::string& tag, const std::string& msg);
    // debug
    static void d(const std::string& msg);
    static void d(const std::string& tag, const std::string& msg);
    // warn
    static void w(const std::string& msg);
    static void w(const std::string& tag, const std::string& msg);
    // error
    static void e(const std::string& msg);
    static void e(const std::string& tag, const std::string& msg);
private:
    static std::string getCurrentTimestampStr();
    static void print(const std::string& level, const std::string& msg);
    static void print(const std::string& level, const std::string& tag, const std::string& msg);
};


#endif //MPC_PACKAGE_LOG_H
