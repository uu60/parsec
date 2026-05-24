#ifndef APPCONFIG_H
#define APPCONFIG_H

#include <map>
#include <string>

class AppConfig {
private:
    inline static std::map<std::string, std::string> _params{};

public:
    static void init(int argc, char **argv);

    static bool has(const std::string &name);

    static std::string getString(const std::string &name, const std::string &defaultValue);

    static int getInt(const std::string &name, int defaultValue);

    static bool getBool(const std::string &name, bool defaultValue);

    static const std::map<std::string, std::string> &params();
};

#endif
