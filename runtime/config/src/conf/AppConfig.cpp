#include "conf/AppConfig.h"

#include <algorithm>
#include <cctype>
#include <iostream>
#include <stdexcept>

namespace {
bool parseBool(const std::string &value) {
    std::string normalized = value;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });

    if (normalized == "true" || normalized == "1" || normalized == "yes" || normalized == "on") {
        return true;
    }
    if (normalized == "false" || normalized == "0" || normalized == "no" || normalized == "off") {
        return false;
    }
    throw std::runtime_error("Invalid bool value: " + value);
}
}

void AppConfig::init(int argc, char **argv) {
    _params.clear();
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--", 0) != 0) {
            throw std::runtime_error("Wrong arg format: " + arg);
        }

        std::string body = arg.substr(2);
        auto pos = body.find('=');
        if (pos == std::string::npos) {
            _params[body] = "true";
        } else {
            _params[body.substr(0, pos)] = body.substr(pos + 1);
        }
    }
}

bool AppConfig::has(const std::string &name) {
    return _params.count(name) != 0;
}

std::string AppConfig::getString(const std::string &name, const std::string &defaultValue) {
    auto it = _params.find(name);
    if (it == _params.end()) {
        return defaultValue;
    }
    return it->second;
}

int AppConfig::getInt(const std::string &name, int defaultValue) {
    auto it = _params.find(name);
    if (it == _params.end()) {
        return defaultValue;
    }
    return std::stoi(it->second);
}

bool AppConfig::getBool(const std::string &name, bool defaultValue) {
    auto it = _params.find(name);
    if (it == _params.end()) {
        return defaultValue;
    }
    return parseBool(it->second);
}

const std::map<std::string, std::string> &AppConfig::params() {
    return _params;
}
