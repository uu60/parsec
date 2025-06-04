//
// Created by 杜建璋 on 25-5-1.
//

#ifndef STRINGUTILS_H
#define STRINGUTILS_H
#include <cstdint>
#include <sstream>
#include <vector>


class StringUtils {
public:
    template<typename T>
    static std::string vecString(const std::vector<T> &vec) {
        std::ostringstream oss;
        oss << "[";
        for (size_t i = 0; i < vec.size(); ++i) {
            oss << vec[i];
            if (i != vec.size() - 1) oss << ", ";
        }
        oss << "]";
        return oss.str();
    }

    static bool hasPrefix(const std::string &str, const std::string &prefix);
};


#endif //STRINGUTILS_H
