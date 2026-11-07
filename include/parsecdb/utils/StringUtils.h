
#ifndef STRINGUTILS_H
#define STRINGUTILS_H
#include <cstdint>
#include <sstream>
#include <vector>

#include <string>

namespace parsecdb {

class StringUtils {
  public:
    template <typename T> static std::string vecToString(const std::vector<T>& vec) {
        std::ostringstream oss;
        oss << "[";
        for (size_t i = 0; i < vec.size(); ++i) {
            oss << vec[i];
            if (i != vec.size() - 1)
                oss << ", ";
        }
        oss << "]";
        return oss.str();
    }

    static bool hasPrefix(const std::string& str, const std::string& prefix);
};

} // namespace parsecdb

#endif
