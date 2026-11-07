
#include "parsec/utils/StringUtils.h"

#include <sstream>

#include <string>

namespace parsec {

bool StringUtils::hasPrefix(const std::string& str, const std::string& prefix) {
    return str.size() >= prefix.size() && str.compare(0, prefix.size(), prefix) == 0;
}

} // namespace parsec
