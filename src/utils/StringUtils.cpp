//
// Created by 杜建璋 on 25-5-1.
//

#include "../../include/utils/StringUtils.h"

#include <sstream>


bool StringUtils::hasPrefix(const std::string &str, const std::string &prefix)  {
    return str.size() >= prefix.size()
           && str.compare(0, prefix.size(), prefix) == 0;
}
