//
// Created by 杜建璋 on 25-5-1.
//

#include "../../include/utils/StringUtils.h"

#include <sstream>

std::string StringUtils::toString(const std::vector<int64_t> &vec) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        oss << vec[i];
        if (i != vec.size() - 1) oss << ", ";
    }
    oss << "]";
    return oss.str();
}


