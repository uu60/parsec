// Compiles against the installed headers and links parsecdb::db; it does not start any ranks.
#include "parsecdb/db/View.h"
#include "parsecdb/utils/Math.h"

#include <iostream>

using namespace parsecdb;

int main() {
    std::cout << "ring(-1, 8) = " << Math::ring(-1, 8) << '\n';
    std::cout << "valid column: " << View::VALID_COL_NAME << '\n';
    return 0;
}
