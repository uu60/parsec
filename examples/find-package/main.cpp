// Compiles against the installed headers and links parsec::db; it does not start any ranks.
#include "parsec/db/View.h"
#include "parsec/utils/Math.h"

#include <iostream>

using namespace parsec;

int main() {
    std::cout << "ring(-1, 8) = " << Math::ring(-1, 8) << '\n';
    std::cout << "valid column: " << View::VALID_COL_NAME << '\n';
    return 0;
}
