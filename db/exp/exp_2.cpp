//
// Created by 杜建璋 on 25-8-14.
//

#include "secret/Secrets.h"
#include "utils/System.h"

#include "../include/basis/View.h"
#include "../include/operator/SelectSupport.h"
#include "basis/Views.h"
#include "utils/Log.h"
#include "utils/StringUtils.h"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <set>

#include "utils/Math.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "compute/batch/arith/ArithAddBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "compute/batch/arith/ArithEqualBatchOperator.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"


/**
 * WITH rcd AS (
 *     SELECT pid, time, row_no
 *     FROM diagnosis WHERE diag=cdiff
 * )
 * SELECT DISTINCT pid
 * FROM rcd r1 JOIN rcd r2
 * ON r1.pid = r2.pid
 * WHERE r2.time - r1.time >= 15 DAYS
 *     AND r2.time - r1.time <= 56 DAYS
 *     AND r2.row_no = r1.row_no + 1
 */
int main(int argc, char *argv[]) {

}
