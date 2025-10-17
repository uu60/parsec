#ifndef SQLPARSER_COLUMN_TYPE_H
#define SQLPARSER_COLUMN_TYPE_H

#include <ostream>

namespace hsql {
enum class DataType {
  UNKNOWN,
  BIGINT,
  BOOLEAN,
  CHAR,
  DATE,
  DATETIME,
  DECIMAL,
  DOUBLE,
  FLOAT,
  INT,
  LONG,
  REAL,
  SMALLINT,
  TEXT,
  TIME,
  VARCHAR,
};

struct ColumnType {
  ColumnType() = default;
  ColumnType(DataType data_type, int64_t length = 0, int64_t precision = 0, int64_t scale = 0);
  DataType data_type;
  int64_t length;
  int64_t precision;
  int64_t scale;
};

bool operator==(const ColumnType& lhs, const ColumnType& rhs);
bool operator!=(const ColumnType& lhs, const ColumnType& rhs);
std::ostream& operator<<(std::ostream&, const ColumnType&);

}

#endif
