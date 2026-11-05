







#ifndef YY_HSQL_BISON_PARSER_H_INCLUDED
# define YY_HSQL_BISON_PARSER_H_INCLUDED

#ifndef HSQL_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define HSQL_DEBUG 1
#  else
#   define HSQL_DEBUG 0
#  endif
# else 
#  define HSQL_DEBUG 0
# endif 
#endif  
#if HSQL_DEBUG
extern int hsql_debug;
#endif

#line 39 "bison_parser.y"


#include "../SQLParserResult.h"
#include "../sql/statements.h"
#include "parser_typedef.h"

#define YY_USER_ACTION                        \
  yylloc->first_line = yylloc->last_line;     \
  yylloc->first_column = yylloc->last_column; \
  for (int i = 0; yytext[i] != '\0'; i++) {   \
    yylloc->total_column++;                   \
    yylloc->string_length++;                  \
    if (yytext[i] == '\n') {                  \
      yylloc->last_line++;                    \
      yylloc->last_column = 0;                \
    } else {                                  \
      yylloc->last_column++;                  \
    }                                         \
  }

#line 80 "bison_parser.h"


#ifndef HSQL_TOKENTYPE
# define HSQL_TOKENTYPE
  enum hsql_tokentype
  {
    SQL_HSQL_EMPTY = -2,
    SQL_YYEOF = 0,                 
    SQL_HSQL_error = 256,          
    SQL_HSQL_UNDEF = 257,          
    SQL_IDENTIFIER = 258,          
    SQL_STRING = 259,              
    SQL_FLOATVAL = 260,            
    SQL_INTVAL = 261,              
    SQL_DEALLOCATE = 262,          
    SQL_PARAMETERS = 263,          
    SQL_INTERSECT = 264,           
    SQL_TEMPORARY = 265,           
    SQL_TIMESTAMP = 266,           
    SQL_DISTINCT = 267,            
    SQL_NVARCHAR = 268,            
    SQL_RESTRICT = 269,            
    SQL_TRUNCATE = 270,            
    SQL_ANALYZE = 271,             
    SQL_BETWEEN = 272,             
    SQL_CASCADE = 273,             
    SQL_COLUMNS = 274,             
    SQL_CONTROL = 275,             
    SQL_DEFAULT = 276,             
    SQL_EXECUTE = 277,             
    SQL_EXPLAIN = 278,             
    SQL_ENCODING = 279,            
    SQL_INTEGER = 280,             
    SQL_NATURAL = 281,             
    SQL_PREPARE = 282,             
    SQL_PRIMARY = 283,             
    SQL_SCHEMAS = 284,             
    SQL_CHARACTER_VARYING = 285,   
    SQL_REAL = 286,                
    SQL_DECIMAL = 287,             
    SQL_SMALLINT = 288,            
    SQL_BIGINT = 289,              
    SQL_SPATIAL = 290,             
    SQL_VARCHAR = 291,             
    SQL_VIRTUAL = 292,             
    SQL_DESCRIBE = 293,            
    SQL_BEFORE = 294,              
    SQL_COLUMN = 295,              
    SQL_CREATE = 296,              
    SQL_DELETE = 297,              
    SQL_DIRECT = 298,              
    SQL_DOUBLE = 299,              
    SQL_ESCAPE = 300,              
    SQL_EXCEPT = 301,              
    SQL_EXISTS = 302,              
    SQL_EXTRACT = 303,             
    SQL_CAST = 304,                
    SQL_FORMAT = 305,              
    SQL_GLOBAL = 306,              
    SQL_HAVING = 307,              
    SQL_IMPORT = 308,              
    SQL_INSERT = 309,              
    SQL_ISNULL = 310,              
    SQL_OFFSET = 311,              
    SQL_RENAME = 312,              
    SQL_SCHEMA = 313,              
    SQL_SELECT = 314,              
    SQL_SORTED = 315,              
    SQL_TABLES = 316,              
    SQL_UNIQUE = 317,              
    SQL_UNLOAD = 318,              
    SQL_UPDATE = 319,              
    SQL_VALUES = 320,              
    SQL_AFTER = 321,               
    SQL_ALTER = 322,               
    SQL_CROSS = 323,               
    SQL_DELTA = 324,               
    SQL_FLOAT = 325,               
    SQL_GROUP = 326,               
    SQL_INDEX = 327,               
    SQL_INNER = 328,               
    SQL_LIMIT = 329,               
    SQL_LOCAL = 330,               
    SQL_MERGE = 331,               
    SQL_MINUS = 332,               
    SQL_ORDER = 333,               
    SQL_OVER = 334,                
    SQL_OUTER = 335,               
    SQL_RIGHT = 336,               
    SQL_TABLE = 337,               
    SQL_UNION = 338,               
    SQL_USING = 339,               
    SQL_WHERE = 340,               
    SQL_CALL = 341,                
    SQL_CASE = 342,                
    SQL_CHAR = 343,                
    SQL_COPY = 344,                
    SQL_DATE = 345,                
    SQL_DATETIME = 346,            
    SQL_DESC = 347,                
    SQL_DROP = 348,                
    SQL_ELSE = 349,                
    SQL_FILE = 350,                
    SQL_FROM = 351,                
    SQL_FULL = 352,                
    SQL_HASH = 353,                
    SQL_HINT = 354,                
    SQL_INTO = 355,                
    SQL_JOIN = 356,                
    SQL_LEFT = 357,                
    SQL_LIKE = 358,                
    SQL_LOAD = 359,                
    SQL_LONG = 360,                
    SQL_NULL = 361,                
    SQL_PARTITION = 362,           
    SQL_PLAN = 363,                
    SQL_SHOW = 364,                
    SQL_TEXT = 365,                
    SQL_THEN = 366,                
    SQL_TIME = 367,                
    SQL_VIEW = 368,                
    SQL_WHEN = 369,                
    SQL_WITH = 370,                
    SQL_ADD = 371,                 
    SQL_ALL = 372,                 
    SQL_AND = 373,                 
    SQL_ASC = 374,                 
    SQL_END = 375,                 
    SQL_FOR = 376,                 
    SQL_INT = 377,                 
    SQL_KEY = 378,                 
    SQL_NOT = 379,                 
    SQL_OFF = 380,                 
    SQL_SET = 381,                 
    SQL_TOP = 382,                 
    SQL_AS = 383,                  
    SQL_BY = 384,                  
    SQL_IF = 385,                  
    SQL_IN = 386,                  
    SQL_IS = 387,                  
    SQL_OF = 388,                  
    SQL_ON = 389,                  
    SQL_OR = 390,                  
    SQL_TO = 391,                  
    SQL_NO = 392,                  
    SQL_ARRAY = 393,               
    SQL_CONCAT = 394,              
    SQL_ILIKE = 395,               
    SQL_SECOND = 396,              
    SQL_MINUTE = 397,              
    SQL_HOUR = 398,                
    SQL_DAY = 399,                 
    SQL_MONTH = 400,               
    SQL_YEAR = 401,                
    SQL_SECONDS = 402,             
    SQL_MINUTES = 403,             
    SQL_HOURS = 404,               
    SQL_DAYS = 405,                
    SQL_MONTHS = 406,              
    SQL_YEARS = 407,               
    SQL_INTERVAL = 408,            
    SQL_TRUE = 409,                
    SQL_FALSE = 410,               
    SQL_BOOLEAN = 411,             
    SQL_TRANSACTION = 412,         
    SQL_BEGIN = 413,               
    SQL_COMMIT = 414,              
    SQL_ROLLBACK = 415,            
    SQL_NOWAIT = 416,              
    SQL_SKIP = 417,                
    SQL_LOCKED = 418,              
    SQL_SHARE = 419,               
    SQL_RANGE = 420,               
    SQL_ROWS = 421,                
    SQL_GROUPS = 422,              
    SQL_UNBOUNDED = 423,           
    SQL_FOLLOWING = 424,           
    SQL_PRECEDING = 425,           
    SQL_CURRENT_ROW = 426,         
    SQL_EQUALS = 427,              
    SQL_NOTEQUALS = 428,           
    SQL_LESS = 429,                
    SQL_GREATER = 430,             
    SQL_LESSEQ = 431,              
    SQL_GREATEREQ = 432,           
    SQL_NOTNULL = 433,             
    SQL_UMINUS = 434               
  };
  typedef enum hsql_tokentype hsql_token_kind_t;
#endif


#if ! defined HSQL_STYPE && ! defined HSQL_STYPE_IS_DECLARED
union HSQL_STYPE
{
#line 97 "bison_parser.y"

  bool bval;
  char* sval;
  double fval;
  int64_t ival;
  uintmax_t uval;

  hsql::AlterStatement* alter_stmt;
  hsql::CreateStatement* create_stmt;
  hsql::DeleteStatement* delete_stmt;
  hsql::DropStatement* drop_stmt;
  hsql::ExecuteStatement* exec_stmt;
  hsql::ExportStatement* export_stmt;
  hsql::ImportStatement* import_stmt;
  hsql::InsertStatement* insert_stmt;
  hsql::PrepareStatement* prep_stmt;
  hsql::SelectStatement* select_stmt;
  hsql::ShowStatement* show_stmt;
  hsql::SQLStatement* statement;
  hsql::TransactionStatement* transaction_stmt;
  hsql::UpdateStatement* update_stmt;

  hsql::Alias* alias_t;
  hsql::AlterAction* alter_action_t;
  hsql::ColumnDefinition* column_t;
  hsql::ColumnType column_type_t;
  hsql::ConstraintType column_constraint_t;
  hsql::DatetimeField datetime_field;
  hsql::DropColumnAction* drop_action_t;
  hsql::Expr* expr;
  hsql::FrameBound* frame_bound;
  hsql::FrameDescription* frame_description;
  hsql::FrameType frame_type;
  hsql::GroupByDescription* group_t;
  hsql::ImportType import_type_t;
  hsql::JoinType join_type;
  hsql::LimitDescription* limit;
  hsql::LockingClause* locking_t;
  hsql::OrderDescription* order;
  hsql::OrderType order_type;
  hsql::SetOperation* set_operator_t;
  hsql::TableConstraint* table_constraint_t;
  hsql::TableElement* table_element_t;
  hsql::TableName table_name;
  hsql::TableRef* table;
  hsql::UpdateClause* update_t;
  hsql::WindowDescription* window_description;
  hsql::WithDescription* with_description_t;

  std::vector<char*>* str_vec;
  std::unordered_set<hsql::ConstraintType>* column_constraint_set;
  std::vector<hsql::Expr*>* expr_vec;
  std::vector<hsql::OrderDescription*>* order_vec;
  std::vector<hsql::SQLStatement*>* stmt_vec;
  std::vector<hsql::TableElement*>* table_element_vec;
  std::vector<hsql::TableRef*>* table_vec;
  std::vector<hsql::UpdateClause*>* update_vec;
  std::vector<hsql::WithDescription*>* with_description_vec;
  std::vector<hsql::LockingClause*>* locking_clause_vec;

  std::pair<int64_t, int64_t>* ival_pair;

  hsql::RowLockMode lock_mode_t;
  hsql::RowLockWaitPolicy lock_wait_policy_t;

  hsql::ImportExportOptions* import_export_option_t;


#line 348 "bison_parser.h"

};
typedef union HSQL_STYPE HSQL_STYPE;
# define HSQL_STYPE_IS_TRIVIAL 1
# define HSQL_STYPE_IS_DECLARED 1
#endif


#if ! defined HSQL_LTYPE && ! defined HSQL_LTYPE_IS_DECLARED
typedef struct HSQL_LTYPE HSQL_LTYPE;
struct HSQL_LTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define HSQL_LTYPE_IS_DECLARED 1
# define HSQL_LTYPE_IS_TRIVIAL 1
#endif




int hsql_parse (hsql::SQLParserResult* result, yyscan_t scanner);


#endif 
