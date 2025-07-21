/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Substitute the type names.  */
#define YYSTYPE         HSQL_STYPE
#define YYLTYPE         HSQL_LTYPE
/* Substitute the variable and function names.  */
#define yyparse         hsql_parse
#define yylex           hsql_lex
#define yyerror         hsql_error
#define yydebug         hsql_debug
#define yynerrs         hsql_nerrs

/* First part of user prologue.  */
#line 2 "bison_parser.y"


/**
 * bison_parser.y
 * defines bison_parser.h
 * outputs bison_parser.c
 *
 * Grammar File Spec: http://dinosaur.compilertools.net/bison/bison_6.html
 *
 */
/*********************************
 ** Section 1: C Declarations
 *********************************/

// clang-format on
#include "bison_parser.h"
#include "flex_lexer.h"

#include <stdio.h>
#include <string.h>

  using namespace hsql;

  int yyerror(YYLTYPE * llocp, SQLParserResult * result, yyscan_t scanner, const char* msg) {
    result->setIsValid(false);
    result->setErrorDetails(strdup(msg), llocp->first_line, llocp->first_column);
    return 0;
  }
  // clang-format off

#line 109 "bison_parser.cpp"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "bison_parser.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_IDENTIFIER = 3,                 /* IDENTIFIER  */
  YYSYMBOL_STRING = 4,                     /* STRING  */
  YYSYMBOL_FLOATVAL = 5,                   /* FLOATVAL  */
  YYSYMBOL_INTVAL = 6,                     /* INTVAL  */
  YYSYMBOL_DEALLOCATE = 7,                 /* DEALLOCATE  */
  YYSYMBOL_PARAMETERS = 8,                 /* PARAMETERS  */
  YYSYMBOL_INTERSECT = 9,                  /* INTERSECT  */
  YYSYMBOL_TEMPORARY = 10,                 /* TEMPORARY  */
  YYSYMBOL_TIMESTAMP = 11,                 /* TIMESTAMP  */
  YYSYMBOL_DISTINCT = 12,                  /* DISTINCT  */
  YYSYMBOL_NVARCHAR = 13,                  /* NVARCHAR  */
  YYSYMBOL_RESTRICT = 14,                  /* RESTRICT  */
  YYSYMBOL_TRUNCATE = 15,                  /* TRUNCATE  */
  YYSYMBOL_ANALYZE = 16,                   /* ANALYZE  */
  YYSYMBOL_BETWEEN = 17,                   /* BETWEEN  */
  YYSYMBOL_CASCADE = 18,                   /* CASCADE  */
  YYSYMBOL_COLUMNS = 19,                   /* COLUMNS  */
  YYSYMBOL_CONTROL = 20,                   /* CONTROL  */
  YYSYMBOL_DEFAULT = 21,                   /* DEFAULT  */
  YYSYMBOL_EXECUTE = 22,                   /* EXECUTE  */
  YYSYMBOL_EXPLAIN = 23,                   /* EXPLAIN  */
  YYSYMBOL_ENCODING = 24,                  /* ENCODING  */
  YYSYMBOL_INTEGER = 25,                   /* INTEGER  */
  YYSYMBOL_NATURAL = 26,                   /* NATURAL  */
  YYSYMBOL_PREPARE = 27,                   /* PREPARE  */
  YYSYMBOL_PRIMARY = 28,                   /* PRIMARY  */
  YYSYMBOL_SCHEMAS = 29,                   /* SCHEMAS  */
  YYSYMBOL_CHARACTER_VARYING = 30,         /* CHARACTER_VARYING  */
  YYSYMBOL_REAL = 31,                      /* REAL  */
  YYSYMBOL_DECIMAL = 32,                   /* DECIMAL  */
  YYSYMBOL_SMALLINT = 33,                  /* SMALLINT  */
  YYSYMBOL_BIGINT = 34,                    /* BIGINT  */
  YYSYMBOL_SPATIAL = 35,                   /* SPATIAL  */
  YYSYMBOL_VARCHAR = 36,                   /* VARCHAR  */
  YYSYMBOL_VIRTUAL = 37,                   /* VIRTUAL  */
  YYSYMBOL_DESCRIBE = 38,                  /* DESCRIBE  */
  YYSYMBOL_BEFORE = 39,                    /* BEFORE  */
  YYSYMBOL_COLUMN = 40,                    /* COLUMN  */
  YYSYMBOL_CREATE = 41,                    /* CREATE  */
  YYSYMBOL_DELETE = 42,                    /* DELETE  */
  YYSYMBOL_DIRECT = 43,                    /* DIRECT  */
  YYSYMBOL_DOUBLE = 44,                    /* DOUBLE  */
  YYSYMBOL_ESCAPE = 45,                    /* ESCAPE  */
  YYSYMBOL_EXCEPT = 46,                    /* EXCEPT  */
  YYSYMBOL_EXISTS = 47,                    /* EXISTS  */
  YYSYMBOL_EXTRACT = 48,                   /* EXTRACT  */
  YYSYMBOL_CAST = 49,                      /* CAST  */
  YYSYMBOL_FORMAT = 50,                    /* FORMAT  */
  YYSYMBOL_GLOBAL = 51,                    /* GLOBAL  */
  YYSYMBOL_HAVING = 52,                    /* HAVING  */
  YYSYMBOL_IMPORT = 53,                    /* IMPORT  */
  YYSYMBOL_INSERT = 54,                    /* INSERT  */
  YYSYMBOL_ISNULL = 55,                    /* ISNULL  */
  YYSYMBOL_OFFSET = 56,                    /* OFFSET  */
  YYSYMBOL_RENAME = 57,                    /* RENAME  */
  YYSYMBOL_SCHEMA = 58,                    /* SCHEMA  */
  YYSYMBOL_SELECT = 59,                    /* SELECT  */
  YYSYMBOL_SORTED = 60,                    /* SORTED  */
  YYSYMBOL_TABLES = 61,                    /* TABLES  */
  YYSYMBOL_UNIQUE = 62,                    /* UNIQUE  */
  YYSYMBOL_UNLOAD = 63,                    /* UNLOAD  */
  YYSYMBOL_UPDATE = 64,                    /* UPDATE  */
  YYSYMBOL_VALUES = 65,                    /* VALUES  */
  YYSYMBOL_AFTER = 66,                     /* AFTER  */
  YYSYMBOL_ALTER = 67,                     /* ALTER  */
  YYSYMBOL_CROSS = 68,                     /* CROSS  */
  YYSYMBOL_DELTA = 69,                     /* DELTA  */
  YYSYMBOL_FLOAT = 70,                     /* FLOAT  */
  YYSYMBOL_GROUP = 71,                     /* GROUP  */
  YYSYMBOL_INDEX = 72,                     /* INDEX  */
  YYSYMBOL_INNER = 73,                     /* INNER  */
  YYSYMBOL_LIMIT = 74,                     /* LIMIT  */
  YYSYMBOL_LOCAL = 75,                     /* LOCAL  */
  YYSYMBOL_MERGE = 76,                     /* MERGE  */
  YYSYMBOL_MINUS = 77,                     /* MINUS  */
  YYSYMBOL_ORDER = 78,                     /* ORDER  */
  YYSYMBOL_OVER = 79,                      /* OVER  */
  YYSYMBOL_OUTER = 80,                     /* OUTER  */
  YYSYMBOL_RIGHT = 81,                     /* RIGHT  */
  YYSYMBOL_TABLE = 82,                     /* TABLE  */
  YYSYMBOL_UNION = 83,                     /* UNION  */
  YYSYMBOL_USING = 84,                     /* USING  */
  YYSYMBOL_WHERE = 85,                     /* WHERE  */
  YYSYMBOL_CALL = 86,                      /* CALL  */
  YYSYMBOL_CASE = 87,                      /* CASE  */
  YYSYMBOL_CHAR = 88,                      /* CHAR  */
  YYSYMBOL_COPY = 89,                      /* COPY  */
  YYSYMBOL_DATE = 90,                      /* DATE  */
  YYSYMBOL_DATETIME = 91,                  /* DATETIME  */
  YYSYMBOL_DESC = 92,                      /* DESC  */
  YYSYMBOL_DROP = 93,                      /* DROP  */
  YYSYMBOL_ELSE = 94,                      /* ELSE  */
  YYSYMBOL_FILE = 95,                      /* FILE  */
  YYSYMBOL_FROM = 96,                      /* FROM  */
  YYSYMBOL_FULL = 97,                      /* FULL  */
  YYSYMBOL_HASH = 98,                      /* HASH  */
  YYSYMBOL_HINT = 99,                      /* HINT  */
  YYSYMBOL_INTO = 100,                     /* INTO  */
  YYSYMBOL_JOIN = 101,                     /* JOIN  */
  YYSYMBOL_LEFT = 102,                     /* LEFT  */
  YYSYMBOL_LIKE = 103,                     /* LIKE  */
  YYSYMBOL_LOAD = 104,                     /* LOAD  */
  YYSYMBOL_LONG = 105,                     /* LONG  */
  YYSYMBOL_NULL = 106,                     /* NULL  */
  YYSYMBOL_PARTITION = 107,                /* PARTITION  */
  YYSYMBOL_PLAN = 108,                     /* PLAN  */
  YYSYMBOL_SHOW = 109,                     /* SHOW  */
  YYSYMBOL_TEXT = 110,                     /* TEXT  */
  YYSYMBOL_THEN = 111,                     /* THEN  */
  YYSYMBOL_TIME = 112,                     /* TIME  */
  YYSYMBOL_VIEW = 113,                     /* VIEW  */
  YYSYMBOL_WHEN = 114,                     /* WHEN  */
  YYSYMBOL_WITH = 115,                     /* WITH  */
  YYSYMBOL_ADD = 116,                      /* ADD  */
  YYSYMBOL_ALL = 117,                      /* ALL  */
  YYSYMBOL_AND = 118,                      /* AND  */
  YYSYMBOL_ASC = 119,                      /* ASC  */
  YYSYMBOL_END = 120,                      /* END  */
  YYSYMBOL_FOR = 121,                      /* FOR  */
  YYSYMBOL_INT = 122,                      /* INT  */
  YYSYMBOL_KEY = 123,                      /* KEY  */
  YYSYMBOL_NOT = 124,                      /* NOT  */
  YYSYMBOL_OFF = 125,                      /* OFF  */
  YYSYMBOL_SET = 126,                      /* SET  */
  YYSYMBOL_TOP = 127,                      /* TOP  */
  YYSYMBOL_AS = 128,                       /* AS  */
  YYSYMBOL_BY = 129,                       /* BY  */
  YYSYMBOL_IF = 130,                       /* IF  */
  YYSYMBOL_IN = 131,                       /* IN  */
  YYSYMBOL_IS = 132,                       /* IS  */
  YYSYMBOL_OF = 133,                       /* OF  */
  YYSYMBOL_ON = 134,                       /* ON  */
  YYSYMBOL_OR = 135,                       /* OR  */
  YYSYMBOL_TO = 136,                       /* TO  */
  YYSYMBOL_NO = 137,                       /* NO  */
  YYSYMBOL_ARRAY = 138,                    /* ARRAY  */
  YYSYMBOL_CONCAT = 139,                   /* CONCAT  */
  YYSYMBOL_ILIKE = 140,                    /* ILIKE  */
  YYSYMBOL_SECOND = 141,                   /* SECOND  */
  YYSYMBOL_MINUTE = 142,                   /* MINUTE  */
  YYSYMBOL_HOUR = 143,                     /* HOUR  */
  YYSYMBOL_DAY = 144,                      /* DAY  */
  YYSYMBOL_MONTH = 145,                    /* MONTH  */
  YYSYMBOL_YEAR = 146,                     /* YEAR  */
  YYSYMBOL_SECONDS = 147,                  /* SECONDS  */
  YYSYMBOL_MINUTES = 148,                  /* MINUTES  */
  YYSYMBOL_HOURS = 149,                    /* HOURS  */
  YYSYMBOL_DAYS = 150,                     /* DAYS  */
  YYSYMBOL_MONTHS = 151,                   /* MONTHS  */
  YYSYMBOL_YEARS = 152,                    /* YEARS  */
  YYSYMBOL_INTERVAL = 153,                 /* INTERVAL  */
  YYSYMBOL_TRUE = 154,                     /* TRUE  */
  YYSYMBOL_FALSE = 155,                    /* FALSE  */
  YYSYMBOL_BOOLEAN = 156,                  /* BOOLEAN  */
  YYSYMBOL_TRANSACTION = 157,              /* TRANSACTION  */
  YYSYMBOL_BEGIN = 158,                    /* BEGIN  */
  YYSYMBOL_COMMIT = 159,                   /* COMMIT  */
  YYSYMBOL_ROLLBACK = 160,                 /* ROLLBACK  */
  YYSYMBOL_NOWAIT = 161,                   /* NOWAIT  */
  YYSYMBOL_SKIP = 162,                     /* SKIP  */
  YYSYMBOL_LOCKED = 163,                   /* LOCKED  */
  YYSYMBOL_SHARE = 164,                    /* SHARE  */
  YYSYMBOL_RANGE = 165,                    /* RANGE  */
  YYSYMBOL_ROWS = 166,                     /* ROWS  */
  YYSYMBOL_GROUPS = 167,                   /* GROUPS  */
  YYSYMBOL_UNBOUNDED = 168,                /* UNBOUNDED  */
  YYSYMBOL_FOLLOWING = 169,                /* FOLLOWING  */
  YYSYMBOL_PRECEDING = 170,                /* PRECEDING  */
  YYSYMBOL_CURRENT_ROW = 171,              /* CURRENT_ROW  */
  YYSYMBOL_172_ = 172,                     /* '='  */
  YYSYMBOL_EQUALS = 173,                   /* EQUALS  */
  YYSYMBOL_NOTEQUALS = 174,                /* NOTEQUALS  */
  YYSYMBOL_175_ = 175,                     /* '<'  */
  YYSYMBOL_176_ = 176,                     /* '>'  */
  YYSYMBOL_LESS = 177,                     /* LESS  */
  YYSYMBOL_GREATER = 178,                  /* GREATER  */
  YYSYMBOL_LESSEQ = 179,                   /* LESSEQ  */
  YYSYMBOL_GREATEREQ = 180,                /* GREATEREQ  */
  YYSYMBOL_NOTNULL = 181,                  /* NOTNULL  */
  YYSYMBOL_182_ = 182,                     /* '+'  */
  YYSYMBOL_183_ = 183,                     /* '-'  */
  YYSYMBOL_184_ = 184,                     /* '*'  */
  YYSYMBOL_185_ = 185,                     /* '/'  */
  YYSYMBOL_186_ = 186,                     /* '%'  */
  YYSYMBOL_187_ = 187,                     /* '^'  */
  YYSYMBOL_UMINUS = 188,                   /* UMINUS  */
  YYSYMBOL_189_ = 189,                     /* '['  */
  YYSYMBOL_190_ = 190,                     /* ']'  */
  YYSYMBOL_191_ = 191,                     /* '('  */
  YYSYMBOL_192_ = 192,                     /* ')'  */
  YYSYMBOL_193_ = 193,                     /* '.'  */
  YYSYMBOL_194_ = 194,                     /* ';'  */
  YYSYMBOL_195_ = 195,                     /* ','  */
  YYSYMBOL_196_ = 196,                     /* '?'  */
  YYSYMBOL_YYACCEPT = 197,                 /* $accept  */
  YYSYMBOL_input = 198,                    /* input  */
  YYSYMBOL_statement_list = 199,           /* statement_list  */
  YYSYMBOL_statement = 200,                /* statement  */
  YYSYMBOL_preparable_statement = 201,     /* preparable_statement  */
  YYSYMBOL_opt_hints = 202,                /* opt_hints  */
  YYSYMBOL_hint_list = 203,                /* hint_list  */
  YYSYMBOL_hint = 204,                     /* hint  */
  YYSYMBOL_transaction_statement = 205,    /* transaction_statement  */
  YYSYMBOL_opt_transaction_keyword = 206,  /* opt_transaction_keyword  */
  YYSYMBOL_prepare_statement = 207,        /* prepare_statement  */
  YYSYMBOL_prepare_target_query = 208,     /* prepare_target_query  */
  YYSYMBOL_execute_statement = 209,        /* execute_statement  */
  YYSYMBOL_import_statement = 210,         /* import_statement  */
  YYSYMBOL_file_type = 211,                /* file_type  */
  YYSYMBOL_file_path = 212,                /* file_path  */
  YYSYMBOL_opt_import_export_options = 213, /* opt_import_export_options  */
  YYSYMBOL_import_export_options = 214,    /* import_export_options  */
  YYSYMBOL_export_statement = 215,         /* export_statement  */
  YYSYMBOL_show_statement = 216,           /* show_statement  */
  YYSYMBOL_create_statement = 217,         /* create_statement  */
  YYSYMBOL_opt_not_exists = 218,           /* opt_not_exists  */
  YYSYMBOL_table_elem_commalist = 219,     /* table_elem_commalist  */
  YYSYMBOL_table_elem = 220,               /* table_elem  */
  YYSYMBOL_column_def = 221,               /* column_def  */
  YYSYMBOL_column_type = 222,              /* column_type  */
  YYSYMBOL_opt_time_precision = 223,       /* opt_time_precision  */
  YYSYMBOL_opt_decimal_specification = 224, /* opt_decimal_specification  */
  YYSYMBOL_opt_column_constraints = 225,   /* opt_column_constraints  */
  YYSYMBOL_column_constraint_set = 226,    /* column_constraint_set  */
  YYSYMBOL_column_constraint = 227,        /* column_constraint  */
  YYSYMBOL_table_constraint = 228,         /* table_constraint  */
  YYSYMBOL_drop_statement = 229,           /* drop_statement  */
  YYSYMBOL_opt_exists = 230,               /* opt_exists  */
  YYSYMBOL_alter_statement = 231,          /* alter_statement  */
  YYSYMBOL_alter_action = 232,             /* alter_action  */
  YYSYMBOL_drop_action = 233,              /* drop_action  */
  YYSYMBOL_delete_statement = 234,         /* delete_statement  */
  YYSYMBOL_truncate_statement = 235,       /* truncate_statement  */
  YYSYMBOL_insert_statement = 236,         /* insert_statement  */
  YYSYMBOL_opt_column_list = 237,          /* opt_column_list  */
  YYSYMBOL_update_statement = 238,         /* update_statement  */
  YYSYMBOL_update_clause_commalist = 239,  /* update_clause_commalist  */
  YYSYMBOL_update_clause = 240,            /* update_clause  */
  YYSYMBOL_select_statement = 241,         /* select_statement  */
  YYSYMBOL_select_within_set_operation = 242, /* select_within_set_operation  */
  YYSYMBOL_select_within_set_operation_no_parentheses = 243, /* select_within_set_operation_no_parentheses  */
  YYSYMBOL_select_with_paren = 244,        /* select_with_paren  */
  YYSYMBOL_select_no_paren = 245,          /* select_no_paren  */
  YYSYMBOL_set_operator = 246,             /* set_operator  */
  YYSYMBOL_set_type = 247,                 /* set_type  */
  YYSYMBOL_opt_all = 248,                  /* opt_all  */
  YYSYMBOL_select_clause = 249,            /* select_clause  */
  YYSYMBOL_opt_distinct = 250,             /* opt_distinct  */
  YYSYMBOL_select_list = 251,              /* select_list  */
  YYSYMBOL_opt_from_clause = 252,          /* opt_from_clause  */
  YYSYMBOL_from_clause = 253,              /* from_clause  */
  YYSYMBOL_opt_where = 254,                /* opt_where  */
  YYSYMBOL_opt_group = 255,                /* opt_group  */
  YYSYMBOL_opt_having = 256,               /* opt_having  */
  YYSYMBOL_opt_order = 257,                /* opt_order  */
  YYSYMBOL_order_list = 258,               /* order_list  */
  YYSYMBOL_order_desc = 259,               /* order_desc  */
  YYSYMBOL_opt_order_type = 260,           /* opt_order_type  */
  YYSYMBOL_opt_top = 261,                  /* opt_top  */
  YYSYMBOL_opt_limit = 262,                /* opt_limit  */
  YYSYMBOL_expr_list = 263,                /* expr_list  */
  YYSYMBOL_opt_extended_literal_list = 264, /* opt_extended_literal_list  */
  YYSYMBOL_extended_literal_list = 265,    /* extended_literal_list  */
  YYSYMBOL_casted_extended_literal = 266,  /* casted_extended_literal  */
  YYSYMBOL_extended_literal = 267,         /* extended_literal  */
  YYSYMBOL_expr_alias = 268,               /* expr_alias  */
  YYSYMBOL_expr = 269,                     /* expr  */
  YYSYMBOL_operand = 270,                  /* operand  */
  YYSYMBOL_scalar_expr = 271,              /* scalar_expr  */
  YYSYMBOL_unary_expr = 272,               /* unary_expr  */
  YYSYMBOL_binary_expr = 273,              /* binary_expr  */
  YYSYMBOL_logic_expr = 274,               /* logic_expr  */
  YYSYMBOL_in_expr = 275,                  /* in_expr  */
  YYSYMBOL_case_expr = 276,                /* case_expr  */
  YYSYMBOL_case_list = 277,                /* case_list  */
  YYSYMBOL_exists_expr = 278,              /* exists_expr  */
  YYSYMBOL_comp_expr = 279,                /* comp_expr  */
  YYSYMBOL_function_expr = 280,            /* function_expr  */
  YYSYMBOL_opt_window = 281,               /* opt_window  */
  YYSYMBOL_opt_partition = 282,            /* opt_partition  */
  YYSYMBOL_opt_frame_clause = 283,         /* opt_frame_clause  */
  YYSYMBOL_frame_type = 284,               /* frame_type  */
  YYSYMBOL_frame_bound = 285,              /* frame_bound  */
  YYSYMBOL_extract_expr = 286,             /* extract_expr  */
  YYSYMBOL_cast_expr = 287,                /* cast_expr  */
  YYSYMBOL_datetime_field = 288,           /* datetime_field  */
  YYSYMBOL_datetime_field_plural = 289,    /* datetime_field_plural  */
  YYSYMBOL_duration_field = 290,           /* duration_field  */
  YYSYMBOL_array_expr = 291,               /* array_expr  */
  YYSYMBOL_array_index = 292,              /* array_index  */
  YYSYMBOL_between_expr = 293,             /* between_expr  */
  YYSYMBOL_column_name = 294,              /* column_name  */
  YYSYMBOL_literal = 295,                  /* literal  */
  YYSYMBOL_string_literal = 296,           /* string_literal  */
  YYSYMBOL_bool_literal = 297,             /* bool_literal  */
  YYSYMBOL_num_literal = 298,              /* num_literal  */
  YYSYMBOL_int_literal = 299,              /* int_literal  */
  YYSYMBOL_null_literal = 300,             /* null_literal  */
  YYSYMBOL_date_literal = 301,             /* date_literal  */
  YYSYMBOL_interval_literal = 302,         /* interval_literal  */
  YYSYMBOL_param_expr = 303,               /* param_expr  */
  YYSYMBOL_table_ref = 304,                /* table_ref  */
  YYSYMBOL_table_ref_atomic = 305,         /* table_ref_atomic  */
  YYSYMBOL_nonjoin_table_ref_atomic = 306, /* nonjoin_table_ref_atomic  */
  YYSYMBOL_table_ref_commalist = 307,      /* table_ref_commalist  */
  YYSYMBOL_table_ref_name = 308,           /* table_ref_name  */
  YYSYMBOL_table_ref_name_no_alias = 309,  /* table_ref_name_no_alias  */
  YYSYMBOL_table_name = 310,               /* table_name  */
  YYSYMBOL_opt_index_name = 311,           /* opt_index_name  */
  YYSYMBOL_table_alias = 312,              /* table_alias  */
  YYSYMBOL_opt_table_alias = 313,          /* opt_table_alias  */
  YYSYMBOL_alias = 314,                    /* alias  */
  YYSYMBOL_opt_alias = 315,                /* opt_alias  */
  YYSYMBOL_opt_locking_clause = 316,       /* opt_locking_clause  */
  YYSYMBOL_opt_locking_clause_list = 317,  /* opt_locking_clause_list  */
  YYSYMBOL_locking_clause = 318,           /* locking_clause  */
  YYSYMBOL_row_lock_mode = 319,            /* row_lock_mode  */
  YYSYMBOL_opt_row_lock_policy = 320,      /* opt_row_lock_policy  */
  YYSYMBOL_opt_with_clause = 321,          /* opt_with_clause  */
  YYSYMBOL_with_clause = 322,              /* with_clause  */
  YYSYMBOL_with_description_list = 323,    /* with_description_list  */
  YYSYMBOL_with_description = 324,         /* with_description  */
  YYSYMBOL_join_clause = 325,              /* join_clause  */
  YYSYMBOL_opt_join_type = 326,            /* opt_join_type  */
  YYSYMBOL_join_condition = 327,           /* join_condition  */
  YYSYMBOL_opt_semicolon = 328,            /* opt_semicolon  */
  YYSYMBOL_ident_commalist = 329           /* ident_commalist  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_int16 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined HSQL_LTYPE_IS_TRIVIAL && HSQL_LTYPE_IS_TRIVIAL \
             && defined HSQL_STYPE_IS_TRIVIAL && HSQL_STYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE) \
             + YYSIZEOF (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  69
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   877

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  197
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  133
/* YYNRULES -- Number of rules.  */
#define YYNRULES  345
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  632

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   434


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,   186,     2,     2,
     191,   192,   184,   182,   195,   183,   193,   185,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   194,
     175,   172,   176,   196,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,   189,     2,   190,   187,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,   168,   169,   170,   171,   173,   174,   177,
     178,   179,   180,   181,   188
};

#if HSQL_DEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   328,   328,   347,   353,   360,   364,   368,   369,   370,
     372,   373,   374,   375,   376,   377,   378,   379,   380,   381,
     387,   388,   390,   394,   399,   403,   413,   414,   415,   417,
     417,   423,   429,   431,   435,   447,   453,   466,   481,   485,
     486,   487,   489,   498,   502,   512,   522,   533,   549,   550,
     555,   566,   579,   591,   598,   605,   614,   615,   617,   621,
     626,   627,   629,   636,   637,   638,   639,   640,   641,   642,
     646,   647,   648,   649,   650,   651,   652,   653,   654,   655,
     656,   658,   659,   661,   662,   663,   665,   666,   668,   672,
     677,   678,   679,   680,   682,   683,   691,   697,   703,   709,
     715,   716,   723,   729,   731,   741,   748,   759,   766,   774,
     775,   782,   789,   793,   798,   808,   812,   816,   828,   828,
     830,   831,   840,   841,   843,   857,   869,   874,   878,   882,
     887,   888,   890,   900,   901,   903,   905,   906,   908,   910,
     911,   913,   918,   920,   921,   923,   924,   926,   930,   935,
     937,   938,   939,   943,   944,   946,   947,   948,   949,   950,
     951,   956,   960,   966,   967,   969,   973,   978,   978,   982,
     990,   991,   993,  1002,  1002,  1002,  1002,  1002,  1004,  1005,
    1005,  1005,  1005,  1005,  1005,  1005,  1005,  1006,  1006,  1010,
    1010,  1012,  1013,  1014,  1015,  1016,  1018,  1018,  1019,  1020,
    1021,  1022,  1023,  1024,  1025,  1026,  1027,  1029,  1030,  1032,
    1033,  1034,  1035,  1039,  1040,  1041,  1042,  1044,  1045,  1047,
    1048,  1050,  1051,  1052,  1053,  1054,  1055,  1056,  1060,  1061,
    1065,  1066,  1068,  1069,  1074,  1075,  1076,  1080,  1081,  1082,
    1084,  1085,  1086,  1087,  1088,  1090,  1092,  1094,  1095,  1096,
    1097,  1098,  1099,  1101,  1102,  1103,  1104,  1105,  1106,  1108,
    1108,  1110,  1112,  1114,  1116,  1117,  1118,  1119,  1121,  1121,
    1121,  1121,  1121,  1121,  1121,  1123,  1125,  1126,  1128,  1129,
    1131,  1133,  1135,  1146,  1147,  1158,  1190,  1199,  1199,  1206,
    1206,  1208,  1208,  1215,  1219,  1224,  1232,  1238,  1242,  1247,
    1248,  1250,  1250,  1252,  1252,  1254,  1255,  1257,  1257,  1263,
    1264,  1266,  1270,  1275,  1281,  1288,  1289,  1290,  1291,  1293,
    1294,  1295,  1301,  1301,  1303,  1305,  1309,  1314,  1324,  1331,
    1339,  1348,  1349,  1350,  1351,  1352,  1353,  1354,  1355,  1356,
    1357,  1359,  1365,  1365,  1368,  1372
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "IDENTIFIER", "STRING",
  "FLOATVAL", "INTVAL", "DEALLOCATE", "PARAMETERS", "INTERSECT",
  "TEMPORARY", "TIMESTAMP", "DISTINCT", "NVARCHAR", "RESTRICT", "TRUNCATE",
  "ANALYZE", "BETWEEN", "CASCADE", "COLUMNS", "CONTROL", "DEFAULT",
  "EXECUTE", "EXPLAIN", "ENCODING", "INTEGER", "NATURAL", "PREPARE",
  "PRIMARY", "SCHEMAS", "CHARACTER_VARYING", "REAL", "DECIMAL", "SMALLINT",
  "BIGINT", "SPATIAL", "VARCHAR", "VIRTUAL", "DESCRIBE", "BEFORE",
  "COLUMN", "CREATE", "DELETE", "DIRECT", "DOUBLE", "ESCAPE", "EXCEPT",
  "EXISTS", "EXTRACT", "CAST", "FORMAT", "GLOBAL", "HAVING", "IMPORT",
  "INSERT", "ISNULL", "OFFSET", "RENAME", "SCHEMA", "SELECT", "SORTED",
  "TABLES", "UNIQUE", "UNLOAD", "UPDATE", "VALUES", "AFTER", "ALTER",
  "CROSS", "DELTA", "FLOAT", "GROUP", "INDEX", "INNER", "LIMIT", "LOCAL",
  "MERGE", "MINUS", "ORDER", "OVER", "OUTER", "RIGHT", "TABLE", "UNION",
  "USING", "WHERE", "CALL", "CASE", "CHAR", "COPY", "DATE", "DATETIME",
  "DESC", "DROP", "ELSE", "FILE", "FROM", "FULL", "HASH", "HINT", "INTO",
  "JOIN", "LEFT", "LIKE", "LOAD", "LONG", "NULL", "PARTITION", "PLAN",
  "SHOW", "TEXT", "THEN", "TIME", "VIEW", "WHEN", "WITH", "ADD", "ALL",
  "AND", "ASC", "END", "FOR", "INT", "KEY", "NOT", "OFF", "SET", "TOP",
  "AS", "BY", "IF", "IN", "IS", "OF", "ON", "OR", "TO", "NO", "ARRAY",
  "CONCAT", "ILIKE", "SECOND", "MINUTE", "HOUR", "DAY", "MONTH", "YEAR",
  "SECONDS", "MINUTES", "HOURS", "DAYS", "MONTHS", "YEARS", "INTERVAL",
  "TRUE", "FALSE", "BOOLEAN", "TRANSACTION", "BEGIN", "COMMIT", "ROLLBACK",
  "NOWAIT", "SKIP", "LOCKED", "SHARE", "RANGE", "ROWS", "GROUPS",
  "UNBOUNDED", "FOLLOWING", "PRECEDING", "CURRENT_ROW", "'='", "EQUALS",
  "NOTEQUALS", "'<'", "'>'", "LESS", "GREATER", "LESSEQ", "GREATEREQ",
  "NOTNULL", "'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "UMINUS", "'['",
  "']'", "'('", "')'", "'.'", "';'", "','", "'?'", "$accept", "input",
  "statement_list", "statement", "preparable_statement", "opt_hints",
  "hint_list", "hint", "transaction_statement", "opt_transaction_keyword",
  "prepare_statement", "prepare_target_query", "execute_statement",
  "import_statement", "file_type", "file_path",
  "opt_import_export_options", "import_export_options", "export_statement",
  "show_statement", "create_statement", "opt_not_exists",
  "table_elem_commalist", "table_elem", "column_def", "column_type",
  "opt_time_precision", "opt_decimal_specification",
  "opt_column_constraints", "column_constraint_set", "column_constraint",
  "table_constraint", "drop_statement", "opt_exists", "alter_statement",
  "alter_action", "drop_action", "delete_statement", "truncate_statement",
  "insert_statement", "opt_column_list", "update_statement",
  "update_clause_commalist", "update_clause", "select_statement",
  "select_within_set_operation",
  "select_within_set_operation_no_parentheses", "select_with_paren",
  "select_no_paren", "set_operator", "set_type", "opt_all",
  "select_clause", "opt_distinct", "select_list", "opt_from_clause",
  "from_clause", "opt_where", "opt_group", "opt_having", "opt_order",
  "order_list", "order_desc", "opt_order_type", "opt_top", "opt_limit",
  "expr_list", "opt_extended_literal_list", "extended_literal_list",
  "casted_extended_literal", "extended_literal", "expr_alias", "expr",
  "operand", "scalar_expr", "unary_expr", "binary_expr", "logic_expr",
  "in_expr", "case_expr", "case_list", "exists_expr", "comp_expr",
  "function_expr", "opt_window", "opt_partition", "opt_frame_clause",
  "frame_type", "frame_bound", "extract_expr", "cast_expr",
  "datetime_field", "datetime_field_plural", "duration_field",
  "array_expr", "array_index", "between_expr", "column_name", "literal",
  "string_literal", "bool_literal", "num_literal", "int_literal",
  "null_literal", "date_literal", "interval_literal", "param_expr",
  "table_ref", "table_ref_atomic", "nonjoin_table_ref_atomic",
  "table_ref_commalist", "table_ref_name", "table_ref_name_no_alias",
  "table_name", "opt_index_name", "table_alias", "opt_table_alias",
  "alias", "opt_alias", "opt_locking_clause", "opt_locking_clause_list",
  "locking_clause", "row_lock_mode", "opt_row_lock_policy",
  "opt_with_clause", "with_clause", "with_description_list",
  "with_description", "join_clause", "opt_join_type", "join_condition",
  "opt_semicolon", "ident_commalist", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-561)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-343)

#define yytable_value_is_error(Yyn) \
  ((Yyn) == YYTABLE_NINF)

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     590,    23,    88,   102,   109,    88,    84,   -24,   118,    15,
      88,   138,     7,   110,    34,   248,   106,   106,   106,   270,
      82,  -561,   186,  -561,   186,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,   -28,  -561,   310,
     126,  -561,   144,   244,  -561,   212,   212,   212,    88,   349,
      88,   264,  -561,   252,   -28,   262,   -48,   252,   252,   252,
      88,  -561,   275,   210,  -561,  -561,  -561,  -561,  -561,  -561,
     581,  -561,   317,  -561,  -561,   296,    97,  -561,   176,  -561,
     411,   230,   426,   308,   431,    88,    88,   379,  -561,   375,
     298,   465,   443,    88,   300,   301,   490,   490,   490,   492,
      88,    88,  -561,   305,   248,  -561,   307,   495,   497,  -561,
    -561,  -561,   -28,   385,   381,   -28,    19,  -561,  -561,  -561,
     371,   334,   525,  -561,   526,  -561,  -561,    31,  -561,   341,
     339,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,   488,  -561,   404,   -49,   298,   282,
    -561,   490,   538,   146,   370,   -42,  -561,  -561,   450,  -561,
    -561,  -561,   -30,   -30,   -30,  -561,  -561,  -561,  -561,  -561,
     542,  -561,  -561,  -561,   282,   468,  -561,  -561,    97,  -561,
    -561,   282,   468,   282,    62,   427,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,    72,  -561,   219,  -561,  -561,  -561,   230,  -561,    88,
     544,   435,   216,   423,    91,   361,   362,   363,   184,   353,
     366,   373,  -561,   190,   -54,   400,  -561,  -561,  -561,  -561,
    -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,  -561,
    -561,  -561,   458,  -561,   -18,   368,  -561,   282,   465,  -561,
     520,  -561,  -561,   372,    39,  -561,   379,  -561,   374,    69,
    -561,   466,   376,  -561,    22,    19,   -28,   382,  -561,   148,
      19,   -54,   505,     2,   -20,  -561,   427,  -561,   438,  -561,
    -561,   377,   472,  -561,   676,   447,   387,   123,  -561,  -561,
    -561,   435,    10,    17,   531,   219,   282,   282,   188,   175,
     401,   373,   639,   282,   208,   399,   -78,   282,   282,   373,
    -561,   373,    68,   402,    49,   373,   373,   373,   373,   373,
     373,   373,   373,   373,   373,   373,   373,   373,   373,   373,
     495,    88,  -561,   591,   230,   -54,  -561,   252,    39,   595,
     349,   158,  -561,   230,  -561,   542,    18,   379,  -561,   282,
    -561,   592,  -561,  -561,  -561,  -561,   282,  -561,  -561,  -561,
     427,   282,   282,  -561,   436,   478,  -561,   -79,  -561,   676,
     538,   490,  -561,  -561,   415,  -561,   416,  -561,  -561,   418,
    -561,  -561,   419,  -561,  -561,  -561,  -561,   420,   422,  -561,
      45,   424,   538,  -561,   216,  -561,   523,   282,  -561,  -561,
     428,   518,   206,   192,   179,   282,   282,  -561,   531,   513,
      57,  -561,  -561,  -561,   500,   549,   662,   373,   430,   190,
    -561,   519,   437,   662,   662,   662,   662,   688,   688,   688,
     688,   208,   208,   -97,   -97,   -97,   -93,   434,  -561,  -561,
     159,   624,   204,  -561,  -561,  -561,   141,   217,  -561,   435,
    -561,   224,  -561,   442,  -561,    36,  -561,   558,  -561,  -561,
    -561,  -561,   -54,   -54,  -561,   566,   538,  -561,   470,  -561,
     444,   223,  -561,   632,   633,  -561,   634,   635,   636,  -561,
     640,   524,  -561,  -561,   543,  -561,    45,  -561,   538,   232,
    -561,   459,  -561,   236,  -561,   282,   676,   282,   282,  -561,
     269,   198,   461,  -561,   373,   662,   190,   463,   247,  -561,
    -561,  -561,  -561,  -561,   647,   349,  -561,   464,   557,  -561,
    -561,  -561,   579,   580,   582,   560,    18,   660,  -561,  -561,
    -561,   535,  -561,  -561,    67,  -561,  -561,  -561,   473,   249,
     474,   476,   477,   479,  -561,  -561,  -561,   255,  -561,   565,
     523,   125,   483,   -54,   290,  -561,   282,  -561,   639,   484,
     256,  -561,  -561,  -561,  -561,    36,    18,  -561,  -561,  -561,
      18,   403,   486,   282,  -561,  -561,  -561,   672,  -561,  -561,
    -561,  -561,  -561,   551,   468,  -561,  -561,  -561,  -561,   -54,
    -561,  -561,  -561,  -561,   365,   538,   -23,   493,   282,   315,
     491,   282,   266,   282,  -561,  -561,   376,  -561,  -561,  -561,
     494,    35,   538,   -54,  -561,  -561,   -54,  -561,   201,    50,
     287,  -561,  -561,   277,  -561,  -561,   573,  -561,  -561,  -561,
      50,  -561
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int16 yydefact[] =
{
     323,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    30,    30,    30,     0,
     343,     3,    21,    19,    21,    18,     8,     9,     7,    11,
      16,    17,    13,    14,    12,    15,    10,     0,   322,     0,
     297,   106,    33,     0,    50,    57,    57,    57,     0,     0,
       0,     0,   296,   101,     0,     0,     0,   101,   101,   101,
       0,    48,     0,   324,   325,    29,    26,    28,    27,     1,
     323,     2,     0,     6,     5,   154,   115,   116,   146,    98,
       0,   164,     0,     0,   300,     0,     0,   140,    37,     0,
     110,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    49,     0,     0,     4,     0,     0,   134,   128,
     129,   127,     0,   131,     0,     0,   160,   298,   275,   278,
     280,     0,     0,   281,     0,   276,   277,     0,   286,     0,
     163,   165,   167,   169,   268,   269,   270,   279,   271,   272,
     273,   274,    32,    31,     0,   299,     0,     0,   110,     0,
     105,     0,     0,     0,     0,   140,   112,   100,     0,   123,
     122,    38,    41,    41,    41,    99,    96,    97,   327,   326,
       0,   280,   153,   133,     0,   146,   119,   118,   120,   130,
     126,     0,   146,     0,     0,   310,   247,   248,   249,   250,
     251,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     283,     0,   282,   285,   170,   171,    34,     0,    56,     0,
       0,   323,     0,     0,   264,     0,     0,     0,     0,     0,
       0,     0,   266,     0,   139,   173,   180,   181,   182,   175,
     177,   183,   176,   196,   184,   185,   186,   187,   179,   174,
     189,   190,     0,   344,     0,     0,   108,     0,     0,   111,
       0,   102,   103,     0,     0,    47,   140,    46,    24,     0,
      22,   137,   135,   161,   308,   160,     0,   145,   147,   152,
     160,   156,   158,   155,     0,   124,   309,   311,     0,   284,
     166,     0,     0,    53,     0,     0,     0,     0,    58,    60,
      61,   323,   134,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   192,     0,   191,     0,     0,     0,     0,     0,
     193,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   109,     0,     0,   114,   113,   101,     0,     0,
       0,     0,    36,     0,    20,     0,     0,   140,   136,     0,
     306,     0,   307,   172,   117,   121,     0,   151,   150,   149,
     310,     0,     0,   315,     0,     0,   317,   321,   312,     0,
       0,     0,    79,    73,     0,    75,    85,    76,    63,     0,
      70,    71,     0,    67,    68,    74,    77,    82,     0,    64,
      87,     0,     0,    52,     0,    55,   231,     0,   265,   267,
       0,     0,     0,     0,     0,     0,     0,   215,     0,     0,
       0,   188,   178,   207,   208,     0,   203,     0,     0,     0,
     194,     0,   206,   205,   221,   222,   223,   224,   225,   226,
     227,   198,   197,   200,   199,   201,   202,     0,    35,   345,
       0,     0,     0,    45,    43,    40,     0,     0,    23,   323,
     138,   287,   289,     0,   291,   304,   290,   142,   162,   305,
     148,   125,   159,   157,   318,     0,     0,   320,     0,   313,
       0,     0,    51,     0,     0,    69,     0,     0,     0,    78,
       0,     0,    91,    92,     0,    62,    86,    88,     0,     0,
      59,     0,   228,     0,   219,     0,     0,     0,     0,   213,
       0,     0,     0,   261,     0,   204,     0,     0,     0,   195,
     262,   107,   104,    39,     0,     0,    25,     0,     0,   339,
     331,   337,   335,   338,   333,     0,     0,     0,   303,   295,
     301,     0,   132,   316,   321,   319,   168,    54,     0,     0,
       0,     0,     0,     0,    90,    93,    89,     0,    95,   233,
     231,     0,     0,   217,     0,   216,     0,   220,   263,     0,
       0,   211,   209,    44,    42,   304,     0,   334,   336,   332,
       0,   288,   305,     0,   314,    66,    84,     0,    80,    65,
      81,    72,    94,     0,   146,   229,   245,   246,   214,   218,
     212,   210,   292,   328,   340,     0,   144,     0,     0,   236,
       0,     0,     0,     0,   141,    83,   232,   237,   238,   239,
       0,     0,     0,   341,   329,   302,   143,   230,     0,     0,
       0,   244,   234,     0,   243,   241,     0,   242,   240,   330,
       0,   235
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -561,  -561,  -561,   614,  -561,   668,  -561,   348,  -561,   457,
    -561,  -561,  -561,  -561,  -332,   -80,   322,   357,  -561,  -561,
    -561,   441,  -561,   303,  -561,  -336,  -561,  -561,  -561,  -561,
     214,  -561,  -561,   -43,  -561,  -561,  -561,  -561,  -561,  -561,
     550,  -561,  -561,   454,  -207,   -91,  -561,    43,   -53,   -40,
    -561,  -561,   -85,   412,  -561,  -561,  -561,  -143,  -561,  -561,
    -173,  -561,   347,  -561,  -561,    42,  -298,  -561,  -167,   504,
     512,   367,  -149,  -193,  -561,  -561,  -561,  -561,  -561,  -561,
     417,  -561,  -561,  -561,   164,  -561,  -561,  -561,  -560,  -561,
    -561,  -141,  -561,  -561,  -561,  -561,  -561,  -561,   -62,  -561,
    -561,   599,  -100,  -561,  -561,   600,  -561,  -561,  -466,   152,
    -561,  -561,  -561,     1,  -561,  -561,   154,   480,  -561,   391,
    -561,   469,  -561,   196,  -561,  -561,  -561,   643,  -561,  -561,
    -561,  -561,  -347
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,    19,    20,    21,    22,    73,   259,   260,    23,    66,
      24,   143,    25,    26,    89,   162,   255,   341,    27,    28,
      29,    84,   287,   288,   289,   390,   479,   475,   485,   486,
     487,   290,    30,    93,    31,   251,   252,    32,    33,    34,
     153,    35,   155,   156,    36,   175,   176,   177,    77,   112,
     113,   180,    78,   174,   261,   347,   348,   150,   532,   604,
     116,   267,   268,   359,   108,   185,   262,   129,   130,   131,
     132,   263,   264,   225,   226,   227,   228,   229,   230,   231,
     299,   232,   233,   234,   492,   584,   610,   611,   622,   235,
     236,   198,   199,   200,   237,   238,   239,   240,   241,   134,
     135,   136,   137,   138,   139,   140,   141,   450,   451,   452,
     453,   454,    51,   455,   146,   528,   529,   530,   353,   275,
     276,   277,   367,   469,    37,    38,    63,    64,   456,   525,
     614,    71,   244
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
     224,    95,   265,    41,   283,   410,    44,   172,   444,   270,
      40,    52,   249,    56,    99,   100,   101,   163,   164,   133,
     398,    40,   173,   471,   182,   350,   302,   178,   304,   603,
     178,    75,   269,   470,   271,   273,   119,   120,   115,   350,
     307,   618,   315,   149,   363,   489,   315,   210,    97,    87,
      39,    90,   619,    60,   466,    55,   618,   308,   362,   626,
     571,   102,   279,   339,   307,   214,   118,   119,   120,   298,
     631,   242,    48,   481,   306,   183,   118,   119,   120,   211,
      76,   308,   467,   468,   395,   253,   147,   148,    98,   340,
     329,    40,   330,   184,   158,    61,   330,    94,   335,   493,
     246,   166,   167,   364,   594,    42,   109,   482,   302,   215,
     216,   217,    43,   342,   412,    50,   415,   365,   416,   534,
     307,   508,   422,   423,   424,   425,   426,   427,   428,   429,
     430,   431,   432,   433,   434,   435,   436,   308,   266,   133,
     307,   547,   212,   110,   366,   133,   168,   402,   403,   218,
     351,   483,   122,   248,   401,   420,    45,   308,   413,   414,
     552,   254,   122,    54,   527,   514,    46,   440,   123,   484,
     305,   417,   349,   421,   332,   355,   447,   333,   123,   272,
     111,   178,    57,   564,   124,   109,   219,   214,   118,   119,
     120,   515,    58,   214,   118,   119,   120,    47,    54,   418,
     220,   399,   396,   620,   457,    75,   621,   269,   560,   449,
     281,   245,   462,   463,    49,   124,   125,   126,   620,   284,
      53,   621,   110,    59,   505,   124,   125,   126,   467,   468,
     437,   215,   216,   217,   118,   119,   120,   215,   216,   217,
     357,   400,   517,   307,   285,   221,   222,   503,   602,    75,
     518,    62,   349,   223,   114,   127,   500,   501,   128,   111,
     308,   344,   333,    65,   345,   623,   307,   358,   128,   405,
      69,   218,   133,   498,   122,   596,    70,   218,   286,   121,
     122,   133,   292,   308,   293,   214,   118,   119,   120,   406,
     123,   472,   519,   406,   441,   407,   123,   520,   297,   499,
     606,    72,   297,   497,   521,   522,   307,   354,   219,   556,
     307,   558,   360,    79,   219,   393,   307,   586,   394,    80,
     122,   523,   220,   308,   307,  -340,   524,   308,   220,   215,
     216,   217,   438,   308,   496,    81,   123,   124,   125,   126,
      82,   308,    83,   124,   125,   126,   551,   315,   553,   554,
     445,   511,    88,   446,   207,   502,   214,   118,   119,   120,
     186,   187,   188,   189,   190,   191,   507,   221,   222,   218,
     624,   625,   122,   221,   222,   223,   214,   118,   119,   120,
     128,   223,    92,   124,   125,   126,   128,   307,   123,   555,
      91,   518,   326,   327,   328,   329,   513,   330,    96,   446,
     300,   216,   217,   103,   308,   104,   219,   589,   307,   516,
     588,   599,   207,   127,   117,   537,   106,   309,   333,  -293,
     220,   216,   217,   107,   548,   308,   128,   333,   550,   518,
     142,   349,   144,   519,   145,   124,   125,   126,   520,   562,
     218,   576,   349,   122,   577,   521,   522,   582,   591,   600,
     333,   349,   613,   559,   616,   310,   627,   628,   615,   123,
     218,   333,   523,   122,   149,   221,   222,   524,   154,   629,
     151,   519,   333,   223,    67,    68,   520,   301,   128,   123,
     607,   608,   609,   521,   522,   256,   257,    85,    86,   152,
     157,   220,   159,   160,   161,   165,    54,   301,   170,   601,
     523,   171,   179,   311,  -340,   524,   124,   125,   126,   173,
     181,   220,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   312,   201,   124,   125,   126,   202,
     203,   313,   314,   206,   207,   208,   221,   222,   209,   315,
     316,   243,   247,   250,   223,   258,   114,   282,   274,   128,
      15,   291,   294,   295,   296,   303,   221,   222,   331,   334,
     337,   361,   346,   338,   223,   343,   369,   371,   370,   128,
     391,   349,   317,   318,   319,   320,   321,   356,   392,   322,
     323,  -342,   324,   325,   326,   327,   328,   329,     1,   330,
      75,   411,   408,   419,   439,   459,     2,     1,  -294,   443,
     464,   465,   491,     3,   310,     2,   473,   474,     4,   476,
     477,   478,     3,   480,   495,   488,   417,     4,   307,     5,
     494,   506,     6,     7,   510,   509,   330,   512,     5,   531,
     533,     6,     7,   535,     8,     9,   536,   526,   538,   539,
     540,   541,   542,     8,     9,    10,   543,   544,    11,   545,
     549,   563,   311,   557,    10,   561,   565,    11,   566,   567,
     568,   570,   569,   572,   573,   575,   578,   504,   579,   580,
      12,   581,   583,   409,    13,   587,   590,   595,   597,    12,
     598,   314,   612,    13,   105,   605,   617,   372,   315,   316,
      14,   630,    74,   448,   310,   442,    15,   490,   213,    14,
     546,   373,   336,   460,   397,    15,   374,   375,   376,   377,
     378,   280,   379,   278,   585,   404,   458,   310,   593,   592,
     380,   317,   318,   319,   320,   321,   204,   205,   322,   323,
     574,   324,   325,   326,   327,   328,   329,     0,   330,    16,
      17,    18,   311,   310,   352,   368,   381,   169,    16,    17,
      18,   461,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   409,   382,  -343,   383,   384,     0,     0,
       0,   314,     0,     0,     0,     0,     0,     0,   315,   316,
       0,   385,     0,     0,     0,     0,   386,     0,   387,     0,
       0,     0,     0,     0,   314,     0,     0,     0,   388,     0,
       0,   315,  -343,     0,     0,     0,     0,     0,     0,     0,
       0,   317,   318,   319,   320,   321,     0,     0,   322,   323,
     314,   324,   325,   326,   327,   328,   329,   315,   330,     0,
       0,     0,   389,     0,  -343,  -343,  -343,   320,   321,     0,
       0,   322,   323,     0,   324,   325,   326,   327,   328,   329,
       0,   330,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,  -343,  -343,     0,     0,  -343,  -343,     0,
     324,   325,   326,   327,   328,   329,     0,   330
};

static const yytype_int16 yycheck[] =
{
     149,    54,   175,     2,   211,   303,     5,   107,   340,   182,
       3,    10,   155,    12,    57,    58,    59,    97,    98,    81,
       3,     3,    12,   370,   115,     3,   219,   112,   221,    52,
     115,    59,   181,   369,   183,   184,     5,     6,    78,     3,
     118,     6,   139,    85,    64,   392,   139,    96,    96,    48,
      27,    50,    17,    19,   133,    12,     6,   135,    56,   619,
     526,    60,   203,    24,   118,     3,     4,     5,     6,   218,
     630,   151,    96,    28,   223,    56,     4,     5,     6,   128,
      37,   135,   161,   162,   291,   115,    85,    86,   136,    50,
     187,     3,   189,    74,    93,    61,   189,    54,   247,   397,
     153,   100,   101,   123,   570,     3,     9,    62,   301,    47,
      48,    49,     3,   256,   192,   100,   309,   137,   311,   466,
     118,   419,   315,   316,   317,   318,   319,   320,   321,   322,
     323,   324,   325,   326,   327,   328,   329,   135,   178,   201,
     118,   488,   191,    46,   164,   207,   103,   296,   297,    87,
     128,   106,    90,   195,   295,   106,    72,   135,   307,   308,
     496,   191,    90,   191,   128,    24,    82,   334,   106,   124,
     223,   103,   195,   124,   192,   266,   343,   195,   106,   117,
      83,   266,    72,   515,   153,     9,   124,     3,     4,     5,
       6,    50,    82,     3,     4,     5,     6,   113,   191,   131,
     138,   184,   192,   168,   347,    59,   171,   356,   506,   191,
     209,    65,   361,   362,    96,   153,   154,   155,   168,     3,
      82,   171,    46,   113,   417,   153,   154,   155,   161,   162,
     330,    47,    48,    49,     4,     5,     6,    47,    48,    49,
      92,   294,   449,   118,    28,   183,   184,   190,   595,    59,
      26,     3,   195,   191,    78,   183,   405,   406,   196,    83,
     135,   192,   195,   157,   195,   612,   118,   119,   196,    94,
       0,    87,   334,    94,    90,   573,   194,    87,    62,    49,
      90,   343,   191,   135,   193,     3,     4,     5,     6,   114,
     106,   371,    68,   114,   337,   120,   106,    73,   114,   120,
     598,   115,   114,   111,    80,    81,   118,   265,   124,   111,
     118,   504,   270,     3,   124,   192,   118,   192,   195,   193,
      90,    97,   138,   135,   118,   101,   102,   135,   138,    47,
      48,    49,   331,   135,   128,   191,   106,   153,   154,   155,
      96,   135,   130,   153,   154,   155,   495,   139,   497,   498,
     192,   192,     3,   195,   195,   408,     3,     4,     5,     6,
     141,   142,   143,   144,   145,   146,   419,   183,   184,    87,
     169,   170,    90,   183,   184,   191,     3,     4,     5,     6,
     196,   191,   130,   153,   154,   155,   196,   118,   106,   120,
     126,    26,   184,   185,   186,   187,   192,   189,   136,   195,
      47,    48,    49,   128,   135,   195,   124,   556,   118,   192,
     120,   584,   195,   183,     3,   192,    99,    17,   195,   195,
     138,    48,    49,   127,   192,   135,   196,   195,   192,    26,
       4,   195,   124,    68,     3,   153,   154,   155,    73,   192,
      87,   192,   195,    90,   195,    80,    81,   192,   192,    84,
     195,   195,   601,   506,   603,    55,   169,   170,   192,   106,
      87,   195,    97,    90,    85,   183,   184,   102,     3,   192,
      95,    68,   195,   191,    17,    18,    73,   124,   196,   106,
     165,   166,   167,    80,    81,   163,   164,    46,    47,   191,
      47,   138,   192,   192,     4,     3,   191,   124,   191,   134,
      97,     6,   117,   103,   101,   102,   153,   154,   155,    12,
     129,   138,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   124,   191,   153,   154,   155,     4,
       4,   131,   132,   192,   195,    47,   183,   184,   134,   139,
     140,     3,   172,    93,   191,     3,    78,     3,   121,   196,
     115,   128,   191,   191,   191,   189,   183,   184,   100,   191,
      40,    56,    96,   191,   191,   191,   128,    95,   191,   196,
     123,   195,   172,   173,   174,   175,   176,   195,   191,   179,
     180,     0,   182,   183,   184,   185,   186,   187,     7,   189,
      59,   192,   191,   191,     3,     3,    15,     7,   195,     4,
     164,   123,    79,    22,    55,    15,   191,   191,    27,   191,
     191,   191,    22,   191,    96,   191,   103,    27,   118,    38,
     192,   191,    41,    42,   190,   106,   189,     3,    38,    71,
      64,    41,    42,   163,    53,    54,   192,   195,     6,     6,
       6,     6,     6,    53,    54,    64,     6,   123,    67,   106,
     191,     4,   103,   192,    64,   192,   192,    67,   101,    80,
      80,   101,    80,     3,   129,   192,   192,   118,   192,   192,
      89,   192,   107,   124,    93,   192,   192,   191,     6,    89,
     129,   132,   191,    93,    70,   192,   192,    11,   139,   140,
     109,   118,    24,   345,    55,   338,   115,   394,   148,   109,
     486,    25,   248,   356,   292,   115,    30,    31,    32,    33,
      34,   207,    36,   201,   550,   298,   349,    55,   566,   565,
      44,   172,   173,   174,   175,   176,   127,   127,   179,   180,
     534,   182,   183,   184,   185,   186,   187,    -1,   189,   158,
     159,   160,   103,    55,   264,   276,    70,   104,   158,   159,
     160,   360,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   124,    88,   103,    90,    91,    -1,    -1,
      -1,   132,    -1,    -1,    -1,    -1,    -1,    -1,   139,   140,
      -1,   105,    -1,    -1,    -1,    -1,   110,    -1,   112,    -1,
      -1,    -1,    -1,    -1,   132,    -1,    -1,    -1,   122,    -1,
      -1,   139,   140,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   172,   173,   174,   175,   176,    -1,    -1,   179,   180,
     132,   182,   183,   184,   185,   186,   187,   139,   189,    -1,
      -1,    -1,   156,    -1,   172,   173,   174,   175,   176,    -1,
      -1,   179,   180,    -1,   182,   183,   184,   185,   186,   187,
      -1,   189,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   175,   176,    -1,    -1,   179,   180,    -1,
     182,   183,   184,   185,   186,   187,    -1,   189
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int16 yystos[] =
{
       0,     7,    15,    22,    27,    38,    41,    42,    53,    54,
      64,    67,    89,    93,   109,   115,   158,   159,   160,   198,
     199,   200,   201,   205,   207,   209,   210,   215,   216,   217,
     229,   231,   234,   235,   236,   238,   241,   321,   322,    27,
       3,   310,     3,     3,   310,    72,    82,   113,    96,    96,
     100,   309,   310,    82,   191,   244,   310,    72,    82,   113,
      19,    61,     3,   323,   324,   157,   206,   206,   206,     0,
     194,   328,   115,   202,   202,    59,   244,   245,   249,     3,
     193,   191,    96,   130,   218,   218,   218,   310,     3,   211,
     310,   126,   130,   230,   244,   245,   136,    96,   136,   230,
     230,   230,   310,   128,   195,   200,    99,   127,   261,     9,
      46,    83,   246,   247,    78,   246,   257,     3,     4,     5,
       6,    49,    90,   106,   153,   154,   155,   183,   196,   264,
     265,   266,   267,   295,   296,   297,   298,   299,   300,   301,
     302,   303,     4,   208,   124,     3,   311,   310,   310,    85,
     254,    95,   191,   237,     3,   239,   240,    47,   310,   192,
     192,     4,   212,   212,   212,     3,   310,   310,   244,   324,
     191,     6,   299,    12,   250,   242,   243,   244,   249,   117,
     248,   129,   242,    56,    74,   262,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   288,   289,
     290,   191,     4,     4,   298,   302,   192,   195,    47,   134,
      96,   128,   191,   237,     3,    47,    48,    49,    87,   124,
     138,   183,   184,   191,   269,   270,   271,   272,   273,   274,
     275,   276,   278,   279,   280,   286,   287,   291,   292,   293,
     294,   295,   212,     3,   329,    65,   245,   172,   195,   254,
      93,   232,   233,   115,   191,   213,   213,   213,     3,   203,
     204,   251,   263,   268,   269,   257,   246,   258,   259,   269,
     257,   269,   117,   269,   121,   316,   317,   318,   267,   288,
     266,   310,     3,   241,     3,    28,    62,   219,   220,   221,
     228,   128,   191,   193,   191,   191,   191,   114,   269,   277,
      47,   124,   270,   189,   270,   245,   269,   118,   135,    17,
      55,   103,   124,   131,   132,   139,   140,   172,   173,   174,
     175,   176,   179,   180,   182,   183,   184,   185,   186,   187,
     189,   100,   192,   195,   191,   269,   240,    40,   191,    24,
      50,   214,   254,   191,   192,   195,    96,   252,   253,   195,
       3,   128,   314,   315,   262,   242,   195,    92,   119,   260,
     262,    56,    56,    64,   123,   137,   164,   319,   318,   128,
     191,    95,    11,    25,    30,    31,    32,    33,    34,    36,
      44,    70,    88,    90,    91,   105,   110,   112,   122,   156,
     222,   123,   191,   192,   195,   241,   192,   250,     3,   184,
     245,   288,   269,   269,   277,    94,   114,   120,   191,   124,
     263,   192,   192,   269,   269,   270,   270,   103,   131,   191,
     106,   124,   270,   270,   270,   270,   270,   270,   270,   270,
     270,   270,   270,   270,   270,   270,   270,   299,   310,     3,
     265,   230,   214,     4,   211,   192,   195,   265,   204,   191,
     304,   305,   306,   307,   308,   310,   325,   254,   268,     3,
     259,   316,   269,   269,   164,   123,   133,   161,   162,   320,
     222,   329,   212,   191,   191,   224,   191,   191,   191,   223,
     191,    28,    62,   106,   124,   225,   226,   227,   191,   329,
     220,    79,   281,   263,   192,    96,   128,   111,    94,   120,
     269,   269,   245,   190,   118,   270,   191,   245,   263,   106,
     190,   192,     3,   192,    24,    50,   192,   241,    26,    68,
      73,    80,    81,    97,   102,   326,   195,   128,   312,   313,
     314,    71,   255,    64,   329,   163,   192,   192,     6,     6,
       6,     6,     6,     6,   123,   106,   227,   329,   192,   191,
     192,   269,   222,   269,   269,   120,   111,   192,   270,   245,
     263,   192,   192,     4,   211,   192,   101,    80,    80,    80,
     101,   305,     3,   129,   320,   192,   192,   195,   192,   192,
     192,   192,   192,   107,   282,   281,   192,   192,   120,   269,
     192,   192,   313,   306,   305,   191,   263,     6,   129,   257,
      84,   134,   329,    52,   256,   192,   263,   165,   166,   167,
     283,   284,   191,   269,   327,   192,   269,   192,     6,    17,
     168,   171,   285,   329,   169,   170,   285,   169,   170,   192,
     118,   285
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int16 yyr1[] =
{
       0,   197,   198,   199,   199,   200,   200,   200,   200,   200,
     201,   201,   201,   201,   201,   201,   201,   201,   201,   201,
     202,   202,   203,   203,   204,   204,   205,   205,   205,   206,
     206,   207,   208,   209,   209,   210,   210,   211,   212,   213,
     213,   213,   214,   214,   214,   214,   215,   215,   216,   216,
     216,   217,   217,   217,   217,   217,   218,   218,   219,   219,
     220,   220,   221,   222,   222,   222,   222,   222,   222,   222,
     222,   222,   222,   222,   222,   222,   222,   222,   222,   222,
     222,   223,   223,   224,   224,   224,   225,   225,   226,   226,
     227,   227,   227,   227,   228,   228,   229,   229,   229,   229,
     230,   230,   231,   232,   233,   234,   235,   236,   236,   237,
     237,   238,   239,   239,   240,   241,   241,   241,   242,   242,
     243,   243,   244,   244,   245,   245,   246,   247,   247,   247,
     248,   248,   249,   250,   250,   251,   252,   252,   253,   254,
     254,   255,   255,   256,   256,   257,   257,   258,   258,   259,
     260,   260,   260,   261,   261,   262,   262,   262,   262,   262,
     262,   263,   263,   264,   264,   265,   265,   266,   266,   267,
     267,   267,   268,   269,   269,   269,   269,   269,   270,   270,
     270,   270,   270,   270,   270,   270,   270,   270,   270,   271,
     271,   272,   272,   272,   272,   272,   273,   273,   273,   273,
     273,   273,   273,   273,   273,   273,   273,   274,   274,   275,
     275,   275,   275,   276,   276,   276,   276,   277,   277,   278,
     278,   279,   279,   279,   279,   279,   279,   279,   280,   280,
     281,   281,   282,   282,   283,   283,   283,   284,   284,   284,
     285,   285,   285,   285,   285,   286,   287,   288,   288,   288,
     288,   288,   288,   289,   289,   289,   289,   289,   289,   290,
     290,   291,   292,   293,   294,   294,   294,   294,   295,   295,
     295,   295,   295,   295,   295,   296,   297,   297,   298,   298,
     299,   300,   301,   302,   302,   302,   303,   304,   304,   305,
     305,   306,   306,   307,   307,   308,   309,   310,   310,   311,
     311,   312,   312,   313,   313,   314,   314,   315,   315,   316,
     316,   317,   317,   318,   318,   319,   319,   319,   319,   320,
     320,   320,   321,   321,   322,   323,   323,   324,   325,   325,
     325,   326,   326,   326,   326,   326,   326,   326,   326,   326,
     326,   327,   328,   328,   329,   329
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     3,     2,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       5,     0,     1,     3,     1,     4,     2,     2,     2,     1,
       0,     4,     1,     2,     5,     7,     6,     1,     1,     4,
       3,     0,     4,     2,     4,     2,     5,     5,     2,     3,
       2,     8,     7,     6,     9,     7,     3,     0,     1,     3,
       1,     1,     3,     1,     1,     4,     4,     1,     1,     2,
       1,     1,     4,     1,     1,     1,     1,     1,     2,     1,
       4,     3,     0,     5,     3,     0,     1,     0,     1,     2,
       2,     1,     1,     2,     5,     4,     4,     4,     3,     4,
       2,     0,     5,     1,     4,     4,     2,     8,     5,     3,
       0,     5,     1,     3,     3,     2,     2,     6,     1,     1,
       1,     3,     3,     3,     4,     6,     2,     1,     1,     1,
       1,     0,     7,     1,     0,     1,     1,     0,     2,     2,
       0,     4,     0,     2,     0,     3,     0,     1,     3,     2,
       1,     1,     0,     2,     0,     2,     2,     4,     2,     4,
       0,     1,     3,     1,     0,     1,     3,     1,     6,     1,
       2,     2,     2,     1,     1,     1,     1,     1,     3,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     3,     1,
       1,     2,     2,     2,     3,     4,     1,     3,     3,     3,
       3,     3,     3,     3,     4,     3,     3,     3,     3,     5,
       6,     5,     6,     4,     6,     3,     5,     4,     5,     4,
       5,     3,     3,     3,     3,     3,     3,     3,     4,     6,
       6,     0,     3,     0,     2,     5,     0,     1,     1,     1,
       2,     2,     2,     2,     1,     6,     6,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     4,     4,     5,     1,     3,     1,     3,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     2,     2,     3,     2,     1,     1,     3,     1,
       1,     1,     4,     1,     3,     2,     1,     1,     3,     1,
       0,     1,     5,     1,     0,     2,     1,     1,     0,     1,
       0,     1,     2,     3,     5,     1,     3,     1,     2,     2,
       1,     0,     1,     0,     2,     1,     3,     3,     4,     6,
       8,     1,     2,     1,     2,     1,     2,     1,     1,     1,
       0,     1,     1,     0,     1,     3
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = SQL_HSQL_EMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == SQL_HSQL_EMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (&yylloc, result, scanner, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use SQL_HSQL_error or SQL_HSQL_UNDEF. */
#define YYERRCODE SQL_HSQL_UNDEF

/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if HSQL_DEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YYLOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

# ifndef YYLOCATION_PRINT

#  if defined YY_LOCATION_PRINT

   /* Temporary convenience wrapper in case some people defined the
      undocumented and private YY_LOCATION_PRINT macros.  */
#   define YYLOCATION_PRINT(File, Loc)  YY_LOCATION_PRINT(File, *(Loc))

#  elif defined HSQL_LTYPE_IS_TRIVIAL && HSQL_LTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static int
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  int res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
}

#   define YYLOCATION_PRINT  yy_location_print_

    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT(File, Loc)  YYLOCATION_PRINT(File, &(Loc))

#  else

#   define YYLOCATION_PRINT(File, Loc) ((void) 0)
    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT  YYLOCATION_PRINT

#  endif
# endif /* !defined YYLOCATION_PRINT */


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, Location, result, scanner); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, hsql::SQLParserResult* result, yyscan_t scanner)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  YY_USE (result);
  YY_USE (scanner);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, hsql::SQLParserResult* result, yyscan_t scanner)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YYLOCATION_PRINT (yyo, yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp, result, scanner);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp,
                 int yyrule, hsql::SQLParserResult* result, yyscan_t scanner)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)],
                       &(yylsp[(yyi + 1) - (yynrhs)]), result, scanner);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule, result, scanner); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !HSQL_DEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !HSQL_DEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
  YYLTYPE *yylloc;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, hsql::SQLParserResult* result, yyscan_t scanner)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  YY_USE (result);
  YY_USE (scanner);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  switch (yykind)
    {
    case YYSYMBOL_IDENTIFIER: /* IDENTIFIER  */
#line 186 "bison_parser.y"
            { free( (((*yyvaluep).sval)) ); }
#line 2085 "bison_parser.cpp"
        break;

    case YYSYMBOL_STRING: /* STRING  */
#line 186 "bison_parser.y"
            { free( (((*yyvaluep).sval)) ); }
#line 2091 "bison_parser.cpp"
        break;

    case YYSYMBOL_FLOATVAL: /* FLOATVAL  */
#line 173 "bison_parser.y"
            { }
#line 2097 "bison_parser.cpp"
        break;

    case YYSYMBOL_INTVAL: /* INTVAL  */
#line 173 "bison_parser.y"
            { }
#line 2103 "bison_parser.cpp"
        break;

    case YYSYMBOL_statement_list: /* statement_list  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).stmt_vec)) {
    for (auto ptr : *(((*yyvaluep).stmt_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).stmt_vec));
}
#line 2116 "bison_parser.cpp"
        break;

    case YYSYMBOL_statement: /* statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).statement)); }
#line 2122 "bison_parser.cpp"
        break;

    case YYSYMBOL_preparable_statement: /* preparable_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).statement)); }
#line 2128 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_hints: /* opt_hints  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).expr_vec)) {
    for (auto ptr : *(((*yyvaluep).expr_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).expr_vec));
}
#line 2141 "bison_parser.cpp"
        break;

    case YYSYMBOL_hint_list: /* hint_list  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).expr_vec)) {
    for (auto ptr : *(((*yyvaluep).expr_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).expr_vec));
}
#line 2154 "bison_parser.cpp"
        break;

    case YYSYMBOL_hint: /* hint  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2160 "bison_parser.cpp"
        break;

    case YYSYMBOL_transaction_statement: /* transaction_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).transaction_stmt)); }
#line 2166 "bison_parser.cpp"
        break;

    case YYSYMBOL_prepare_statement: /* prepare_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).prep_stmt)); }
#line 2172 "bison_parser.cpp"
        break;

    case YYSYMBOL_prepare_target_query: /* prepare_target_query  */
#line 186 "bison_parser.y"
            { free( (((*yyvaluep).sval)) ); }
#line 2178 "bison_parser.cpp"
        break;

    case YYSYMBOL_execute_statement: /* execute_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).exec_stmt)); }
#line 2184 "bison_parser.cpp"
        break;

    case YYSYMBOL_import_statement: /* import_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).import_stmt)); }
#line 2190 "bison_parser.cpp"
        break;

    case YYSYMBOL_file_type: /* file_type  */
#line 173 "bison_parser.y"
            { }
#line 2196 "bison_parser.cpp"
        break;

    case YYSYMBOL_file_path: /* file_path  */
#line 186 "bison_parser.y"
            { free( (((*yyvaluep).sval)) ); }
#line 2202 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_import_export_options: /* opt_import_export_options  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).import_export_option_t)); }
#line 2208 "bison_parser.cpp"
        break;

    case YYSYMBOL_import_export_options: /* import_export_options  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).import_export_option_t)); }
#line 2214 "bison_parser.cpp"
        break;

    case YYSYMBOL_export_statement: /* export_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).export_stmt)); }
#line 2220 "bison_parser.cpp"
        break;

    case YYSYMBOL_show_statement: /* show_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).show_stmt)); }
#line 2226 "bison_parser.cpp"
        break;

    case YYSYMBOL_create_statement: /* create_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).create_stmt)); }
#line 2232 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_not_exists: /* opt_not_exists  */
#line 173 "bison_parser.y"
            { }
#line 2238 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_elem_commalist: /* table_elem_commalist  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).table_element_vec)) {
    for (auto ptr : *(((*yyvaluep).table_element_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).table_element_vec));
}
#line 2251 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_elem: /* table_elem  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table_element_t)); }
#line 2257 "bison_parser.cpp"
        break;

    case YYSYMBOL_column_def: /* column_def  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).column_t)); }
#line 2263 "bison_parser.cpp"
        break;

    case YYSYMBOL_column_type: /* column_type  */
#line 173 "bison_parser.y"
            { }
#line 2269 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_time_precision: /* opt_time_precision  */
#line 173 "bison_parser.y"
            { }
#line 2275 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_decimal_specification: /* opt_decimal_specification  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).ival_pair)); }
#line 2281 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_column_constraints: /* opt_column_constraints  */
#line 173 "bison_parser.y"
            { }
#line 2287 "bison_parser.cpp"
        break;

    case YYSYMBOL_column_constraint_set: /* column_constraint_set  */
#line 173 "bison_parser.y"
            { }
#line 2293 "bison_parser.cpp"
        break;

    case YYSYMBOL_column_constraint: /* column_constraint  */
#line 173 "bison_parser.y"
            { }
#line 2299 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_constraint: /* table_constraint  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table_constraint_t)); }
#line 2305 "bison_parser.cpp"
        break;

    case YYSYMBOL_drop_statement: /* drop_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).drop_stmt)); }
#line 2311 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_exists: /* opt_exists  */
#line 173 "bison_parser.y"
            { }
#line 2317 "bison_parser.cpp"
        break;

    case YYSYMBOL_alter_statement: /* alter_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).alter_stmt)); }
#line 2323 "bison_parser.cpp"
        break;

    case YYSYMBOL_alter_action: /* alter_action  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).alter_action_t)); }
#line 2329 "bison_parser.cpp"
        break;

    case YYSYMBOL_drop_action: /* drop_action  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).drop_action_t)); }
#line 2335 "bison_parser.cpp"
        break;

    case YYSYMBOL_delete_statement: /* delete_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).delete_stmt)); }
#line 2341 "bison_parser.cpp"
        break;

    case YYSYMBOL_truncate_statement: /* truncate_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).delete_stmt)); }
#line 2347 "bison_parser.cpp"
        break;

    case YYSYMBOL_insert_statement: /* insert_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).insert_stmt)); }
#line 2353 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_column_list: /* opt_column_list  */
#line 178 "bison_parser.y"
            {
  if (((*yyvaluep).str_vec)) {
    for (auto ptr : *(((*yyvaluep).str_vec))) {
      free(ptr);
    }
  }
  delete (((*yyvaluep).str_vec));
}
#line 2366 "bison_parser.cpp"
        break;

    case YYSYMBOL_update_statement: /* update_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).update_stmt)); }
#line 2372 "bison_parser.cpp"
        break;

    case YYSYMBOL_update_clause_commalist: /* update_clause_commalist  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).update_vec)) {
    for (auto ptr : *(((*yyvaluep).update_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).update_vec));
}
#line 2385 "bison_parser.cpp"
        break;

    case YYSYMBOL_update_clause: /* update_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).update_t)); }
#line 2391 "bison_parser.cpp"
        break;

    case YYSYMBOL_select_statement: /* select_statement  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).select_stmt)); }
#line 2397 "bison_parser.cpp"
        break;

    case YYSYMBOL_select_within_set_operation: /* select_within_set_operation  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).select_stmt)); }
#line 2403 "bison_parser.cpp"
        break;

    case YYSYMBOL_select_within_set_operation_no_parentheses: /* select_within_set_operation_no_parentheses  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).select_stmt)); }
#line 2409 "bison_parser.cpp"
        break;

    case YYSYMBOL_select_with_paren: /* select_with_paren  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).select_stmt)); }
#line 2415 "bison_parser.cpp"
        break;

    case YYSYMBOL_select_no_paren: /* select_no_paren  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).select_stmt)); }
#line 2421 "bison_parser.cpp"
        break;

    case YYSYMBOL_set_operator: /* set_operator  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).set_operator_t)); }
#line 2427 "bison_parser.cpp"
        break;

    case YYSYMBOL_set_type: /* set_type  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).set_operator_t)); }
#line 2433 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_all: /* opt_all  */
#line 173 "bison_parser.y"
            { }
#line 2439 "bison_parser.cpp"
        break;

    case YYSYMBOL_select_clause: /* select_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).select_stmt)); }
#line 2445 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_distinct: /* opt_distinct  */
#line 173 "bison_parser.y"
            { }
#line 2451 "bison_parser.cpp"
        break;

    case YYSYMBOL_select_list: /* select_list  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).expr_vec)) {
    for (auto ptr : *(((*yyvaluep).expr_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).expr_vec));
}
#line 2464 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_from_clause: /* opt_from_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2470 "bison_parser.cpp"
        break;

    case YYSYMBOL_from_clause: /* from_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2476 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_where: /* opt_where  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2482 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_group: /* opt_group  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).group_t)); }
#line 2488 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_having: /* opt_having  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2494 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_order: /* opt_order  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).order_vec)) {
    for (auto ptr : *(((*yyvaluep).order_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).order_vec));
}
#line 2507 "bison_parser.cpp"
        break;

    case YYSYMBOL_order_list: /* order_list  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).order_vec)) {
    for (auto ptr : *(((*yyvaluep).order_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).order_vec));
}
#line 2520 "bison_parser.cpp"
        break;

    case YYSYMBOL_order_desc: /* order_desc  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).order)); }
#line 2526 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_order_type: /* opt_order_type  */
#line 173 "bison_parser.y"
            { }
#line 2532 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_top: /* opt_top  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).limit)); }
#line 2538 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_limit: /* opt_limit  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).limit)); }
#line 2544 "bison_parser.cpp"
        break;

    case YYSYMBOL_expr_list: /* expr_list  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).expr_vec)) {
    for (auto ptr : *(((*yyvaluep).expr_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).expr_vec));
}
#line 2557 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_extended_literal_list: /* opt_extended_literal_list  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).expr_vec)) {
    for (auto ptr : *(((*yyvaluep).expr_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).expr_vec));
}
#line 2570 "bison_parser.cpp"
        break;

    case YYSYMBOL_extended_literal_list: /* extended_literal_list  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).expr_vec)) {
    for (auto ptr : *(((*yyvaluep).expr_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).expr_vec));
}
#line 2583 "bison_parser.cpp"
        break;

    case YYSYMBOL_casted_extended_literal: /* casted_extended_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2589 "bison_parser.cpp"
        break;

    case YYSYMBOL_extended_literal: /* extended_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2595 "bison_parser.cpp"
        break;

    case YYSYMBOL_expr_alias: /* expr_alias  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2601 "bison_parser.cpp"
        break;

    case YYSYMBOL_expr: /* expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2607 "bison_parser.cpp"
        break;

    case YYSYMBOL_operand: /* operand  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2613 "bison_parser.cpp"
        break;

    case YYSYMBOL_scalar_expr: /* scalar_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2619 "bison_parser.cpp"
        break;

    case YYSYMBOL_unary_expr: /* unary_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2625 "bison_parser.cpp"
        break;

    case YYSYMBOL_binary_expr: /* binary_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2631 "bison_parser.cpp"
        break;

    case YYSYMBOL_logic_expr: /* logic_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2637 "bison_parser.cpp"
        break;

    case YYSYMBOL_in_expr: /* in_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2643 "bison_parser.cpp"
        break;

    case YYSYMBOL_case_expr: /* case_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2649 "bison_parser.cpp"
        break;

    case YYSYMBOL_case_list: /* case_list  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2655 "bison_parser.cpp"
        break;

    case YYSYMBOL_exists_expr: /* exists_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2661 "bison_parser.cpp"
        break;

    case YYSYMBOL_comp_expr: /* comp_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2667 "bison_parser.cpp"
        break;

    case YYSYMBOL_function_expr: /* function_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2673 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_window: /* opt_window  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).window_description)); }
#line 2679 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_partition: /* opt_partition  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).expr_vec)) {
    for (auto ptr : *(((*yyvaluep).expr_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).expr_vec));
}
#line 2692 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_frame_clause: /* opt_frame_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).frame_description)); }
#line 2698 "bison_parser.cpp"
        break;

    case YYSYMBOL_frame_type: /* frame_type  */
#line 173 "bison_parser.y"
            { }
#line 2704 "bison_parser.cpp"
        break;

    case YYSYMBOL_frame_bound: /* frame_bound  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).frame_bound)); }
#line 2710 "bison_parser.cpp"
        break;

    case YYSYMBOL_extract_expr: /* extract_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2716 "bison_parser.cpp"
        break;

    case YYSYMBOL_cast_expr: /* cast_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2722 "bison_parser.cpp"
        break;

    case YYSYMBOL_datetime_field: /* datetime_field  */
#line 173 "bison_parser.y"
            { }
#line 2728 "bison_parser.cpp"
        break;

    case YYSYMBOL_datetime_field_plural: /* datetime_field_plural  */
#line 173 "bison_parser.y"
            { }
#line 2734 "bison_parser.cpp"
        break;

    case YYSYMBOL_duration_field: /* duration_field  */
#line 173 "bison_parser.y"
            { }
#line 2740 "bison_parser.cpp"
        break;

    case YYSYMBOL_array_expr: /* array_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2746 "bison_parser.cpp"
        break;

    case YYSYMBOL_array_index: /* array_index  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2752 "bison_parser.cpp"
        break;

    case YYSYMBOL_between_expr: /* between_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2758 "bison_parser.cpp"
        break;

    case YYSYMBOL_column_name: /* column_name  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2764 "bison_parser.cpp"
        break;

    case YYSYMBOL_literal: /* literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2770 "bison_parser.cpp"
        break;

    case YYSYMBOL_string_literal: /* string_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2776 "bison_parser.cpp"
        break;

    case YYSYMBOL_bool_literal: /* bool_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2782 "bison_parser.cpp"
        break;

    case YYSYMBOL_num_literal: /* num_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2788 "bison_parser.cpp"
        break;

    case YYSYMBOL_int_literal: /* int_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2794 "bison_parser.cpp"
        break;

    case YYSYMBOL_null_literal: /* null_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2800 "bison_parser.cpp"
        break;

    case YYSYMBOL_date_literal: /* date_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2806 "bison_parser.cpp"
        break;

    case YYSYMBOL_interval_literal: /* interval_literal  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2812 "bison_parser.cpp"
        break;

    case YYSYMBOL_param_expr: /* param_expr  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2818 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_ref: /* table_ref  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2824 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_ref_atomic: /* table_ref_atomic  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2830 "bison_parser.cpp"
        break;

    case YYSYMBOL_nonjoin_table_ref_atomic: /* nonjoin_table_ref_atomic  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2836 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_ref_commalist: /* table_ref_commalist  */
#line 187 "bison_parser.y"
            {
  if (((*yyvaluep).table_vec)) {
    for (auto ptr : *(((*yyvaluep).table_vec))) {
      delete ptr;
    }
  }
  delete (((*yyvaluep).table_vec));
}
#line 2849 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_ref_name: /* table_ref_name  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2855 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_ref_name_no_alias: /* table_ref_name_no_alias  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2861 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_name: /* table_name  */
#line 174 "bison_parser.y"
            {
  free( (((*yyvaluep).table_name).name) );
  free( (((*yyvaluep).table_name).schema) );
}
#line 2870 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_index_name: /* opt_index_name  */
#line 186 "bison_parser.y"
            { free( (((*yyvaluep).sval)) ); }
#line 2876 "bison_parser.cpp"
        break;

    case YYSYMBOL_table_alias: /* table_alias  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).alias_t)); }
#line 2882 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_table_alias: /* opt_table_alias  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).alias_t)); }
#line 2888 "bison_parser.cpp"
        break;

    case YYSYMBOL_alias: /* alias  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).alias_t)); }
#line 2894 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_alias: /* opt_alias  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).alias_t)); }
#line 2900 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_locking_clause: /* opt_locking_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).locking_clause_vec)); }
#line 2906 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_locking_clause_list: /* opt_locking_clause_list  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).locking_clause_vec)); }
#line 2912 "bison_parser.cpp"
        break;

    case YYSYMBOL_locking_clause: /* locking_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).locking_t)); }
#line 2918 "bison_parser.cpp"
        break;

    case YYSYMBOL_row_lock_mode: /* row_lock_mode  */
#line 173 "bison_parser.y"
            { }
#line 2924 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_row_lock_policy: /* opt_row_lock_policy  */
#line 173 "bison_parser.y"
            { }
#line 2930 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_with_clause: /* opt_with_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).with_description_vec)); }
#line 2936 "bison_parser.cpp"
        break;

    case YYSYMBOL_with_clause: /* with_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).with_description_vec)); }
#line 2942 "bison_parser.cpp"
        break;

    case YYSYMBOL_with_description_list: /* with_description_list  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).with_description_vec)); }
#line 2948 "bison_parser.cpp"
        break;

    case YYSYMBOL_with_description: /* with_description  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).with_description_t)); }
#line 2954 "bison_parser.cpp"
        break;

    case YYSYMBOL_join_clause: /* join_clause  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).table)); }
#line 2960 "bison_parser.cpp"
        break;

    case YYSYMBOL_opt_join_type: /* opt_join_type  */
#line 173 "bison_parser.y"
            { }
#line 2966 "bison_parser.cpp"
        break;

    case YYSYMBOL_join_condition: /* join_condition  */
#line 195 "bison_parser.y"
            { delete (((*yyvaluep).expr)); }
#line 2972 "bison_parser.cpp"
        break;

    case YYSYMBOL_ident_commalist: /* ident_commalist  */
#line 178 "bison_parser.y"
            {
  if (((*yyvaluep).str_vec)) {
    for (auto ptr : *(((*yyvaluep).str_vec))) {
      free(ptr);
    }
  }
  delete (((*yyvaluep).str_vec));
}
#line 2985 "bison_parser.cpp"
        break;

      default:
        break;
    }
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (hsql::SQLParserResult* result, yyscan_t scanner)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

/* Location data for the lookahead symbol.  */
static YYLTYPE yyloc_default
# if defined HSQL_LTYPE_IS_TRIVIAL && HSQL_LTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
YYLTYPE yylloc = yyloc_default;

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

    /* The location stack: array, bottom, top.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls = yylsa;
    YYLTYPE *yylsp = yyls;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

  /* The locations where the error started and ended.  */
  YYLTYPE yyerror_range[3];

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = SQL_HSQL_EMPTY; /* Cause a token to be read.  */


/* User initialization code.  */
#line 76 "bison_parser.y"
{
  // Initialize
  yylloc.first_column = 0;
  yylloc.last_column = 0;
  yylloc.first_line = 0;
  yylloc.last_line = 0;
  yylloc.total_column = 0;
  yylloc.string_length = 0;
}

#line 3093 "bison_parser.cpp"

  yylsp[0] = yylloc;
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yyls1, yysize * YYSIZEOF (*yylsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
        yyls = yyls1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == SQL_HSQL_EMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, &yylloc, scanner);
    }

  if (yychar <= SQL_YYEOF)
    {
      yychar = SQL_YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == SQL_HSQL_error)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = SQL_HSQL_UNDEF;
      yytoken = YYSYMBOL_YYerror;
      yyerror_range[1] = yylloc;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;

  /* Discard the shifted token.  */
  yychar = SQL_HSQL_EMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location. */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  yyerror_range[1] = yyloc;
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* input: statement_list opt_semicolon  */
#line 328 "bison_parser.y"
                                     {
  for (SQLStatement* stmt : *(yyvsp[-1].stmt_vec)) {
    // Transfers ownership of the statement.
    result->addStatement(stmt);
  }

  unsigned param_id = 0;
  for (void* param : yyloc.param_list) {
    if (param) {
      Expr* expr = (Expr*)param;
      expr->ival = param_id;
      result->addParameter(expr);
      ++param_id;
    }
  }
    delete (yyvsp[-1].stmt_vec);
  }
#line 3322 "bison_parser.cpp"
    break;

  case 3: /* statement_list: statement  */
#line 347 "bison_parser.y"
                           {
  (yyvsp[0].statement)->stringLength = yylloc.string_length;
  yylloc.string_length = 0;
  (yyval.stmt_vec) = new std::vector<SQLStatement*>();
  (yyval.stmt_vec)->push_back((yyvsp[0].statement));
}
#line 3333 "bison_parser.cpp"
    break;

  case 4: /* statement_list: statement_list ';' statement  */
#line 353 "bison_parser.y"
                               {
  (yyvsp[0].statement)->stringLength = yylloc.string_length;
  yylloc.string_length = 0;
  (yyvsp[-2].stmt_vec)->push_back((yyvsp[0].statement));
  (yyval.stmt_vec) = (yyvsp[-2].stmt_vec);
}
#line 3344 "bison_parser.cpp"
    break;

  case 5: /* statement: prepare_statement opt_hints  */
#line 360 "bison_parser.y"
                                        {
  (yyval.statement) = (yyvsp[-1].prep_stmt);
  (yyval.statement)->hints = (yyvsp[0].expr_vec);
}
#line 3353 "bison_parser.cpp"
    break;

  case 6: /* statement: preparable_statement opt_hints  */
#line 364 "bison_parser.y"
                                 {
  (yyval.statement) = (yyvsp[-1].statement);
  (yyval.statement)->hints = (yyvsp[0].expr_vec);
}
#line 3362 "bison_parser.cpp"
    break;

  case 7: /* statement: show_statement  */
#line 368 "bison_parser.y"
                 { (yyval.statement) = (yyvsp[0].show_stmt); }
#line 3368 "bison_parser.cpp"
    break;

  case 8: /* statement: import_statement  */
#line 369 "bison_parser.y"
                   { (yyval.statement) = (yyvsp[0].import_stmt); }
#line 3374 "bison_parser.cpp"
    break;

  case 9: /* statement: export_statement  */
#line 370 "bison_parser.y"
                   { (yyval.statement) = (yyvsp[0].export_stmt); }
#line 3380 "bison_parser.cpp"
    break;

  case 10: /* preparable_statement: select_statement  */
#line 372 "bison_parser.y"
                                        { (yyval.statement) = (yyvsp[0].select_stmt); }
#line 3386 "bison_parser.cpp"
    break;

  case 11: /* preparable_statement: create_statement  */
#line 373 "bison_parser.y"
                   { (yyval.statement) = (yyvsp[0].create_stmt); }
#line 3392 "bison_parser.cpp"
    break;

  case 12: /* preparable_statement: insert_statement  */
#line 374 "bison_parser.y"
                   { (yyval.statement) = (yyvsp[0].insert_stmt); }
#line 3398 "bison_parser.cpp"
    break;

  case 13: /* preparable_statement: delete_statement  */
#line 375 "bison_parser.y"
                   { (yyval.statement) = (yyvsp[0].delete_stmt); }
#line 3404 "bison_parser.cpp"
    break;

  case 14: /* preparable_statement: truncate_statement  */
#line 376 "bison_parser.y"
                     { (yyval.statement) = (yyvsp[0].delete_stmt); }
#line 3410 "bison_parser.cpp"
    break;

  case 15: /* preparable_statement: update_statement  */
#line 377 "bison_parser.y"
                   { (yyval.statement) = (yyvsp[0].update_stmt); }
#line 3416 "bison_parser.cpp"
    break;

  case 16: /* preparable_statement: drop_statement  */
#line 378 "bison_parser.y"
                 { (yyval.statement) = (yyvsp[0].drop_stmt); }
#line 3422 "bison_parser.cpp"
    break;

  case 17: /* preparable_statement: alter_statement  */
#line 379 "bison_parser.y"
                  { (yyval.statement) = (yyvsp[0].alter_stmt); }
#line 3428 "bison_parser.cpp"
    break;

  case 18: /* preparable_statement: execute_statement  */
#line 380 "bison_parser.y"
                    { (yyval.statement) = (yyvsp[0].exec_stmt); }
#line 3434 "bison_parser.cpp"
    break;

  case 19: /* preparable_statement: transaction_statement  */
#line 381 "bison_parser.y"
                        { (yyval.statement) = (yyvsp[0].transaction_stmt); }
#line 3440 "bison_parser.cpp"
    break;

  case 20: /* opt_hints: WITH HINT '(' hint_list ')'  */
#line 387 "bison_parser.y"
                                        { (yyval.expr_vec) = (yyvsp[-1].expr_vec); }
#line 3446 "bison_parser.cpp"
    break;

  case 21: /* opt_hints: %empty  */
#line 388 "bison_parser.y"
              { (yyval.expr_vec) = nullptr; }
#line 3452 "bison_parser.cpp"
    break;

  case 22: /* hint_list: hint  */
#line 390 "bison_parser.y"
                 {
  (yyval.expr_vec) = new std::vector<Expr*>();
  (yyval.expr_vec)->push_back((yyvsp[0].expr));
}
#line 3461 "bison_parser.cpp"
    break;

  case 23: /* hint_list: hint_list ',' hint  */
#line 394 "bison_parser.y"
                     {
  (yyvsp[-2].expr_vec)->push_back((yyvsp[0].expr));
  (yyval.expr_vec) = (yyvsp[-2].expr_vec);
}
#line 3470 "bison_parser.cpp"
    break;

  case 24: /* hint: IDENTIFIER  */
#line 399 "bison_parser.y"
                  {
  (yyval.expr) = Expr::make(kExprHint);
  (yyval.expr)->name = (yyvsp[0].sval);
}
#line 3479 "bison_parser.cpp"
    break;

  case 25: /* hint: IDENTIFIER '(' extended_literal_list ')'  */
#line 403 "bison_parser.y"
                                           {
  (yyval.expr) = Expr::make(kExprHint);
  (yyval.expr)->name = (yyvsp[-3].sval);
  (yyval.expr)->exprList = (yyvsp[-1].expr_vec);
}
#line 3489 "bison_parser.cpp"
    break;

  case 26: /* transaction_statement: BEGIN opt_transaction_keyword  */
#line 413 "bison_parser.y"
                                                      { (yyval.transaction_stmt) = new TransactionStatement(kBeginTransaction); }
#line 3495 "bison_parser.cpp"
    break;

  case 27: /* transaction_statement: ROLLBACK opt_transaction_keyword  */
#line 414 "bison_parser.y"
                                   { (yyval.transaction_stmt) = new TransactionStatement(kRollbackTransaction); }
#line 3501 "bison_parser.cpp"
    break;

  case 28: /* transaction_statement: COMMIT opt_transaction_keyword  */
#line 415 "bison_parser.y"
                                 { (yyval.transaction_stmt) = new TransactionStatement(kCommitTransaction); }
#line 3507 "bison_parser.cpp"
    break;

  case 31: /* prepare_statement: PREPARE IDENTIFIER FROM prepare_target_query  */
#line 423 "bison_parser.y"
                                                                 {
  (yyval.prep_stmt) = new PrepareStatement();
  (yyval.prep_stmt)->name = (yyvsp[-2].sval);
  (yyval.prep_stmt)->query = (yyvsp[0].sval);
}
#line 3517 "bison_parser.cpp"
    break;

  case 33: /* execute_statement: EXECUTE IDENTIFIER  */
#line 431 "bison_parser.y"
                                       {
  (yyval.exec_stmt) = new ExecuteStatement();
  (yyval.exec_stmt)->name = (yyvsp[0].sval);
}
#line 3526 "bison_parser.cpp"
    break;

  case 34: /* execute_statement: EXECUTE IDENTIFIER '(' opt_extended_literal_list ')'  */
#line 435 "bison_parser.y"
                                                       {
  (yyval.exec_stmt) = new ExecuteStatement();
  (yyval.exec_stmt)->name = (yyvsp[-3].sval);
  (yyval.exec_stmt)->parameters = (yyvsp[-1].expr_vec);
}
#line 3536 "bison_parser.cpp"
    break;

  case 35: /* import_statement: IMPORT FROM file_type FILE file_path INTO table_name  */
#line 447 "bison_parser.y"
                                                                        {
  (yyval.import_stmt) = new ImportStatement((yyvsp[-4].import_type_t));
  (yyval.import_stmt)->filePath = (yyvsp[-2].sval);
  (yyval.import_stmt)->schema = (yyvsp[0].table_name).schema;
  (yyval.import_stmt)->tableName = (yyvsp[0].table_name).name;
}
#line 3547 "bison_parser.cpp"
    break;

  case 36: /* import_statement: COPY table_name FROM file_path opt_import_export_options opt_where  */
#line 453 "bison_parser.y"
                                                                     {
  (yyval.import_stmt) = new ImportStatement((yyvsp[-1].import_export_option_t)->format);
  (yyval.import_stmt)->filePath = (yyvsp[-2].sval);
  (yyval.import_stmt)->schema = (yyvsp[-4].table_name).schema;
  (yyval.import_stmt)->tableName = (yyvsp[-4].table_name).name;
  (yyval.import_stmt)->whereClause = (yyvsp[0].expr);
  if ((yyvsp[-1].import_export_option_t)->encoding) {
    (yyval.import_stmt)->encoding = (yyvsp[-1].import_export_option_t)->encoding;
    (yyvsp[-1].import_export_option_t)->encoding = nullptr;
  }
  delete (yyvsp[-1].import_export_option_t);
}
#line 3564 "bison_parser.cpp"
    break;

  case 37: /* file_type: IDENTIFIER  */
#line 466 "bison_parser.y"
                       {
  if (strcasecmp((yyvsp[0].sval), "csv") == 0) {
    (yyval.import_type_t) = kImportCSV;
  } else if (strcasecmp((yyvsp[0].sval), "tbl") == 0) {
    (yyval.import_type_t) = kImportTbl;
  } else if (strcasecmp((yyvsp[0].sval), "binary") == 0 || strcasecmp((yyvsp[0].sval), "bin") == 0) {
    (yyval.import_type_t) = kImportBinary;
  } else {
    free((yyvsp[0].sval));
    yyerror(&yyloc, result, scanner, "File type is unknown.");
    YYERROR;
  }
  free((yyvsp[0].sval));
}
#line 3583 "bison_parser.cpp"
    break;

  case 38: /* file_path: STRING  */
#line 481 "bison_parser.y"
                   {
  (yyval.sval) = (yyvsp[0].sval);
}
#line 3591 "bison_parser.cpp"
    break;

  case 39: /* opt_import_export_options: WITH '(' import_export_options ')'  */
#line 485 "bison_parser.y"
                                                               { (yyval.import_export_option_t) = (yyvsp[-1].import_export_option_t); }
#line 3597 "bison_parser.cpp"
    break;

  case 40: /* opt_import_export_options: '(' import_export_options ')'  */
#line 486 "bison_parser.y"
                                { (yyval.import_export_option_t) = (yyvsp[-1].import_export_option_t); }
#line 3603 "bison_parser.cpp"
    break;

  case 41: /* opt_import_export_options: %empty  */
#line 487 "bison_parser.y"
              { (yyval.import_export_option_t) = new ImportExportOptions{}; }
#line 3609 "bison_parser.cpp"
    break;

  case 42: /* import_export_options: import_export_options ',' FORMAT file_type  */
#line 489 "bison_parser.y"
                                                                   {
  if ((yyvsp[-3].import_export_option_t)->format != kImportAuto) {
    delete (yyvsp[-3].import_export_option_t);
    yyerror(&yyloc, result, scanner, "File type must only be provided once.");
    YYERROR;
  }
  (yyvsp[-3].import_export_option_t)->format = (yyvsp[0].import_type_t);
  (yyval.import_export_option_t) = (yyvsp[-3].import_export_option_t);
}
#line 3623 "bison_parser.cpp"
    break;

  case 43: /* import_export_options: FORMAT file_type  */
#line 498 "bison_parser.y"
                   {
  (yyval.import_export_option_t) = new ImportExportOptions{};
  (yyval.import_export_option_t)->format = (yyvsp[0].import_type_t);
}
#line 3632 "bison_parser.cpp"
    break;

  case 44: /* import_export_options: import_export_options ',' ENCODING STRING  */
#line 502 "bison_parser.y"
                                            {
  if ((yyvsp[-3].import_export_option_t)->encoding) {
    delete (yyvsp[-3].import_export_option_t);
    free((yyvsp[0].sval));
    yyerror(&yyloc, result, scanner, "Encoding type must only be provided once.");
    YYERROR;
  }
  (yyvsp[-3].import_export_option_t)->encoding = (yyvsp[0].sval);
  (yyval.import_export_option_t) = (yyvsp[-3].import_export_option_t);
}
#line 3647 "bison_parser.cpp"
    break;

  case 45: /* import_export_options: ENCODING STRING  */
#line 512 "bison_parser.y"
                  {
  (yyval.import_export_option_t) = new ImportExportOptions{};
  (yyval.import_export_option_t)->encoding = (yyvsp[0].sval);
}
#line 3656 "bison_parser.cpp"
    break;

  case 46: /* export_statement: COPY table_name TO file_path opt_import_export_options  */
#line 522 "bison_parser.y"
                                                                          {
  (yyval.export_stmt) = new ExportStatement((yyvsp[0].import_export_option_t)->format);
  (yyval.export_stmt)->filePath = (yyvsp[-1].sval);
  (yyval.export_stmt)->schema = (yyvsp[-3].table_name).schema;
  (yyval.export_stmt)->tableName = (yyvsp[-3].table_name).name;
  if ((yyvsp[0].import_export_option_t)->encoding) {
    (yyval.export_stmt)->encoding = (yyvsp[0].import_export_option_t)->encoding;
    (yyvsp[0].import_export_option_t)->encoding = nullptr;
  }
  delete (yyvsp[0].import_export_option_t);
}
#line 3672 "bison_parser.cpp"
    break;

  case 47: /* export_statement: COPY select_with_paren TO file_path opt_import_export_options  */
#line 533 "bison_parser.y"
                                                                {
  (yyval.export_stmt) = new ExportStatement((yyvsp[0].import_export_option_t)->format);
  (yyval.export_stmt)->filePath = (yyvsp[-1].sval);
  (yyval.export_stmt)->select = (yyvsp[-3].select_stmt);
  if ((yyvsp[0].import_export_option_t)->encoding) {
    (yyval.export_stmt)->encoding = (yyvsp[0].import_export_option_t)->encoding;
    (yyvsp[0].import_export_option_t)->encoding = nullptr;
  }
  delete (yyvsp[0].import_export_option_t);
}
#line 3687 "bison_parser.cpp"
    break;

  case 48: /* show_statement: SHOW TABLES  */
#line 549 "bison_parser.y"
                             { (yyval.show_stmt) = new ShowStatement(kShowTables); }
#line 3693 "bison_parser.cpp"
    break;

  case 49: /* show_statement: SHOW COLUMNS table_name  */
#line 550 "bison_parser.y"
                          {
  (yyval.show_stmt) = new ShowStatement(kShowColumns);
  (yyval.show_stmt)->schema = (yyvsp[0].table_name).schema;
  (yyval.show_stmt)->name = (yyvsp[0].table_name).name;
}
#line 3703 "bison_parser.cpp"
    break;

  case 50: /* show_statement: DESCRIBE table_name  */
#line 555 "bison_parser.y"
                      {
  (yyval.show_stmt) = new ShowStatement(kShowColumns);
  (yyval.show_stmt)->schema = (yyvsp[0].table_name).schema;
  (yyval.show_stmt)->name = (yyvsp[0].table_name).name;
}
#line 3713 "bison_parser.cpp"
    break;

  case 51: /* create_statement: CREATE TABLE opt_not_exists table_name FROM IDENTIFIER FILE file_path  */
#line 566 "bison_parser.y"
                                                                                         {
  (yyval.create_stmt) = new CreateStatement(kCreateTableFromTbl);
  (yyval.create_stmt)->ifNotExists = (yyvsp[-5].bval);
  (yyval.create_stmt)->schema = (yyvsp[-4].table_name).schema;
  (yyval.create_stmt)->tableName = (yyvsp[-4].table_name).name;
  if (strcasecmp((yyvsp[-2].sval), "tbl") != 0) {
    free((yyvsp[-2].sval));
    yyerror(&yyloc, result, scanner, "File type is unknown.");
    YYERROR;
  }
  free((yyvsp[-2].sval));
  (yyval.create_stmt)->filePath = (yyvsp[0].sval);
}
#line 3731 "bison_parser.cpp"
    break;

  case 52: /* create_statement: CREATE TABLE opt_not_exists table_name '(' table_elem_commalist ')'  */
#line 579 "bison_parser.y"
                                                                      {
  (yyval.create_stmt) = new CreateStatement(kCreateTable);
  (yyval.create_stmt)->ifNotExists = (yyvsp[-4].bval);
  (yyval.create_stmt)->schema = (yyvsp[-3].table_name).schema;
  (yyval.create_stmt)->tableName = (yyvsp[-3].table_name).name;
  (yyval.create_stmt)->setColumnDefsAndConstraints((yyvsp[-1].table_element_vec));
  delete (yyvsp[-1].table_element_vec);
  if (result->errorMsg()) {
    delete (yyval.create_stmt);
    YYERROR;
  }
}
#line 3748 "bison_parser.cpp"
    break;

  case 53: /* create_statement: CREATE TABLE opt_not_exists table_name AS select_statement  */
#line 591 "bison_parser.y"
                                                             {
  (yyval.create_stmt) = new CreateStatement(kCreateTable);
  (yyval.create_stmt)->ifNotExists = (yyvsp[-3].bval);
  (yyval.create_stmt)->schema = (yyvsp[-2].table_name).schema;
  (yyval.create_stmt)->tableName = (yyvsp[-2].table_name).name;
  (yyval.create_stmt)->select = (yyvsp[0].select_stmt);
}
#line 3760 "bison_parser.cpp"
    break;

  case 54: /* create_statement: CREATE INDEX opt_not_exists opt_index_name ON table_name '(' ident_commalist ')'  */
#line 598 "bison_parser.y"
                                                                                   {
  (yyval.create_stmt) = new CreateStatement(kCreateIndex);
  (yyval.create_stmt)->indexName = (yyvsp[-5].sval);
  (yyval.create_stmt)->ifNotExists = (yyvsp[-6].bval);
  (yyval.create_stmt)->tableName = (yyvsp[-3].table_name).name;
  (yyval.create_stmt)->indexColumns = (yyvsp[-1].str_vec);
}
#line 3772 "bison_parser.cpp"
    break;

  case 55: /* create_statement: CREATE VIEW opt_not_exists table_name opt_column_list AS select_statement  */
#line 605 "bison_parser.y"
                                                                            {
  (yyval.create_stmt) = new CreateStatement(kCreateView);
  (yyval.create_stmt)->ifNotExists = (yyvsp[-4].bval);
  (yyval.create_stmt)->schema = (yyvsp[-3].table_name).schema;
  (yyval.create_stmt)->tableName = (yyvsp[-3].table_name).name;
  (yyval.create_stmt)->viewColumns = (yyvsp[-2].str_vec);
  (yyval.create_stmt)->select = (yyvsp[0].select_stmt);
}
#line 3785 "bison_parser.cpp"
    break;

  case 56: /* opt_not_exists: IF NOT EXISTS  */
#line 614 "bison_parser.y"
                               { (yyval.bval) = true; }
#line 3791 "bison_parser.cpp"
    break;

  case 57: /* opt_not_exists: %empty  */
#line 615 "bison_parser.y"
              { (yyval.bval) = false; }
#line 3797 "bison_parser.cpp"
    break;

  case 58: /* table_elem_commalist: table_elem  */
#line 617 "bison_parser.y"
                                  {
  (yyval.table_element_vec) = new std::vector<TableElement*>();
  (yyval.table_element_vec)->push_back((yyvsp[0].table_element_t));
}
#line 3806 "bison_parser.cpp"
    break;

  case 59: /* table_elem_commalist: table_elem_commalist ',' table_elem  */
#line 621 "bison_parser.y"
                                      {
  (yyvsp[-2].table_element_vec)->push_back((yyvsp[0].table_element_t));
  (yyval.table_element_vec) = (yyvsp[-2].table_element_vec);
}
#line 3815 "bison_parser.cpp"
    break;

  case 60: /* table_elem: column_def  */
#line 626 "bison_parser.y"
                        { (yyval.table_element_t) = (yyvsp[0].column_t); }
#line 3821 "bison_parser.cpp"
    break;

  case 61: /* table_elem: table_constraint  */
#line 627 "bison_parser.y"
                   { (yyval.table_element_t) = (yyvsp[0].table_constraint_t); }
#line 3827 "bison_parser.cpp"
    break;

  case 62: /* column_def: IDENTIFIER column_type opt_column_constraints  */
#line 629 "bison_parser.y"
                                                           {
  (yyval.column_t) = new ColumnDefinition((yyvsp[-2].sval), (yyvsp[-1].column_type_t), (yyvsp[0].column_constraint_set));
  if (!(yyval.column_t)->trySetNullableExplicit()) {
    yyerror(&yyloc, result, scanner, ("Conflicting nullability constraints for " + std::string{(yyvsp[-2].sval)}).c_str());
  }
}
#line 3838 "bison_parser.cpp"
    break;

  case 63: /* column_type: BIGINT  */
#line 636 "bison_parser.y"
                     { (yyval.column_type_t) = ColumnType{DataType::BIGINT}; }
#line 3844 "bison_parser.cpp"
    break;

  case 64: /* column_type: BOOLEAN  */
#line 637 "bison_parser.y"
          { (yyval.column_type_t) = ColumnType{DataType::BOOLEAN}; }
#line 3850 "bison_parser.cpp"
    break;

  case 65: /* column_type: CHAR '(' INTVAL ')'  */
#line 638 "bison_parser.y"
                      { (yyval.column_type_t) = ColumnType{DataType::CHAR, (yyvsp[-1].ival)}; }
#line 3856 "bison_parser.cpp"
    break;

  case 66: /* column_type: CHARACTER_VARYING '(' INTVAL ')'  */
#line 639 "bison_parser.y"
                                   { (yyval.column_type_t) = ColumnType{DataType::VARCHAR, (yyvsp[-1].ival)}; }
#line 3862 "bison_parser.cpp"
    break;

  case 67: /* column_type: DATE  */
#line 640 "bison_parser.y"
       { (yyval.column_type_t) = ColumnType{DataType::DATE}; }
#line 3868 "bison_parser.cpp"
    break;

  case 68: /* column_type: DATETIME  */
#line 641 "bison_parser.y"
           { (yyval.column_type_t) = ColumnType{DataType::DATETIME}; }
#line 3874 "bison_parser.cpp"
    break;

  case 69: /* column_type: DECIMAL opt_decimal_specification  */
#line 642 "bison_parser.y"
                                    {
  (yyval.column_type_t) = ColumnType{DataType::DECIMAL, 0, (yyvsp[0].ival_pair)->first, (yyvsp[0].ival_pair)->second};
  delete (yyvsp[0].ival_pair);
}
#line 3883 "bison_parser.cpp"
    break;

  case 70: /* column_type: DOUBLE  */
#line 646 "bison_parser.y"
         { (yyval.column_type_t) = ColumnType{DataType::DOUBLE}; }
#line 3889 "bison_parser.cpp"
    break;

  case 71: /* column_type: FLOAT  */
#line 647 "bison_parser.y"
        { (yyval.column_type_t) = ColumnType{DataType::FLOAT}; }
#line 3895 "bison_parser.cpp"
    break;

  case 72: /* column_type: INT '(' INTVAL ')'  */
#line 648 "bison_parser.y"
                     { (yyval.column_type_t) = ColumnType{DataType::INT, (yyvsp[-1].ival)}; }
#line 3901 "bison_parser.cpp"
    break;

  case 73: /* column_type: INTEGER  */
#line 649 "bison_parser.y"
          { (yyval.column_type_t) = ColumnType{DataType::INT}; }
#line 3907 "bison_parser.cpp"
    break;

  case 74: /* column_type: LONG  */
#line 650 "bison_parser.y"
       { (yyval.column_type_t) = ColumnType{DataType::LONG}; }
#line 3913 "bison_parser.cpp"
    break;

  case 75: /* column_type: REAL  */
#line 651 "bison_parser.y"
       { (yyval.column_type_t) = ColumnType{DataType::REAL}; }
#line 3919 "bison_parser.cpp"
    break;

  case 76: /* column_type: SMALLINT  */
#line 652 "bison_parser.y"
           { (yyval.column_type_t) = ColumnType{DataType::SMALLINT}; }
#line 3925 "bison_parser.cpp"
    break;

  case 77: /* column_type: TEXT  */
#line 653 "bison_parser.y"
       { (yyval.column_type_t) = ColumnType{DataType::TEXT}; }
#line 3931 "bison_parser.cpp"
    break;

  case 78: /* column_type: TIME opt_time_precision  */
#line 654 "bison_parser.y"
                          { (yyval.column_type_t) = ColumnType{DataType::TIME, 0, (yyvsp[0].ival)}; }
#line 3937 "bison_parser.cpp"
    break;

  case 79: /* column_type: TIMESTAMP  */
#line 655 "bison_parser.y"
            { (yyval.column_type_t) = ColumnType{DataType::DATETIME}; }
#line 3943 "bison_parser.cpp"
    break;

  case 80: /* column_type: VARCHAR '(' INTVAL ')'  */
#line 656 "bison_parser.y"
                         { (yyval.column_type_t) = ColumnType{DataType::VARCHAR, (yyvsp[-1].ival)}; }
#line 3949 "bison_parser.cpp"
    break;

  case 81: /* opt_time_precision: '(' INTVAL ')'  */
#line 658 "bison_parser.y"
                                    { (yyval.ival) = (yyvsp[-1].ival); }
#line 3955 "bison_parser.cpp"
    break;

  case 82: /* opt_time_precision: %empty  */
#line 659 "bison_parser.y"
              { (yyval.ival) = 0; }
#line 3961 "bison_parser.cpp"
    break;

  case 83: /* opt_decimal_specification: '(' INTVAL ',' INTVAL ')'  */
#line 661 "bison_parser.y"
                                                      { (yyval.ival_pair) = new std::pair<int64_t, int64_t>{(yyvsp[-3].ival), (yyvsp[-1].ival)}; }
#line 3967 "bison_parser.cpp"
    break;

  case 84: /* opt_decimal_specification: '(' INTVAL ')'  */
#line 662 "bison_parser.y"
                 { (yyval.ival_pair) = new std::pair<int64_t, int64_t>{(yyvsp[-1].ival), 0}; }
#line 3973 "bison_parser.cpp"
    break;

  case 85: /* opt_decimal_specification: %empty  */
#line 663 "bison_parser.y"
              { (yyval.ival_pair) = new std::pair<int64_t, int64_t>{0, 0}; }
#line 3979 "bison_parser.cpp"
    break;

  case 86: /* opt_column_constraints: column_constraint_set  */
#line 665 "bison_parser.y"
                                               { (yyval.column_constraint_set) = (yyvsp[0].column_constraint_set); }
#line 3985 "bison_parser.cpp"
    break;

  case 87: /* opt_column_constraints: %empty  */
#line 666 "bison_parser.y"
              { (yyval.column_constraint_set) = new std::unordered_set<ConstraintType>(); }
#line 3991 "bison_parser.cpp"
    break;

  case 88: /* column_constraint_set: column_constraint  */
#line 668 "bison_parser.y"
                                          {
  (yyval.column_constraint_set) = new std::unordered_set<ConstraintType>();
  (yyval.column_constraint_set)->insert((yyvsp[0].column_constraint_t));
}
#line 4000 "bison_parser.cpp"
    break;

  case 89: /* column_constraint_set: column_constraint_set column_constraint  */
#line 672 "bison_parser.y"
                                          {
  (yyvsp[-1].column_constraint_set)->insert((yyvsp[0].column_constraint_t));
  (yyval.column_constraint_set) = (yyvsp[-1].column_constraint_set);
}
#line 4009 "bison_parser.cpp"
    break;

  case 90: /* column_constraint: PRIMARY KEY  */
#line 677 "bison_parser.y"
                                { (yyval.column_constraint_t) = ConstraintType::PrimaryKey; }
#line 4015 "bison_parser.cpp"
    break;

  case 91: /* column_constraint: UNIQUE  */
#line 678 "bison_parser.y"
         { (yyval.column_constraint_t) = ConstraintType::Unique; }
#line 4021 "bison_parser.cpp"
    break;

  case 92: /* column_constraint: NULL  */
#line 679 "bison_parser.y"
       { (yyval.column_constraint_t) = ConstraintType::Null; }
#line 4027 "bison_parser.cpp"
    break;

  case 93: /* column_constraint: NOT NULL  */
#line 680 "bison_parser.y"
           { (yyval.column_constraint_t) = ConstraintType::NotNull; }
#line 4033 "bison_parser.cpp"
    break;

  case 94: /* table_constraint: PRIMARY KEY '(' ident_commalist ')'  */
#line 682 "bison_parser.y"
                                                       { (yyval.table_constraint_t) = new TableConstraint(ConstraintType::PrimaryKey, (yyvsp[-1].str_vec)); }
#line 4039 "bison_parser.cpp"
    break;

  case 95: /* table_constraint: UNIQUE '(' ident_commalist ')'  */
#line 683 "bison_parser.y"
                                 { (yyval.table_constraint_t) = new TableConstraint(ConstraintType::Unique, (yyvsp[-1].str_vec)); }
#line 4045 "bison_parser.cpp"
    break;

  case 96: /* drop_statement: DROP TABLE opt_exists table_name  */
#line 691 "bison_parser.y"
                                                  {
  (yyval.drop_stmt) = new DropStatement(kDropTable);
  (yyval.drop_stmt)->ifExists = (yyvsp[-1].bval);
  (yyval.drop_stmt)->schema = (yyvsp[0].table_name).schema;
  (yyval.drop_stmt)->name = (yyvsp[0].table_name).name;
}
#line 4056 "bison_parser.cpp"
    break;

  case 97: /* drop_statement: DROP VIEW opt_exists table_name  */
#line 697 "bison_parser.y"
                                  {
  (yyval.drop_stmt) = new DropStatement(kDropView);
  (yyval.drop_stmt)->ifExists = (yyvsp[-1].bval);
  (yyval.drop_stmt)->schema = (yyvsp[0].table_name).schema;
  (yyval.drop_stmt)->name = (yyvsp[0].table_name).name;
}
#line 4067 "bison_parser.cpp"
    break;

  case 98: /* drop_statement: DEALLOCATE PREPARE IDENTIFIER  */
#line 703 "bison_parser.y"
                                {
  (yyval.drop_stmt) = new DropStatement(kDropPreparedStatement);
  (yyval.drop_stmt)->ifExists = false;
  (yyval.drop_stmt)->name = (yyvsp[0].sval);
}
#line 4077 "bison_parser.cpp"
    break;

  case 99: /* drop_statement: DROP INDEX opt_exists IDENTIFIER  */
#line 709 "bison_parser.y"
                                   {
  (yyval.drop_stmt) = new DropStatement(kDropIndex);
  (yyval.drop_stmt)->ifExists = (yyvsp[-1].bval);
  (yyval.drop_stmt)->indexName = (yyvsp[0].sval);
}
#line 4087 "bison_parser.cpp"
    break;

  case 100: /* opt_exists: IF EXISTS  */
#line 715 "bison_parser.y"
                       { (yyval.bval) = true; }
#line 4093 "bison_parser.cpp"
    break;

  case 101: /* opt_exists: %empty  */
#line 716 "bison_parser.y"
              { (yyval.bval) = false; }
#line 4099 "bison_parser.cpp"
    break;

  case 102: /* alter_statement: ALTER TABLE opt_exists table_name alter_action  */
#line 723 "bison_parser.y"
                                                                 {
  (yyval.alter_stmt) = new AlterStatement((yyvsp[-1].table_name).name, (yyvsp[0].alter_action_t));
  (yyval.alter_stmt)->ifTableExists = (yyvsp[-2].bval);
  (yyval.alter_stmt)->schema = (yyvsp[-1].table_name).schema;
}
#line 4109 "bison_parser.cpp"
    break;

  case 103: /* alter_action: drop_action  */
#line 729 "bison_parser.y"
                           { (yyval.alter_action_t) = (yyvsp[0].drop_action_t); }
#line 4115 "bison_parser.cpp"
    break;

  case 104: /* drop_action: DROP COLUMN opt_exists IDENTIFIER  */
#line 731 "bison_parser.y"
                                                {
  (yyval.drop_action_t) = new DropColumnAction((yyvsp[0].sval));
  (yyval.drop_action_t)->ifExists = (yyvsp[-1].bval);
}
#line 4124 "bison_parser.cpp"
    break;

  case 105: /* delete_statement: DELETE FROM table_name opt_where  */
#line 741 "bison_parser.y"
                                                    {
  (yyval.delete_stmt) = new DeleteStatement();
  (yyval.delete_stmt)->schema = (yyvsp[-1].table_name).schema;
  (yyval.delete_stmt)->tableName = (yyvsp[-1].table_name).name;
  (yyval.delete_stmt)->expr = (yyvsp[0].expr);
}
#line 4135 "bison_parser.cpp"
    break;

  case 106: /* truncate_statement: TRUNCATE table_name  */
#line 748 "bison_parser.y"
                                         {
  (yyval.delete_stmt) = new DeleteStatement();
  (yyval.delete_stmt)->schema = (yyvsp[0].table_name).schema;
  (yyval.delete_stmt)->tableName = (yyvsp[0].table_name).name;
}
#line 4145 "bison_parser.cpp"
    break;

  case 107: /* insert_statement: INSERT INTO table_name opt_column_list VALUES '(' extended_literal_list ')'  */
#line 759 "bison_parser.y"
                                                                                               {
  (yyval.insert_stmt) = new InsertStatement(kInsertValues);
  (yyval.insert_stmt)->schema = (yyvsp[-5].table_name).schema;
  (yyval.insert_stmt)->tableName = (yyvsp[-5].table_name).name;
  (yyval.insert_stmt)->columns = (yyvsp[-4].str_vec);
  (yyval.insert_stmt)->values = (yyvsp[-1].expr_vec);
}
#line 4157 "bison_parser.cpp"
    break;

  case 108: /* insert_statement: INSERT INTO table_name opt_column_list select_no_paren  */
#line 766 "bison_parser.y"
                                                         {
  (yyval.insert_stmt) = new InsertStatement(kInsertSelect);
  (yyval.insert_stmt)->schema = (yyvsp[-2].table_name).schema;
  (yyval.insert_stmt)->tableName = (yyvsp[-2].table_name).name;
  (yyval.insert_stmt)->columns = (yyvsp[-1].str_vec);
  (yyval.insert_stmt)->select = (yyvsp[0].select_stmt);
}
#line 4169 "bison_parser.cpp"
    break;

  case 109: /* opt_column_list: '(' ident_commalist ')'  */
#line 774 "bison_parser.y"
                                          { (yyval.str_vec) = (yyvsp[-1].str_vec); }
#line 4175 "bison_parser.cpp"
    break;

  case 110: /* opt_column_list: %empty  */
#line 775 "bison_parser.y"
              { (yyval.str_vec) = nullptr; }
#line 4181 "bison_parser.cpp"
    break;

  case 111: /* update_statement: UPDATE table_ref_name_no_alias SET update_clause_commalist opt_where  */
#line 782 "bison_parser.y"
                                                                                        {
  (yyval.update_stmt) = new UpdateStatement();
  (yyval.update_stmt)->table = (yyvsp[-3].table);
  (yyval.update_stmt)->updates = (yyvsp[-1].update_vec);
  (yyval.update_stmt)->where = (yyvsp[0].expr);
}
#line 4192 "bison_parser.cpp"
    break;

  case 112: /* update_clause_commalist: update_clause  */
#line 789 "bison_parser.y"
                                        {
  (yyval.update_vec) = new std::vector<UpdateClause*>();
  (yyval.update_vec)->push_back((yyvsp[0].update_t));
}
#line 4201 "bison_parser.cpp"
    break;

  case 113: /* update_clause_commalist: update_clause_commalist ',' update_clause  */
#line 793 "bison_parser.y"
                                            {
  (yyvsp[-2].update_vec)->push_back((yyvsp[0].update_t));
  (yyval.update_vec) = (yyvsp[-2].update_vec);
}
#line 4210 "bison_parser.cpp"
    break;

  case 114: /* update_clause: IDENTIFIER '=' expr  */
#line 798 "bison_parser.y"
                                    {
  (yyval.update_t) = new UpdateClause();
  (yyval.update_t)->column = (yyvsp[-2].sval);
  (yyval.update_t)->value = (yyvsp[0].expr);
}
#line 4220 "bison_parser.cpp"
    break;

  case 115: /* select_statement: opt_with_clause select_with_paren  */
#line 808 "bison_parser.y"
                                                     {
  (yyval.select_stmt) = (yyvsp[0].select_stmt);
  (yyval.select_stmt)->withDescriptions = (yyvsp[-1].with_description_vec);
}
#line 4229 "bison_parser.cpp"
    break;

  case 116: /* select_statement: opt_with_clause select_no_paren  */
#line 812 "bison_parser.y"
                                  {
  (yyval.select_stmt) = (yyvsp[0].select_stmt);
  (yyval.select_stmt)->withDescriptions = (yyvsp[-1].with_description_vec);
}
#line 4238 "bison_parser.cpp"
    break;

  case 117: /* select_statement: opt_with_clause select_with_paren set_operator select_within_set_operation opt_order opt_limit  */
#line 816 "bison_parser.y"
                                                                                                 {
  (yyval.select_stmt) = (yyvsp[-4].select_stmt);
  if ((yyval.select_stmt)->setOperations == nullptr) {
    (yyval.select_stmt)->setOperations = new std::vector<SetOperation*>();
  }
  (yyval.select_stmt)->setOperations->push_back((yyvsp[-3].set_operator_t));
  (yyval.select_stmt)->setOperations->back()->nestedSelectStatement = (yyvsp[-2].select_stmt);
  (yyval.select_stmt)->setOperations->back()->resultOrder = (yyvsp[-1].order_vec);
  (yyval.select_stmt)->setOperations->back()->resultLimit = (yyvsp[0].limit);
  (yyval.select_stmt)->withDescriptions = (yyvsp[-5].with_description_vec);
}
#line 4254 "bison_parser.cpp"
    break;

  case 120: /* select_within_set_operation_no_parentheses: select_clause  */
#line 830 "bison_parser.y"
                                                           { (yyval.select_stmt) = (yyvsp[0].select_stmt); }
#line 4260 "bison_parser.cpp"
    break;

  case 121: /* select_within_set_operation_no_parentheses: select_clause set_operator select_within_set_operation  */
#line 831 "bison_parser.y"
                                                         {
  (yyval.select_stmt) = (yyvsp[-2].select_stmt);
  if ((yyval.select_stmt)->setOperations == nullptr) {
    (yyval.select_stmt)->setOperations = new std::vector<SetOperation*>();
  }
  (yyval.select_stmt)->setOperations->push_back((yyvsp[-1].set_operator_t));
  (yyval.select_stmt)->setOperations->back()->nestedSelectStatement = (yyvsp[0].select_stmt);
}
#line 4273 "bison_parser.cpp"
    break;

  case 122: /* select_with_paren: '(' select_no_paren ')'  */
#line 840 "bison_parser.y"
                                            { (yyval.select_stmt) = (yyvsp[-1].select_stmt); }
#line 4279 "bison_parser.cpp"
    break;

  case 123: /* select_with_paren: '(' select_with_paren ')'  */
#line 841 "bison_parser.y"
                            { (yyval.select_stmt) = (yyvsp[-1].select_stmt); }
#line 4285 "bison_parser.cpp"
    break;

  case 124: /* select_no_paren: select_clause opt_order opt_limit opt_locking_clause  */
#line 843 "bison_parser.y"
                                                                       {
  (yyval.select_stmt) = (yyvsp[-3].select_stmt);
  (yyval.select_stmt)->order = (yyvsp[-2].order_vec);

  // Limit could have been set by TOP.
  if ((yyvsp[-1].limit)) {
    delete (yyval.select_stmt)->limit;
    (yyval.select_stmt)->limit = (yyvsp[-1].limit);
  }

  if ((yyvsp[0].locking_clause_vec)) {
    (yyval.select_stmt)->lockings = (yyvsp[0].locking_clause_vec);
  }
}
#line 4304 "bison_parser.cpp"
    break;

  case 125: /* select_no_paren: select_clause set_operator select_within_set_operation opt_order opt_limit opt_locking_clause  */
#line 857 "bison_parser.y"
                                                                                                {
  (yyval.select_stmt) = (yyvsp[-5].select_stmt);
  if ((yyval.select_stmt)->setOperations == nullptr) {
    (yyval.select_stmt)->setOperations = new std::vector<SetOperation*>();
  }
  (yyval.select_stmt)->setOperations->push_back((yyvsp[-4].set_operator_t));
  (yyval.select_stmt)->setOperations->back()->nestedSelectStatement = (yyvsp[-3].select_stmt);
  (yyval.select_stmt)->setOperations->back()->resultOrder = (yyvsp[-2].order_vec);
  (yyval.select_stmt)->setOperations->back()->resultLimit = (yyvsp[-1].limit);
  (yyval.select_stmt)->lockings = (yyvsp[0].locking_clause_vec);
}
#line 4320 "bison_parser.cpp"
    break;

  case 126: /* set_operator: set_type opt_all  */
#line 869 "bison_parser.y"
                                {
  (yyval.set_operator_t) = (yyvsp[-1].set_operator_t);
  (yyval.set_operator_t)->isAll = (yyvsp[0].bval);
}
#line 4329 "bison_parser.cpp"
    break;

  case 127: /* set_type: UNION  */
#line 874 "bison_parser.y"
                 {
  (yyval.set_operator_t) = new SetOperation();
  (yyval.set_operator_t)->setType = SetType::kSetUnion;
}
#line 4338 "bison_parser.cpp"
    break;

  case 128: /* set_type: INTERSECT  */
#line 878 "bison_parser.y"
            {
  (yyval.set_operator_t) = new SetOperation();
  (yyval.set_operator_t)->setType = SetType::kSetIntersect;
}
#line 4347 "bison_parser.cpp"
    break;

  case 129: /* set_type: EXCEPT  */
#line 882 "bison_parser.y"
         {
  (yyval.set_operator_t) = new SetOperation();
  (yyval.set_operator_t)->setType = SetType::kSetExcept;
}
#line 4356 "bison_parser.cpp"
    break;

  case 130: /* opt_all: ALL  */
#line 887 "bison_parser.y"
              { (yyval.bval) = true; }
#line 4362 "bison_parser.cpp"
    break;

  case 131: /* opt_all: %empty  */
#line 888 "bison_parser.y"
              { (yyval.bval) = false; }
#line 4368 "bison_parser.cpp"
    break;

  case 132: /* select_clause: SELECT opt_top opt_distinct select_list opt_from_clause opt_where opt_group  */
#line 890 "bison_parser.y"
                                                                                            {
  (yyval.select_stmt) = new SelectStatement();
  (yyval.select_stmt)->limit = (yyvsp[-5].limit);
  (yyval.select_stmt)->selectDistinct = (yyvsp[-4].bval);
  (yyval.select_stmt)->selectList = (yyvsp[-3].expr_vec);
  (yyval.select_stmt)->fromTable = (yyvsp[-2].table);
  (yyval.select_stmt)->whereClause = (yyvsp[-1].expr);
  (yyval.select_stmt)->groupBy = (yyvsp[0].group_t);
}
#line 4382 "bison_parser.cpp"
    break;

  case 133: /* opt_distinct: DISTINCT  */
#line 900 "bison_parser.y"
                        { (yyval.bval) = true; }
#line 4388 "bison_parser.cpp"
    break;

  case 134: /* opt_distinct: %empty  */
#line 901 "bison_parser.y"
              { (yyval.bval) = false; }
#line 4394 "bison_parser.cpp"
    break;

  case 136: /* opt_from_clause: from_clause  */
#line 905 "bison_parser.y"
                              { (yyval.table) = (yyvsp[0].table); }
#line 4400 "bison_parser.cpp"
    break;

  case 137: /* opt_from_clause: %empty  */
#line 906 "bison_parser.y"
              { (yyval.table) = nullptr; }
#line 4406 "bison_parser.cpp"
    break;

  case 138: /* from_clause: FROM table_ref  */
#line 908 "bison_parser.y"
                             { (yyval.table) = (yyvsp[0].table); }
#line 4412 "bison_parser.cpp"
    break;

  case 139: /* opt_where: WHERE expr  */
#line 910 "bison_parser.y"
                       { (yyval.expr) = (yyvsp[0].expr); }
#line 4418 "bison_parser.cpp"
    break;

  case 140: /* opt_where: %empty  */
#line 911 "bison_parser.y"
              { (yyval.expr) = nullptr; }
#line 4424 "bison_parser.cpp"
    break;

  case 141: /* opt_group: GROUP BY expr_list opt_having  */
#line 913 "bison_parser.y"
                                          {
  (yyval.group_t) = new GroupByDescription();
  (yyval.group_t)->columns = (yyvsp[-1].expr_vec);
  (yyval.group_t)->having = (yyvsp[0].expr);
}
#line 4434 "bison_parser.cpp"
    break;

  case 142: /* opt_group: %empty  */
#line 918 "bison_parser.y"
              { (yyval.group_t) = nullptr; }
#line 4440 "bison_parser.cpp"
    break;

  case 143: /* opt_having: HAVING expr  */
#line 920 "bison_parser.y"
                         { (yyval.expr) = (yyvsp[0].expr); }
#line 4446 "bison_parser.cpp"
    break;

  case 144: /* opt_having: %empty  */
#line 921 "bison_parser.y"
              { (yyval.expr) = nullptr; }
#line 4452 "bison_parser.cpp"
    break;

  case 145: /* opt_order: ORDER BY order_list  */
#line 923 "bison_parser.y"
                                { (yyval.order_vec) = (yyvsp[0].order_vec); }
#line 4458 "bison_parser.cpp"
    break;

  case 146: /* opt_order: %empty  */
#line 924 "bison_parser.y"
              { (yyval.order_vec) = nullptr; }
#line 4464 "bison_parser.cpp"
    break;

  case 147: /* order_list: order_desc  */
#line 926 "bison_parser.y"
                        {
  (yyval.order_vec) = new std::vector<OrderDescription*>();
  (yyval.order_vec)->push_back((yyvsp[0].order));
}
#line 4473 "bison_parser.cpp"
    break;

  case 148: /* order_list: order_list ',' order_desc  */
#line 930 "bison_parser.y"
                            {
  (yyvsp[-2].order_vec)->push_back((yyvsp[0].order));
  (yyval.order_vec) = (yyvsp[-2].order_vec);
}
#line 4482 "bison_parser.cpp"
    break;

  case 149: /* order_desc: expr opt_order_type  */
#line 935 "bison_parser.y"
                                 { (yyval.order) = new OrderDescription((yyvsp[0].order_type), (yyvsp[-1].expr)); }
#line 4488 "bison_parser.cpp"
    break;

  case 150: /* opt_order_type: ASC  */
#line 937 "bison_parser.y"
                     { (yyval.order_type) = kOrderAsc; }
#line 4494 "bison_parser.cpp"
    break;

  case 151: /* opt_order_type: DESC  */
#line 938 "bison_parser.y"
       { (yyval.order_type) = kOrderDesc; }
#line 4500 "bison_parser.cpp"
    break;

  case 152: /* opt_order_type: %empty  */
#line 939 "bison_parser.y"
              { (yyval.order_type) = kOrderAsc; }
#line 4506 "bison_parser.cpp"
    break;

  case 153: /* opt_top: TOP int_literal  */
#line 943 "bison_parser.y"
                          { (yyval.limit) = new LimitDescription((yyvsp[0].expr), nullptr); }
#line 4512 "bison_parser.cpp"
    break;

  case 154: /* opt_top: %empty  */
#line 944 "bison_parser.y"
              { (yyval.limit) = nullptr; }
#line 4518 "bison_parser.cpp"
    break;

  case 155: /* opt_limit: LIMIT expr  */
#line 946 "bison_parser.y"
                       { (yyval.limit) = new LimitDescription((yyvsp[0].expr), nullptr); }
#line 4524 "bison_parser.cpp"
    break;

  case 156: /* opt_limit: OFFSET expr  */
#line 947 "bison_parser.y"
              { (yyval.limit) = new LimitDescription(nullptr, (yyvsp[0].expr)); }
#line 4530 "bison_parser.cpp"
    break;

  case 157: /* opt_limit: LIMIT expr OFFSET expr  */
#line 948 "bison_parser.y"
                         { (yyval.limit) = new LimitDescription((yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 4536 "bison_parser.cpp"
    break;

  case 158: /* opt_limit: LIMIT ALL  */
#line 949 "bison_parser.y"
            { (yyval.limit) = new LimitDescription(nullptr, nullptr); }
#line 4542 "bison_parser.cpp"
    break;

  case 159: /* opt_limit: LIMIT ALL OFFSET expr  */
#line 950 "bison_parser.y"
                        { (yyval.limit) = new LimitDescription(nullptr, (yyvsp[0].expr)); }
#line 4548 "bison_parser.cpp"
    break;

  case 160: /* opt_limit: %empty  */
#line 951 "bison_parser.y"
              { (yyval.limit) = nullptr; }
#line 4554 "bison_parser.cpp"
    break;

  case 161: /* expr_list: expr_alias  */
#line 956 "bison_parser.y"
                       {
  (yyval.expr_vec) = new std::vector<Expr*>();
  (yyval.expr_vec)->push_back((yyvsp[0].expr));
}
#line 4563 "bison_parser.cpp"
    break;

  case 162: /* expr_list: expr_list ',' expr_alias  */
#line 960 "bison_parser.y"
                           {
  (yyvsp[-2].expr_vec)->push_back((yyvsp[0].expr));
  (yyval.expr_vec) = (yyvsp[-2].expr_vec);
}
#line 4572 "bison_parser.cpp"
    break;

  case 163: /* opt_extended_literal_list: extended_literal_list  */
#line 966 "bison_parser.y"
                                                  { (yyval.expr_vec) = (yyvsp[0].expr_vec); }
#line 4578 "bison_parser.cpp"
    break;

  case 164: /* opt_extended_literal_list: %empty  */
#line 967 "bison_parser.y"
              { (yyval.expr_vec) = nullptr; }
#line 4584 "bison_parser.cpp"
    break;

  case 165: /* extended_literal_list: casted_extended_literal  */
#line 969 "bison_parser.y"
                                                {
  (yyval.expr_vec) = new std::vector<Expr*>();
  (yyval.expr_vec)->push_back((yyvsp[0].expr));
}
#line 4593 "bison_parser.cpp"
    break;

  case 166: /* extended_literal_list: extended_literal_list ',' casted_extended_literal  */
#line 973 "bison_parser.y"
                                                    {
  (yyvsp[-2].expr_vec)->push_back((yyvsp[0].expr));
  (yyval.expr_vec) = (yyvsp[-2].expr_vec);
}
#line 4602 "bison_parser.cpp"
    break;

  case 168: /* casted_extended_literal: CAST '(' extended_literal AS column_type ')'  */
#line 978 "bison_parser.y"
                                                                                          {
  (yyval.expr) = Expr::makeCast((yyvsp[-3].expr), (yyvsp[-1].column_type_t));
}
#line 4610 "bison_parser.cpp"
    break;

  case 169: /* extended_literal: literal  */
#line 982 "bison_parser.y"
                           {
  if ((yyvsp[0].expr)->type == ExprType::kExprParameter) {
    delete (yyvsp[0].expr);
    yyerror(&yyloc, result, scanner, "Parameter ? is not a valid literal.");
    YYERROR;
  }
  (yyval.expr) = (yyvsp[0].expr);
}
#line 4623 "bison_parser.cpp"
    break;

  case 170: /* extended_literal: '-' num_literal  */
#line 990 "bison_parser.y"
                  { (yyval.expr) = Expr::makeOpUnary(kOpUnaryMinus, (yyvsp[0].expr)); }
#line 4629 "bison_parser.cpp"
    break;

  case 171: /* extended_literal: '-' interval_literal  */
#line 991 "bison_parser.y"
                       { (yyval.expr) = Expr::makeOpUnary(kOpUnaryMinus, (yyvsp[0].expr)); }
#line 4635 "bison_parser.cpp"
    break;

  case 172: /* expr_alias: expr opt_alias  */
#line 993 "bison_parser.y"
                            {
  (yyval.expr) = (yyvsp[-1].expr);
  if ((yyvsp[0].alias_t)) {
    (yyval.expr)->alias = (yyvsp[0].alias_t)->name;
    (yyvsp[0].alias_t)->name = nullptr;
    delete (yyvsp[0].alias_t);
  }
}
#line 4648 "bison_parser.cpp"
    break;

  case 178: /* operand: '(' expr ')'  */
#line 1004 "bison_parser.y"
                       { (yyval.expr) = (yyvsp[-1].expr); }
#line 4654 "bison_parser.cpp"
    break;

  case 188: /* operand: '(' select_no_paren ')'  */
#line 1006 "bison_parser.y"
                                         {
  (yyval.expr) = Expr::makeSelect((yyvsp[-1].select_stmt));
}
#line 4662 "bison_parser.cpp"
    break;

  case 191: /* unary_expr: '-' operand  */
#line 1012 "bison_parser.y"
                         { (yyval.expr) = Expr::makeOpUnary(kOpUnaryMinus, (yyvsp[0].expr)); }
#line 4668 "bison_parser.cpp"
    break;

  case 192: /* unary_expr: NOT operand  */
#line 1013 "bison_parser.y"
              { (yyval.expr) = Expr::makeOpUnary(kOpNot, (yyvsp[0].expr)); }
#line 4674 "bison_parser.cpp"
    break;

  case 193: /* unary_expr: operand ISNULL  */
#line 1014 "bison_parser.y"
                 { (yyval.expr) = Expr::makeOpUnary(kOpIsNull, (yyvsp[-1].expr)); }
#line 4680 "bison_parser.cpp"
    break;

  case 194: /* unary_expr: operand IS NULL  */
#line 1015 "bison_parser.y"
                  { (yyval.expr) = Expr::makeOpUnary(kOpIsNull, (yyvsp[-2].expr)); }
#line 4686 "bison_parser.cpp"
    break;

  case 195: /* unary_expr: operand IS NOT NULL  */
#line 1016 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpUnary(kOpNot, Expr::makeOpUnary(kOpIsNull, (yyvsp[-3].expr))); }
#line 4692 "bison_parser.cpp"
    break;

  case 197: /* binary_expr: operand '-' operand  */
#line 1018 "bison_parser.y"
                                              { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpMinus, (yyvsp[0].expr)); }
#line 4698 "bison_parser.cpp"
    break;

  case 198: /* binary_expr: operand '+' operand  */
#line 1019 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpPlus, (yyvsp[0].expr)); }
#line 4704 "bison_parser.cpp"
    break;

  case 199: /* binary_expr: operand '/' operand  */
#line 1020 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpSlash, (yyvsp[0].expr)); }
#line 4710 "bison_parser.cpp"
    break;

  case 200: /* binary_expr: operand '*' operand  */
#line 1021 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpAsterisk, (yyvsp[0].expr)); }
#line 4716 "bison_parser.cpp"
    break;

  case 201: /* binary_expr: operand '%' operand  */
#line 1022 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpPercentage, (yyvsp[0].expr)); }
#line 4722 "bison_parser.cpp"
    break;

  case 202: /* binary_expr: operand '^' operand  */
#line 1023 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpCaret, (yyvsp[0].expr)); }
#line 4728 "bison_parser.cpp"
    break;

  case 203: /* binary_expr: operand LIKE operand  */
#line 1024 "bison_parser.y"
                       { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpLike, (yyvsp[0].expr)); }
#line 4734 "bison_parser.cpp"
    break;

  case 204: /* binary_expr: operand NOT LIKE operand  */
#line 1025 "bison_parser.y"
                           { (yyval.expr) = Expr::makeOpBinary((yyvsp[-3].expr), kOpNotLike, (yyvsp[0].expr)); }
#line 4740 "bison_parser.cpp"
    break;

  case 205: /* binary_expr: operand ILIKE operand  */
#line 1026 "bison_parser.y"
                        { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpILike, (yyvsp[0].expr)); }
#line 4746 "bison_parser.cpp"
    break;

  case 206: /* binary_expr: operand CONCAT operand  */
#line 1027 "bison_parser.y"
                         { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpConcat, (yyvsp[0].expr)); }
#line 4752 "bison_parser.cpp"
    break;

  case 207: /* logic_expr: expr AND expr  */
#line 1029 "bison_parser.y"
                           { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpAnd, (yyvsp[0].expr)); }
#line 4758 "bison_parser.cpp"
    break;

  case 208: /* logic_expr: expr OR expr  */
#line 1030 "bison_parser.y"
               { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpOr, (yyvsp[0].expr)); }
#line 4764 "bison_parser.cpp"
    break;

  case 209: /* in_expr: operand IN '(' expr_list ')'  */
#line 1032 "bison_parser.y"
                                       { (yyval.expr) = Expr::makeInOperator((yyvsp[-4].expr), (yyvsp[-1].expr_vec)); }
#line 4770 "bison_parser.cpp"
    break;

  case 210: /* in_expr: operand NOT IN '(' expr_list ')'  */
#line 1033 "bison_parser.y"
                                   { (yyval.expr) = Expr::makeOpUnary(kOpNot, Expr::makeInOperator((yyvsp[-5].expr), (yyvsp[-1].expr_vec))); }
#line 4776 "bison_parser.cpp"
    break;

  case 211: /* in_expr: operand IN '(' select_no_paren ')'  */
#line 1034 "bison_parser.y"
                                     { (yyval.expr) = Expr::makeInOperator((yyvsp[-4].expr), (yyvsp[-1].select_stmt)); }
#line 4782 "bison_parser.cpp"
    break;

  case 212: /* in_expr: operand NOT IN '(' select_no_paren ')'  */
#line 1035 "bison_parser.y"
                                         { (yyval.expr) = Expr::makeOpUnary(kOpNot, Expr::makeInOperator((yyvsp[-5].expr), (yyvsp[-1].select_stmt))); }
#line 4788 "bison_parser.cpp"
    break;

  case 213: /* case_expr: CASE expr case_list END  */
#line 1039 "bison_parser.y"
                                    { (yyval.expr) = Expr::makeCase((yyvsp[-2].expr), (yyvsp[-1].expr), nullptr); }
#line 4794 "bison_parser.cpp"
    break;

  case 214: /* case_expr: CASE expr case_list ELSE expr END  */
#line 1040 "bison_parser.y"
                                    { (yyval.expr) = Expr::makeCase((yyvsp[-4].expr), (yyvsp[-3].expr), (yyvsp[-1].expr)); }
#line 4800 "bison_parser.cpp"
    break;

  case 215: /* case_expr: CASE case_list END  */
#line 1041 "bison_parser.y"
                     { (yyval.expr) = Expr::makeCase(nullptr, (yyvsp[-1].expr), nullptr); }
#line 4806 "bison_parser.cpp"
    break;

  case 216: /* case_expr: CASE case_list ELSE expr END  */
#line 1042 "bison_parser.y"
                               { (yyval.expr) = Expr::makeCase(nullptr, (yyvsp[-3].expr), (yyvsp[-1].expr)); }
#line 4812 "bison_parser.cpp"
    break;

  case 217: /* case_list: WHEN expr THEN expr  */
#line 1044 "bison_parser.y"
                                { (yyval.expr) = Expr::makeCaseList(Expr::makeCaseListElement((yyvsp[-2].expr), (yyvsp[0].expr))); }
#line 4818 "bison_parser.cpp"
    break;

  case 218: /* case_list: case_list WHEN expr THEN expr  */
#line 1045 "bison_parser.y"
                                { (yyval.expr) = Expr::caseListAppend((yyvsp[-4].expr), Expr::makeCaseListElement((yyvsp[-2].expr), (yyvsp[0].expr))); }
#line 4824 "bison_parser.cpp"
    break;

  case 219: /* exists_expr: EXISTS '(' select_no_paren ')'  */
#line 1047 "bison_parser.y"
                                             { (yyval.expr) = Expr::makeExists((yyvsp[-1].select_stmt)); }
#line 4830 "bison_parser.cpp"
    break;

  case 220: /* exists_expr: NOT EXISTS '(' select_no_paren ')'  */
#line 1048 "bison_parser.y"
                                     { (yyval.expr) = Expr::makeOpUnary(kOpNot, Expr::makeExists((yyvsp[-1].select_stmt))); }
#line 4836 "bison_parser.cpp"
    break;

  case 221: /* comp_expr: operand '=' operand  */
#line 1050 "bison_parser.y"
                                { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpEquals, (yyvsp[0].expr)); }
#line 4842 "bison_parser.cpp"
    break;

  case 222: /* comp_expr: operand EQUALS operand  */
#line 1051 "bison_parser.y"
                         { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpEquals, (yyvsp[0].expr)); }
#line 4848 "bison_parser.cpp"
    break;

  case 223: /* comp_expr: operand NOTEQUALS operand  */
#line 1052 "bison_parser.y"
                            { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpNotEquals, (yyvsp[0].expr)); }
#line 4854 "bison_parser.cpp"
    break;

  case 224: /* comp_expr: operand '<' operand  */
#line 1053 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpLess, (yyvsp[0].expr)); }
#line 4860 "bison_parser.cpp"
    break;

  case 225: /* comp_expr: operand '>' operand  */
#line 1054 "bison_parser.y"
                      { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpGreater, (yyvsp[0].expr)); }
#line 4866 "bison_parser.cpp"
    break;

  case 226: /* comp_expr: operand LESSEQ operand  */
#line 1055 "bison_parser.y"
                         { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpLessEq, (yyvsp[0].expr)); }
#line 4872 "bison_parser.cpp"
    break;

  case 227: /* comp_expr: operand GREATEREQ operand  */
#line 1056 "bison_parser.y"
                            { (yyval.expr) = Expr::makeOpBinary((yyvsp[-2].expr), kOpGreaterEq, (yyvsp[0].expr)); }
#line 4878 "bison_parser.cpp"
    break;

  case 228: /* function_expr: IDENTIFIER '(' ')' opt_window  */
#line 1060 "bison_parser.y"
                                              { (yyval.expr) = Expr::makeFunctionRef((yyvsp[-3].sval), new std::vector<Expr*>(), false, (yyvsp[0].window_description)); }
#line 4884 "bison_parser.cpp"
    break;

  case 229: /* function_expr: IDENTIFIER '(' opt_distinct expr_list ')' opt_window  */
#line 1061 "bison_parser.y"
                                                       { (yyval.expr) = Expr::makeFunctionRef((yyvsp[-5].sval), (yyvsp[-2].expr_vec), (yyvsp[-3].bval), (yyvsp[0].window_description)); }
#line 4890 "bison_parser.cpp"
    break;

  case 230: /* opt_window: OVER '(' opt_partition opt_order opt_frame_clause ')'  */
#line 1065 "bison_parser.y"
                                                                   { (yyval.window_description) = new WindowDescription((yyvsp[-3].expr_vec), (yyvsp[-2].order_vec), (yyvsp[-1].frame_description)); }
#line 4896 "bison_parser.cpp"
    break;

  case 231: /* opt_window: %empty  */
#line 1066 "bison_parser.y"
              { (yyval.window_description) = nullptr; }
#line 4902 "bison_parser.cpp"
    break;

  case 232: /* opt_partition: PARTITION BY expr_list  */
#line 1068 "bison_parser.y"
                                       { (yyval.expr_vec) = (yyvsp[0].expr_vec); }
#line 4908 "bison_parser.cpp"
    break;

  case 233: /* opt_partition: %empty  */
#line 1069 "bison_parser.y"
              { (yyval.expr_vec) = nullptr; }
#line 4914 "bison_parser.cpp"
    break;

  case 234: /* opt_frame_clause: frame_type frame_bound  */
#line 1074 "bison_parser.y"
                                          { (yyval.frame_description) = new FrameDescription{(yyvsp[-1].frame_type), (yyvsp[0].frame_bound), new FrameBound{0, kCurrentRow, false}}; }
#line 4920 "bison_parser.cpp"
    break;

  case 235: /* opt_frame_clause: frame_type BETWEEN frame_bound AND frame_bound  */
#line 1075 "bison_parser.y"
                                                 { (yyval.frame_description) = new FrameDescription{(yyvsp[-4].frame_type), (yyvsp[-2].frame_bound), (yyvsp[0].frame_bound)}; }
#line 4926 "bison_parser.cpp"
    break;

  case 236: /* opt_frame_clause: %empty  */
#line 1076 "bison_parser.y"
              {
  (yyval.frame_description) = new FrameDescription{kRange, new FrameBound{0, kPreceding, true}, new FrameBound{0, kCurrentRow, false}};
}
#line 4934 "bison_parser.cpp"
    break;

  case 237: /* frame_type: RANGE  */
#line 1080 "bison_parser.y"
                   { (yyval.frame_type) = kRange; }
#line 4940 "bison_parser.cpp"
    break;

  case 238: /* frame_type: ROWS  */
#line 1081 "bison_parser.y"
       { (yyval.frame_type) = kRows; }
#line 4946 "bison_parser.cpp"
    break;

  case 239: /* frame_type: GROUPS  */
#line 1082 "bison_parser.y"
         { (yyval.frame_type) = kGroups; }
#line 4952 "bison_parser.cpp"
    break;

  case 240: /* frame_bound: UNBOUNDED PRECEDING  */
#line 1084 "bison_parser.y"
                                  { (yyval.frame_bound) = new FrameBound{0, kPreceding, true}; }
#line 4958 "bison_parser.cpp"
    break;

  case 241: /* frame_bound: INTVAL PRECEDING  */
#line 1085 "bison_parser.y"
                   { (yyval.frame_bound) = new FrameBound{(yyvsp[-1].ival), kPreceding, false}; }
#line 4964 "bison_parser.cpp"
    break;

  case 242: /* frame_bound: UNBOUNDED FOLLOWING  */
#line 1086 "bison_parser.y"
                      { (yyval.frame_bound) = new FrameBound{0, kFollowing, true}; }
#line 4970 "bison_parser.cpp"
    break;

  case 243: /* frame_bound: INTVAL FOLLOWING  */
#line 1087 "bison_parser.y"
                   { (yyval.frame_bound) = new FrameBound{(yyvsp[-1].ival), kFollowing, false}; }
#line 4976 "bison_parser.cpp"
    break;

  case 244: /* frame_bound: CURRENT_ROW  */
#line 1088 "bison_parser.y"
              { (yyval.frame_bound) = new FrameBound{0, kCurrentRow, false}; }
#line 4982 "bison_parser.cpp"
    break;

  case 245: /* extract_expr: EXTRACT '(' datetime_field FROM expr ')'  */
#line 1090 "bison_parser.y"
                                                        { (yyval.expr) = Expr::makeExtract((yyvsp[-3].datetime_field), (yyvsp[-1].expr)); }
#line 4988 "bison_parser.cpp"
    break;

  case 246: /* cast_expr: CAST '(' expr AS column_type ')'  */
#line 1092 "bison_parser.y"
                                             { (yyval.expr) = Expr::makeCast((yyvsp[-3].expr), (yyvsp[-1].column_type_t)); }
#line 4994 "bison_parser.cpp"
    break;

  case 247: /* datetime_field: SECOND  */
#line 1094 "bison_parser.y"
                        { (yyval.datetime_field) = kDatetimeSecond; }
#line 5000 "bison_parser.cpp"
    break;

  case 248: /* datetime_field: MINUTE  */
#line 1095 "bison_parser.y"
         { (yyval.datetime_field) = kDatetimeMinute; }
#line 5006 "bison_parser.cpp"
    break;

  case 249: /* datetime_field: HOUR  */
#line 1096 "bison_parser.y"
       { (yyval.datetime_field) = kDatetimeHour; }
#line 5012 "bison_parser.cpp"
    break;

  case 250: /* datetime_field: DAY  */
#line 1097 "bison_parser.y"
      { (yyval.datetime_field) = kDatetimeDay; }
#line 5018 "bison_parser.cpp"
    break;

  case 251: /* datetime_field: MONTH  */
#line 1098 "bison_parser.y"
        { (yyval.datetime_field) = kDatetimeMonth; }
#line 5024 "bison_parser.cpp"
    break;

  case 252: /* datetime_field: YEAR  */
#line 1099 "bison_parser.y"
       { (yyval.datetime_field) = kDatetimeYear; }
#line 5030 "bison_parser.cpp"
    break;

  case 253: /* datetime_field_plural: SECONDS  */
#line 1101 "bison_parser.y"
                                { (yyval.datetime_field) = kDatetimeSecond; }
#line 5036 "bison_parser.cpp"
    break;

  case 254: /* datetime_field_plural: MINUTES  */
#line 1102 "bison_parser.y"
          { (yyval.datetime_field) = kDatetimeMinute; }
#line 5042 "bison_parser.cpp"
    break;

  case 255: /* datetime_field_plural: HOURS  */
#line 1103 "bison_parser.y"
        { (yyval.datetime_field) = kDatetimeHour; }
#line 5048 "bison_parser.cpp"
    break;

  case 256: /* datetime_field_plural: DAYS  */
#line 1104 "bison_parser.y"
       { (yyval.datetime_field) = kDatetimeDay; }
#line 5054 "bison_parser.cpp"
    break;

  case 257: /* datetime_field_plural: MONTHS  */
#line 1105 "bison_parser.y"
         { (yyval.datetime_field) = kDatetimeMonth; }
#line 5060 "bison_parser.cpp"
    break;

  case 258: /* datetime_field_plural: YEARS  */
#line 1106 "bison_parser.y"
        { (yyval.datetime_field) = kDatetimeYear; }
#line 5066 "bison_parser.cpp"
    break;

  case 261: /* array_expr: ARRAY '[' expr_list ']'  */
#line 1110 "bison_parser.y"
                                     { (yyval.expr) = Expr::makeArray((yyvsp[-1].expr_vec)); }
#line 5072 "bison_parser.cpp"
    break;

  case 262: /* array_index: operand '[' int_literal ']'  */
#line 1112 "bison_parser.y"
                                          { (yyval.expr) = Expr::makeArrayIndex((yyvsp[-3].expr), (yyvsp[-1].expr)->ival); }
#line 5078 "bison_parser.cpp"
    break;

  case 263: /* between_expr: operand BETWEEN operand AND operand  */
#line 1114 "bison_parser.y"
                                                   { (yyval.expr) = Expr::makeBetween((yyvsp[-4].expr), (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 5084 "bison_parser.cpp"
    break;

  case 264: /* column_name: IDENTIFIER  */
#line 1116 "bison_parser.y"
                         { (yyval.expr) = Expr::makeColumnRef((yyvsp[0].sval)); }
#line 5090 "bison_parser.cpp"
    break;

  case 265: /* column_name: IDENTIFIER '.' IDENTIFIER  */
#line 1117 "bison_parser.y"
                            { (yyval.expr) = Expr::makeColumnRef((yyvsp[-2].sval), (yyvsp[0].sval)); }
#line 5096 "bison_parser.cpp"
    break;

  case 266: /* column_name: '*'  */
#line 1118 "bison_parser.y"
      { (yyval.expr) = Expr::makeStar(); }
#line 5102 "bison_parser.cpp"
    break;

  case 267: /* column_name: IDENTIFIER '.' '*'  */
#line 1119 "bison_parser.y"
                     { (yyval.expr) = Expr::makeStar((yyvsp[-2].sval)); }
#line 5108 "bison_parser.cpp"
    break;

  case 275: /* string_literal: STRING  */
#line 1123 "bison_parser.y"
                        { (yyval.expr) = Expr::makeLiteral((yyvsp[0].sval)); }
#line 5114 "bison_parser.cpp"
    break;

  case 276: /* bool_literal: TRUE  */
#line 1125 "bison_parser.y"
                    { (yyval.expr) = Expr::makeLiteral(true); }
#line 5120 "bison_parser.cpp"
    break;

  case 277: /* bool_literal: FALSE  */
#line 1126 "bison_parser.y"
        { (yyval.expr) = Expr::makeLiteral(false); }
#line 5126 "bison_parser.cpp"
    break;

  case 278: /* num_literal: FLOATVAL  */
#line 1128 "bison_parser.y"
                       { (yyval.expr) = Expr::makeLiteral((yyvsp[0].fval)); }
#line 5132 "bison_parser.cpp"
    break;

  case 280: /* int_literal: INTVAL  */
#line 1131 "bison_parser.y"
                     { (yyval.expr) = Expr::makeLiteral((yyvsp[0].ival)); }
#line 5138 "bison_parser.cpp"
    break;

  case 281: /* null_literal: NULL  */
#line 1133 "bison_parser.y"
                    { (yyval.expr) = Expr::makeNullLiteral(); }
#line 5144 "bison_parser.cpp"
    break;

  case 282: /* date_literal: DATE STRING  */
#line 1135 "bison_parser.y"
                           {
  int day{0}, month{0}, year{0}, chars_parsed{0};
  // If the whole string is parsed, chars_parsed points to the terminating null byte after the last character
  if (sscanf((yyvsp[0].sval), "%4d-%2d-%2d%n", &day, &month, &year, &chars_parsed) != 3 || (yyvsp[0].sval)[chars_parsed] != 0) {
    free((yyvsp[0].sval));
    yyerror(&yyloc, result, scanner, "Found incorrect date format. Expected format: YYYY-MM-DD");
    YYERROR;
  }
  (yyval.expr) = Expr::makeDateLiteral((yyvsp[0].sval));
}
#line 5159 "bison_parser.cpp"
    break;

  case 283: /* interval_literal: INTVAL duration_field  */
#line 1146 "bison_parser.y"
                                         { (yyval.expr) = Expr::makeIntervalLiteral((yyvsp[-1].ival), (yyvsp[0].datetime_field)); }
#line 5165 "bison_parser.cpp"
    break;

  case 284: /* interval_literal: INTERVAL STRING datetime_field  */
#line 1147 "bison_parser.y"
                                 {
  int duration{0}, chars_parsed{0};
  // If the whole string is parsed, chars_parsed points to the terminating null byte after the last character
  if (sscanf((yyvsp[-1].sval), "%d%n", &duration, &chars_parsed) != 1 || (yyvsp[-1].sval)[chars_parsed] != 0) {
    free((yyvsp[-1].sval));
    yyerror(&yyloc, result, scanner, "Found incorrect interval format. Expected format: INTEGER");
    YYERROR;
  }
  free((yyvsp[-1].sval));
  (yyval.expr) = Expr::makeIntervalLiteral(duration, (yyvsp[0].datetime_field));
}
#line 5181 "bison_parser.cpp"
    break;

  case 285: /* interval_literal: INTERVAL STRING  */
#line 1158 "bison_parser.y"
                  {
  int duration{0}, chars_parsed{0};
  // 'seconds' and 'minutes' are the longest accepted interval qualifiers (7 chars) + null byte
  char unit_string[8];
  // If the whole string is parsed, chars_parsed points to the terminating null byte after the last character
  if (sscanf((yyvsp[0].sval), "%d %7s%n", &duration, unit_string, &chars_parsed) != 2 || (yyvsp[0].sval)[chars_parsed] != 0) {
    free((yyvsp[0].sval));
    yyerror(&yyloc, result, scanner, "Found incorrect interval format. Expected format: INTEGER INTERVAL_QUALIIFIER");
    YYERROR;
  }
  free((yyvsp[0].sval));

  DatetimeField unit;
  if (strcasecmp(unit_string, "second") == 0 || strcasecmp(unit_string, "seconds") == 0) {
    unit = kDatetimeSecond;
  } else if (strcasecmp(unit_string, "minute") == 0 || strcasecmp(unit_string, "minutes") == 0) {
    unit = kDatetimeMinute;
  } else if (strcasecmp(unit_string, "hour") == 0 || strcasecmp(unit_string, "hours") == 0) {
    unit = kDatetimeHour;
  } else if (strcasecmp(unit_string, "day") == 0 || strcasecmp(unit_string, "days") == 0) {
    unit = kDatetimeDay;
  } else if (strcasecmp(unit_string, "month") == 0 || strcasecmp(unit_string, "months") == 0) {
    unit = kDatetimeMonth;
  } else if (strcasecmp(unit_string, "year") == 0 || strcasecmp(unit_string, "years") == 0) {
    unit = kDatetimeYear;
  } else {
    yyerror(&yyloc, result, scanner, "Interval qualifier is unknown.");
    YYERROR;
  }
  (yyval.expr) = Expr::makeIntervalLiteral(duration, unit);
}
#line 5217 "bison_parser.cpp"
    break;

  case 286: /* param_expr: '?'  */
#line 1190 "bison_parser.y"
                 {
  (yyval.expr) = Expr::makeParameter(yylloc.total_column);
  (yyval.expr)->ival2 = yyloc.param_list.size();
  yyloc.param_list.push_back((yyval.expr));
}
#line 5227 "bison_parser.cpp"
    break;

  case 288: /* table_ref: table_ref_commalist ',' table_ref_atomic  */
#line 1199 "bison_parser.y"
                                                                        {
  (yyvsp[-2].table_vec)->push_back((yyvsp[0].table));
  auto tbl = new TableRef(kTableCrossProduct);
  tbl->list = (yyvsp[-2].table_vec);
  (yyval.table) = tbl;
}
#line 5238 "bison_parser.cpp"
    break;

  case 292: /* nonjoin_table_ref_atomic: '(' select_statement ')' opt_table_alias  */
#line 1208 "bison_parser.y"
                                                                                     {
  auto tbl = new TableRef(kTableSelect);
  tbl->select = (yyvsp[-2].select_stmt);
  tbl->alias = (yyvsp[0].alias_t);
  (yyval.table) = tbl;
}
#line 5249 "bison_parser.cpp"
    break;

  case 293: /* table_ref_commalist: table_ref_atomic  */
#line 1215 "bison_parser.y"
                                       {
  (yyval.table_vec) = new std::vector<TableRef*>();
  (yyval.table_vec)->push_back((yyvsp[0].table));
}
#line 5258 "bison_parser.cpp"
    break;

  case 294: /* table_ref_commalist: table_ref_commalist ',' table_ref_atomic  */
#line 1219 "bison_parser.y"
                                           {
  (yyvsp[-2].table_vec)->push_back((yyvsp[0].table));
  (yyval.table_vec) = (yyvsp[-2].table_vec);
}
#line 5267 "bison_parser.cpp"
    break;

  case 295: /* table_ref_name: table_name opt_table_alias  */
#line 1224 "bison_parser.y"
                                            {
  auto tbl = new TableRef(kTableName);
  tbl->schema = (yyvsp[-1].table_name).schema;
  tbl->name = (yyvsp[-1].table_name).name;
  tbl->alias = (yyvsp[0].alias_t);
  (yyval.table) = tbl;
}
#line 5279 "bison_parser.cpp"
    break;

  case 296: /* table_ref_name_no_alias: table_name  */
#line 1232 "bison_parser.y"
                                     {
  (yyval.table) = new TableRef(kTableName);
  (yyval.table)->schema = (yyvsp[0].table_name).schema;
  (yyval.table)->name = (yyvsp[0].table_name).name;
}
#line 5289 "bison_parser.cpp"
    break;

  case 297: /* table_name: IDENTIFIER  */
#line 1238 "bison_parser.y"
                        {
  (yyval.table_name).schema = nullptr;
  (yyval.table_name).name = (yyvsp[0].sval);
}
#line 5298 "bison_parser.cpp"
    break;

  case 298: /* table_name: IDENTIFIER '.' IDENTIFIER  */
#line 1242 "bison_parser.y"
                            {
  (yyval.table_name).schema = (yyvsp[-2].sval);
  (yyval.table_name).name = (yyvsp[0].sval);
}
#line 5307 "bison_parser.cpp"
    break;

  case 299: /* opt_index_name: IDENTIFIER  */
#line 1247 "bison_parser.y"
                            { (yyval.sval) = (yyvsp[0].sval); }
#line 5313 "bison_parser.cpp"
    break;

  case 300: /* opt_index_name: %empty  */
#line 1248 "bison_parser.y"
              { (yyval.sval) = nullptr; }
#line 5319 "bison_parser.cpp"
    break;

  case 302: /* table_alias: AS IDENTIFIER '(' ident_commalist ')'  */
#line 1250 "bison_parser.y"
                                                            { (yyval.alias_t) = new Alias((yyvsp[-3].sval), (yyvsp[-1].str_vec)); }
#line 5325 "bison_parser.cpp"
    break;

  case 304: /* opt_table_alias: %empty  */
#line 1252 "bison_parser.y"
                                            { (yyval.alias_t) = nullptr; }
#line 5331 "bison_parser.cpp"
    break;

  case 305: /* alias: AS IDENTIFIER  */
#line 1254 "bison_parser.y"
                      { (yyval.alias_t) = new Alias((yyvsp[0].sval)); }
#line 5337 "bison_parser.cpp"
    break;

  case 306: /* alias: IDENTIFIER  */
#line 1255 "bison_parser.y"
             { (yyval.alias_t) = new Alias((yyvsp[0].sval)); }
#line 5343 "bison_parser.cpp"
    break;

  case 308: /* opt_alias: %empty  */
#line 1257 "bison_parser.y"
                                { (yyval.alias_t) = nullptr; }
#line 5349 "bison_parser.cpp"
    break;

  case 309: /* opt_locking_clause: opt_locking_clause_list  */
#line 1263 "bison_parser.y"
                                             { (yyval.locking_clause_vec) = (yyvsp[0].locking_clause_vec); }
#line 5355 "bison_parser.cpp"
    break;

  case 310: /* opt_locking_clause: %empty  */
#line 1264 "bison_parser.y"
              { (yyval.locking_clause_vec) = nullptr; }
#line 5361 "bison_parser.cpp"
    break;

  case 311: /* opt_locking_clause_list: locking_clause  */
#line 1266 "bison_parser.y"
                                         {
  (yyval.locking_clause_vec) = new std::vector<LockingClause*>();
  (yyval.locking_clause_vec)->push_back((yyvsp[0].locking_t));
}
#line 5370 "bison_parser.cpp"
    break;

  case 312: /* opt_locking_clause_list: opt_locking_clause_list locking_clause  */
#line 1270 "bison_parser.y"
                                         {
  (yyvsp[-1].locking_clause_vec)->push_back((yyvsp[0].locking_t));
  (yyval.locking_clause_vec) = (yyvsp[-1].locking_clause_vec);
}
#line 5379 "bison_parser.cpp"
    break;

  case 313: /* locking_clause: FOR row_lock_mode opt_row_lock_policy  */
#line 1275 "bison_parser.y"
                                                       {
  (yyval.locking_t) = new LockingClause();
  (yyval.locking_t)->rowLockMode = (yyvsp[-1].lock_mode_t);
  (yyval.locking_t)->rowLockWaitPolicy = (yyvsp[0].lock_wait_policy_t);
  (yyval.locking_t)->tables = nullptr;
}
#line 5390 "bison_parser.cpp"
    break;

  case 314: /* locking_clause: FOR row_lock_mode OF ident_commalist opt_row_lock_policy  */
#line 1281 "bison_parser.y"
                                                           {
  (yyval.locking_t) = new LockingClause();
  (yyval.locking_t)->rowLockMode = (yyvsp[-3].lock_mode_t);
  (yyval.locking_t)->tables = (yyvsp[-1].str_vec);
  (yyval.locking_t)->rowLockWaitPolicy = (yyvsp[0].lock_wait_policy_t);
}
#line 5401 "bison_parser.cpp"
    break;

  case 315: /* row_lock_mode: UPDATE  */
#line 1288 "bison_parser.y"
                       { (yyval.lock_mode_t) = RowLockMode::ForUpdate; }
#line 5407 "bison_parser.cpp"
    break;

  case 316: /* row_lock_mode: NO KEY UPDATE  */
#line 1289 "bison_parser.y"
                { (yyval.lock_mode_t) = RowLockMode::ForNoKeyUpdate; }
#line 5413 "bison_parser.cpp"
    break;

  case 317: /* row_lock_mode: SHARE  */
#line 1290 "bison_parser.y"
        { (yyval.lock_mode_t) = RowLockMode::ForShare; }
#line 5419 "bison_parser.cpp"
    break;

  case 318: /* row_lock_mode: KEY SHARE  */
#line 1291 "bison_parser.y"
            { (yyval.lock_mode_t) = RowLockMode::ForKeyShare; }
#line 5425 "bison_parser.cpp"
    break;

  case 319: /* opt_row_lock_policy: SKIP LOCKED  */
#line 1293 "bison_parser.y"
                                  { (yyval.lock_wait_policy_t) = RowLockWaitPolicy::SkipLocked; }
#line 5431 "bison_parser.cpp"
    break;

  case 320: /* opt_row_lock_policy: NOWAIT  */
#line 1294 "bison_parser.y"
         { (yyval.lock_wait_policy_t) = RowLockWaitPolicy::NoWait; }
#line 5437 "bison_parser.cpp"
    break;

  case 321: /* opt_row_lock_policy: %empty  */
#line 1295 "bison_parser.y"
              { (yyval.lock_wait_policy_t) = RowLockWaitPolicy::None; }
#line 5443 "bison_parser.cpp"
    break;

  case 323: /* opt_with_clause: %empty  */
#line 1301 "bison_parser.y"
                                            { (yyval.with_description_vec) = nullptr; }
#line 5449 "bison_parser.cpp"
    break;

  case 324: /* with_clause: WITH with_description_list  */
#line 1303 "bison_parser.y"
                                         { (yyval.with_description_vec) = (yyvsp[0].with_description_vec); }
#line 5455 "bison_parser.cpp"
    break;

  case 325: /* with_description_list: with_description  */
#line 1305 "bison_parser.y"
                                         {
  (yyval.with_description_vec) = new std::vector<WithDescription*>();
  (yyval.with_description_vec)->push_back((yyvsp[0].with_description_t));
}
#line 5464 "bison_parser.cpp"
    break;

  case 326: /* with_description_list: with_description_list ',' with_description  */
#line 1309 "bison_parser.y"
                                             {
  (yyvsp[-2].with_description_vec)->push_back((yyvsp[0].with_description_t));
  (yyval.with_description_vec) = (yyvsp[-2].with_description_vec);
}
#line 5473 "bison_parser.cpp"
    break;

  case 327: /* with_description: IDENTIFIER AS select_with_paren  */
#line 1314 "bison_parser.y"
                                                   {
  (yyval.with_description_t) = new WithDescription();
  (yyval.with_description_t)->alias = (yyvsp[-2].sval);
  (yyval.with_description_t)->select = (yyvsp[0].select_stmt);
}
#line 5483 "bison_parser.cpp"
    break;

  case 328: /* join_clause: table_ref_atomic NATURAL JOIN nonjoin_table_ref_atomic  */
#line 1324 "bison_parser.y"
                                                                     {
  (yyval.table) = new TableRef(kTableJoin);
  (yyval.table)->join = new JoinDefinition();
  (yyval.table)->join->type = kJoinNatural;
  (yyval.table)->join->left = (yyvsp[-3].table);
  (yyval.table)->join->right = (yyvsp[0].table);
}
#line 5495 "bison_parser.cpp"
    break;

  case 329: /* join_clause: table_ref_atomic opt_join_type JOIN table_ref_atomic ON join_condition  */
#line 1331 "bison_parser.y"
                                                                         {
  (yyval.table) = new TableRef(kTableJoin);
  (yyval.table)->join = new JoinDefinition();
  (yyval.table)->join->type = (JoinType)(yyvsp[-4].join_type);
  (yyval.table)->join->left = (yyvsp[-5].table);
  (yyval.table)->join->right = (yyvsp[-2].table);
  (yyval.table)->join->condition = (yyvsp[0].expr);
}
#line 5508 "bison_parser.cpp"
    break;

  case 330: /* join_clause: table_ref_atomic opt_join_type JOIN table_ref_atomic USING '(' ident_commalist ')'  */
#line 1339 "bison_parser.y"
                                                                                     {
  (yyval.table) = new TableRef(kTableJoin);
  (yyval.table)->join = new JoinDefinition();
  (yyval.table)->join->type = (yyvsp[-6].join_type);
  (yyval.table)->join->left = (yyvsp[-7].table);
  (yyval.table)->join->right = (yyvsp[-4].table);
  (yyval.table)->join->namedColumns = (yyvsp[-1].str_vec);
}
#line 5521 "bison_parser.cpp"
    break;

  case 331: /* opt_join_type: INNER  */
#line 1348 "bison_parser.y"
                      { (yyval.join_type) = kJoinInner; }
#line 5527 "bison_parser.cpp"
    break;

  case 332: /* opt_join_type: LEFT OUTER  */
#line 1349 "bison_parser.y"
             { (yyval.join_type) = kJoinLeft; }
#line 5533 "bison_parser.cpp"
    break;

  case 333: /* opt_join_type: LEFT  */
#line 1350 "bison_parser.y"
       { (yyval.join_type) = kJoinLeft; }
#line 5539 "bison_parser.cpp"
    break;

  case 334: /* opt_join_type: RIGHT OUTER  */
#line 1351 "bison_parser.y"
              { (yyval.join_type) = kJoinRight; }
#line 5545 "bison_parser.cpp"
    break;

  case 335: /* opt_join_type: RIGHT  */
#line 1352 "bison_parser.y"
        { (yyval.join_type) = kJoinRight; }
#line 5551 "bison_parser.cpp"
    break;

  case 336: /* opt_join_type: FULL OUTER  */
#line 1353 "bison_parser.y"
             { (yyval.join_type) = kJoinFull; }
#line 5557 "bison_parser.cpp"
    break;

  case 337: /* opt_join_type: OUTER  */
#line 1354 "bison_parser.y"
        { (yyval.join_type) = kJoinFull; }
#line 5563 "bison_parser.cpp"
    break;

  case 338: /* opt_join_type: FULL  */
#line 1355 "bison_parser.y"
       { (yyval.join_type) = kJoinFull; }
#line 5569 "bison_parser.cpp"
    break;

  case 339: /* opt_join_type: CROSS  */
#line 1356 "bison_parser.y"
        { (yyval.join_type) = kJoinCross; }
#line 5575 "bison_parser.cpp"
    break;

  case 340: /* opt_join_type: %empty  */
#line 1357 "bison_parser.y"
                       { (yyval.join_type) = kJoinInner; }
#line 5581 "bison_parser.cpp"
    break;

  case 344: /* ident_commalist: IDENTIFIER  */
#line 1368 "bison_parser.y"
                             {
  (yyval.str_vec) = new std::vector<char*>();
  (yyval.str_vec)->push_back((yyvsp[0].sval));
}
#line 5590 "bison_parser.cpp"
    break;

  case 345: /* ident_commalist: ident_commalist ',' IDENTIFIER  */
#line 1372 "bison_parser.y"
                                 {
  (yyvsp[-2].str_vec)->push_back((yyvsp[0].sval));
  (yyval.str_vec) = (yyvsp[-2].str_vec);
}
#line 5599 "bison_parser.cpp"
    break;


#line 5603 "bison_parser.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == SQL_HSQL_EMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken, &yylloc};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (&yylloc, result, scanner, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= SQL_YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == SQL_YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc, result, scanner);
          yychar = SQL_HSQL_EMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp, result, scanner);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  ++yylsp;
  YYLLOC_DEFAULT (*yylsp, yyerror_range, 2);

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, scanner, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != SQL_HSQL_EMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, result, scanner);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp, result, scanner);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 1378 "bison_parser.y"


/*********************************
 ** Section 4: Additional C code
 *********************************/

/* empty */

    // clang-format on
