#ifndef hsql_HEADER_H
#define hsql_HEADER_H 1
#define hsql_IN_HEADER 1

#line 5 "flex_lexer.h"

#line 7 "flex_lexer.h"

#define  YY_INT_ALIGNED short int



#define FLEX_SCANNER
#define YY_FLEX_MAJOR_VERSION 2
#define YY_FLEX_MINOR_VERSION 6
#define YY_FLEX_SUBMINOR_VERSION 4
#if YY_FLEX_SUBMINOR_VERSION > 0
#define FLEX_BETA
#endif

#ifdef yy_create_buffer
#define hsql__create_buffer_ALREADY_DEFINED
#else
#define yy_create_buffer hsql__create_buffer
#endif

#ifdef yy_delete_buffer
#define hsql__delete_buffer_ALREADY_DEFINED
#else
#define yy_delete_buffer hsql__delete_buffer
#endif

#ifdef yy_scan_buffer
#define hsql__scan_buffer_ALREADY_DEFINED
#else
#define yy_scan_buffer hsql__scan_buffer
#endif

#ifdef yy_scan_string
#define hsql__scan_string_ALREADY_DEFINED
#else
#define yy_scan_string hsql__scan_string
#endif

#ifdef yy_scan_bytes
#define hsql__scan_bytes_ALREADY_DEFINED
#else
#define yy_scan_bytes hsql__scan_bytes
#endif

#ifdef yy_init_buffer
#define hsql__init_buffer_ALREADY_DEFINED
#else
#define yy_init_buffer hsql__init_buffer
#endif

#ifdef yy_flush_buffer
#define hsql__flush_buffer_ALREADY_DEFINED
#else
#define yy_flush_buffer hsql__flush_buffer
#endif

#ifdef yy_load_buffer_state
#define hsql__load_buffer_state_ALREADY_DEFINED
#else
#define yy_load_buffer_state hsql__load_buffer_state
#endif

#ifdef yy_switch_to_buffer
#define hsql__switch_to_buffer_ALREADY_DEFINED
#else
#define yy_switch_to_buffer hsql__switch_to_buffer
#endif

#ifdef yypush_buffer_state
#define hsql_push_buffer_state_ALREADY_DEFINED
#else
#define yypush_buffer_state hsql_push_buffer_state
#endif

#ifdef yypop_buffer_state
#define hsql_pop_buffer_state_ALREADY_DEFINED
#else
#define yypop_buffer_state hsql_pop_buffer_state
#endif

#ifdef yyensure_buffer_stack
#define hsql_ensure_buffer_stack_ALREADY_DEFINED
#else
#define yyensure_buffer_stack hsql_ensure_buffer_stack
#endif

#ifdef yylex
#define hsql_lex_ALREADY_DEFINED
#else
#define yylex hsql_lex
#endif

#ifdef yyrestart
#define hsql_restart_ALREADY_DEFINED
#else
#define yyrestart hsql_restart
#endif

#ifdef yylex_init
#define hsql_lex_init_ALREADY_DEFINED
#else
#define yylex_init hsql_lex_init
#endif

#ifdef yylex_init_extra
#define hsql_lex_init_extra_ALREADY_DEFINED
#else
#define yylex_init_extra hsql_lex_init_extra
#endif

#ifdef yylex_destroy
#define hsql_lex_destroy_ALREADY_DEFINED
#else
#define yylex_destroy hsql_lex_destroy
#endif

#ifdef yyget_debug
#define hsql_get_debug_ALREADY_DEFINED
#else
#define yyget_debug hsql_get_debug
#endif

#ifdef yyset_debug
#define hsql_set_debug_ALREADY_DEFINED
#else
#define yyset_debug hsql_set_debug
#endif

#ifdef yyget_extra
#define hsql_get_extra_ALREADY_DEFINED
#else
#define yyget_extra hsql_get_extra
#endif

#ifdef yyset_extra
#define hsql_set_extra_ALREADY_DEFINED
#else
#define yyset_extra hsql_set_extra
#endif

#ifdef yyget_in
#define hsql_get_in_ALREADY_DEFINED
#else
#define yyget_in hsql_get_in
#endif

#ifdef yyset_in
#define hsql_set_in_ALREADY_DEFINED
#else
#define yyset_in hsql_set_in
#endif

#ifdef yyget_out
#define hsql_get_out_ALREADY_DEFINED
#else
#define yyget_out hsql_get_out
#endif

#ifdef yyset_out
#define hsql_set_out_ALREADY_DEFINED
#else
#define yyset_out hsql_set_out
#endif

#ifdef yyget_leng
#define hsql_get_leng_ALREADY_DEFINED
#else
#define yyget_leng hsql_get_leng
#endif

#ifdef yyget_text
#define hsql_get_text_ALREADY_DEFINED
#else
#define yyget_text hsql_get_text
#endif

#ifdef yyget_lineno
#define hsql_get_lineno_ALREADY_DEFINED
#else
#define yyget_lineno hsql_get_lineno
#endif

#ifdef yyset_lineno
#define hsql_set_lineno_ALREADY_DEFINED
#else
#define yyset_lineno hsql_set_lineno
#endif

#ifdef yyget_column
#define hsql_get_column_ALREADY_DEFINED
#else
#define yyget_column hsql_get_column
#endif

#ifdef yyset_column
#define hsql_set_column_ALREADY_DEFINED
#else
#define yyset_column hsql_set_column
#endif

#ifdef yywrap
#define hsql_wrap_ALREADY_DEFINED
#else
#define yywrap hsql_wrap
#endif

#ifdef yyget_lval
#define hsql_get_lval_ALREADY_DEFINED
#else
#define yyget_lval hsql_get_lval
#endif

#ifdef yyset_lval
#define hsql_set_lval_ALREADY_DEFINED
#else
#define yyset_lval hsql_set_lval
#endif

#ifdef yyget_lloc
#define hsql_get_lloc_ALREADY_DEFINED
#else
#define yyget_lloc hsql_get_lloc
#endif

#ifdef yyset_lloc
#define hsql_set_lloc_ALREADY_DEFINED
#else
#define yyset_lloc hsql_set_lloc
#endif

#ifdef yyalloc
#define hsql_alloc_ALREADY_DEFINED
#else
#define yyalloc hsql_alloc
#endif

#ifdef yyrealloc
#define hsql_realloc_ALREADY_DEFINED
#else
#define yyrealloc hsql_realloc
#endif

#ifdef yyfree
#define hsql_free_ALREADY_DEFINED
#else
#define yyfree hsql_free
#endif




#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>





#ifndef FLEXINT_H
#define FLEXINT_H



#if defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L


#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS 1
#endif

#include <inttypes.h>
typedef int8_t flex_int8_t;
typedef uint8_t flex_uint8_t;
typedef int16_t flex_int16_t;
typedef uint16_t flex_uint16_t;
typedef int32_t flex_int32_t;
typedef uint32_t flex_uint32_t;
#else
typedef signed char flex_int8_t;
typedef short int flex_int16_t;
typedef int flex_int32_t;
typedef unsigned char flex_uint8_t; 
typedef unsigned short int flex_uint16_t;
typedef unsigned int flex_uint32_t;


#ifndef INT8_MIN
#define INT8_MIN               (-128)
#endif
#ifndef INT16_MIN
#define INT16_MIN              (-32767-1)
#endif
#ifndef INT32_MIN
#define INT32_MIN              (-2147483647-1)
#endif
#ifndef INT8_MAX
#define INT8_MAX               (127)
#endif
#ifndef INT16_MAX
#define INT16_MAX              (32767)
#endif
#ifndef INT32_MAX
#define INT32_MAX              (2147483647)
#endif
#ifndef UINT8_MAX
#define UINT8_MAX              (255U)
#endif
#ifndef UINT16_MAX
#define UINT16_MAX             (65535U)
#endif
#ifndef UINT32_MAX
#define UINT32_MAX             (4294967295U)
#endif

#ifndef SIZE_MAX
#define SIZE_MAX               (~(size_t)0)
#endif

#endif 

#endif 




#define yyconst const

#if defined(__GNUC__) && __GNUC__ >= 3
#define yynoreturn __attribute__((__noreturn__))
#else
#define yynoreturn
#endif


#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif


#define yyin yyg->yyin_r
#define yyout yyg->yyout_r
#define yyextra yyg->yyextra_r
#define yyleng yyg->yyleng_r
#define yytext yyg->yytext_r
#define yylineno (YY_CURRENT_BUFFER_LVALUE->yy_bs_lineno)
#define yycolumn (YY_CURRENT_BUFFER_LVALUE->yy_bs_column)
#define yy_flex_debug yyg->yy_flex_debug_r


#ifndef YY_BUF_SIZE
#ifdef __ia64__

#define YY_BUF_SIZE 32768
#else
#define YY_BUF_SIZE 16384
#endif 
#endif

#ifndef YY_TYPEDEF_YY_BUFFER_STATE
#define YY_TYPEDEF_YY_BUFFER_STATE
typedef struct yy_buffer_state *YY_BUFFER_STATE;
#endif

#ifndef YY_TYPEDEF_YY_SIZE_T
#define YY_TYPEDEF_YY_SIZE_T
typedef size_t yy_size_t;
#endif

#ifndef YY_STRUCT_YY_BUFFER_STATE
#define YY_STRUCT_YY_BUFFER_STATE
struct yy_buffer_state
	{
	FILE *yy_input_file;

	char *yy_ch_buf;		
	char *yy_buf_pos;		

	
	int yy_buf_size;

	
	int yy_n_chars;

	
	int yy_is_our_buffer;

	
	int yy_is_interactive;

	
	int yy_at_bol;

    int yy_bs_lineno; 
    int yy_bs_column; 

	
	int yy_fill_buffer;

	int yy_buffer_status;

	};
#endif 

void yyrestart ( FILE *input_file , yyscan_t yyscanner );
void yy_switch_to_buffer ( YY_BUFFER_STATE new_buffer , yyscan_t yyscanner );
YY_BUFFER_STATE yy_create_buffer ( FILE *file, int size , yyscan_t yyscanner );
void yy_delete_buffer ( YY_BUFFER_STATE b , yyscan_t yyscanner );
void yy_flush_buffer ( YY_BUFFER_STATE b , yyscan_t yyscanner );
void yypush_buffer_state ( YY_BUFFER_STATE new_buffer , yyscan_t yyscanner );
void yypop_buffer_state ( yyscan_t yyscanner );

YY_BUFFER_STATE yy_scan_buffer ( char *base, yy_size_t size , yyscan_t yyscanner );
YY_BUFFER_STATE yy_scan_string ( const char *yy_str , yyscan_t yyscanner );
YY_BUFFER_STATE yy_scan_bytes ( const char *bytes, int len , yyscan_t yyscanner );

void *yyalloc ( yy_size_t , yyscan_t yyscanner );
void *yyrealloc ( void *, yy_size_t , yyscan_t yyscanner );
void yyfree ( void * , yyscan_t yyscanner );



#define hsql_wrap(yyscanner) (1)
#define YY_SKIP_YYWRAP

#define yytext_ptr yytext_r

#ifdef YY_HEADER_EXPORT_START_CONDITIONS
#define INITIAL 0
#define singlequotedstring 1
#define COMMENT 2

#endif

#ifndef YY_NO_UNISTD_H

#include <unistd.h>
#endif

#ifndef YY_EXTRA_TYPE
#define YY_EXTRA_TYPE void *
#endif

int yylex_init (yyscan_t* scanner);

int yylex_init_extra ( YY_EXTRA_TYPE user_defined, yyscan_t* scanner);



int yylex_destroy ( yyscan_t yyscanner );

int yyget_debug ( yyscan_t yyscanner );

void yyset_debug ( int debug_flag , yyscan_t yyscanner );

YY_EXTRA_TYPE yyget_extra ( yyscan_t yyscanner );

void yyset_extra ( YY_EXTRA_TYPE user_defined , yyscan_t yyscanner );

FILE *yyget_in ( yyscan_t yyscanner );

void yyset_in  ( FILE * _in_str , yyscan_t yyscanner );

FILE *yyget_out ( yyscan_t yyscanner );

void yyset_out  ( FILE * _out_str , yyscan_t yyscanner );

			int yyget_leng ( yyscan_t yyscanner );

char *yyget_text ( yyscan_t yyscanner );

int yyget_lineno ( yyscan_t yyscanner );

void yyset_lineno ( int _line_number , yyscan_t yyscanner );

int yyget_column  ( yyscan_t yyscanner );

void yyset_column ( int _column_no , yyscan_t yyscanner );

YYSTYPE * yyget_lval ( yyscan_t yyscanner );

void yyset_lval ( YYSTYPE * yylval_param , yyscan_t yyscanner );

       YYLTYPE *yyget_lloc ( yyscan_t yyscanner );
    
        void yyset_lloc ( YYLTYPE * yylloc_param , yyscan_t yyscanner );
    


#ifndef YY_SKIP_YYWRAP
#ifdef __cplusplus
extern "C" int yywrap ( yyscan_t yyscanner );
#else
extern int yywrap ( yyscan_t yyscanner );
#endif
#endif

#ifndef yytext_ptr
static void yy_flex_strncpy ( char *, const char *, int , yyscan_t yyscanner);
#endif

#ifdef YY_NEED_STRLEN
static int yy_flex_strlen ( const char * , yyscan_t yyscanner);
#endif

#ifndef YY_NO_INPUT

#endif


#ifndef YY_READ_BUF_SIZE
#ifdef __ia64__

#define YY_READ_BUF_SIZE 16384
#else
#define YY_READ_BUF_SIZE 8192
#endif 
#endif


#ifndef YY_START_STACK_INCR
#define YY_START_STACK_INCR 25
#endif


#ifndef YY_DECL
#define YY_DECL_IS_OURS 1

extern int yylex \
               (YYSTYPE * yylval_param, YYLTYPE * yylloc_param , yyscan_t yyscanner);

#define YY_DECL int yylex \
               (YYSTYPE * yylval_param, YYLTYPE * yylloc_param , yyscan_t yyscanner)
#endif 



#undef YY_NEW_FILE
#undef YY_FLUSH_BUFFER
#undef yy_set_bol
#undef yy_new_buffer
#undef yy_set_interactive
#undef YY_DO_BEFORE_ACTION

#ifdef YY_DECL_IS_OURS
#undef YY_DECL_IS_OURS
#undef YY_DECL
#endif

#ifndef hsql__create_buffer_ALREADY_DEFINED
#undef yy_create_buffer
#endif
#ifndef hsql__delete_buffer_ALREADY_DEFINED
#undef yy_delete_buffer
#endif
#ifndef hsql__scan_buffer_ALREADY_DEFINED
#undef yy_scan_buffer
#endif
#ifndef hsql__scan_string_ALREADY_DEFINED
#undef yy_scan_string
#endif
#ifndef hsql__scan_bytes_ALREADY_DEFINED
#undef yy_scan_bytes
#endif
#ifndef hsql__init_buffer_ALREADY_DEFINED
#undef yy_init_buffer
#endif
#ifndef hsql__flush_buffer_ALREADY_DEFINED
#undef yy_flush_buffer
#endif
#ifndef hsql__load_buffer_state_ALREADY_DEFINED
#undef yy_load_buffer_state
#endif
#ifndef hsql__switch_to_buffer_ALREADY_DEFINED
#undef yy_switch_to_buffer
#endif
#ifndef hsql_push_buffer_state_ALREADY_DEFINED
#undef yypush_buffer_state
#endif
#ifndef hsql_pop_buffer_state_ALREADY_DEFINED
#undef yypop_buffer_state
#endif
#ifndef hsql_ensure_buffer_stack_ALREADY_DEFINED
#undef yyensure_buffer_stack
#endif
#ifndef hsql_lex_ALREADY_DEFINED
#undef yylex
#endif
#ifndef hsql_restart_ALREADY_DEFINED
#undef yyrestart
#endif
#ifndef hsql_lex_init_ALREADY_DEFINED
#undef yylex_init
#endif
#ifndef hsql_lex_init_extra_ALREADY_DEFINED
#undef yylex_init_extra
#endif
#ifndef hsql_lex_destroy_ALREADY_DEFINED
#undef yylex_destroy
#endif
#ifndef hsql_get_debug_ALREADY_DEFINED
#undef yyget_debug
#endif
#ifndef hsql_set_debug_ALREADY_DEFINED
#undef yyset_debug
#endif
#ifndef hsql_get_extra_ALREADY_DEFINED
#undef yyget_extra
#endif
#ifndef hsql_set_extra_ALREADY_DEFINED
#undef yyset_extra
#endif
#ifndef hsql_get_in_ALREADY_DEFINED
#undef yyget_in
#endif
#ifndef hsql_set_in_ALREADY_DEFINED
#undef yyset_in
#endif
#ifndef hsql_get_out_ALREADY_DEFINED
#undef yyget_out
#endif
#ifndef hsql_set_out_ALREADY_DEFINED
#undef yyset_out
#endif
#ifndef hsql_get_leng_ALREADY_DEFINED
#undef yyget_leng
#endif
#ifndef hsql_get_text_ALREADY_DEFINED
#undef yyget_text
#endif
#ifndef hsql_get_lineno_ALREADY_DEFINED
#undef yyget_lineno
#endif
#ifndef hsql_set_lineno_ALREADY_DEFINED
#undef yyset_lineno
#endif
#ifndef hsql_get_column_ALREADY_DEFINED
#undef yyget_column
#endif
#ifndef hsql_set_column_ALREADY_DEFINED
#undef yyset_column
#endif
#ifndef hsql_wrap_ALREADY_DEFINED
#undef yywrap
#endif
#ifndef hsql_get_lval_ALREADY_DEFINED
#undef yyget_lval
#endif
#ifndef hsql_set_lval_ALREADY_DEFINED
#undef yyset_lval
#endif
#ifndef hsql_get_lloc_ALREADY_DEFINED
#undef yyget_lloc
#endif
#ifndef hsql_set_lloc_ALREADY_DEFINED
#undef yyset_lloc
#endif
#ifndef hsql_alloc_ALREADY_DEFINED
#undef yyalloc
#endif
#ifndef hsql_realloc_ALREADY_DEFINED
#undef yyrealloc
#endif
#ifndef hsql_free_ALREADY_DEFINED
#undef yyfree
#endif
#ifndef hsql_text_ALREADY_DEFINED
#undef yytext
#endif
#ifndef hsql_leng_ALREADY_DEFINED
#undef yyleng
#endif
#ifndef hsql_in_ALREADY_DEFINED
#undef yyin
#endif
#ifndef hsql_out_ALREADY_DEFINED
#undef yyout
#endif
#ifndef hsql__flex_debug_ALREADY_DEFINED
#undef yy_flex_debug
#endif
#ifndef hsql_lineno_ALREADY_DEFINED
#undef yylineno
#endif
#ifndef hsql_tables_fload_ALREADY_DEFINED
#undef yytables_fload
#endif
#ifndef hsql_tables_destroy_ALREADY_DEFINED
#undef yytables_destroy
#endif
#ifndef hsql_TABLES_NAME_ALREADY_DEFINED
#undef yyTABLES_NAME
#endif

#line 286 "flex_lexer.l"


#line 736 "flex_lexer.h"
#undef hsql_IN_HEADER
#endif 
