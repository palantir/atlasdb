
parser grammar AtlasSQLParser;

options
   { tokenVocab = AtlasSQLLexer; }


select_clause
    : SELECT column_clause FROM table_name
    ;

table_name
    : ID
    ;


column_clause
    : all_columns | column_list
    ;

all_columns
    : STAR
    ;

column_list
   : column_name ( COMMA column_name )*
   ;

column_name
   : ID
   ;
