
parser grammar AtlasSQLParser;

options
   { tokenVocab = AtlasSQLLexer; }


query
    : select_query
    ;


select_query
    : SELECT column_clause FROM table_reference ( where_clause )?
    ;

table_reference
    : ( keyspace DOT )? table_name
    ;

keyspace
    : ID
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


where_clause
   : WHERE expression
   ;

expression
   : left=expression relational_op right=expression
   | left=expression AND right=expression
   | bool
   | DECIMAL
   | ID
   ;

relational_op
   : EQ
   | LTH
   | GTH
   | LET
   | GET
   | NOT_EQ
   ;

is_or_is_not
   : IS | IS NOT
   ;

bool
   : TRUE
   | FALSE
   ;
