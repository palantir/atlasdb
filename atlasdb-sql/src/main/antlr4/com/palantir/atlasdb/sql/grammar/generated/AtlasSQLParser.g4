
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
   : simple_expression ( expr_op simple_expression )*
   ;

simple_expression
   : left_element relational_op right_element | target_element between_op left_element AND right_element | target_element is_or_is_not NULL
   ;

element
   : USER_VAR | ID | ( '|' ID '|' ) | INT | column_name
   ;

right_element
   : element
   ;

left_element
   : element
   ;

target_element
   : element
   ;

relational_op
   : EQ | LTH | GTH | NOT_EQ | LET | GET
   ;

expr_op
   : AND | XOR | OR | NOT
   ;

between_op
   : BETWEEN
   ;

is_or_is_not
   : IS | IS NOT
   ;
