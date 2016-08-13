
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
   : WHERE expr
   ;

expr
   : LPAREN expr RPAREN                             # parenExpr
   | left=expr AND right=expr                       # boolAndExpr
   | left=expr OR  right=expr                       # boolOrExpr
   | left=term_expr relational_op right=term_expr   # relationalExpr
   | term_expr                                      # terminalExpr
   ;

term_expr
   : literal
   | identifier
   ;

literal
   : DECIMAL
   | BOOLEAN
   | string_literal
   ;

string_literal
   : SINGLE_QUOTE ID SINGLE_QUOTE
   | DOUBLE_QUOTE ID DOUBLE_QUOTE
   ;

identifier
   : ID
   ;

relational_op
   : EQ
   | NOT_EQ
   | LTH
   | GTH
   | LET
   | GET
   ;

