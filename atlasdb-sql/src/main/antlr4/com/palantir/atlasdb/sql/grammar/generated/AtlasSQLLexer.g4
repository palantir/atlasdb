
lexer grammar AtlasSQLLexer;
@ header { 
 }

// syntax markers

SELECT
   : 'select'
   ;

FROM
   : 'from'
   ;

WHERE
   : 'where'
   ;

// useful tokens

STAR
   : '*'
   ;

COMMA
   : ','
   ;

DOT
   : '.'
   ;

SINGLE_QUOTE
   : '\''
   ;

DOUBLE_QUOTE
   : '"'
   ;

LPAREN
   : '('
   ;

RPAREN
   : ')'
   ;

// boolean ops

AND
   : 'and' | '&&'
   ;

OR
   : 'or' | '||'
   ;

// expression ops

EQ
   : '='
   ;

LTH
   : '<'
   ;

GTH
   : '>'
   ;

NOT_EQ
   : '<>' | '!='
   ;

NOT
   : 'not'
   ;

LET
   : '<='
   ;

GET
   : '>='
   ;

// identifier and literals

ID
   : ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )+
   ;

DECIMAL
   : '-'? [0-9]+ ( '.' [0-9]+ )?
   ;

BOOLEAN
   : ('true'|'false')
   ;

// skip tokens

NEWLINE
   : '\r'? '\n' -> skip
   ;

WS
   : ( ' ' | '\t' | '\n' | '\r' )+ -> skip
   ;
