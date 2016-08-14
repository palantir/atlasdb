
lexer grammar AtlasSQLLexer;
@ header {
 }

// syntax markers

SELECT
   : S E L E C T
   ;

FROM
   : F R O M
   ;

WHERE
   : W H E R E
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
   : A N D | '&&'
   ;

OR
   : O R | '||'
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
   : N O T
   ;

MAX
   : M A X
   ;

MIN
   : M I N
   ;

COUNT
   : C O U N T
   ;

LET
   : '<='
   ;

GET
   : '>='
   ;

// identifier and literals

ID
   : NON_DIGIT_ID ( DIGIT | NON_DIGIT_ID ) *
   | DIGIT+ NON_DIGIT_ID ( DIGIT | NON_DIGIT_ID ) *
   ;

DECIMAL
   : DIGIT+ ( DOT DIGIT+ )?
   ;

fragment DIGIT : ( '0' .. '9' ) ;
fragment NON_DIGIT_ID : ( 'a' .. 'z' | 'A' .. 'Z' | '_' )+ ;

BOOLEAN
   : T R U E | F A L S E
   ;

// skip tokens

NEWLINE
   : '\r'? '\n' -> skip
   ;

WS
   : ( ' ' | '\t' | '\n' | '\r' )+ -> skip
   ;

// case insensitive letters

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');
