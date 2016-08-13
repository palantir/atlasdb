// Generated from /Volumes/git/atlasdb-2/atlasdb-sql/src/main/antlr4/com/palantir/atlasdb/sql/grammar/generated/AtlasSQLLexer.g4 by ANTLR 4.5
package com.palantir.atlasdb.sql.grammar.generated;
 
 
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class AtlasSQLLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		SELECT=1, FROM=2, WHERE=3, STAR=4, COMMA=5, DOT=6, SINGLE_QUOTE=7, DOUBLE_QUOTE=8, 
		LPAREN=9, RPAREN=10, AND=11, OR=12, EQ=13, LTH=14, GTH=15, NOT_EQ=16, 
		NOT=17, LET=18, GET=19, ID=20, DECIMAL=21, BOOLEAN=22, NEWLINE=23, WS=24;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"SELECT", "FROM", "WHERE", "STAR", "COMMA", "DOT", "SINGLE_QUOTE", "DOUBLE_QUOTE", 
		"LPAREN", "RPAREN", "AND", "OR", "EQ", "LTH", "GTH", "NOT_EQ", "NOT", 
		"LET", "GET", "ID", "DECIMAL", "BOOLEAN", "NEWLINE", "WS"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'select'", "'from'", "'where'", "'*'", "','", "'.'", "'''", "'\"'", 
		"'('", "')'", null, null, "'='", "'<'", "'>'", null, "'not'", "'<='", 
		"'>='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "SELECT", "FROM", "WHERE", "STAR", "COMMA", "DOT", "SINGLE_QUOTE", 
		"DOUBLE_QUOTE", "LPAREN", "RPAREN", "AND", "OR", "EQ", "LTH", "GTH", "NOT_EQ", 
		"NOT", "LET", "GET", "ID", "DECIMAL", "BOOLEAN", "NEWLINE", "WS"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public AtlasSQLLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "AtlasSQLLexer.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\32\u00a4\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f"+
		"\3\f\3\f\3\f\3\f\5\fY\n\f\3\r\3\r\3\r\3\r\5\r_\n\r\3\16\3\16\3\17\3\17"+
		"\3\20\3\20\3\21\3\21\3\21\3\21\5\21k\n\21\3\22\3\22\3\22\3\22\3\23\3\23"+
		"\3\23\3\24\3\24\3\24\3\25\6\25x\n\25\r\25\16\25y\3\26\5\26}\n\26\3\26"+
		"\6\26\u0080\n\26\r\26\16\26\u0081\3\26\3\26\6\26\u0086\n\26\r\26\16\26"+
		"\u0087\5\26\u008a\n\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\5"+
		"\27\u0095\n\27\3\30\5\30\u0098\n\30\3\30\3\30\3\30\3\30\3\31\6\31\u009f"+
		"\n\31\r\31\16\31\u00a0\3\31\3\31\2\2\32\3\3\5\4\7\5\t\6\13\7\r\b\17\t"+
		"\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27"+
		"-\30/\31\61\32\3\2\5\6\2\62;C\\aac|\3\2\62;\5\2\13\f\17\17\"\"\u00ae\2"+
		"\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2"+
		"\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2"+
		"\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2"+
		"\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2"+
		"\2\61\3\2\2\2\3\63\3\2\2\2\5:\3\2\2\2\7?\3\2\2\2\tE\3\2\2\2\13G\3\2\2"+
		"\2\rI\3\2\2\2\17K\3\2\2\2\21M\3\2\2\2\23O\3\2\2\2\25Q\3\2\2\2\27X\3\2"+
		"\2\2\31^\3\2\2\2\33`\3\2\2\2\35b\3\2\2\2\37d\3\2\2\2!j\3\2\2\2#l\3\2\2"+
		"\2%p\3\2\2\2\'s\3\2\2\2)w\3\2\2\2+|\3\2\2\2-\u0094\3\2\2\2/\u0097\3\2"+
		"\2\2\61\u009e\3\2\2\2\63\64\7u\2\2\64\65\7g\2\2\65\66\7n\2\2\66\67\7g"+
		"\2\2\678\7e\2\289\7v\2\29\4\3\2\2\2:;\7h\2\2;<\7t\2\2<=\7q\2\2=>\7o\2"+
		"\2>\6\3\2\2\2?@\7y\2\2@A\7j\2\2AB\7g\2\2BC\7t\2\2CD\7g\2\2D\b\3\2\2\2"+
		"EF\7,\2\2F\n\3\2\2\2GH\7.\2\2H\f\3\2\2\2IJ\7\60\2\2J\16\3\2\2\2KL\7)\2"+
		"\2L\20\3\2\2\2MN\7$\2\2N\22\3\2\2\2OP\7*\2\2P\24\3\2\2\2QR\7+\2\2R\26"+
		"\3\2\2\2ST\7c\2\2TU\7p\2\2UY\7f\2\2VW\7(\2\2WY\7(\2\2XS\3\2\2\2XV\3\2"+
		"\2\2Y\30\3\2\2\2Z[\7q\2\2[_\7t\2\2\\]\7~\2\2]_\7~\2\2^Z\3\2\2\2^\\\3\2"+
		"\2\2_\32\3\2\2\2`a\7?\2\2a\34\3\2\2\2bc\7>\2\2c\36\3\2\2\2de\7@\2\2e "+
		"\3\2\2\2fg\7>\2\2gk\7@\2\2hi\7#\2\2ik\7?\2\2jf\3\2\2\2jh\3\2\2\2k\"\3"+
		"\2\2\2lm\7p\2\2mn\7q\2\2no\7v\2\2o$\3\2\2\2pq\7>\2\2qr\7?\2\2r&\3\2\2"+
		"\2st\7@\2\2tu\7?\2\2u(\3\2\2\2vx\t\2\2\2wv\3\2\2\2xy\3\2\2\2yw\3\2\2\2"+
		"yz\3\2\2\2z*\3\2\2\2{}\7/\2\2|{\3\2\2\2|}\3\2\2\2}\177\3\2\2\2~\u0080"+
		"\t\3\2\2\177~\3\2\2\2\u0080\u0081\3\2\2\2\u0081\177\3\2\2\2\u0081\u0082"+
		"\3\2\2\2\u0082\u0089\3\2\2\2\u0083\u0085\7\60\2\2\u0084\u0086\t\3\2\2"+
		"\u0085\u0084\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0085\3\2\2\2\u0087\u0088"+
		"\3\2\2\2\u0088\u008a\3\2\2\2\u0089\u0083\3\2\2\2\u0089\u008a\3\2\2\2\u008a"+
		",\3\2\2\2\u008b\u008c\7v\2\2\u008c\u008d\7t\2\2\u008d\u008e\7w\2\2\u008e"+
		"\u0095\7g\2\2\u008f\u0090\7h\2\2\u0090\u0091\7c\2\2\u0091\u0092\7n\2\2"+
		"\u0092\u0093\7u\2\2\u0093\u0095\7g\2\2\u0094\u008b\3\2\2\2\u0094\u008f"+
		"\3\2\2\2\u0095.\3\2\2\2\u0096\u0098\7\17\2\2\u0097\u0096\3\2\2\2\u0097"+
		"\u0098\3\2\2\2\u0098\u0099\3\2\2\2\u0099\u009a\7\f\2\2\u009a\u009b\3\2"+
		"\2\2\u009b\u009c\b\30\2\2\u009c\60\3\2\2\2\u009d\u009f\t\4\2\2\u009e\u009d"+
		"\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\u009e\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1"+
		"\u00a2\3\2\2\2\u00a2\u00a3\b\31\2\2\u00a3\62\3\2\2\2\16\2X^jy|\u0081\u0087"+
		"\u0089\u0094\u0097\u00a0\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}