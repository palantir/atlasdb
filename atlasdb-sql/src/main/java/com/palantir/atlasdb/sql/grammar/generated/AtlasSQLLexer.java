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
		"LET", "GET", "ID", "DECIMAL", "BOOLEAN", "NEWLINE", "WS", "A", "B", "C", 
		"D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", 
		"R", "S", "T", "U", "V", "W", "X", "Y", "Z"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, "'*'", "','", "'.'", "'''", "'\"'", "'('", "')'", 
		null, null, "'='", "'<'", "'>'", null, null, "'<='", "'>='"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\32\u0110\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
		" \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
		"+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\3\2"+
		"\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3"+
		"\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3"+
		"\f\3\f\3\f\5\f\u008e\n\f\3\r\3\r\3\r\3\r\3\r\5\r\u0095\n\r\3\16\3\16\3"+
		"\17\3\17\3\20\3\20\3\21\3\21\3\21\3\21\5\21\u00a1\n\21\3\22\3\22\3\22"+
		"\3\22\3\23\3\23\3\23\3\24\3\24\3\24\3\25\6\25\u00ae\n\25\r\25\16\25\u00af"+
		"\3\26\5\26\u00b3\n\26\3\26\6\26\u00b6\n\26\r\26\16\26\u00b7\3\26\3\26"+
		"\6\26\u00bc\n\26\r\26\16\26\u00bd\5\26\u00c0\n\26\3\27\3\27\3\27\3\27"+
		"\3\27\3\27\3\27\3\27\3\27\3\27\3\27\5\27\u00cd\n\27\3\30\5\30\u00d0\n"+
		"\30\3\30\3\30\3\30\3\30\3\31\6\31\u00d7\n\31\r\31\16\31\u00d8\3\31\3\31"+
		"\3\32\3\32\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3!"+
		"\3!\3\"\3\"\3#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3+\3"+
		",\3,\3-\3-\3.\3.\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\2\2\64"+
		"\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20"+
		"\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\2\65\2\67\29\2;\2=\2"+
		"?\2A\2C\2E\2G\2I\2K\2M\2O\2Q\2S\2U\2W\2Y\2[\2]\2_\2a\2c\2e\2\3\2\37\6"+
		"\2\62;C\\aac|\3\2\62;\5\2\13\f\17\17\"\"\4\2CCcc\4\2DDdd\4\2EEee\4\2F"+
		"Fff\4\2GGgg\4\2HHhh\4\2IIii\4\2JJjj\4\2KKkk\4\2LLll\4\2MMmm\4\2NNnn\4"+
		"\2OOoo\4\2PPpp\4\2QQqq\4\2RRrr\4\2SSss\4\2TTtt\4\2UUuu\4\2VVvv\4\2WWw"+
		"w\4\2XXxx\4\2YYyy\4\2ZZzz\4\2[[{{\4\2\\\\||\u0100\2\3\3\2\2\2\2\5\3\2"+
		"\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21"+
		"\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2"+
		"\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3"+
		"\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\3g\3\2"+
		"\2\2\5n\3\2\2\2\7s\3\2\2\2\ty\3\2\2\2\13{\3\2\2\2\r}\3\2\2\2\17\177\3"+
		"\2\2\2\21\u0081\3\2\2\2\23\u0083\3\2\2\2\25\u0085\3\2\2\2\27\u008d\3\2"+
		"\2\2\31\u0094\3\2\2\2\33\u0096\3\2\2\2\35\u0098\3\2\2\2\37\u009a\3\2\2"+
		"\2!\u00a0\3\2\2\2#\u00a2\3\2\2\2%\u00a6\3\2\2\2\'\u00a9\3\2\2\2)\u00ad"+
		"\3\2\2\2+\u00b2\3\2\2\2-\u00cc\3\2\2\2/\u00cf\3\2\2\2\61\u00d6\3\2\2\2"+
		"\63\u00dc\3\2\2\2\65\u00de\3\2\2\2\67\u00e0\3\2\2\29\u00e2\3\2\2\2;\u00e4"+
		"\3\2\2\2=\u00e6\3\2\2\2?\u00e8\3\2\2\2A\u00ea\3\2\2\2C\u00ec\3\2\2\2E"+
		"\u00ee\3\2\2\2G\u00f0\3\2\2\2I\u00f2\3\2\2\2K\u00f4\3\2\2\2M\u00f6\3\2"+
		"\2\2O\u00f8\3\2\2\2Q\u00fa\3\2\2\2S\u00fc\3\2\2\2U\u00fe\3\2\2\2W\u0100"+
		"\3\2\2\2Y\u0102\3\2\2\2[\u0104\3\2\2\2]\u0106\3\2\2\2_\u0108\3\2\2\2a"+
		"\u010a\3\2\2\2c\u010c\3\2\2\2e\u010e\3\2\2\2gh\5W,\2hi\5;\36\2ij\5I%\2"+
		"jk\5;\36\2kl\5\67\34\2lm\5Y-\2m\4\3\2\2\2no\5=\37\2op\5U+\2pq\5O(\2qr"+
		"\5K&\2r\6\3\2\2\2st\5_\60\2tu\5A!\2uv\5;\36\2vw\5U+\2wx\5;\36\2x\b\3\2"+
		"\2\2yz\7,\2\2z\n\3\2\2\2{|\7.\2\2|\f\3\2\2\2}~\7\60\2\2~\16\3\2\2\2\177"+
		"\u0080\7)\2\2\u0080\20\3\2\2\2\u0081\u0082\7$\2\2\u0082\22\3\2\2\2\u0083"+
		"\u0084\7*\2\2\u0084\24\3\2\2\2\u0085\u0086\7+\2\2\u0086\26\3\2\2\2\u0087"+
		"\u0088\5\63\32\2\u0088\u0089\5M\'\2\u0089\u008a\59\35\2\u008a\u008e\3"+
		"\2\2\2\u008b\u008c\7(\2\2\u008c\u008e\7(\2\2\u008d\u0087\3\2\2\2\u008d"+
		"\u008b\3\2\2\2\u008e\30\3\2\2\2\u008f\u0090\5O(\2\u0090\u0091\5U+\2\u0091"+
		"\u0095\3\2\2\2\u0092\u0093\7~\2\2\u0093\u0095\7~\2\2\u0094\u008f\3\2\2"+
		"\2\u0094\u0092\3\2\2\2\u0095\32\3\2\2\2\u0096\u0097\7?\2\2\u0097\34\3"+
		"\2\2\2\u0098\u0099\7>\2\2\u0099\36\3\2\2\2\u009a\u009b\7@\2\2\u009b \3"+
		"\2\2\2\u009c\u009d\7>\2\2\u009d\u00a1\7@\2\2\u009e\u009f\7#\2\2\u009f"+
		"\u00a1\7?\2\2\u00a0\u009c\3\2\2\2\u00a0\u009e\3\2\2\2\u00a1\"\3\2\2\2"+
		"\u00a2\u00a3\5M\'\2\u00a3\u00a4\5O(\2\u00a4\u00a5\5Y-\2\u00a5$\3\2\2\2"+
		"\u00a6\u00a7\7>\2\2\u00a7\u00a8\7?\2\2\u00a8&\3\2\2\2\u00a9\u00aa\7@\2"+
		"\2\u00aa\u00ab\7?\2\2\u00ab(\3\2\2\2\u00ac\u00ae\t\2\2\2\u00ad\u00ac\3"+
		"\2\2\2\u00ae\u00af\3\2\2\2\u00af\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0"+
		"*\3\2\2\2\u00b1\u00b3\7/\2\2\u00b2\u00b1\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3"+
		"\u00b5\3\2\2\2\u00b4\u00b6\t\3\2\2\u00b5\u00b4\3\2\2\2\u00b6\u00b7\3\2"+
		"\2\2\u00b7\u00b5\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00bf\3\2\2\2\u00b9"+
		"\u00bb\7\60\2\2\u00ba\u00bc\t\3\2\2\u00bb\u00ba\3\2\2\2\u00bc\u00bd\3"+
		"\2\2\2\u00bd\u00bb\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00c0\3\2\2\2\u00bf"+
		"\u00b9\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0,\3\2\2\2\u00c1\u00c2\5Y-\2\u00c2"+
		"\u00c3\5U+\2\u00c3\u00c4\5[.\2\u00c4\u00c5\5;\36\2\u00c5\u00cd\3\2\2\2"+
		"\u00c6\u00c7\5=\37\2\u00c7\u00c8\5\63\32\2\u00c8\u00c9\5I%\2\u00c9\u00ca"+
		"\5W,\2\u00ca\u00cb\5;\36\2\u00cb\u00cd\3\2\2\2\u00cc\u00c1\3\2\2\2\u00cc"+
		"\u00c6\3\2\2\2\u00cd.\3\2\2\2\u00ce\u00d0\7\17\2\2\u00cf\u00ce\3\2\2\2"+
		"\u00cf\u00d0\3\2\2\2\u00d0\u00d1\3\2\2\2\u00d1\u00d2\7\f\2\2\u00d2\u00d3"+
		"\3\2\2\2\u00d3\u00d4\b\30\2\2\u00d4\60\3\2\2\2\u00d5\u00d7\t\4\2\2\u00d6"+
		"\u00d5\3\2\2\2\u00d7\u00d8\3\2\2\2\u00d8\u00d6\3\2\2\2\u00d8\u00d9\3\2"+
		"\2\2\u00d9\u00da\3\2\2\2\u00da\u00db\b\31\2\2\u00db\62\3\2\2\2\u00dc\u00dd"+
		"\t\5\2\2\u00dd\64\3\2\2\2\u00de\u00df\t\6\2\2\u00df\66\3\2\2\2\u00e0\u00e1"+
		"\t\7\2\2\u00e18\3\2\2\2\u00e2\u00e3\t\b\2\2\u00e3:\3\2\2\2\u00e4\u00e5"+
		"\t\t\2\2\u00e5<\3\2\2\2\u00e6\u00e7\t\n\2\2\u00e7>\3\2\2\2\u00e8\u00e9"+
		"\t\13\2\2\u00e9@\3\2\2\2\u00ea\u00eb\t\f\2\2\u00ebB\3\2\2\2\u00ec\u00ed"+
		"\t\r\2\2\u00edD\3\2\2\2\u00ee\u00ef\t\16\2\2\u00efF\3\2\2\2\u00f0\u00f1"+
		"\t\17\2\2\u00f1H\3\2\2\2\u00f2\u00f3\t\20\2\2\u00f3J\3\2\2\2\u00f4\u00f5"+
		"\t\21\2\2\u00f5L\3\2\2\2\u00f6\u00f7\t\22\2\2\u00f7N\3\2\2\2\u00f8\u00f9"+
		"\t\23\2\2\u00f9P\3\2\2\2\u00fa\u00fb\t\24\2\2\u00fbR\3\2\2\2\u00fc\u00fd"+
		"\t\25\2\2\u00fdT\3\2\2\2\u00fe\u00ff\t\26\2\2\u00ffV\3\2\2\2\u0100\u0101"+
		"\t\27\2\2\u0101X\3\2\2\2\u0102\u0103\t\30\2\2\u0103Z\3\2\2\2\u0104\u0105"+
		"\t\31\2\2\u0105\\\3\2\2\2\u0106\u0107\t\32\2\2\u0107^\3\2\2\2\u0108\u0109"+
		"\t\33\2\2\u0109`\3\2\2\2\u010a\u010b\t\34\2\2\u010bb\3\2\2\2\u010c\u010d"+
		"\t\35\2\2\u010dd\3\2\2\2\u010e\u010f\t\36\2\2\u010ff\3\2\2\2\16\2\u008d"+
		"\u0094\u00a0\u00af\u00b2\u00b7\u00bd\u00bf\u00cc\u00cf\u00d8\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}