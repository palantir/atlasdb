// Generated from /Volumes/git/code/atlasdb/atlasdb-sql/src/main/antlr4/com/palantir/atlasdb/sql/grammar/generated/AtlasSQLLexer.g4 by ANTLR 4.5
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
		"LET", "GET", "ID", "DECIMAL", "DIGIT", "NON_DIGIT_ID", "BOOLEAN", "NEWLINE", 
		"WS", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", 
		"N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\32\u012a\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
		" \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
		"+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
		"\t\64\4\65\t\65\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13"+
		"\3\13\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u0092\n\f\3\r\3\r\3\r\3\r\3\r\5\r\u0099"+
		"\n\r\3\16\3\16\3\17\3\17\3\20\3\20\3\21\3\21\3\21\3\21\5\21\u00a5\n\21"+
		"\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\24\3\24\3\24\3\25\3\25\3\25\7\25"+
		"\u00b4\n\25\f\25\16\25\u00b7\13\25\3\25\6\25\u00ba\n\25\r\25\16\25\u00bb"+
		"\3\25\3\25\3\25\7\25\u00c1\n\25\f\25\16\25\u00c4\13\25\5\25\u00c6\n\25"+
		"\3\26\6\26\u00c9\n\26\r\26\16\26\u00ca\3\26\3\26\6\26\u00cf\n\26\r\26"+
		"\16\26\u00d0\5\26\u00d3\n\26\3\27\3\27\3\30\6\30\u00d8\n\30\r\30\16\30"+
		"\u00d9\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\5\31\u00e7"+
		"\n\31\3\32\5\32\u00ea\n\32\3\32\3\32\3\32\3\32\3\33\6\33\u00f1\n\33\r"+
		"\33\16\33\u00f2\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3"+
		" \3!\3!\3\"\3\"\3#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3"+
		"+\3,\3,\3-\3-\3.\3.\3/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3\64"+
		"\3\64\3\65\3\65\2\2\66\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27"+
		"\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\2/\2\61\30\63\31"+
		"\65\32\67\29\2;\2=\2?\2A\2C\2E\2G\2I\2K\2M\2O\2Q\2S\2U\2W\2Y\2[\2]\2_"+
		"\2a\2c\2e\2g\2i\2\3\2\36\5\2C\\aac|\5\2\13\f\17\17\"\"\4\2CCcc\4\2DDd"+
		"d\4\2EEee\4\2FFff\4\2GGgg\4\2HHhh\4\2IIii\4\2JJjj\4\2KKkk\4\2LLll\4\2"+
		"MMmm\4\2NNnn\4\2OOoo\4\2PPpp\4\2QQqq\4\2RRrr\4\2SSss\4\2TTtt\4\2UUuu\4"+
		"\2VVvv\4\2WWww\4\2XXxx\4\2YYyy\4\2ZZzz\4\2[[{{\4\2\\\\||\u011d\2\3\3\2"+
		"\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17"+
		"\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2"+
		"\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3"+
		"\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65"+
		"\3\2\2\2\3k\3\2\2\2\5r\3\2\2\2\7w\3\2\2\2\t}\3\2\2\2\13\177\3\2\2\2\r"+
		"\u0081\3\2\2\2\17\u0083\3\2\2\2\21\u0085\3\2\2\2\23\u0087\3\2\2\2\25\u0089"+
		"\3\2\2\2\27\u0091\3\2\2\2\31\u0098\3\2\2\2\33\u009a\3\2\2\2\35\u009c\3"+
		"\2\2\2\37\u009e\3\2\2\2!\u00a4\3\2\2\2#\u00a6\3\2\2\2%\u00aa\3\2\2\2\'"+
		"\u00ad\3\2\2\2)\u00c5\3\2\2\2+\u00c8\3\2\2\2-\u00d4\3\2\2\2/\u00d7\3\2"+
		"\2\2\61\u00e6\3\2\2\2\63\u00e9\3\2\2\2\65\u00f0\3\2\2\2\67\u00f6\3\2\2"+
		"\29\u00f8\3\2\2\2;\u00fa\3\2\2\2=\u00fc\3\2\2\2?\u00fe\3\2\2\2A\u0100"+
		"\3\2\2\2C\u0102\3\2\2\2E\u0104\3\2\2\2G\u0106\3\2\2\2I\u0108\3\2\2\2K"+
		"\u010a\3\2\2\2M\u010c\3\2\2\2O\u010e\3\2\2\2Q\u0110\3\2\2\2S\u0112\3\2"+
		"\2\2U\u0114\3\2\2\2W\u0116\3\2\2\2Y\u0118\3\2\2\2[\u011a\3\2\2\2]\u011c"+
		"\3\2\2\2_\u011e\3\2\2\2a\u0120\3\2\2\2c\u0122\3\2\2\2e\u0124\3\2\2\2g"+
		"\u0126\3\2\2\2i\u0128\3\2\2\2kl\5[.\2lm\5? \2mn\5M\'\2no\5? \2op\5;\36"+
		"\2pq\5]/\2q\4\3\2\2\2rs\5A!\2st\5Y-\2tu\5S*\2uv\5O(\2v\6\3\2\2\2wx\5c"+
		"\62\2xy\5E#\2yz\5? \2z{\5Y-\2{|\5? \2|\b\3\2\2\2}~\7,\2\2~\n\3\2\2\2\177"+
		"\u0080\7.\2\2\u0080\f\3\2\2\2\u0081\u0082\7\60\2\2\u0082\16\3\2\2\2\u0083"+
		"\u0084\7)\2\2\u0084\20\3\2\2\2\u0085\u0086\7$\2\2\u0086\22\3\2\2\2\u0087"+
		"\u0088\7*\2\2\u0088\24\3\2\2\2\u0089\u008a\7+\2\2\u008a\26\3\2\2\2\u008b"+
		"\u008c\5\67\34\2\u008c\u008d\5Q)\2\u008d\u008e\5=\37\2\u008e\u0092\3\2"+
		"\2\2\u008f\u0090\7(\2\2\u0090\u0092\7(\2\2\u0091\u008b\3\2\2\2\u0091\u008f"+
		"\3\2\2\2\u0092\30\3\2\2\2\u0093\u0094\5S*\2\u0094\u0095\5Y-\2\u0095\u0099"+
		"\3\2\2\2\u0096\u0097\7~\2\2\u0097\u0099\7~\2\2\u0098\u0093\3\2\2\2\u0098"+
		"\u0096\3\2\2\2\u0099\32\3\2\2\2\u009a\u009b\7?\2\2\u009b\34\3\2\2\2\u009c"+
		"\u009d\7>\2\2\u009d\36\3\2\2\2\u009e\u009f\7@\2\2\u009f \3\2\2\2\u00a0"+
		"\u00a1\7>\2\2\u00a1\u00a5\7@\2\2\u00a2\u00a3\7#\2\2\u00a3\u00a5\7?\2\2"+
		"\u00a4\u00a0\3\2\2\2\u00a4\u00a2\3\2\2\2\u00a5\"\3\2\2\2\u00a6\u00a7\5"+
		"Q)\2\u00a7\u00a8\5S*\2\u00a8\u00a9\5]/\2\u00a9$\3\2\2\2\u00aa\u00ab\7"+
		">\2\2\u00ab\u00ac\7?\2\2\u00ac&\3\2\2\2\u00ad\u00ae\7@\2\2\u00ae\u00af"+
		"\7?\2\2\u00af(\3\2\2\2\u00b0\u00b5\5/\30\2\u00b1\u00b4\5-\27\2\u00b2\u00b4"+
		"\5/\30\2\u00b3\u00b1\3\2\2\2\u00b3\u00b2\3\2\2\2\u00b4\u00b7\3\2\2\2\u00b5"+
		"\u00b3\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00c6\3\2\2\2\u00b7\u00b5\3\2"+
		"\2\2\u00b8\u00ba\5-\27\2\u00b9\u00b8\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb"+
		"\u00b9\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00bd\3\2\2\2\u00bd\u00c2\5/"+
		"\30\2\u00be\u00c1\5-\27\2\u00bf\u00c1\5/\30\2\u00c0\u00be\3\2\2\2\u00c0"+
		"\u00bf\3\2\2\2\u00c1\u00c4\3\2\2\2\u00c2\u00c0\3\2\2\2\u00c2\u00c3\3\2"+
		"\2\2\u00c3\u00c6\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c5\u00b0\3\2\2\2\u00c5"+
		"\u00b9\3\2\2\2\u00c6*\3\2\2\2\u00c7\u00c9\5-\27\2\u00c8\u00c7\3\2\2\2"+
		"\u00c9\u00ca\3\2\2\2\u00ca\u00c8\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb\u00d2"+
		"\3\2\2\2\u00cc\u00ce\5\r\7\2\u00cd\u00cf\5-\27\2\u00ce\u00cd\3\2\2\2\u00cf"+
		"\u00d0\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d0\u00d1\3\2\2\2\u00d1\u00d3\3\2"+
		"\2\2\u00d2\u00cc\3\2\2\2\u00d2\u00d3\3\2\2\2\u00d3,\3\2\2\2\u00d4\u00d5"+
		"\4\62;\2\u00d5.\3\2\2\2\u00d6\u00d8\t\2\2\2\u00d7\u00d6\3\2\2\2\u00d8"+
		"\u00d9\3\2\2\2\u00d9\u00d7\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\60\3\2\2"+
		"\2\u00db\u00dc\5]/\2\u00dc\u00dd\5Y-\2\u00dd\u00de\5_\60\2\u00de\u00df"+
		"\5? \2\u00df\u00e7\3\2\2\2\u00e0\u00e1\5A!\2\u00e1\u00e2\5\67\34\2\u00e2"+
		"\u00e3\5M\'\2\u00e3\u00e4\5[.\2\u00e4\u00e5\5? \2\u00e5\u00e7\3\2\2\2"+
		"\u00e6\u00db\3\2\2\2\u00e6\u00e0\3\2\2\2\u00e7\62\3\2\2\2\u00e8\u00ea"+
		"\7\17\2\2\u00e9\u00e8\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00eb\3\2\2\2"+
		"\u00eb\u00ec\7\f\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ee\b\32\2\2\u00ee\64"+
		"\3\2\2\2\u00ef\u00f1\t\3\2\2\u00f0\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2"+
		"\u00f0\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f5\b\33"+
		"\2\2\u00f5\66\3\2\2\2\u00f6\u00f7\t\4\2\2\u00f78\3\2\2\2\u00f8\u00f9\t"+
		"\5\2\2\u00f9:\3\2\2\2\u00fa\u00fb\t\6\2\2\u00fb<\3\2\2\2\u00fc\u00fd\t"+
		"\7\2\2\u00fd>\3\2\2\2\u00fe\u00ff\t\b\2\2\u00ff@\3\2\2\2\u0100\u0101\t"+
		"\t\2\2\u0101B\3\2\2\2\u0102\u0103\t\n\2\2\u0103D\3\2\2\2\u0104\u0105\t"+
		"\13\2\2\u0105F\3\2\2\2\u0106\u0107\t\f\2\2\u0107H\3\2\2\2\u0108\u0109"+
		"\t\r\2\2\u0109J\3\2\2\2\u010a\u010b\t\16\2\2\u010bL\3\2\2\2\u010c\u010d"+
		"\t\17\2\2\u010dN\3\2\2\2\u010e\u010f\t\20\2\2\u010fP\3\2\2\2\u0110\u0111"+
		"\t\21\2\2\u0111R\3\2\2\2\u0112\u0113\t\22\2\2\u0113T\3\2\2\2\u0114\u0115"+
		"\t\23\2\2\u0115V\3\2\2\2\u0116\u0117\t\24\2\2\u0117X\3\2\2\2\u0118\u0119"+
		"\t\25\2\2\u0119Z\3\2\2\2\u011a\u011b\t\26\2\2\u011b\\\3\2\2\2\u011c\u011d"+
		"\t\27\2\2\u011d^\3\2\2\2\u011e\u011f\t\30\2\2\u011f`\3\2\2\2\u0120\u0121"+
		"\t\31\2\2\u0121b\3\2\2\2\u0122\u0123\t\32\2\2\u0123d\3\2\2\2\u0124\u0125"+
		"\t\33\2\2\u0125f\3\2\2\2\u0126\u0127\t\34\2\2\u0127h\3\2\2\2\u0128\u0129"+
		"\t\35\2\2\u0129j\3\2\2\2\23\2\u0091\u0098\u00a4\u00b3\u00b5\u00bb\u00c0"+
		"\u00c2\u00c5\u00ca\u00d0\u00d2\u00d9\u00e6\u00e9\u00f2\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}