// Generated from /Volumes/git/code/atlasdb/atlasdb-sql/src/main/antlr4/com/palantir/atlasdb/sql/grammar/generated/AtlasSQLParser.g4 by ANTLR 4.5
package com.palantir.atlasdb.sql.grammar.generated;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class AtlasSQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		SELECT=1, FROM=2, STAR=3, WHERE=4, AND=5, OR=6, XOR=7, IS=8, NULL=9, LIKE=10, 
		IN=11, EXISTS=12, ALL=13, ANY=14, TRUE=15, FALSE=16, DIVIDE=17, MOD=18, 
		BETWEEN=19, REGEXP=20, PLUS=21, MINUS=22, NEGATION=23, VERTBAR=24, BITAND=25, 
		POWER_OP=26, BINARY=27, SHIFT_LEFT=28, SHIFT_RIGHT=29, ESCAPE=30, RPAREN=31, 
		LPAREN=32, RBRACK=33, LBRACK=34, COLON=35, ALL_FIELDS=36, EQ=37, LTH=38, 
		GTH=39, NOT_EQ=40, NOT=41, LET=42, GET=43, SEMI=44, COMMA=45, DOT=46, 
		COLLATE=47, INNER=48, OUTER=49, JOIN=50, CROSS=51, USING=52, INDEX=53, 
		KEY=54, ORDER=55, GROUP=56, BY=57, FOR=58, USE=59, IGNORE=60, PARTITION=61, 
		STRAIGHT_JOIN=62, NATURAL=63, LEFT=64, RIGHT=65, OJ=66, ON=67, ID=68, 
		INT=69, DECIMAL=70, NEWLINE=71, WS=72, USER_VAR=73;
	public static final int
		RULE_query = 0, RULE_select_query = 1, RULE_table_reference = 2, RULE_keyspace = 3, 
		RULE_table_name = 4, RULE_column_clause = 5, RULE_all_columns = 6, RULE_column_list = 7, 
		RULE_column_name = 8, RULE_where_clause = 9, RULE_expression = 10, RULE_relational_op = 11, 
		RULE_is_or_is_not = 12, RULE_bool = 13;
	public static final String[] ruleNames = {
		"query", "select_query", "table_reference", "keyspace", "table_name", 
		"column_clause", "all_columns", "column_list", "column_name", "where_clause", 
		"expression", "relational_op", "is_or_is_not", "bool"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'select'", "'from'", "'*'", "'where'", null, null, "'xor'", "'is'", 
		"'null'", "'like'", "'in'", "'exists'", "'all'", "'any'", "'true'", "'false'", 
		null, null, "'between'", "'regexp'", "'+'", "'-'", "'~'", "'|'", "'&'", 
		"'^'", "'binary'", "'<<'", "'>>'", "'escape'", "')'", "'('", "']'", "'['", 
		"':'", "'.*'", "'='", "'<'", "'>'", "'!='", "'not'", "'<='", "'>='", "';'", 
		"','", "'.'", "'collate'", "'inner'", "'outer'", "'join'", "'cross'", 
		"'using'", "'index'", "'key'", "'order'", "'group'", "'by'", "'for'", 
		"'use'", "'ignore'", "'partition'", "'straight_join'", "'natural'", "'left'", 
		"'right'", "'oj'", "'on'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "SELECT", "FROM", "STAR", "WHERE", "AND", "OR", "XOR", "IS", "NULL", 
		"LIKE", "IN", "EXISTS", "ALL", "ANY", "TRUE", "FALSE", "DIVIDE", "MOD", 
		"BETWEEN", "REGEXP", "PLUS", "MINUS", "NEGATION", "VERTBAR", "BITAND", 
		"POWER_OP", "BINARY", "SHIFT_LEFT", "SHIFT_RIGHT", "ESCAPE", "RPAREN", 
		"LPAREN", "RBRACK", "LBRACK", "COLON", "ALL_FIELDS", "EQ", "LTH", "GTH", 
		"NOT_EQ", "NOT", "LET", "GET", "SEMI", "COMMA", "DOT", "COLLATE", "INNER", 
		"OUTER", "JOIN", "CROSS", "USING", "INDEX", "KEY", "ORDER", "GROUP", "BY", 
		"FOR", "USE", "IGNORE", "PARTITION", "STRAIGHT_JOIN", "NATURAL", "LEFT", 
		"RIGHT", "OJ", "ON", "ID", "INT", "DECIMAL", "NEWLINE", "WS", "USER_VAR"
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

	@Override
	public String getGrammarFileName() { return "AtlasSQLParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public AtlasSQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class QueryContext extends ParserRuleContext {
		public Select_queryContext select_query() {
			return getRuleContext(Select_queryContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_query);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(28);
			select_query();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_queryContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(AtlasSQLParser.SELECT, 0); }
		public Column_clauseContext column_clause() {
			return getRuleContext(Column_clauseContext.class,0);
		}
		public TerminalNode FROM() { return getToken(AtlasSQLParser.FROM, 0); }
		public Table_referenceContext table_reference() {
			return getRuleContext(Table_referenceContext.class,0);
		}
		public Where_clauseContext where_clause() {
			return getRuleContext(Where_clauseContext.class,0);
		}
		public Select_queryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_query; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitSelect_query(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_queryContext select_query() throws RecognitionException {
		Select_queryContext _localctx = new Select_queryContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_select_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(30);
			match(SELECT);
			setState(31);
			column_clause();
			setState(32);
			match(FROM);
			setState(33);
			table_reference();
			setState(35);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(34);
				where_clause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_referenceContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public KeyspaceContext keyspace() {
			return getRuleContext(KeyspaceContext.class,0);
		}
		public TerminalNode DOT() { return getToken(AtlasSQLParser.DOT, 0); }
		public Table_referenceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_reference; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitTable_reference(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_referenceContext table_reference() throws RecognitionException {
		Table_referenceContext _localctx = new Table_referenceContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_table_reference);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(40);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				{
				setState(37);
				keyspace();
				setState(38);
				match(DOT);
				}
				break;
			}
			setState(42);
			table_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeyspaceContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(AtlasSQLParser.ID, 0); }
		public KeyspaceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyspace; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitKeyspace(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeyspaceContext keyspace() throws RecognitionException {
		KeyspaceContext _localctx = new KeyspaceContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_keyspace);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(44);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(AtlasSQLParser.ID, 0); }
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitTable_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(46);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_clauseContext extends ParserRuleContext {
		public All_columnsContext all_columns() {
			return getRuleContext(All_columnsContext.class,0);
		}
		public Column_listContext column_list() {
			return getRuleContext(Column_listContext.class,0);
		}
		public Column_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_clause; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitColumn_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_clauseContext column_clause() throws RecognitionException {
		Column_clauseContext _localctx = new Column_clauseContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_column_clause);
		try {
			setState(50);
			switch (_input.LA(1)) {
			case STAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(48);
				all_columns();
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(49);
				column_list();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class All_columnsContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(AtlasSQLParser.STAR, 0); }
		public All_columnsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_all_columns; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitAll_columns(this);
			else return visitor.visitChildren(this);
		}
	}

	public final All_columnsContext all_columns() throws RecognitionException {
		All_columnsContext _localctx = new All_columnsContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_all_columns);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(52);
			match(STAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_listContext extends ParserRuleContext {
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(AtlasSQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(AtlasSQLParser.COMMA, i);
		}
		public Column_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_list; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitColumn_list(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_listContext column_list() throws RecognitionException {
		Column_listContext _localctx = new Column_listContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_column_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(54);
			column_name();
			setState(59);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(55);
				match(COMMA);
				setState(56);
				column_name();
				}
				}
				setState(61);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(AtlasSQLParser.ID, 0); }
		public Column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitColumn_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_nameContext column_name() throws RecognitionException {
		Column_nameContext _localctx = new Column_nameContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_column_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(62);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Where_clauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(AtlasSQLParser.WHERE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public Where_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_where_clause; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitWhere_clause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Where_clauseContext where_clause() throws RecognitionException {
		Where_clauseContext _localctx = new Where_clauseContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_where_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(64);
			match(WHERE);
			setState(65);
			expression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext left;
		public ExpressionContext right;
		public BoolContext bool() {
			return getRuleContext(BoolContext.class,0);
		}
		public TerminalNode DECIMAL() { return getToken(AtlasSQLParser.DECIMAL, 0); }
		public TerminalNode ID() { return getToken(AtlasSQLParser.ID, 0); }
		public Relational_opContext relational_op() {
			return getRuleContext(Relational_opContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(AtlasSQLParser.AND, 0); }
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 20;
		enterRecursionRule(_localctx, 20, RULE_expression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(71);
			switch (_input.LA(1)) {
			case TRUE:
			case FALSE:
				{
				setState(68);
				bool();
				}
				break;
			case DECIMAL:
				{
				setState(69);
				match(DECIMAL);
				}
				break;
			case ID:
				{
				setState(70);
				match(ID);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(82);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(80);
					switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
					case 1:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.left = _prevctx;
						_localctx.left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(73);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(74);
						relational_op();
						setState(75);
						((ExpressionContext)_localctx).right = expression(6);
						}
						break;
					case 2:
						{
						_localctx = new ExpressionContext(_parentctx, _parentState);
						_localctx.left = _prevctx;
						_localctx.left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(77);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(78);
						match(AND);
						setState(79);
						((ExpressionContext)_localctx).right = expression(5);
						}
						break;
					}
					} 
				}
				setState(84);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class Relational_opContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(AtlasSQLParser.EQ, 0); }
		public TerminalNode LTH() { return getToken(AtlasSQLParser.LTH, 0); }
		public TerminalNode GTH() { return getToken(AtlasSQLParser.GTH, 0); }
		public TerminalNode LET() { return getToken(AtlasSQLParser.LET, 0); }
		public TerminalNode GET() { return getToken(AtlasSQLParser.GET, 0); }
		public TerminalNode NOT_EQ() { return getToken(AtlasSQLParser.NOT_EQ, 0); }
		public Relational_opContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relational_op; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitRelational_op(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Relational_opContext relational_op() throws RecognitionException {
		Relational_opContext _localctx = new Relational_opContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_relational_op);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(85);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQ) | (1L << LTH) | (1L << GTH) | (1L << NOT_EQ) | (1L << LET) | (1L << GET))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Is_or_is_notContext extends ParserRuleContext {
		public TerminalNode IS() { return getToken(AtlasSQLParser.IS, 0); }
		public TerminalNode NOT() { return getToken(AtlasSQLParser.NOT, 0); }
		public Is_or_is_notContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_is_or_is_not; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitIs_or_is_not(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Is_or_is_notContext is_or_is_not() throws RecognitionException {
		Is_or_is_notContext _localctx = new Is_or_is_notContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_is_or_is_not);
		try {
			setState(90);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(87);
				match(IS);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(88);
				match(IS);
				setState(89);
				match(NOT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BoolContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(AtlasSQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(AtlasSQLParser.FALSE, 0); }
		public BoolContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bool; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitBool(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BoolContext bool() throws RecognitionException {
		BoolContext _localctx = new BoolContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_bool);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(92);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 10:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 5);
		case 1:
			return precpred(_ctx, 4);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3Ka\4\2\t\2\4\3\t\3"+
		"\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4\f"+
		"\t\f\4\r\t\r\4\16\t\16\4\17\t\17\3\2\3\2\3\3\3\3\3\3\3\3\3\3\5\3&\n\3"+
		"\3\4\3\4\3\4\5\4+\n\4\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\5\7\65\n\7\3\b\3"+
		"\b\3\t\3\t\3\t\7\t<\n\t\f\t\16\t?\13\t\3\n\3\n\3\13\3\13\3\13\3\f\3\f"+
		"\3\f\3\f\5\fJ\n\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\7\fS\n\f\f\f\16\fV\13\f"+
		"\3\r\3\r\3\16\3\16\3\16\5\16]\n\16\3\17\3\17\3\17\2\3\26\20\2\4\6\b\n"+
		"\f\16\20\22\24\26\30\32\34\2\4\4\2\'*,-\3\2\21\22[\2\36\3\2\2\2\4 \3\2"+
		"\2\2\6*\3\2\2\2\b.\3\2\2\2\n\60\3\2\2\2\f\64\3\2\2\2\16\66\3\2\2\2\20"+
		"8\3\2\2\2\22@\3\2\2\2\24B\3\2\2\2\26I\3\2\2\2\30W\3\2\2\2\32\\\3\2\2\2"+
		"\34^\3\2\2\2\36\37\5\4\3\2\37\3\3\2\2\2 !\7\3\2\2!\"\5\f\7\2\"#\7\4\2"+
		"\2#%\5\6\4\2$&\5\24\13\2%$\3\2\2\2%&\3\2\2\2&\5\3\2\2\2\'(\5\b\5\2()\7"+
		"\60\2\2)+\3\2\2\2*\'\3\2\2\2*+\3\2\2\2+,\3\2\2\2,-\5\n\6\2-\7\3\2\2\2"+
		"./\7F\2\2/\t\3\2\2\2\60\61\7F\2\2\61\13\3\2\2\2\62\65\5\16\b\2\63\65\5"+
		"\20\t\2\64\62\3\2\2\2\64\63\3\2\2\2\65\r\3\2\2\2\66\67\7\5\2\2\67\17\3"+
		"\2\2\28=\5\22\n\29:\7/\2\2:<\5\22\n\2;9\3\2\2\2<?\3\2\2\2=;\3\2\2\2=>"+
		"\3\2\2\2>\21\3\2\2\2?=\3\2\2\2@A\7F\2\2A\23\3\2\2\2BC\7\6\2\2CD\5\26\f"+
		"\2D\25\3\2\2\2EF\b\f\1\2FJ\5\34\17\2GJ\7H\2\2HJ\7F\2\2IE\3\2\2\2IG\3\2"+
		"\2\2IH\3\2\2\2JT\3\2\2\2KL\f\7\2\2LM\5\30\r\2MN\5\26\f\bNS\3\2\2\2OP\f"+
		"\6\2\2PQ\7\7\2\2QS\5\26\f\7RK\3\2\2\2RO\3\2\2\2SV\3\2\2\2TR\3\2\2\2TU"+
		"\3\2\2\2U\27\3\2\2\2VT\3\2\2\2WX\t\2\2\2X\31\3\2\2\2Y]\7\n\2\2Z[\7\n\2"+
		"\2[]\7+\2\2\\Y\3\2\2\2\\Z\3\2\2\2]\33\3\2\2\2^_\t\3\2\2_\35\3\2\2\2\n"+
		"%*\64=IRT\\";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}