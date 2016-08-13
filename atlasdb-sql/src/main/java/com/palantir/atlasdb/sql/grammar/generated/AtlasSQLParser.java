// Generated from /Volumes/git/atlasdb-2/atlasdb-sql/src/main/antlr4/com/palantir/atlasdb/sql/grammar/generated/AtlasSQLParser.g4 by ANTLR 4.5
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
		SELECT=1, FROM=2, WHERE=3, STAR=4, COMMA=5, DOT=6, SINGLE_QUOTE=7, DOUBLE_QUOTE=8, 
		LPAREN=9, RPAREN=10, AND=11, OR=12, EQ=13, LTH=14, GTH=15, NOT_EQ=16, 
		NOT=17, LET=18, GET=19, ID=20, DECIMAL=21, BOOLEAN=22, NEWLINE=23, WS=24;
	public static final int
		RULE_query = 0, RULE_select_query = 1, RULE_table_reference = 2, RULE_keyspace = 3, 
		RULE_table_name = 4, RULE_column_clause = 5, RULE_all_columns = 6, RULE_column_list = 7, 
		RULE_column_name = 8, RULE_where_clause = 9, RULE_expr = 10, RULE_term_expr = 11, 
		RULE_literal = 12, RULE_string_literal = 13, RULE_identifier = 14, RULE_relational_op = 15;
	public static final String[] ruleNames = {
		"query", "select_query", "table_reference", "keyspace", "table_name", 
		"column_clause", "all_columns", "column_list", "column_name", "where_clause", 
		"expr", "term_expr", "literal", "string_literal", "identifier", "relational_op"
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
			setState(32);
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
			setState(34);
			match(SELECT);
			setState(35);
			column_clause();
			setState(36);
			match(FROM);
			setState(37);
			table_reference();
			setState(39);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(38);
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
			setState(44);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				{
				setState(41);
				keyspace();
				setState(42);
				match(DOT);
				}
				break;
			}
			setState(46);
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
			setState(48);
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
			setState(50);
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
			setState(54);
			switch (_input.LA(1)) {
			case STAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(52);
				all_columns();
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(53);
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
			setState(56);
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
			setState(58);
			column_name();
			setState(63);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(59);
				match(COMMA);
				setState(60);
				column_name();
				}
				}
				setState(65);
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
			setState(66);
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
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
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
			setState(68);
			match(WHERE);
			setState(69);
			expr(0);
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

	public static class ExprContext extends ParserRuleContext {
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
	 
		public ExprContext() { }
		public void copyFrom(ExprContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BoolOrExprContext extends ExprContext {
		public ExprContext left;
		public ExprContext right;
		public TerminalNode OR() { return getToken(AtlasSQLParser.OR, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public BoolOrExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitBoolOrExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TerminalExprContext extends ExprContext {
		public Term_exprContext term_expr() {
			return getRuleContext(Term_exprContext.class,0);
		}
		public TerminalExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitTerminalExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RelationalExprContext extends ExprContext {
		public Term_exprContext left;
		public Term_exprContext right;
		public Relational_opContext relational_op() {
			return getRuleContext(Relational_opContext.class,0);
		}
		public List<Term_exprContext> term_expr() {
			return getRuleContexts(Term_exprContext.class);
		}
		public Term_exprContext term_expr(int i) {
			return getRuleContext(Term_exprContext.class,i);
		}
		public RelationalExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitRelationalExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenExprContext extends ExprContext {
		public TerminalNode LPAREN() { return getToken(AtlasSQLParser.LPAREN, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RPAREN() { return getToken(AtlasSQLParser.RPAREN, 0); }
		public ParenExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitParenExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BoolAndExprContext extends ExprContext {
		public ExprContext left;
		public ExprContext right;
		public TerminalNode AND() { return getToken(AtlasSQLParser.AND, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public BoolAndExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitBoolAndExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 20;
		enterRecursionRule(_localctx, 20, RULE_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(81);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				{
				_localctx = new ParenExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(72);
				match(LPAREN);
				setState(73);
				expr(0);
				setState(74);
				match(RPAREN);
				}
				break;
			case 2:
				{
				_localctx = new RelationalExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(76);
				((RelationalExprContext)_localctx).left = term_expr();
				setState(77);
				relational_op();
				setState(78);
				((RelationalExprContext)_localctx).right = term_expr();
				}
				break;
			case 3:
				{
				_localctx = new TerminalExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(80);
				term_expr();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(91);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(89);
					switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
					case 1:
						{
						_localctx = new BoolAndExprContext(new ExprContext(_parentctx, _parentState));
						((BoolAndExprContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(83);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(84);
						match(AND);
						setState(85);
						((BoolAndExprContext)_localctx).right = expr(5);
						}
						break;
					case 2:
						{
						_localctx = new BoolOrExprContext(new ExprContext(_parentctx, _parentState));
						((BoolOrExprContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(86);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(87);
						match(OR);
						setState(88);
						((BoolOrExprContext)_localctx).right = expr(4);
						}
						break;
					}
					} 
				}
				setState(93);
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

	public static class Term_exprContext extends ParserRuleContext {
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public Term_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_term_expr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitTerm_expr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Term_exprContext term_expr() throws RecognitionException {
		Term_exprContext _localctx = new Term_exprContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_term_expr);
		try {
			setState(96);
			switch (_input.LA(1)) {
			case SINGLE_QUOTE:
			case DOUBLE_QUOTE:
			case DECIMAL:
			case BOOLEAN:
				enterOuterAlt(_localctx, 1);
				{
				setState(94);
				literal();
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(95);
				identifier();
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

	public static class LiteralContext extends ParserRuleContext {
		public TerminalNode DECIMAL() { return getToken(AtlasSQLParser.DECIMAL, 0); }
		public TerminalNode BOOLEAN() { return getToken(AtlasSQLParser.BOOLEAN, 0); }
		public String_literalContext string_literal() {
			return getRuleContext(String_literalContext.class,0);
		}
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_literal);
		try {
			setState(101);
			switch (_input.LA(1)) {
			case DECIMAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(98);
				match(DECIMAL);
				}
				break;
			case BOOLEAN:
				enterOuterAlt(_localctx, 2);
				{
				setState(99);
				match(BOOLEAN);
				}
				break;
			case SINGLE_QUOTE:
			case DOUBLE_QUOTE:
				enterOuterAlt(_localctx, 3);
				{
				setState(100);
				string_literal();
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

	public static class String_literalContext extends ParserRuleContext {
		public List<TerminalNode> SINGLE_QUOTE() { return getTokens(AtlasSQLParser.SINGLE_QUOTE); }
		public TerminalNode SINGLE_QUOTE(int i) {
			return getToken(AtlasSQLParser.SINGLE_QUOTE, i);
		}
		public TerminalNode ID() { return getToken(AtlasSQLParser.ID, 0); }
		public List<TerminalNode> DOUBLE_QUOTE() { return getTokens(AtlasSQLParser.DOUBLE_QUOTE); }
		public TerminalNode DOUBLE_QUOTE(int i) {
			return getToken(AtlasSQLParser.DOUBLE_QUOTE, i);
		}
		public String_literalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string_literal; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitString_literal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final String_literalContext string_literal() throws RecognitionException {
		String_literalContext _localctx = new String_literalContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_string_literal);
		try {
			setState(109);
			switch (_input.LA(1)) {
			case SINGLE_QUOTE:
				enterOuterAlt(_localctx, 1);
				{
				setState(103);
				match(SINGLE_QUOTE);
				setState(104);
				match(ID);
				setState(105);
				match(SINGLE_QUOTE);
				}
				break;
			case DOUBLE_QUOTE:
				enterOuterAlt(_localctx, 2);
				{
				setState(106);
				match(DOUBLE_QUOTE);
				setState(107);
				match(ID);
				setState(108);
				match(DOUBLE_QUOTE);
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

	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(AtlasSQLParser.ID, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof AtlasSQLParserVisitor ) return ((AtlasSQLParserVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_identifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(111);
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

	public static class Relational_opContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(AtlasSQLParser.EQ, 0); }
		public TerminalNode NOT_EQ() { return getToken(AtlasSQLParser.NOT_EQ, 0); }
		public TerminalNode LTH() { return getToken(AtlasSQLParser.LTH, 0); }
		public TerminalNode GTH() { return getToken(AtlasSQLParser.GTH, 0); }
		public TerminalNode LET() { return getToken(AtlasSQLParser.LET, 0); }
		public TerminalNode GET() { return getToken(AtlasSQLParser.GET, 0); }
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
		enterRule(_localctx, 30, RULE_relational_op);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(113);
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

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 10:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 4);
		case 1:
			return precpred(_ctx, 3);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\32v\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4"+
		"\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\3\2\3\2\3\3\3\3"+
		"\3\3\3\3\3\3\5\3*\n\3\3\4\3\4\3\4\5\4/\n\4\3\4\3\4\3\5\3\5\3\6\3\6\3\7"+
		"\3\7\5\79\n\7\3\b\3\b\3\t\3\t\3\t\7\t@\n\t\f\t\16\tC\13\t\3\n\3\n\3\13"+
		"\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\fT\n\f\3\f\3\f\3"+
		"\f\3\f\3\f\3\f\7\f\\\n\f\f\f\16\f_\13\f\3\r\3\r\5\rc\n\r\3\16\3\16\3\16"+
		"\5\16h\n\16\3\17\3\17\3\17\3\17\3\17\3\17\5\17p\n\17\3\20\3\20\3\21\3"+
		"\21\3\21\2\3\26\22\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \2\3\4\2\17"+
		"\22\24\25q\2\"\3\2\2\2\4$\3\2\2\2\6.\3\2\2\2\b\62\3\2\2\2\n\64\3\2\2\2"+
		"\f8\3\2\2\2\16:\3\2\2\2\20<\3\2\2\2\22D\3\2\2\2\24F\3\2\2\2\26S\3\2\2"+
		"\2\30b\3\2\2\2\32g\3\2\2\2\34o\3\2\2\2\36q\3\2\2\2 s\3\2\2\2\"#\5\4\3"+
		"\2#\3\3\2\2\2$%\7\3\2\2%&\5\f\7\2&\'\7\4\2\2\')\5\6\4\2(*\5\24\13\2)("+
		"\3\2\2\2)*\3\2\2\2*\5\3\2\2\2+,\5\b\5\2,-\7\b\2\2-/\3\2\2\2.+\3\2\2\2"+
		"./\3\2\2\2/\60\3\2\2\2\60\61\5\n\6\2\61\7\3\2\2\2\62\63\7\26\2\2\63\t"+
		"\3\2\2\2\64\65\7\26\2\2\65\13\3\2\2\2\669\5\16\b\2\679\5\20\t\28\66\3"+
		"\2\2\28\67\3\2\2\29\r\3\2\2\2:;\7\6\2\2;\17\3\2\2\2<A\5\22\n\2=>\7\7\2"+
		"\2>@\5\22\n\2?=\3\2\2\2@C\3\2\2\2A?\3\2\2\2AB\3\2\2\2B\21\3\2\2\2CA\3"+
		"\2\2\2DE\7\26\2\2E\23\3\2\2\2FG\7\5\2\2GH\5\26\f\2H\25\3\2\2\2IJ\b\f\1"+
		"\2JK\7\13\2\2KL\5\26\f\2LM\7\f\2\2MT\3\2\2\2NO\5\30\r\2OP\5 \21\2PQ\5"+
		"\30\r\2QT\3\2\2\2RT\5\30\r\2SI\3\2\2\2SN\3\2\2\2SR\3\2\2\2T]\3\2\2\2U"+
		"V\f\6\2\2VW\7\r\2\2W\\\5\26\f\7XY\f\5\2\2YZ\7\16\2\2Z\\\5\26\f\6[U\3\2"+
		"\2\2[X\3\2\2\2\\_\3\2\2\2][\3\2\2\2]^\3\2\2\2^\27\3\2\2\2_]\3\2\2\2`c"+
		"\5\32\16\2ac\5\36\20\2b`\3\2\2\2ba\3\2\2\2c\31\3\2\2\2dh\7\27\2\2eh\7"+
		"\30\2\2fh\5\34\17\2gd\3\2\2\2ge\3\2\2\2gf\3\2\2\2h\33\3\2\2\2ij\7\t\2"+
		"\2jk\7\26\2\2kp\7\t\2\2lm\7\n\2\2mn\7\26\2\2np\7\n\2\2oi\3\2\2\2ol\3\2"+
		"\2\2p\35\3\2\2\2qr\7\26\2\2r\37\3\2\2\2st\t\2\2\2t!\3\2\2\2\f).8AS[]b"+
		"go";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}