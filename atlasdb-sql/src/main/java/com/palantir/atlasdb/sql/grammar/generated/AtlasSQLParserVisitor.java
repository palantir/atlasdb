// Generated from /Volumes/git/code/atlasdb/atlasdb-sql/src/main/antlr4/com/palantir/atlasdb/sql/grammar/generated/AtlasSQLParser.g4 by ANTLR 4.5
package com.palantir.atlasdb.sql.grammar.generated;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link AtlasSQLParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface AtlasSQLParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(AtlasSQLParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#select_query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_query(AtlasSQLParser.Select_queryContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#table_reference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_reference(AtlasSQLParser.Table_referenceContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#keyspace}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKeyspace(AtlasSQLParser.KeyspaceContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#table_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable_name(AtlasSQLParser.Table_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#column_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_clause(AtlasSQLParser.Column_clauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#all_columns}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAll_columns(AtlasSQLParser.All_columnsContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#column_list}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_list(AtlasSQLParser.Column_listContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#column_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumn_name(AtlasSQLParser.Column_nameContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#where_clause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhere_clause(AtlasSQLParser.Where_clauseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code boolOrExpr}
	 * labeled alternative in {@link AtlasSQLParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBoolOrExpr(AtlasSQLParser.BoolOrExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code terminalExpr}
	 * labeled alternative in {@link AtlasSQLParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTerminalExpr(AtlasSQLParser.TerminalExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code relationalExpr}
	 * labeled alternative in {@link AtlasSQLParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelationalExpr(AtlasSQLParser.RelationalExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenExpr}
	 * labeled alternative in {@link AtlasSQLParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenExpr(AtlasSQLParser.ParenExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code boolAndExpr}
	 * labeled alternative in {@link AtlasSQLParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBoolAndExpr(AtlasSQLParser.BoolAndExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#term_expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTerm_expr(AtlasSQLParser.Term_exprContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteral(AtlasSQLParser.LiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#string_literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitString_literal(AtlasSQLParser.String_literalContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(AtlasSQLParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link AtlasSQLParser#relational_op}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational_op(AtlasSQLParser.Relational_opContext ctx);
}