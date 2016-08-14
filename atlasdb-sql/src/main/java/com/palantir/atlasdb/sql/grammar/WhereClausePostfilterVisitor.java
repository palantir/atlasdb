package com.palantir.atlasdb.sql.grammar;

import java.sql.SQLException;
import java.util.EnumSet;
import java.util.function.Predicate;

import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParserBaseVisitor;
import com.palantir.atlasdb.sql.jdbc.results.JdbcReturnType;
import com.palantir.atlasdb.sql.jdbc.results.ParsedRowResult;

public class WhereClausePostfilterVisitor extends AtlasSQLParserBaseVisitor<Predicate<ParsedRowResult>> {

    @Override public Predicate<ParsedRowResult> visitRelationalExpr(AtlasSQLParser.RelationalExprContext ctx) {
        AtlasSQLParser.Term_exprContext l = ctx.left;
        AtlasSQLParser.Term_exprContext r = ctx.right;
        RelationalOp op = RelationalOp.from(ctx.relational_op().getText());
        if (l.literal() != null && r.literal() != null) {
          return res -> fuzzyCompare(parseLiteral(l.literal()), parseLiteral(r.literal()), op);
        } else if (l.literal() != null && r.identifier() != null) {
           return res -> fuzzyCompare(parseLiteral(l.literal()), lookup(r.identifier().getText(), res), op);
        } else if (l.identifier() != null && r.literal() != null) {
            return res -> fuzzyCompare(lookup(l.identifier().getText(), res), parseLiteral(r.literal()), op);
        } else if (l.identifier() != null && r.identifier() != null) {
            throw new UnsupportedOperationException("comparing two identifiers indicates some kind of join and we do not support joins");
        }
        throw new UnsupportedOperationException(
                String.format("found an unknown combination of terminal expressions: '%s' and '%s'", l.getText(), r.getText()));
    }

    private String parseLiteral(AtlasSQLParser.LiteralContext literal) {
        if (literal.string_literal() != null) {
            return literal.string_literal().ID().getText();
        } else {
            return literal.getText();
        }
    }

    private String lookup(String rowComp, ParsedRowResult res) {
        try {
            return res.get(rowComp, JdbcReturnType.STRING).toString();
        } catch (SQLException e) {
            throw new RuntimeException(String.format("identifier %s not found in results", rowComp));
        }
    }

    private boolean fuzzyCompare(String s1, String s2, RelationalOp op) {
        if (isNumber(s1) && isNumber(s2)) {
            return compare(Double.parseDouble(s1), Double.parseDouble(s2), op);
        } else if (isBool(s1) && isBool(s2)) {
            return compare(Boolean.parseBoolean(s1), Boolean.parseBoolean(s2), op);
        } else {
            return compare(s1, s2, op);
        }
    }
    private boolean compare(Comparable s1, Comparable s2, RelationalOp rel) {
        int result = s1.compareTo(s2);
        if (result < 0) {
            return EnumSet.of(RelationalOp.LTH, RelationalOp.LET, RelationalOp.NOT_EQ).contains(rel);
        } else if (result > 0) {
            return EnumSet.of(RelationalOp.GTH, RelationalOp.GET, RelationalOp.NOT_EQ).contains(rel);
        } else {
            return EnumSet.of(RelationalOp.LET, RelationalOp.GET, RelationalOp.EQ).contains(rel);
        }
    }

    private boolean isBool(String s) {
        try {
            return Boolean.parseBoolean(s);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isNumber(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override public Predicate<ParsedRowResult> visitBoolAndExpr(AtlasSQLParser.BoolAndExprContext ctx) {
        return visit(ctx.left).and(visit(ctx.right));
    }

    @Override public Predicate<ParsedRowResult> visitBoolOrExpr(AtlasSQLParser.BoolOrExprContext ctx) {
        return visit(ctx.left).or(visit(ctx.right));
    }

    @Override public Predicate<ParsedRowResult> visitParenExpr(AtlasSQLParser.ParenExprContext ctx) {
        return visit(ctx.expr());
    }

}
