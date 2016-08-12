package com.palantir.atlasdb.sql.grammar;

import java.sql.SQLException;
import java.util.EnumSet;

import org.antlr.v4.runtime.ParserRuleContext;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParserBaseVisitor;
import com.palantir.atlasdb.sql.jdbc.results.JdbcReturnType;
import com.palantir.atlasdb.sql.jdbc.results.ParsedRowResult;

public class WhereClauseVisitor extends AtlasSQLParserBaseVisitor<Object> {

    @Override public Object visitExpression(AtlasSQLParser.ExpressionContext ctx) {
        if (ctx.AND() != null) {
            return Predicates.and((Predicate<ParsedRowResult>) visit(ctx.left), (Predicate<ParsedRowResult>) visit(ctx.right));
        } else if (ctx.relational_op() != null) {
            String left = (String) visit(ctx.left);
            String right = (String) visit(ctx.right);
            RelationalOp rel = (RelationalOp) visit(ctx.relational_op());
            return (Predicate<ParsedRowResult>) res -> {
                String l = lookupIfPossible(left, res);
                String r = lookupIfPossible(right, res);
                return fuzzyCompare(l, r, rel);
            };
        } else if (ctx.DECIMAL() != null || ctx.bool() != null || ctx.ID() != null) {
            return ctx.getText();
        }
        throw new UnsupportedOperationException("unknown expression type: " + ctx.getText());
    }

    private String lookupIfPossible(String key, ParsedRowResult res) {
        try {
            return (String) res.get(key, JdbcReturnType.STRING);
        } catch (SQLException e) {
            return key;
        }
    }

    private boolean fuzzyCompare(String s1, String s2, RelationalOp rel) {
        if (isNumber(s1) && isNumber(s2)) {
            return compare(Double.parseDouble(s1), Double.parseDouble(s2), rel);
        } else if (isBool(s1) && isBool(s2)) {
            return compare(Boolean.parseBoolean(s1), Boolean.parseBoolean(s2), rel);
        } else {
            return compare(s1, s2, rel);
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

    @Override public Object visitRelational_op(AtlasSQLParser.Relational_opContext ctx) {
        if (is(ctx, "=")) {
            return RelationalOp.EQ;
        } else if (is(ctx, "<")) {
            return RelationalOp.LTH;
        } else if (is(ctx, ">")) {
            return RelationalOp.GTH;
        } else if (is(ctx, "<=")) {
            return RelationalOp.LET;
        } else if (is(ctx, ">=")) {
            return RelationalOp.GET;
        } else if (is(ctx, "<>")) {
            return RelationalOp.NOT_EQ;
        }
        throw new UnsupportedOperationException("unknown relational_op type: " + ctx.getText());
    }

    private boolean is(ParserRuleContext ctx, String str) {
        return ctx.getText().toLowerCase().equals(str);
    }

    enum RelationalOp {
        EQ, LTH, GTH, LET, GET, NOT_EQ
    }

}
