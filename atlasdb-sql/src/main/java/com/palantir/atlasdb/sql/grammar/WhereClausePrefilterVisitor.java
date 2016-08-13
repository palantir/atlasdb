package com.palantir.atlasdb.sql.grammar;

import java.util.Arrays;

import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParserBaseVisitor;
import com.palantir.atlasdb.sql.jdbc.results.JdbcColumnMetadata;

public class WhereClausePrefilterVisitor extends AtlasSQLParserBaseVisitor<RowComponentConstraint> {

    private static final RowComponentConstraint BYTE_ARRAY_BOUNDS_UNBOUNDED = new RowComponentConstraint(null, null);

    private final JdbcColumnMetadata metadata;

    public WhereClausePrefilterVisitor(JdbcColumnMetadata metadata) {
        this.metadata = metadata;
    }

    @Override public RowComponentConstraint visitRelationalExpr(AtlasSQLParser.RelationalExprContext ctx) {
        AtlasSQLParser.Term_exprContext l = ctx.left;
        AtlasSQLParser.Term_exprContext r = ctx.right;
        RelationalOp op = RelationalOp.from(ctx.relational_op().getText());
        if (l.literal() != null && r.identifier() != null) {
            if (matchesRow(r.identifier().getText())) {
                return createConstraint(op.flip(), parseLiteral(l.literal()));
            }
        } else if (l.identifier() != null && r.literal() != null) {
            if (matchesRow(l.identifier().getText())) {
                return createConstraint(op, parseLiteral(r.literal()));
            }
        } else if (l.identifier() != null && r.identifier() != null) {
            throw new UnsupportedOperationException("comparing two identifiers indicates some kind of join and we do not support joins");
        }
        return BYTE_ARRAY_BOUNDS_UNBOUNDED;
    }

    private boolean matchesRow(String idStr) {
        return metadata.getName().equals(idStr) || metadata.getLabel().equals(idStr);
    }

    private RowComponentConstraint createConstraint(RelationalOp op, String literal) {
        byte[] bytes = metadata.getValueType().convertFromString(literal);
        switch (op) {
            case EQ:
                return new RowComponentConstraint(bytes, addOneByte(bytes));
            case LTH:
                return new RowComponentConstraint(null, bytes);
            case GTH:
                return new RowComponentConstraint(addOneByte(bytes), null);
            case LET:
                return new RowComponentConstraint(null, addOneByte(bytes));
            case GET:
                return new RowComponentConstraint(bytes, null);
            case NOT_EQ:
                // do not prefilter by NOT_EQ since that will give you the world anyways
                return BYTE_ARRAY_BOUNDS_UNBOUNDED;
        }
        throw new IllegalArgumentException("unknown operation type: " + op);
    }

    private byte[] addOneByte(byte[] bytes) {
        byte[] newBytes = Arrays.copyOf(bytes, bytes.length);
        for (int i = newBytes.length - 1; i >= 0; i--) {
            if (++newBytes[i] != Byte.MIN_VALUE) {
                return newBytes;
            }
        }
        throw new RuntimeException("overflow incrementing byte array");
    }

    private String parseLiteral(AtlasSQLParser.LiteralContext literal) {
        if (literal.string_literal() != null) {
            return literal.string_literal().ID().getText();
        } else {
            return literal.getText();
        }
    }

    @Override public RowComponentConstraint visitBoolAndExpr(AtlasSQLParser.BoolAndExprContext ctx) {
        return visit(ctx.left).intersectWith(visit(ctx.right));
    }

    @Override public RowComponentConstraint visitBoolOrExpr(AtlasSQLParser.BoolOrExprContext ctx) {
        RowComponentConstraint l = visit(ctx.left);
        RowComponentConstraint r = visit(ctx.right);
        if (!l.isUnbounded() || !r.isUnbounded()) {
            throw new UnsupportedOperationException("you cannot restrict row components in OR clauses: " + ctx.getText());
        }
        return l;
    }

    @Override public RowComponentConstraint visitParenExpr(AtlasSQLParser.ParenExprContext ctx) {
        return visit(ctx.expr());
    }

}
