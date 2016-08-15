package com.palantir.atlasdb.sql.grammar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.jdbc.results.columns.JdbcColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.parsed.ParsedRowResult;

@Value.Immutable
public abstract class WhereClause {

    public static WhereClause create(@Nullable AtlasSQLParser.Where_clauseContext whereCtx, List<JdbcColumnMetadata> cols) {
        if (whereCtx == null) {
            return ImmutableWhereClause.builder().build();
        }
        List<RowComponentConstraint> constraints =
                cols.stream()
                        .filter(JdbcColumnMetadata::isRowComp)
                        .map(m -> new WhereClausePrefilterVisitor(m).visit(whereCtx))
                        .collect(Collectors.toList());
        RowComponentConstraint combineConstraint =
                new RowComponentConstraint(
                        combineBounds(constraints.stream().map(RowComponentConstraint::getLowerBound).collect(Collectors.toList())),
                        combineBounds(constraints.stream().map(RowComponentConstraint::getUpperBound).collect(Collectors.toList())));
        Predicate<ParsedRowResult> postfilter = new WhereClausePostfilterVisitor().visit(whereCtx);
        return ImmutableWhereClause.builder()
                .prefilterConstraints(combineConstraint)
                .postfilterPredicate(postfilter)
                .build();
    }

    private static byte[] combineBounds(List<byte[]> bounds) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (byte[] bound : bounds) {
                if (bound != null) {
                    out.write(bound);
                } else {
                    break;
                }
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Problem writing bytes to combine constraint bounds.", e);
        }
    }

    @Value.Default
    public RowComponentConstraint prefilterConstraints() {
        return new RowComponentConstraint(null, null);
    }

    @Value.Default
    public Predicate<ParsedRowResult> postfilterPredicate() {
        return x -> true;
    }

}
