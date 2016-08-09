package com.palantir.atlasdb.sql.grammar;

import org.immutables.value.Value;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;

@Value.Immutable
public abstract class SelectClause {

    public static SelectClause create(AtlasSQLParser.Select_clauseContext selectClause) {
        ImmutableSelectClause.Builder builder = ImmutableSelectClause.builder();

        if (selectClause.table_reference().keyspace() != null) {
            builder.table(
                    TableReference.create(
                            Namespace.create(selectClause.table_reference().keyspace().getText()),
                            selectClause.table_reference().table_name().getText()
                    ));
        } else {
            builder.table(TableReference.createWithEmptyNamespace(selectClause.table_reference().table_name().getText()));
        }

        builder.range(RangeRequest.all());

        return builder.build();
    }

    public abstract TableReference table();

    public abstract RangeRequest range();

}
