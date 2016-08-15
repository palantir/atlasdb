package com.palantir.atlasdb.sql.jdbc.results.parsed;

import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TransactionToken;
import com.palantir.atlasdb.sql.grammar.SelectQuery;

public class IterableParsedRowResults implements Iterable<ParsedRowResult> {
    private final AtlasDbService service;
    private final TransactionToken transactionToken;
    private final SelectQuery query;

    public IterableParsedRowResults(AtlasDbService service, TransactionToken transactionToken, SelectQuery query) {
        this.service = service;
        this.transactionToken = transactionToken;
        this.query = query;
    }

    @Override
    public PagingParsedRowResultsIterator iterator() {
        RangeToken rangeToken = service.getRange(transactionToken, query.tableRange());
        return new PagingParsedRowResultsIterator(service, transactionToken, query, rangeToken);
    }

}
