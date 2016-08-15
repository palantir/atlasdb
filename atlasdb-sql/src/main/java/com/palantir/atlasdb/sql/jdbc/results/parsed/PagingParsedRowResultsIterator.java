package com.palantir.atlasdb.sql.jdbc.results.parsed;

import java.util.NoSuchElementException;

import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TransactionToken;
import com.palantir.atlasdb.sql.grammar.SelectQuery;

public class PagingParsedRowResultsIterator implements ParsedRowResultsIterator {
    private final AtlasDbService service;
    private final TransactionToken transactionToken;
    private final SelectQuery query;

    private RangeToken rangeToken;
    private LocalParsedRowResultsIterator curIter;

    public static PagingParsedRowResultsIterator create(AtlasDbService service, TransactionToken transactionToken, SelectQuery query) {
        RangeToken rangeToken = service.getRange(transactionToken, query.tableRange());
        return new PagingParsedRowResultsIterator(service, transactionToken, query, rangeToken);
    }

    PagingParsedRowResultsIterator(AtlasDbService service,
                                   TransactionToken transactionToken,
                                   SelectQuery query,
                                   RangeToken rangeToken) {
        this.service = service;
        this.transactionToken = transactionToken;
        this.query = query;
        this.rangeToken = rangeToken;
        this.curIter = null;
    }

    @Override
    public ParsedRowResult next() {
        if (isClosed()) {
            throw new NoSuchElementException("Results iterator is closed.");
        }
        if (hasNext()) {
            return curIter.next();
        }
        throw new NoSuchElementException("Out of results.");
    }

    @Override
    public ParsedRowResult current() {
        if (isClosed()) {
            return null;
        }
        return curIter.current();
    }

    @Override
    public boolean hasNext() {
        if (isClosed()) {
            return false;
        }

        if (curIter == null) {
            curIter = nextIterator();
        }

        if (curIter.hasNext()) {
            return true;
        } else { // page to the next range
            if (rangeToken.hasMoreResults()) {
                rangeToken = service.getRange(transactionToken, rangeToken.getNextRange());
                curIter = nextIterator();
                return hasNext();
            } else { // all done
                rangeToken = null;
                return false;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return rangeToken == null || (curIter != null && curIter.isClosed());
    }

    @Override
    public void close() {
        rangeToken = null;
        if (curIter != null) {
            curIter.close();
        }
    }

    private LocalParsedRowResultsIterator nextIterator() {
        return new LocalParsedRowResultsIterator(
                ParsedRowResult.parseRowResults(rangeToken.getResults().getResults(),
                        query.postfilterPredicate(),
                        query.columns()));
    }
}
