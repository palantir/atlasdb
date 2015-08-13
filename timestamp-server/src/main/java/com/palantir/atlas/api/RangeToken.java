package com.palantir.atlas.api;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * <pre>
 * {
 *   "data": &lt;results>,
 *   "next": &lt;nextRange>
 * }
 * </pre>
 */
public class RangeToken {
    private final TableRowResult results;
    private final @Nullable TableRange nextRange;

    public RangeToken(TableRowResult results,
                      @Nullable TableRange nextRange) {
        this.results = Preconditions.checkNotNull(results);
        this.nextRange = nextRange;
    }

    public TableRowResult getResults() {
        return results;
    }

    public boolean hasMoreResults() {
        return nextRange != null;
    }

    public @Nullable TableRange getNextRange() {
        return nextRange;
    }

    @Override
    public String toString() {
        return "RangeToken [results=" + results + ", nextRange=" + nextRange + "]";
    }
}
