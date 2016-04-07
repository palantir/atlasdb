package com.palantir.nexus.db.chunked;

import java.util.List;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;

/**
 * Visitor which visits a result set in chunks.
 *
 * @param <T> the type that each row is hydrated into.
 */
public interface ChunkedRowVisitor<T> {
    /**
     * Hydrates an individual row. Once a given number of individual rows
     * are hydrated, the hydrated values are aggregated into a chunk and
     * passed to {@link #processChunk(List)}.
     */
    public T hydrateRow(AgnosticLightResultRow row) throws PalantirSqlException;

    /**
     * Called when enough rows have been hydrated to form a chunk, or when
     * the result set is exhausted.
     *
     * @return true if further chunks should be visited, or false if
     *         visiting should be aborted.
     */
    public boolean processChunk(List<T> chunk) throws PalantirSqlException;
}
