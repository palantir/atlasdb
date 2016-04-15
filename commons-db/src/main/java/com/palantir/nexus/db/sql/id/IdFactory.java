package com.palantir.nexus.db.sql.id;

import com.palantir.exception.PalantirSqlException;

/**
 * An IdFactory is the core abstraction of ID generation.
 * @author eporter
 */
public interface IdFactory {

    /**
     * Gets a single ID.
     * @return The next ID from the backing sequence.
     * @throws PalantirSqlException
     */
    public long getNextId() throws PalantirSqlException;

    /**
     * Gets IDs in batch, which is far more efficient than getting IDs one at a time.
     * @param size
     * @return A new array with <code>size</code> new IDs.
     * @throws PalantirSqlException
     */
    public long [] getNextIds(int size) throws PalantirSqlException;

    /**
     * An alternative method to get new IDs without creating a new array every time.
     * @param ids An array to be completely filled with new IDs.
     * @throws PalantirSqlException
     */
    public void getNextIds(long [] ids) throws PalantirSqlException;
}
