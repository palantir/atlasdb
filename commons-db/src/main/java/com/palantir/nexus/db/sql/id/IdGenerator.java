package com.palantir.nexus.db.sql.id;

import com.palantir.exception.PalantirSqlException;

/**
 * An interface for a class which can produce IDs from a sequence.
 * @author eporter
 */
public interface IdGenerator {

    /**
     * Tries to generate up to <code>ids.length</code> ids from a database sequence.  Because some
     * IDs may not be valid, it can return less.
     * @param ids The array to be filled with IDs.
     * @return The count of how many valid IDs were put into the beginning of the array.
     */
    int generate(long [] ids) throws PalantirSqlException;
}
