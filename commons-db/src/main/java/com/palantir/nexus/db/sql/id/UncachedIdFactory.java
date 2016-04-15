package com.palantir.nexus.db.sql.id;

import org.apache.commons.lang.Validate;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.ReentrantConnectionSupplier;

/**
 * An IdFactory which does not cache any IDs and goes to the source IdGenerator every time.
 * @author eporter
 *
 */
public class UncachedIdFactory implements IdFactory {

    // Use an IdGenerator which is guaranteed to fill up the entire array.
    private final DbSequenceBackedIdGenerator idGenerator;
    private final String sequenceName;

    public UncachedIdFactory(DBType dbType, String sequenceName, ReentrantConnectionSupplier connectionSupplier) {
        idGenerator = new DbSequenceBackedIdGenerator(dbType, sequenceName, connectionSupplier);
        this.sequenceName = sequenceName;
    }

    @Override
    public long getNextId() throws PalantirSqlException {
        return getNextIds(1)[0];
    }

    @Override
    public long [] getNextIds(int size) throws PalantirSqlException {
        long [] ids = new long[size];
        int generated = idGenerator.generate(ids);
        Validate.isTrue(generated == size, "The backing ID generator is broken."); //$NON-NLS-1$
        return ids;
    }

    @Override
    public void getNextIds(long[] ids) throws PalantirSqlException {
        idGenerator.generate(ids);
    }

    public String getBackingSequence() {
        return sequenceName;
    }

}
