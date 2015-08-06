package com.palantir.atlasdb.rdbms.impl.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbms;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnection;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnectionCallable;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsExecutionException;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsResultSetHandler;
import com.palantir.common.base.Throwables;

@ThreadSafe
public class IdCacheImpl implements IdCache {

    public static final long DEFAULT_SEQUENCE_INTERVAL = 100L;

    private final AtlasRdbms database;
    private final Map<String, Long> incrementBySequenceName;
    private final Map<String, IdRange> idsBySequenceName;

    public IdCacheImpl(AtlasRdbms database, Map<String, Long> incrementBySequenceName) {
        this.database = database;
        this.incrementBySequenceName = incrementBySequenceName;
        this.idsBySequenceName = Maps.newHashMap();
    }

    @Override
    public synchronized Iterable<Long> getIds(String sequenceName, int size) {
        Preconditions.checkArgument(size > 0, "Must request a positive number of IDs");
        Preconditions.checkArgument(incrementBySequenceName.containsKey(sequenceName), "Sequence not recognized: " + sequenceName);
        int foundIds = 0;
        Collection<IdRange> ranges = Lists.newArrayList();
        while (foundIds < size) {
            IdRange range = getUpToNIds(sequenceName, size - foundIds);
            foundIds += range.size();
            ranges.add(range);
        }
        return Iterables.concat(ranges);
    }

    @Override
    public synchronized void invalidate() {
        idsBySequenceName.clear();
    }

    private IdRange getUpToNIds(String sequenceName, int numIds) {
        IdRange ids = idsBySequenceName.get(sequenceName);
        if (ids == null) {
            ids = getMoreIds(sequenceName);
            idsBySequenceName.put(sequenceName, ids);
        }
        if (ids.size() <= numIds) {
            idsBySequenceName.remove(sequenceName);
            return ids;
        } else {
            return ids.strip(numIds);
        }
    }

    private IdRange getMoreIds(final String sequenceName) {
        long id;
        try {
            id = database.runWithDbConnection(new AtlasRdbmsConnectionCallable<Long>() {
                @Override
                public Long call(AtlasRdbmsConnection c) throws SQLException {
                    return c.query("SELECT nextval('" + sequenceName + "') AS nextval", new AtlasRdbmsResultSetHandler<Long>() {
                        @Override
                        public Long handle(ResultSet rs) throws SQLException {
                            Preconditions.checkArgument(rs.next(), "Could not select more ids for nodes");
                            return rs.getLong("nextval");
                        }
                    });
                }
            });
        } catch (AtlasRdbmsExecutionException e) {
            // This should not happen
            throw Throwables.throwUncheckedException(e.getCause());
        }

        long sequenceIncrement = incrementBySequenceName.get(sequenceName);
        return new IdRange(id, id + sequenceIncrement);
    }
}
