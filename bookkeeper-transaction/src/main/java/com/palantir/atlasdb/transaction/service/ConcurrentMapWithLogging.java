package com.palantir.atlasdb.transaction.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Striped;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

public class ConcurrentMapWithLogging {
    private final int LOCK_STRIPES_NUMBER = 1000;

    private final WriteAheadLog log;
    private final ConcurrentMap<Long, Long> map;
    private final Striped<Lock> stripedLock;

    private ConcurrentMapWithLogging(WriteAheadLog log) {
        this.log = log;
        map = new ConcurrentHashMap<Long, Long>();
        stripedLock = Striped.lock(LOCK_STRIPES_NUMBER);
    }

    public static Supplier<ConcurrentMapWithLogging> supplier(final WriteAheadLogManager manager) {
        return new Supplier<ConcurrentMapWithLogging>() {
            @Override
            public ConcurrentMapWithLogging get() {
                return new ConcurrentMapWithLogging(manager.create());
            }
        };
    }

    public Long get(Long startTimestamp) {
        stripedLock.get(startTimestamp).lock();
        try {
            return map.get(startTimestamp);
        } finally {
            stripedLock.get(startTimestamp).unlock();
        }
    }

    public void putUnlessExists(long startTimestamp, long commitTimestamp)
            throws KeyAlreadyExistsException {
        stripedLock.get(startTimestamp).lock();
        try {
            if (map.containsKey(startTimestamp))
                throw new KeyAlreadyExistsException("Key " + startTimestamp + " already exists and is mapped to " + commitTimestamp);
            map.put(startTimestamp, commitTimestamp);
            log.append(startTimestamp, commitTimestamp);
        } finally {
            stripedLock.get(startTimestamp).unlock();
        }
    }

    public void close() {
        log.close();
    }

    public long getLogId() {
        return log.getId();
    }

    public ImmutableMap<Long, Long> getTimestampMap() {
        if (!log.isClosed())
            throw new IllegalStateException("Cannot get the map until log is closed");
        return ImmutableMap.copyOf(map);
    }
}
