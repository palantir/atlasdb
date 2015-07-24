package com.palantir.atlasdb.transaction.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Striped;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.common.concurrent.PTExecutors;

public class TransactionServiceImpl implements TransactionService {
    private static final String CURRENT_MAP_STRING = "current_map";
    private static final String MAP_BEING_FLUSHED_STRING = "map_being_flushed";
    private static final int LOCK_STRIPES_NUMBER = 1000;
    private static final int NO_LOG_ID = -1;

    private final Supplier<ConcurrentMapWithLogging> mapSupplier;
    private final LockableReference<ConcurrentMapWithLogging> currentMapSwapper;
    private volatile ConcurrentMapWithLogging mapBeingFlushed;
    private final TransactionKVSWrapper kvsWrapper;
    private final MetadataStorageService metadataStorageService;

    private ScheduledExecutorService executor;
    private boolean running;

    private final Striped<Lock> stripedLock;
    private final Lock flushLock;

    private TransactionServiceImpl(Supplier<ConcurrentMapWithLogging> mapSupplier,
                                   ConcurrentMapWithLogging currentMap,
                                   TransactionKVSWrapper kvsWrapper,
                                   MetadataStorageService metadataStorageService) {
        this.mapSupplier = mapSupplier;
        this.currentMapSwapper = new LockableReference<ConcurrentMapWithLogging>(currentMap);
        this.mapBeingFlushed = null;
        this.kvsWrapper = kvsWrapper;
        this.stripedLock = Striped.lock(LOCK_STRIPES_NUMBER);
        this.flushLock = new ReentrantLock();
        this.metadataStorageService = metadataStorageService;
        this.executor = null;
        this.running = false;
    }

    public static TransactionServiceImpl create(WriteAheadLogManager logManager,
                                                TransactionKVSWrapper kvsWrapper,
                                                MetadataStorageService metadataStorageService,
                                                long flushPeriod) {
        byte[] oldMapBeingFlushLogId = metadataStorageService.get(MAP_BEING_FLUSHED_STRING);
        if (oldMapBeingFlushLogId != null && PtBytes.toLong(oldMapBeingFlushLogId) != NO_LOG_ID)
            kvsWrapper.flushLog(logManager.retrieve(PtBytes.toLong(oldMapBeingFlushLogId)));

        byte[] oldCurrentMapLogId = metadataStorageService.get(CURRENT_MAP_STRING);
        if (oldCurrentMapLogId != null)
            kvsWrapper.flushLog(logManager.retrieve(PtBytes.toLong(oldCurrentMapLogId)));


        Supplier<ConcurrentMapWithLogging> mapSupplier = ConcurrentMapWithLogging.supplier(logManager);

        metadataStorageService.put(MAP_BEING_FLUSHED_STRING, PtBytes.toBytes(NO_LOG_ID));
        ConcurrentMapWithLogging map = mapSupplier.get();
        metadataStorageService.put(CURRENT_MAP_STRING, PtBytes.toBytes(map.getLogId()));
        TransactionServiceImpl service = new TransactionServiceImpl(mapSupplier, map, kvsWrapper, metadataStorageService);
        service.start(flushPeriod);

        return service;
    }

    public static TransactionServiceImpl create(KeyValueService keyValueService, String bookKeeperServers, long flushPeriod) {
        return create(
                BookKeeperWriteAheadLogManager.create(bookKeeperServers),
                new TransactionKVSWrapper(keyValueService),
                ZooKeeperMetadataStorageService.create(bookKeeperServers),
                flushPeriod);
    }

    // not thread safe
    private void start(long flushPeriod) {
        if (!running) {
            executor = PTExecutors.newScheduledThreadPool(1);
            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    flush();
                }
            }, flushPeriod, flushPeriod, TimeUnit.MILLISECONDS);
            running = true;
        }
    }

    public void shutdown() {
        if (running) {
            executor.shutdown();
            running = false;
        }
    }

    @Override
    public Long get(final long startTimestamp) {
        return get(ImmutableSet.of(startTimestamp)).get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(final Iterable<Long> startTimestamps) {
        List<Lock> lockedLocks = Lists.newArrayList();
        try {
            for (Lock lock : stripedLock.bulkGet(startTimestamps)) {
                lock.lock();
                lockedLocks.add(lock);
            }

            return currentMapSwapper.runAgainstCurrentValue(new Function<ConcurrentMapWithLogging, Map<Long, Long>>() {
                @Override
                public Map<Long, Long> apply(ConcurrentMapWithLogging currentMap) {
                    Map<Long, Long> result = Maps.newHashMap();
                    Set<Long> notInMaps = Sets.newHashSet();
                    for (Long startTimestamp: startTimestamps) {
                        Long commitTimestamp = currentMap.get(startTimestamp);
                        if (commitTimestamp == null)
                            commitTimestamp = getFromMapBeingFlushed(startTimestamp);
                        if (commitTimestamp == null)
                            notInMaps.add(startTimestamp);
                        else
                            result.put(startTimestamp, commitTimestamp);
                    }
                    result.putAll(kvsWrapper.get(notInMaps));
                    return result;
                }
            });
        } finally {
            for (Lock lock : lockedLocks) {
                lock.unlock();
            }
        }
    }

    @Override
    public void putUnlessExists(final long startTimestamp, final long commitTimestamp)
            throws KeyAlreadyExistsException {
        stripedLock.get(startTimestamp).lock();
        try {
            currentMapSwapper.runAgainstCurrentValue(new Function<ConcurrentMapWithLogging, Void>() {
                @Override
                public Void apply(ConcurrentMapWithLogging currentMap) {
                    if (getFromMapBeingFlushed(startTimestamp) != null)
                        throw new KeyAlreadyExistsException("Key " + startTimestamp + " already exists and is mapped to " + commitTimestamp);
                    if (kvsWrapper.get(startTimestamp) != null)
                        throw new KeyAlreadyExistsException("Key " + startTimestamp + " already exists and is mapped to " + commitTimestamp);
                    currentMap.putUnlessExists(startTimestamp, commitTimestamp);
                    return null;
                }
            });
        } finally {
            stripedLock.get(startTimestamp).unlock();
        }
    }

    public void flush() {
        flushLock.lock();
        try {
            mapBeingFlushed = currentMapSwapper.getValue();
            metadataStorageService.put(MAP_BEING_FLUSHED_STRING, PtBytes.toBytes(mapBeingFlushed.getLogId()));
            ConcurrentMapWithLogging nextMap = mapSupplier.get();
            metadataStorageService.put(CURRENT_MAP_STRING, PtBytes.toBytes(nextMap.getLogId()));
            Lock mapLock = currentMapSwapper.swap(nextMap).getLock();
            mapLock.lock();

            mapBeingFlushed.close();
            kvsWrapper.putAll(mapBeingFlushed.getTimestampMap());
            metadataStorageService.put(MAP_BEING_FLUSHED_STRING, PtBytes.toBytes(NO_LOG_ID));
            mapBeingFlushed = null;
        } finally {
            flushLock.unlock();
        }

    }

    private Long getFromMapBeingFlushed(Long startTimestamp) {
        ConcurrentMapWithLogging map = mapBeingFlushed;
        if (map == null)
            return null;
        else
            return map.get(startTimestamp);
    }

}
