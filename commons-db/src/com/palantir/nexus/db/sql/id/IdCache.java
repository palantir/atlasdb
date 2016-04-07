package com.palantir.nexus.db.sql.id;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.PalantirSqlException;

/**
 * Takes in bulk IDs from a generator and hands them out in bulk.  Supports caching and prefetching.
 * <p>IDs are put into a queue to try to main the order in which IDs are generated.
 *
 * Multithreading is likely to cause IDs to be returned in a different order from the order generated.
 * Use DbSequenceBackedIdGenerator if you need guarantees on id order.
 *
 * @author eporter
 */
@ThreadSafe
public class IdCache implements IdFactory {
    public static IdCache createIdCache(IdGenerator idGenerator,
                                        int minCacheSize,
                                        int cacheSize,
                                        int threadCount) {
       // Start the queue fairly small and let it grow if needed.
       int initialSize = cacheSize;
       while (initialSize / 2 > minCacheSize)
           initialSize /= 2;
       TLongQueue idCache = new TLongQueue(initialSize);
       return new IdCache(cacheSize,  threadCount, idGenerator, idCache);
   }

    private static final Logger log = LoggerFactory.getLogger(IdFactory.class);


    private final int cacheSize;
    public static final int BLOCK_SIZE = 10000;

    private final int threadCount;

    private final IdGenerator idGenerator;
    @GuardedBy("this")
    private final TLongQueue idCache;
    @GuardedBy("this")
    private long idsGenerated = 0;

    @GuardedBy("this")
    private volatile ThreadPoolExecutor backgroundThreadPool = null;

    public IdCache(int cacheSize,
                   int threadCount,
                   IdGenerator idGenerator,
                   TLongQueue idCache) {
        this.cacheSize = cacheSize;
        this.threadCount = threadCount;
        this.idGenerator = idGenerator;
        this.idCache = idCache;
    }

    // Controls whether or not to block while creating IDs.
    private enum CacheFill {BLOCKING, NON_BLOCKING}

    @Override
    public long getNextId() throws PalantirSqlException {
        return getNextIds(1)[0];
    }

    @Override
    public long [] getNextIds(int size) throws PalantirSqlException {
        long [] ids = new long [size];
        getNextIds(ids);
        return ids;
    }

    @Override
    public void getNextIds(long [] ids) throws PalantirSqlException {
        if (ids.length <= BLOCK_SIZE) {
            getIdsSmall(ids);
        } else {
            getIdsJumbo(ids);
        }
    }

    /**
     * Starts a thread to populate IDs before they are needed.  Having background thread can cause
     * IDs from the IdGenerator to be returned out of order.  Background threads should be started
     * if the backing sequence is random values.
     */
    public synchronized void startBackgroundThread() {
        if (threadCount == 0) {
            return;
        }
        stopBackgroundThread();
        ThreadFactory threadFactory = new NamedThreadFactory("Background ID generation thread", //$NON-NLS-1$
                /* isDaemon */ true);
        backgroundThreadPool = PTExecutors.newFixedThreadPool(threadCount, threadFactory);
        // Submit THREAD_COUNT tasks which will run until the thread pool is shutdownNow().
        for (int i = 0; i < threadCount; i++) {
            backgroundThreadPool.submit(
            new Runnable() {

                @Override
                public void run() {
                    try {
                        // Wait until we've already generated some IDs before prefetching.
                        // Ensures that we don't waste more than half of the IDs.
                        synchronized (IdCache.this) {
                            while (idsGenerated < cacheSize) {
                                IdCache.this.wait();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        while (!Thread.interrupted()) {
                            createIds(CacheFill.BLOCKING);
                        }
                    } catch (PalantirSqlException e) {
                        log.error("Error creating IDs in the background", e); //$NON-NLS-1$
                    }
                }

            });
        }
    }

    public synchronized void stopBackgroundThread() {
        if (backgroundThreadPool != null) {
            // Interrupt the workers.  It's the only way to kill them.
            backgroundThreadPool.shutdownNow();
            backgroundThreadPool = null;
        }
    }

    /**
     *
     * @return How many IDs have been given out to callers.
     */
    public synchronized long countIdsGenerated() {
        return idsGenerated;
    }

    /**
     * Fills the array with random IDs with a single copy.
     * @param ids
     */
    private void getIdsSmall(long[] ids) throws PalantirSqlException {
        Validate.isTrue(ids.length <= BLOCK_SIZE, "Illegal size used while getting random IDs."); //$NON-NLS-1$
        // Before grabbing a lock, see if we need to generate some new IDs.
        ensureSufficientIds(ids.length);
        synchronized (this) {
            // Another thread may have beaten us to the IDs.
            ensureSufficientIds(ids.length);
            idCache.remove(ids);
            idsGenerated += ids.length;
            // Wake up any background threads waiting to write IDs.
            notifyAll();
        }
    }

    /**
     * Fills the array with random IDs using multiple passes to accommodate
     * array sizes even larger than the cache size.
     * @param ids
     */
    private void getIdsJumbo(long [] ids) throws PalantirSqlException {
        long [] tempIds = new long[BLOCK_SIZE];
        int offset = 0;
        // Get as many blocks as possible.
        for ( ; offset + BLOCK_SIZE <= ids.length; offset += BLOCK_SIZE) {
            getIdsSmall(tempIds);
            System.arraycopy(tempIds, 0, ids, offset, BLOCK_SIZE);
        }
        // Get one smaller block.
        if (offset < ids.length) {
            tempIds = new long[ids.length - offset];
            getIdsSmall(tempIds);
            System.arraycopy(tempIds, 0, ids, offset, tempIds.length);
        }
    }

    /* Tries to ensure that there are enough IDs.  It isn't synchronized
     * so that we can generate IDs without blocking other threads.
     */
    private void ensureSufficientIds(int size) throws PalantirSqlException {
        while (idsAvailable() < size) {
            createIds(CacheFill.NON_BLOCKING);
        }
    }

    private synchronized int idsAvailable() {
        return idCache.size();
    }

    private void createIds(CacheFill strategy) throws PalantirSqlException {
        long [] ids = new long [BLOCK_SIZE];
        int size = idGenerator.generate(ids);
        insertIds(ids, size, strategy);
    }

    private synchronized void insertIds(long [] ids, int len, CacheFill strategy) {
        // If we're blocking, then we're in a background thread creating IDs.
        // If we're not blocking, then we're in a thread that's requesting IDs.
        if (strategy == CacheFill.BLOCKING) {
            while (len + idsAvailable() > cacheSize) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        idCache.add(ids, len);
    }

    int getBlockSize() {
        return BLOCK_SIZE;
    }
}
