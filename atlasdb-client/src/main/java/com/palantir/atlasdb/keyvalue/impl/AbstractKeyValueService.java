/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.Maps2;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;

public abstract class AbstractKeyValueService implements KeyValueService {
    private static final Logger log = LoggerFactory.getLogger(KeyValueService.class);

    protected ExecutorService executor;

    private final TracingPrefsConfig tracingPrefs;
    private final ScheduledExecutorService scheduledExecutor;

    /**
     * Note: This takes ownership of the given executor. It will be shutdown when the key
     * value service is closed.
     */
    public AbstractKeyValueService(ExecutorService executor) {
        this.executor = executor;
        this.tracingPrefs = new TracingPrefsConfig();
        this.scheduledExecutor = PTExecutors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(getClass().getSimpleName() + "-tracing-prefs", true));
        this.scheduledExecutor.scheduleWithFixedDelay(this.tracingPrefs, 0, 1, TimeUnit.MINUTES); // reload every minute
    }

    /**
     * @param threadNamePrefix thread name prefix
     * @param poolSize fixed thread pool size
     * @return a new fixed size thread pool with a keep alive time of 1 minute.
     */
    protected static ThreadPoolExecutor createFixedThreadPool(String threadNamePrefix, int poolSize) {
        ThreadPoolExecutor executor = PTExecutors.newFixedThreadPool(poolSize,
                new NamedThreadFactory(threadNamePrefix, false));
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        return executor;
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata) {
        for (Entry<String, byte[]> entry : tableNameToTableMetadata.entrySet()) {
            createTable(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void dropTables(Set<String> tableNames) {
        for (String tableName : tableNames) {
            dropTable(tableName);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of();
    }

    /*
     * This version just does a regular read and returns the timestamp of the fetched value.
     * This is slower than it needs to be so implementers are encouraged to make a faster impl.
     */
    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> keys) {
        return Maps.newHashMap(Maps.transformValues(get(tableName, keys), Value.GET_TIMESTAMP));
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        for (String table: getAllTableNames()) {
            builder.put(table, getMetadataForTable(table));
        }
        return builder.build();
    }

    protected int getMinimumDurationToTraceMillis() {
        return tracingPrefs.getMinimumDurationToTraceMillis();
    }

    protected int getMultiPutBatchCount() {
        return AtlasDbPerformanceConstants.MAX_BATCH_SIZE;
    }

    protected long getMultiPutBatchSizeBytes() {
        return AtlasDbPerformanceConstants.MAX_BATCH_SIZE_BYTES;
    }

    /* (non-Javadoc)
     * @see com.palantir.metropolis.keyvalue.api.KeyValueService#multiPut(java.util.Map, long)
     */
    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp) throws KeyAlreadyExistsException {
        List<Callable<Void>> callables = Lists.newArrayList();
        for (Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final String table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = partitionByCountAndBytes(sortedMap.entrySet(),
                    getMultiPutBatchCount(), getMultiPutBatchSizeBytes(), table, new Function<Entry<Cell, byte[]>, Long>(){

                        @Override
                        public Long apply(Entry<Cell, byte[]> entry) {
                            long totalSize = 0;
                            totalSize += entry.getValue().length;
                            totalSize += Cells.getApproxSizeOfCell(entry.getKey());
                            return totalSize;
                        }});


            for (final List<Entry<Cell, byte[]>> p : partitions) {
                callables.add(new Callable<Void>() {
                    @Override
                    public Void call() {
                        String originalName = Thread.currentThread().getName();
                        Thread.currentThread().setName("Atlas multiPut of " + p.size() + " cells into " + table);
                        try {
                            put(table, Maps2.fromEntries(p), timestamp);
                            return null;
                        } finally {
                            Thread.currentThread().setName(originalName);
                        }
                    }
                });
            }
        }

        List<Future<Void>> futures;
        try {
            futures = executor.invokeAll(callables);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
            }
        }
    }

    protected <T> Iterable<List<T>> partitionByCountAndBytes(final Iterable<T> iterable,
                                                             final int maximumCountPerPartition,
                                                             final long maximumBytesPerPartition,
                                                             final String tablename,
                                                             final Function<T, Long> sizingFunction) {
        return new Iterable<List<T>>() {
            @Override
            public Iterator<List<T>> iterator() {
                return new UnmodifiableIterator<List<T>>() {
                    PeekingIterator<T> pi = Iterators.peekingIterator(iterable.iterator());
                    private int remainingEntries = Iterables.size(iterable);

                    @Override
                    public boolean hasNext() {
                        return pi.hasNext();
                    }
                    @Override
                    public List<T> next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        List<T> entries =
                                Lists.newArrayListWithCapacity(Math.min(maximumCountPerPartition, remainingEntries));
                        long runningSize = 0;

                        // limit on: maximum count, pending data, maximum size, but allow at least one even if it's too huge
                        T firstEntry = pi.next();
                        runningSize += sizingFunction.apply(firstEntry);
                        entries.add(firstEntry);
                        if (runningSize > maximumBytesPerPartition && log.isWarnEnabled()) {
                            String message = "Encountered an entry of approximate size " + sizingFunction.apply(firstEntry) +
                                    " bytes, larger than maximum size of " + maximumBytesPerPartition +
                                    " defined per entire batch, while doing a write to " + tablename +
                                    ". Attempting to batch anyways.";
                            if (AtlasDbConstants.TABLES_KNOWN_TO_BE_POORLY_DESIGNED.contains(tablename)) {
                                log.warn(message);
                            } else {
                                log.warn(message + " This can potentially cause out-of-memory errors.");
                            }
                        }

                        while (pi.hasNext() && entries.size() < maximumCountPerPartition) {
                            runningSize += sizingFunction.apply(pi.peek());
                            if (runningSize > maximumBytesPerPartition) {
                                break;
                            }
                            entries.add(pi.next());
                        }
                        remainingEntries -= entries.size();
                        return entries;
                    }
                };
            }
        };
    }

    @Override
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        for (Map.Entry<String, byte[]> entry : tableNameToMetadata.entrySet()) {
            putMetadataForTable(entry.getKey(), entry.getValue());
        }
    }

    public boolean shouldTraceQuery(final String tableName) {
        return tracingPrefs.shouldTraceQuery(tableName);
    }

    @Override
    public void close() {
        scheduledExecutor.shutdown();
        executor.shutdown();
    }

    @Override
    public void teardown() {
        scheduledExecutor.shutdown();
        executor.shutdown();
    }

    @Override
    public void truncateTables(final Set<String> tableNames) {
        List<Future<Void>> futures = Lists.newArrayList();
        for (final String tableName : tableNames) {
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    truncateTable(tableName);
                    return null;
                }
            }));
        }

        for (Future<Void> future : futures) {
            Futures.getUnchecked(future);
        }
    }
}
