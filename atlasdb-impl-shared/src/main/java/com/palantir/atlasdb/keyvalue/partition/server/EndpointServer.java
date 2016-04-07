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
package com.palantir.atlasdb.keyvalue.partition.server;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.map.InKvsPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * This class is to ensure that the partition map will not be
 * updated while any key value operations are in progress.
 *
 * @author htarasiuk
 *
 */
public class EndpointServer implements PartitionMapService, KeyValueService {

    private final KeyValueService kvsDelegate;
    public KeyValueService kvs() {
        return kvsDelegate;
    }

    private final PartitionMapService pmsDelegate;
    public PartitionMapService pms() {
        return pmsDelegate;
    }

    final ReadWriteLock lock = new ReentrantReadWriteLock();

    public EndpointServer(KeyValueService kvs, PartitionMapService pms) {
        this.kvsDelegate = kvs;
        this.pmsDelegate = pms;
    }

    private <T> T runPartitionMapReadOperation(Function<Void, T> c) {
        try {
            lock.readLock().lockInterruptibly();
            return c.apply(null);
        } catch (InterruptedException ex) {
            throw Throwables.throwUncheckedException(ex);
        } finally {
            lock.readLock().unlock();
        }
    }

    private <T> T runPartitionMapWriteOperation(Function<Void, T> c) {
        try {
            lock.writeLock().lockInterruptibly();
            return c.apply(null);
        } catch (InterruptedException ex) {
            throw Throwables.throwUncheckedException(ex);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public DynamicPartitionMap getMap() {
        return runPartitionMapReadOperation(new Function<Void, DynamicPartitionMap>() {
            @Override
            public DynamicPartitionMap apply(Void input) {
                return pms().getMap();
            }
        });
    }

    @Override
    public long getMapVersion() {
        return runPartitionMapReadOperation(new Function<Void, Long>() {
            @Override
            public Long apply(Void input) {
                return pms().getMapVersion();
            }
        });
    }

    @Override
    public void updateMap(final DynamicPartitionMap partitionMap) {
        runPartitionMapWriteOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                pms().updateMap(partitionMap);
                return null;
            }

        });
    }

    @Override
    public long updateMapIfNewer(final DynamicPartitionMap partitionMap) {
        return runPartitionMapWriteOperation(new Function<Void, Long>() {
            @Override
            public Long apply(Void input) {
                return pms().updateMapIfNewer(partitionMap);
            }
        });
    }

    @Override
    public void initializeFromFreshInstance() {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().initializeFromFreshInstance();
                return null;
            }
        });
    }

    @Override
    public void close() {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().close();
                return null;
            }

        });
    }

    @Override
    public void teardown() {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().teardown();
                return null;
            }
        });
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return runPartitionMapReadOperation(new Function<Void, Collection<? extends KeyValueService>>() {
            @Override
            public Collection<? extends KeyValueService> apply(Void input) {
                return kvs().getDelegates();
            }
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final String tableName, final Iterable<byte[]> rows,
            final ColumnSelection columnSelection, final long timestamp) {
        return runPartitionMapReadOperation(new Function<Void, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(Void input) {
                return kvs().getRows(tableName, rows, columnSelection, timestamp);
            }
        });
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final String tableName,
            final Map<Cell, Long> timestampByCell) {
        return runPartitionMapReadOperation(new Function<Void, Map<Cell, Value>>() {
            @Override
            public Map<Cell, Value> apply(Void input) {
                return kvs().get(tableName, timestampByCell);
            }

        });
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final String tableName,
            final Map<Cell, Long> timestampByCell) {
        return runPartitionMapReadOperation(new Function<Void, Map<Cell, Long>>() {
            @Override
            public Map<Cell, Long> apply(Void input) {
                return kvs().getLatestTimestamps(tableName, timestampByCell);
            }
        });
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void> () {
            @Override
            public Void apply(Void input) {
                kvs().put(tableName, values, timestamp);
                return null;
            }
        });
    }

    @Override
    public void multiPut(
            final Map<String, ? extends Map<Cell, byte[]>> valuesByTable,
            final long timestamp) throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void> () {
            @Override
            public Void apply(Void input) {
                kvs().multiPut(valuesByTable, timestamp);
                return null;
            }
        });
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final String tableName,
            final Multimap<Cell, Value> cellValues) throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void> () {
            @Override
            public Void apply(Void input) {
                kvs().putWithTimestamps(tableName, cellValues);
                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void> () {
            @Override
            public Void apply(Void input) {
                kvs().putUnlessExists(tableName, values);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().delete(tableName, keys);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTable(final String tableName)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void> () {
            @Override
            public Void apply(Void input) {
                kvs().truncateTable(tableName);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTables(final Set<String> tableNames)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void> () {
            @Override
            public Void apply(Void input) {
                kvs().truncateTables(tableNames);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
            final RangeRequest rangeRequest, final long timestamp) {
        return runPartitionMapReadOperation(new Function<Void, ClosableIterator<RowResult<Value>>>() {
            @Override
            public ClosableIterator<RowResult<Value>> apply(Void input) {
                return kvs().getRange(tableName, rangeRequest, timestamp);
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(
            final String tableName, final RangeRequest rangeRequest, final long timestamp) {
        return runPartitionMapReadOperation(new Function<Void, ClosableIterator<RowResult<Set<Value>>>>() {
            @Override
            public ClosableIterator<RowResult<Set<Value>>> apply(Void input) {
                return kvs().getRangeWithHistory(tableName, rangeRequest, timestamp);
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            final String tableName, final RangeRequest rangeRequest, final long timestamp)
            throws InsufficientConsistencyException {
        return runPartitionMapReadOperation(new Function<Void, ClosableIterator<RowResult<Set<Long>>>> () {
            @Override
            public ClosableIterator<RowResult<Set<Long>>> apply(Void input) {
                return kvs().getRangeOfTimestamps(tableName, rangeRequest, timestamp);
            }
        });
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            final String tableName, final Iterable<RangeRequest> rangeRequests,
            final long timestamp) {
        return runPartitionMapReadOperation(new Function<Void, Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>>() {
            @Override
            public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> apply(Void input) {
                return kvs().getFirstBatchForRanges(tableName, rangeRequests, timestamp);
            }
        });
    }

    @Override
    @Idempotent
    public void dropTable(final String tableName)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().dropTable(tableName);
                return null;
            }

        });
    }

    @Override
    public void dropTables(final Set<String> tableNames) throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().dropTables(tableNames);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTable(final String tableName, final byte[] tableMetadata)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void> () {
            @Override
            public Void apply(Void input) {
                kvs().createTable(tableName, tableMetadata);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTables(
            final Map<String, byte[]> tableNameToTableMetadata)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().createTables(tableNameToTableMetadata);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Set<String> getAllTableNames() {
        return runPartitionMapReadOperation(new Function<Void, Set<String>>() {
            @Override
            public Set<String> apply(Void input) {
                // TODO: Hack
                Set<String> ret = Sets.newHashSet(kvs().getAllTableNames());
                ret.remove(InKvsPartitionMapService.PARTITION_MAP_TABLE);
                return ret;
            }
        });
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(final String tableName) {
        return runPartitionMapReadOperation(new Function<Void, byte[]>() {
            @Override
            public byte[] apply(Void input) {
                return kvs().getMetadataForTable(tableName);
            }});
    }

    @Override
    @Idempotent
    public Map<String, byte[]> getMetadataForTables() {
        return runPartitionMapReadOperation(new Function<Void, Map<String, byte[]>>() {
            @Override
            public Map<String, byte[]> apply(Void input) {
                return kvs().getMetadataForTables();
            }

        });
    }

    @Override
    @Idempotent
    public void putMetadataForTable(final String tableName, final byte[] metadata) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().putMetadataForTable(tableName, metadata);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().putMetadataForTables(tableNameToMetadata);
                return null;
            }

        });
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final String tableName,
            final Set<Cell> cells) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().addGarbageCollectionSentinelValues(tableName, cells);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(final String tableName,
            final Set<Cell> cells, final long timestamp)
            throws InsufficientConsistencyException {
        return runPartitionMapReadOperation(new Function<Void, Multimap<Cell, Long>>() {
            @Override
            public Multimap<Cell, Long> apply(@Nullable Void input) {
                return kvs().getAllTimestamps(tableName, cells, timestamp);
            }
        });
    }

    @Override
    public void compactInternally(final String tableName) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().compactInternally(tableName);
                return null;
            }
        });
    }

}
