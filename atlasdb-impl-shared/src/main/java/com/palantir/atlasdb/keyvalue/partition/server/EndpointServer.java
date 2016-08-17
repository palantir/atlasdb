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
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final KeyValueService kvsDelegate;
    private final PartitionMapService pmsDelegate;

    public EndpointServer(KeyValueService kvs, PartitionMapService pms) {
        this.kvsDelegate = kvs;
        this.pmsDelegate = pms;
    }

    public KeyValueService kvs() {
        return kvsDelegate;
    }

    public PartitionMapService pms() {
        return pmsDelegate;
    }

    private <T> T runPartitionMapReadOperation(Function<Void, T> operation) {
        try {
            lock.readLock().lockInterruptibly();
            return operation.apply(null);
        } catch (InterruptedException ex) {
            throw Throwables.throwUncheckedException(ex);
        } finally {
            lock.readLock().unlock();
        }
    }

    private <T> T runPartitionMapWriteOperation(Function<Void, T> operation) {
        try {
            lock.writeLock().lockInterruptibly();
            return operation.apply(null);
        } catch (InterruptedException ex) {
            throw Throwables.throwUncheckedException(ex);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public DynamicPartitionMap getMap() {
        return runPartitionMapReadOperation(input -> pms().getMap());
    }

    @Override
    public long getMapVersion() {
        return runPartitionMapReadOperation(input -> pms().getMapVersion());
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
        return runPartitionMapWriteOperation(input -> pms().updateMapIfNewer(partitionMap));
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
        return runPartitionMapReadOperation(input -> kvs().getDelegates());
    }

    @Override
    @Idempotent
    public Map<Cell, Value> getRows(final TableReference tableRef, final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection, final long timestamp) {
        return runPartitionMapReadOperation(input -> kvs().getRows(tableRef, rows, columnSelection, timestamp));
    }

    @Override
    @Idempotent
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            long timestamp) {
        return runPartitionMapReadOperation(input ->
                kvs().getRowsColumnRange(tableRef, rows, columnRangeSelection, timestamp));
    }

    @Override
    @Idempotent
    public Map<Cell, Value> get(final TableReference tableRef,
            final Map<Cell, Long> timestampByCell) {
        return runPartitionMapReadOperation(input -> kvs().get(tableRef, timestampByCell));
    }

    @Override
    @Idempotent
    public Map<Cell, Long> getLatestTimestamps(final TableReference tableRef,
            final Map<Cell, Long> timestampByCell) {
        return runPartitionMapReadOperation(input -> kvs().getLatestTimestamps(tableRef, timestampByCell));
    }

    @Override
    public void put(final TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp)
            throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().put(tableRef, values, timestamp);
                return null;
            }
        });
    }

    @Override
    public void multiPut(
            final Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable,
            final long timestamp) throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().multiPut(valuesByTable, timestamp);
                return null;
            }
        });
    }

    @Override
    @NonIdempotent
    public void putWithTimestamps(final TableReference tableRef,
            final Multimap<Cell, Value> cellValues) throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().putWithTimestamps(tableRef, cellValues);
                return null;
            }
        });
    }

    @Override
    public void putUnlessExists(final TableReference tableRef, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().putUnlessExists(tableRef, values);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void delete(final TableReference tableRef, final Multimap<Cell, Long> keys) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().delete(tableRef, keys);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTable(final TableReference tableRef)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().truncateTable(tableRef);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void truncateTables(final Set<TableReference> tableRefs)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().truncateTables(tableRefs);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(final TableReference tableRef,
            final RangeRequest rangeRequest, final long timestamp) {
        return runPartitionMapReadOperation(input -> kvs().getRange(tableRef, rangeRequest, timestamp));
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(
            final TableReference tableRef, final RangeRequest rangeRequest, final long timestamp) {
        return runPartitionMapReadOperation(input -> kvs().getRangeWithHistory(tableRef, rangeRequest, timestamp));
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            final TableReference tableRef, final RangeRequest rangeRequest, final long timestamp)
            throws InsufficientConsistencyException {
        return runPartitionMapReadOperation(input -> kvs().getRangeOfTimestamps(tableRef, rangeRequest, timestamp));
    }

    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            final TableReference tableRef, final Iterable<RangeRequest> rangeRequests,
            final long timestamp) {
        return runPartitionMapReadOperation(input -> kvs().getFirstBatchForRanges(tableRef, rangeRequests, timestamp));
    }

    @Override
    @Idempotent
    public void dropTable(final TableReference tableRef)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().dropTable(tableRef);
                return null;
            }

        });
    }

    @Override
    public void dropTables(final Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().dropTables(tableRefs);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTable(final TableReference tableRef, final byte[] tableMetadata)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().createTable(tableRef, tableMetadata);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void createTables(
            final Map<TableReference, byte[]> tableRefToTableMetadata)
            throws InsufficientConsistencyException {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                kvs().createTables(tableRefToTableMetadata);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Set<TableReference> getAllTableNames() {
        return runPartitionMapReadOperation(input -> {
            // TODO: Hack
            Set<TableReference> ret = Sets.newHashSet(kvs().getAllTableNames());
            ret.remove(InKvsPartitionMapService.PARTITION_MAP_TABLE);
            return ret;
        });
    }

    @Override
    @Idempotent
    public byte[] getMetadataForTable(final TableReference tableRef) {
        return runPartitionMapReadOperation(input -> kvs().getMetadataForTable(tableRef));
    }

    @Override
    @Idempotent
    public Map<TableReference, byte[]> getMetadataForTables() {
        return runPartitionMapReadOperation(input -> kvs().getMetadataForTables());
    }

    @Override
    @Idempotent
    public void putMetadataForTable(final TableReference tableRef, final byte[] metadata) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().putMetadataForTable(tableRef, metadata);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().putMetadataForTables(tableRefToMetadata);
                return null;
            }

        });
    }

    @Override
    @Idempotent
    public void addGarbageCollectionSentinelValues(final TableReference tableRef,
            final Set<Cell> cells) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().addGarbageCollectionSentinelValues(tableRef, cells);
                return null;
            }
        });
    }

    @Override
    @Idempotent
    public Multimap<Cell, Long> getAllTimestamps(final TableReference tableRef,
            final Set<Cell> cells, final long timestamp)
            throws InsufficientConsistencyException {
        return runPartitionMapReadOperation(input -> kvs().getAllTimestamps(tableRef, cells, timestamp));
    }

    @Override
    public void compactInternally(final TableReference tableRef) {
        runPartitionMapReadOperation(new Function<Void, Void>() {
            @Override
            public Void apply(@Nullable Void input) {
                kvs().compactInternally(tableRef);
                return null;
            }
        });
    }
}
