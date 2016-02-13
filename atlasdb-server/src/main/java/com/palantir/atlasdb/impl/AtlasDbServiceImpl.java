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
package com.palantir.atlasdb.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TableCell;
import com.palantir.atlasdb.api.TableCellVal;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.api.TableRowResult;
import com.palantir.atlasdb.api.TableRowSelection;
import com.palantir.atlasdb.api.TransactionToken;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.RawTransaction;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.lock.LockRefreshToken;

import jersey.repackaged.com.google.common.collect.ImmutableList;

public class AtlasDbServiceImpl implements AtlasDbService {
    private static final TableMetadata RAW_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("row", ValueType.STRING))),
            new ColumnMetadataDescription(new DynamicColumnDescription(NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("col", ValueType.STRING))), ColumnValueDescription.forType(ValueType.STRING))),
            ConflictHandler.SERIALIZABLE);

    private final KeyValueService kvs;
    private final SnapshotTransactionManager txManager;
    private final Cache<TransactionToken, RawTransaction> transactions =
            CacheBuilder.newBuilder().expireAfterAccess(12, TimeUnit.HOURS).build();
    private final TableMetadataCache metadataCache;


    public AtlasDbServiceImpl(KeyValueService kvs,
                              SnapshotTransactionManager txManager,
                              TableMetadataCache metadataCache) {
        this.kvs = kvs;
        this.txManager = txManager;
        this.metadataCache = metadataCache;
    }

    @Override
    public Set<String> getAllTableNames() {
        return kvs.getAllTableNames();
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) {
        return metadataCache.getMetadata(tableName);
    }

    @Override
    public void createTable(String tableName) {
        kvs.createTable(tableName, RAW_METADATA.persistToBytes());
    }

    @Override
    public TableRowResult getRows(TransactionToken token,
                                  final TableRowSelection rows) {
        return runReadOnly(token, new RuntimeTransactionTask<TableRowResult>() {
            @Override
            public TableRowResult execute(Transaction t) {
                Collection<RowResult<byte[]>> values = t.getRows(
                        rows.getTableName(), rows.getRows(), rows.getColumnSelection()).values();
                return new TableRowResult(rows.getTableName(), values);
            }
        });
    }

    @Override
    public TableCellVal getCells(TransactionToken token,
                                 final TableCell cells) {
        return runReadOnly(token, new RuntimeTransactionTask<TableCellVal>() {
            @Override
            public TableCellVal execute(Transaction t) {
                Map<Cell, byte[]> values = t.get(cells.getTableName(), ImmutableSet.copyOf(cells.getCells()));
                return new TableCellVal(cells.getTableName(), values);
            }
        });
    }

    @Override
    public RangeToken getRange(TransactionToken token,
                               final TableRange range) {
        return runReadOnly(token, new RuntimeTransactionTask<RangeToken>() {
            @Override
            public RangeToken execute(Transaction t) {
                int limit = range.getBatchSize();
                RangeRequest request = RangeRequest.builder()
                    .startRowInclusive(range.getStartRow())
                    .endRowExclusive(range.getEndRow())
                    .batchHint(limit)
                    .retainColumns(range.getColumns())
                    .build();
                BatchingVisitable<RowResult<byte[]>> visitable = t.getRange(range.getTableName(), request);
                List<RowResult<byte[]>> results = BatchingVisitables.limit(visitable, limit).immutableCopy();
                TableRowResult data = new TableRowResult(range.getTableName(), results);
                if (results.size() == limit) {
                    RowResult<byte[]> lastResult = results.get(limit - 1);
                    TableRange nextRange = range.withStartRow(RangeRequests.nextLexicographicName(lastResult.getRowName()));
                    return new RangeToken(data, nextRange);
                } else {
                    return new RangeToken(data, null);
                }
            }
        });
    }

    @Override
    public void put(TransactionToken token,
                    final TableCellVal data) {
        runWithRetry(token, new TxTask() {
            @Override
            public Void execute(Transaction t) {
                t.put(data.getTableName(), data.getResults());
                return null;
            }
        });
    }

    @Override
    public void delete(TransactionToken token,
                       final TableCell cells) {
        runWithRetry(token, new TxTask() {
            @Override
            public Void execute(Transaction t) {
                t.delete(cells.getTableName(), ImmutableSet.copyOf(cells.getCells()));
                return null;
            }
        });
    }

    private <T> T runReadOnly(TransactionToken token, RuntimeTransactionTask<T> task) {
        if (token.shouldAutoCommit()) {
            return txManager.runTaskWithRetry(task);
        } else {
            RawTransaction tx = transactions.getIfPresent(token);
            Preconditions.checkNotNull(tx, "The given transaction does not exist.");
            return task.execute(tx);
        }
    }

    private <T> T runWithRetry(TransactionToken token, RuntimeTransactionTask<T> task) {
        if (token.shouldAutoCommit()) {
            return txManager.runTaskWithRetry(task);
        } else {
            RawTransaction tx = transactions.getIfPresent(token);
            Preconditions.checkNotNull(tx, "The given transaction does not exist.");
            return task.execute(tx);
        }
    }

    @Override
    public TransactionToken startTransaction() {
        String id = UUID.randomUUID().toString();
        TransactionToken token = new TransactionToken(id);
        RawTransaction tx = txManager.setupRunTaskWithLocksThrowOnConflict(ImmutableList.<LockRefreshToken>of());
        transactions.put(token, tx);
        return token;
    }

    @Override
    public void commit(TransactionToken token) {
        RawTransaction tx = transactions.getIfPresent(token);
        if (tx != null) {
            txManager.finishRunTaskWithLockThrowOnConflict(tx, new TxTask() {
                @Override
                public Void execute(Transaction t) {
                    return null;
                }
            });
            transactions.invalidate(token);
        }
    }

    @Override
    public void abort(TransactionToken token) {
        RawTransaction tx = transactions.getIfPresent(token);
        if (tx != null) {
            txManager.finishRunTaskWithLockThrowOnConflict(tx, new TxTask() {
                @Override
                public Void execute(Transaction t) {
                    t.abort();
                    return null;
                }
            });
            transactions.invalidate(token);
        }
    }
}
