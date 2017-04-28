/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
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
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
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
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.lock.LockRefreshToken;

public class AtlasDbServiceImpl implements AtlasDbService {
    private static final TableMetadata RAW_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("row", ValueType.STRING))),
            new ColumnMetadataDescription(new DynamicColumnDescription(NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("col", ValueType.STRING))), ColumnValueDescription.forType(ValueType.STRING))),
            ConflictHandler.SERIALIZABLE);

    private final KeyValueService kvs;
    private final SerializableTransactionManager txManager;
    private final Cache<TransactionToken, RawTransaction> transactions =
            CacheBuilder.newBuilder().expireAfterAccess(12, TimeUnit.HOURS).build();
    private final TableMetadataCache metadataCache;

    @Inject
    public AtlasDbServiceImpl(KeyValueService kvs,
                              SerializableTransactionManager txManager,
                              TableMetadataCache metadataCache) {
        this.kvs = kvs;
        this.txManager = txManager;
        this.metadataCache = metadataCache;
    }

    @Override
    public Set<String> getAllTableNames() {
        return kvs.getAllTableNames().stream().map(TableReference::getQualifiedName).collect(Collectors.toSet());
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) {
        return metadataCache.getMetadata(tableName);
    }

    @Override
    public void createTable(String tableName) {
        kvs.createTable(getTableRef(tableName), RAW_METADATA.persistToBytes());
    }

    @Override
    public TableRowResult getRows(TransactionToken token,
                                  final TableRowSelection rows) {
        return runReadOnly(token, new RuntimeTransactionTask<TableRowResult>() {
            @Override
            public TableRowResult execute(Transaction t) {
                Collection<RowResult<byte[]>> values = t.getRows(
                        getTableRef(rows.getTableName()), rows.getRows(), rows.getColumnSelection()).values();
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
                Map<Cell, byte[]> values = t.get(getTableRef(cells.getTableName()), ImmutableSet.copyOf(cells.getCells()));
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
                int limit = range.getBatchSize() + 1;
                RangeRequest request = RangeRequest.builder()
                    .startRowInclusive(range.getStartRow())
                    .endRowExclusive(range.getEndRow())
                    .batchHint(limit)
                    .retainColumns(range.getColumns())
                    .build();
                BatchingVisitable<RowResult<byte[]>> visitable = t.getRange(getTableRef(range.getTableName()), request);
                List<RowResult<byte[]>> results = BatchingVisitables.limit(visitable, limit).immutableCopy();
                if (results.size() == limit) {
                    TableRowResult data = new TableRowResult(range.getTableName(), results.subList(0, limit - 1));
                    RowResult<byte[]> lastResultInBatch = results.get(limit - 1);
                    TableRange nextRange = range.withStartRow(lastResultInBatch.getRowName());
                    return new RangeToken(data, nextRange);
                } else {
                    TableRowResult data = new TableRowResult(range.getTableName(), results);
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
                t.put(getTableRef(data.getTableName()), data.getResults());
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
                t.delete(getTableRef(cells.getTableName()), ImmutableSet.copyOf(cells.getCells()));
                return null;
            }
        });
    }

    @Override
    public void truncateTable(final String fullyQualifiedTableName) {
        kvs.truncateTable(getTableRef(fullyQualifiedTableName));
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

    private TableReference getTableRef(String tableName) {
        return TableReference.createUnsafe(tableName);
    }
}
