package com.palantir.atlas.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlas.api.AtlasService;
import com.palantir.atlas.api.RangeToken;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.api.TableCellVal;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.api.TableRowResult;
import com.palantir.atlas.api.TableRowSelection;
import com.palantir.atlas.api.TransactionToken;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.concurrent.PTExecutors;

public class AtlasServiceImpl implements AtlasService {
    private static final AtomicLong ID_GENERATOR = new AtomicLong();
    private final KeyValueService kvs;
    private final TransactionManager txManager;
    private final ExecutorService exec =
            PTExecutors.newCachedThreadPool(PTExecutors.newNamedThreadFactory(true));
    private final Cache<TransactionToken, TransactionRunner> transactions =
            CacheBuilder.newBuilder().expireAfterAccess(12, TimeUnit.HOURS).build();
    private final TableMetadataCache metadataCache;

    @Inject
    public AtlasServiceImpl(KeyValueService kvs,
                            TransactionManager txManager,
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
            TransactionRunner runner = transactions.getIfPresent(token);
            Preconditions.checkNotNull(runner, "The given transaction does not exist.");
            return runner.submit(task);
        }
    }

    private <T> T runWithRetry(TransactionToken token, RuntimeTransactionTask<T> task) {
        if (token.shouldAutoCommit()) {
            return txManager.runTaskWithRetry(task);
        } else {
            TransactionRunner runner = transactions.getIfPresent(token);
            Preconditions.checkNotNull(runner, "The given transaction does not exist.");
            return runner.submit(task);
        }
    }

    @Override
    public TransactionToken startTransaction() {
        long id = ID_GENERATOR.getAndIncrement();
        TransactionToken token = new TransactionToken(id);
        TransactionRunner runner = new TransactionRunner(txManager);
        exec.execute(runner);
        transactions.put(token, runner);
        return token;
    }

    @Override
    public void commit(TransactionToken token) {
        TransactionRunner runner = transactions.getIfPresent(token);
        if (runner != null) {
            runner.commit();
            transactions.invalidate(token);
        }
    }

    @Override
    public void abort(TransactionToken token) {
        TransactionRunner runner = transactions.getIfPresent(token);
        if (runner != null) {
            runner.abort();
            transactions.invalidate(token);
        }
    }
}
