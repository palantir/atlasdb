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
package com.palantir.atlasdb.shell;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.jruby.RubyProc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.description.DefaultTableMetadata;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ReadOnlyTransaction;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.ptoss.util.Throwables;

/**
 * An {@link AtlasShellConnection} encapsulates the knowledge of the outside world that must be
 * available for AtlasDB Shell to manipulate a database. Refer to {@link AtlasShellConnectionFactory}
 * for factory methods.
 */
final public class AtlasShellConnection {
    private AtlasShellConnection(KeyValueService keyValueService,
                                 TransactionManager transactionManager,
                                 LoadingCache<String, TableMetadata> metadataCache) {
        this.keyValueService = keyValueService;
        this.transactionManager = transactionManager;
        this.metadataCache = metadataCache;
    }

    private final KeyValueService keyValueService;
    private final TransactionManager transactionManager;
    private final LoadingCache<String, TableMetadata> metadataCache;
    private AtlasShellRuby atlasShellRuby;

    /**
     * Some of the methods require callbacks into Ruby, which means we have to know about the Ruby
     * context. This method passes that dependency in.
     *
     * @param atlasShellRuby The {@link AtlasShellRuby} to use for callbacks into Ruby
     */
    public void setAtlasShellRuby(AtlasShellRuby atlasShellRuby) {
        this.atlasShellRuby = atlasShellRuby;
    }


public static AtlasShellConnection createAtlasShellConnection(final AtlasContext atlasContext) {
        LoadingCache<String, TableMetadata> metadataCache = CacheBuilder.newBuilder().build(
                new CacheLoader<String, TableMetadata>() {
                    @Override
                    public TableMetadata load(String tableName) throws Exception {
                        byte[] metadataBytes = atlasContext.getKeyValueService().getMetadataForTable(tableName);
                        if (metadataBytes == null || metadataBytes.length == 0) {
                            return new DefaultTableMetadata();
                        }
                        return DefaultTableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataBytes);
                    }
                });
        for (Entry<String, byte[]> e : atlasContext.getKeyValueService().getMetadataForTables().entrySet()) {
            if (e.getValue() == null || e.getValue().length == 0) {
                metadataCache.put(e.getKey(), new DefaultTableMetadata());
            } else {
                metadataCache.put(e.getKey(), DefaultTableMetadata.BYTES_HYDRATOR.hydrateFromBytes(e.getValue()));
            }
        }
        return new AtlasShellConnection(atlasContext.getKeyValueService(), atlasContext.getTransactionManager(), metadataCache);
    }

    /**
     * @return table names
     */
    public Set<String> getTableNames() {
        return keyValueService.getAllTableNames();
    }

    /**
     * @param tableName name of a table in AtlasDB
     * @return the corresponding {@link DefaultTableMetadata} object
     */
    public TableMetadata getTableMetadata(String tableName) {
        try {
            return metadataCache.get(tableName);
        } catch (ExecutionException e) {
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }

    /**
     * Return all timestamps in the KVS for given cells.
     */
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells) {
        return keyValueService.getAllTimestamps(tableName, cells, Long.MAX_VALUE);
    }

    /**
     * Given a ruby callback block, run it passing in an {@link AtlasShellTransactionAdapter}
     * object, and execute the resulting transaction task, with retry.
     *
     * @param block a ruby callback that describes what to do with the transaction
     * @return whatever the return value of the block was
     * @throws Exception
     */
    public Object runTaskWithRetry(RubyProc block) throws Exception {
        return transactionManager.runTaskWithRetry(makeTransactionTask(block));
    }

    /**
     * Given a ruby callback block, run it passing in an {@link AtlasShellTransactionAdapter}
     * object, and execute the resulting transaction task, without retry.
     *
     * @param block a ruby callback that describes what to do with the transaction
     * @return whatever the return value of the block was
     * @throws Exception
     */
    public Object runTaskThrowOnConflict(RubyProc block) throws TransactionFailedRetriableException,
            Exception {
        return transactionManager.runTaskThrowOnConflict(makeTransactionTask(block));
    }

    /**
     * Given a ruby callback block, run it passing in an {@link AtlasShellTransactionAdapter}
     * object, but using a read-only transaction, i.e., there can be no write-write conflict
     *
     * @param block a ruby callback that describes what to do with the transaction
     * @return whatever the return value of the block was
     * @throws Exception
     */
    public Object runTaskReadOnly(RubyProc block) throws Exception {
        return transactionManager.runTaskReadOnly(makeTransactionTask(block));
    }

    /**
     * @return a bare {@link AtlasShellTransactionAdapter} object. It is allowed to escape, which is
     *         safe (sort of) because it's read-only.
     */
    public AtlasShellTransactionAdapter unsafeReadOnlyTransaction() {
        return new AtlasShellTransactionAdapter(new ReadOnlyTransaction(
                keyValueService,
                TransactionServices
                        .createTransactionService(keyValueService),
                Long.MAX_VALUE,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                true), atlasShellRuby);

    }

    private TransactionTask<Object, Exception> makeTransactionTask(final RubyProc transactionTaskAdapter) {
        return new TransactionTask<Object, Exception>() {
            @Override
            public Object execute(Transaction transaction) throws Exception {
                AtlasShellTransactionAdapter transactionAdapter = new AtlasShellTransactionAdapter(
                        transaction,
                        atlasShellRuby);
                Object[] args = new Object[] { transactionAdapter };
                return atlasShellRuby.call(transactionTaskAdapter, "call", args);
            }
        };
    }
}
