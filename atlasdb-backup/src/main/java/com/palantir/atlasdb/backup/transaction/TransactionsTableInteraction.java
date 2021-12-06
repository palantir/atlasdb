/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.backup.transaction;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.policies.RetryPolicy;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Encapsulates certain operations on transaction tables for a given range of timestamps that are needed for restores
 * from backups.
 */
public interface TransactionsTableInteraction<T> {
    int LONG_READ_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(2);
    // reduce this from default because we run CleanTransactionsTableTask across N keyspaces at the same time
    int SELECT_TRANSACTIONS_FETCH_SIZE = 1_000;

    List<Statement> createSelectStatements(TableMetadata transactionsTable);

    PreparedStatement prepareAbortStatement(TableMetadata transactionsTable, Session session);

    PreparedStatement prepareCheckStatement(TableMetadata transactionsTable, Session session);

    Statement bindCheckStatement(PreparedStatement preparedCheckStatement, long startTs, T commitTs);

    Statement bindAbortStatement(PreparedStatement preparedAbortStatement, long startTs, T commitTs);

    String getTransactionsTableName();

    FullyBoundedTimestampRange getTimestampRange();

    TransactionTableEntry<T> extractTimestamps(Row row);

    boolean isRowAbortedTransaction(Row row);

    default Set<Token> getPartitionTokens(TableMetadata transactionsTable, Session session) {
        return createSelectStatements(transactionsTable).stream()
                .map(statement -> statement.setConsistencyLevel(ConsistencyLevel.ALL))
                .flatMap(select -> StreamSupport.stream(session.execute(select).spliterator(), false))
                .map(row -> row.getToken(CassandraConstants.ROW))
                .collect(Collectors.toSet());
    }

    static List<TransactionsTableInteraction<?>> getTransactionTableInteractions(
            Map<FullyBoundedTimestampRange, Integer> coordinationMap, RetryPolicy abortRetryPolicy) {
        return coordinationMap.entrySet().stream()
                .map(entry -> {
                    switch (entry.getValue()) {
                        case TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION:
                            return new Transactions1TableInteraction(entry.getKey(), abortRetryPolicy);
                        case TransactionConstants.TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION:
                            return new Transactions2TableInteraction(entry.getKey(), abortRetryPolicy);
                        case TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION:
                            return new Transactions3TableInteraction(entry.getKey(), abortRetryPolicy);
                        default:
                            throw new SafeIllegalArgumentException(
                                    "Found unsupported transactions schema version",
                                    SafeArg.of("transactionsSchema", entry.getValue()));
                    }
                })
                .collect(Collectors.toList());
    }
}
