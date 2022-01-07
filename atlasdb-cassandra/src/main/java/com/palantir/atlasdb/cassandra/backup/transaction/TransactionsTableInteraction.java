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

package com.palantir.atlasdb.cassandra.backup.transaction;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.RetryPolicy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Encapsulates certain operations on transaction tables for a given range of timestamps that are needed for restores
 * from backups.
 */
public interface TransactionsTableInteraction {
    int LONG_READ_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(2);
    // reduce this from default because we run CleanTransactionsTableTask across N keyspaces at the same time
    int SELECT_TRANSACTIONS_FETCH_SIZE = 1_000;

    List<Statement> createSelectStatementsForScanningFullTimestampRange(TableMetadata transactionsTable);

    PreparedStatement prepareAbortStatement(TableMetadata transactionsTable, Session session);

    PreparedStatement prepareCheckStatement(TableMetadata transactionsTable, Session session);

    Statement bindCheckStatement(PreparedStatement preparedCheckStatement, TransactionTableEntry entry);

    Statement bindAbortStatement(PreparedStatement preparedAbortStatement, TransactionTableEntry entry);

    String getTransactionsTableName();

    FullyBoundedTimestampRange getTimestampRange();

    TransactionTableEntry extractTimestamps(Row row);

    default SafeIllegalArgumentException illegalEntry(TransactionTableEntry entry) {
        throw new SafeIllegalArgumentException("Illegal entry type", SafeArg.of("entry", entry));
    }

    static List<TransactionsTableInteraction> getTransactionTableInteractions(
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
