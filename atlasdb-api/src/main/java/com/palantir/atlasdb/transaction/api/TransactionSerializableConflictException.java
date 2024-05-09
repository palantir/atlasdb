/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.api;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

public final class TransactionSerializableConflictException extends TransactionFailedRetriableException
        implements SafeLoggable {
    private static final long serialVersionUID = 1L;

    private final TableReference conflictingTable;
    private final List<Arg<?>> args;

    public TransactionSerializableConflictException(
            String message, TableReference conflictingTable, List<Arg<?>> args) {
        super(message);
        this.conflictingTable = conflictingTable;
        this.args = List.copyOf(args);
    }

    public static TransactionSerializableConflictException create(
            TableReference tableRef, long timestamp, long elapsedMillis, List<Arg<?>> args) {
        String msg = String.format(
                "There was a read-write conflict on table %s.  This means that this table was marked as Serializable"
                        + " and another transaction wrote a different value than this transaction read.  startTs: %d "
                        + " elapsedMillis: %d",
                tableRef.getQualifiedName(), timestamp, elapsedMillis);
        return new TransactionSerializableConflictException(msg, tableRef, args);
    }

    @Override
    public @Safe String getLogMessage() {
        return "There was a read-write conflict. This transaction read a cell to which a concurrent transaction wrote a"
                + " different value.";
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }

    public TableReference getConflictingTable() {
        return conflictingTable;
    }
}
