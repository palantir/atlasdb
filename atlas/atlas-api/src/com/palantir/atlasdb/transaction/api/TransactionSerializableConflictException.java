// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.api;


public class TransactionSerializableConflictException extends TransactionFailedRetriableException {
    private static final long serialVersionUID = 1L;

    public TransactionSerializableConflictException(String message) {
        super(message);
    }

    public static TransactionSerializableConflictException create(String tableName, long timestamp, long elapsedMillis) {
        String msg = String.format("There was a read-write conflict on table %s.  This means that this table was " +
                "marked as Serializable and another transacton wrote a different value than this transaction read.  " +
                "startTs: %d  elapsedMillis: %d", tableName, timestamp, elapsedMillis);
        return new TransactionSerializableConflictException(msg);
    }

}
