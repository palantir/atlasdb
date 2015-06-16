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

package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;

public class UnmodifiableTransaction extends ForwardingTransaction {
    final Transaction delegate;

    public UnmodifiableTransaction(Transaction delegate) {
        this.delegate = delegate;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String tableName, Set<Cell> keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws TransactionConflictException {
        throw new UnsupportedOperationException();
    }

}
