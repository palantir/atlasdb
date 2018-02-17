/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.AutoDelegate_Transaction;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = Transaction.class)
public abstract class ServiceReadOnlyTransaction implements AutoDelegate_Transaction {
    private final SnapshotTransaction delegate;

    public ServiceReadOnlyTransaction(SnapshotTransaction delegate) {
        this.delegate = delegate;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    public abstract void commit();

    @Override
    public void put(TableReference arg0, Map<Cell, byte[]> arg1) {
        throw new UnsupportedOperationException("Read only transaction cannot perform writes");
    }

    @Override
    public void delete(TableReference arg0, Set<Cell> arg1) {
        throw new UnsupportedOperationException("Read only transaction cannot perform writes");
    }

    @Override
    public void setTransactionType(TransactionType arg0) {
        throw new UnsupportedOperationException("Read only transaction cannot perform writes");
    }

    @Override
    public TransactionType getTransactionType() {
        throw new UnsupportedOperationException("Read only transaction cannot perform writes");
    }

    @Override
    public void useTable(TableReference arg0, ConstraintCheckable arg1) {
        throw new UnsupportedOperationException("Read only transaction cannot perform writes");
    }
}
