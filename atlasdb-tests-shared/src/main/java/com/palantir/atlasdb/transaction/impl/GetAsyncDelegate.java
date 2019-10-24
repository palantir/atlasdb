/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.Throwables;

public class GetAsyncDelegate extends ForwardingTransaction {

    private final Transaction delegate;
    private final Runnable enterFunction;
    private final Runnable exitFunction;

    public GetAsyncDelegate(Transaction transaction) {
        this.delegate = transaction;
        this.enterFunction = () -> { };
        this.exitFunction = () -> { };
    }

    public GetAsyncDelegate(Transaction transaction, AtomicInteger atomicInteger) {
        this.delegate = transaction;
        this.enterFunction = atomicInteger::incrementAndGet;
        this.exitFunction = atomicInteger::decrementAndGet;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        try {
            enterFunction.run();
            return delegate().getAsync(tableRef, cells).get();
        } catch (InterruptedException e) {
            exitFunction.run();
            throw Throwables.rewrapAndThrowUncheckedException(e);
        } catch (ExecutionException e) {
            exitFunction.run();
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        } finally {
            exitFunction.run();
        }
    }
}
