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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.Map;
import java.util.Set;

public abstract class TracingTransaction extends ForwardingTransaction {
    private final Transaction delegate;

    protected TracingTransaction(Transaction delegate) {
        this.delegate = delegate;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        if (isTraceEnabled()) {
            for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                Cell key = e.getKey();
                byte[] value = e.getValue();
                trace(
                    "PUT: timestamp=%d table=%s row=%s column=%s value=%s",
                    delegate.getTimestamp(),
                        tableRef,
                    toHex(key.getRowName()),
                    toHex(key.getColumnName()),
                    toHex(value));
            }
        }
        super.put(tableRef, values);
    }

    @Override
    public void delete(TableReference tableRef, Set<Cell> keys) {
        if (isTraceEnabled()) {
            for (Cell key : keys) {
                trace(
                    "DELETE: timestamp=%d table=%s row=%s column=%s",
                    delegate.getTimestamp(),
                        tableRef,
                    toHex(key.getRowName()),
                    toHex(key.getColumnName()));
            }
        }
        super.delete(tableRef, keys);
    }

    @Override
    public void abort() {
        if (isTraceEnabled()) {
            trace(String.format("ABORT: timestamp=%d", delegate.getTimestamp()));
        }
        super.abort();
    }

    @Override
    public void commit() {
        if (isTraceEnabled()) {
            trace(String.format("COMMIT: timestamp=%d", delegate.getTimestamp()));
        }
        super.commit();
    }

    private String toHex(byte[] bytes) {
        return BaseEncoding.base16().lowerCase().encode(bytes);
    }

    protected abstract boolean isTraceEnabled();

    protected abstract void trace(String format, Object... args);
}
