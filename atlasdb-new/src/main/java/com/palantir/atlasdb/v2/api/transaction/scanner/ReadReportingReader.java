/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.scanner;

import java.util.Optional;
import java.util.function.Consumer;

import com.palantir.atlasdb.v2.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.AsyncIterators;
import com.palantir.atlasdb.v2.api.NewIds;
import com.palantir.atlasdb.v2.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.NewValue;
import com.palantir.atlasdb.v2.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.transaction.Reader;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

public class ReadReportingReader<In extends NewValue> implements Reader<ReadReportingReader.RecordingNewValue> {
    private final Reader<In> delegate;
    private final AsyncIterators iterators;

    public ReadReportingReader(Reader<In> delegate, AsyncIterators iterators) {
        this.delegate = delegate;
        this.iterators = iterators;
    }

    @Override
    public AsyncIterator<RecordingNewValue> scan(TransactionState state, ScanDefinition definition) {
        AsyncIterator<In> scan = delegate.scan(state, definition);
        if (!state.checkReadWriteConflicts(definition.table())) {
            return iterators.transform(scan, NoopRecordingNewValue::new);
        }
        return iterators.transform(scan, value -> new ReadRecordingNewValue(definition.table(), value, definition));
    }

    // todo hashcode etc
    public static abstract class RecordingNewValue extends NewValue implements Consumer<TransactionState.Builder> {
        private final NewValue delegate;

        private RecordingNewValue(NewValue delegate) {
            this.delegate = delegate;
        }

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return delegate.accept(visitor);
        }

        @Override
        public final NewIds.Cell cell() {
            return delegate.cell();
        }

        @Override
        public final Optional<NewIds.StoredValue> maybeData() {
            return delegate.maybeData();
        }
    }

    private static final class NoopRecordingNewValue extends RecordingNewValue {
        private NoopRecordingNewValue(NewValue delegate) {
            super(delegate);
        }

        @Override
        public final void accept(TransactionState.Builder builder) {}
    }

    private static final class ReadRecordingNewValue extends RecordingNewValue {
        private final Table table;
        private final NewValue delegate;
        private final ScanDefinition scan;

        private ReadRecordingNewValue(Table table, NewValue delegate, ScanDefinition scan) {
            super(delegate);
            this.table = table;
            this.delegate = delegate;
            this.scan = scan;
        }

        @Override
        public final void accept(TransactionState.Builder builder) {
            builder.readsBuilder().mutateReads(table, tableReads -> tableReads.putScanEnd(scan, delegate));
        }
    }
}
