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

package com.palantir.atlasdb.v2.api.api;

import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.collect.Streams;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;

public abstract class NewLockDescriptor {
    private NewLockDescriptor() {}

    public abstract <T> T accept(Visitor<T> visitor);

    public static NewLockDescriptor timestamp(long timestamp) {
        return ImmutableTimestampLock.of(timestamp);
    }

    public static NewLockDescriptor cell(Table table, Cell cell) {
        return ImmutableCellLock.of(table, cell);
    }

    public static NewLockDescriptor row(Table table, Row row) {
        return ImmutableRowLock.of(table, row);
    }

    public interface Visitor<T> {
        T timestamp(long timestamp);
        T cell(Table table, Cell cell);
        T row(Table table, Row row);
    }

    @Value.Immutable
    static abstract class TimestampLock extends NewLockDescriptor {
        @Value.Parameter
        abstract long timestamp();

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.timestamp(timestamp());
        }
    }

    @Value.Immutable
    static abstract class CellLock extends NewLockDescriptor {
        @Value.Parameter
        abstract Table table();

        @Value.Parameter
        abstract Cell cell();

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.cell(table(), cell());
        }
    }

    @Value.Immutable
    static abstract class RowLock extends NewLockDescriptor {
        @Value.Parameter
        abstract Table table();

        @Value.Parameter
        abstract Row row();

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.row(table(), row());
        }
    }

    public static Set<NewLockDescriptor> fromTimestamps(Iterable<Long> timestamps) {
        return Streams.stream(timestamps).map(NewLockDescriptor::timestamp).collect(Collectors.toSet());
    }
}
