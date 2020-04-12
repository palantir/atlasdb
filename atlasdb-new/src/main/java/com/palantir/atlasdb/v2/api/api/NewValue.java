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

import static com.palantir.logsafe.Preconditions.checkState;

import java.util.Optional;

import org.immutables.value.Value;

import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.StoredValue;

public abstract class NewValue {
    public abstract <T> T accept(Visitor<T> visitor);

    public final boolean isLive() {
        return maybeData().isPresent();
    }

    public interface Visitor<T> {
        T kvsValue(Cell cell, long startTimestamp, Optional<StoredValue> data);
        T committedValue(Cell cell, long commitTimestamp, Optional<StoredValue> data);
        T transactionValue(Cell cell, Optional<StoredValue> maybeData);

        default T visit(NewValue value) {
            return value.accept(this);
        }
    }

    public abstract Cell cell();
    public abstract Optional<StoredValue> maybeData();

    public static CommittedValue committedValue(Cell cell, long commitTimestamp, Optional<StoredValue> data) {
        return new CommittedValue.Builder().cell(cell).commitTimestamp(commitTimestamp).maybeData(data).build();
    }

    public static TransactionValue transactionValue(Cell cell, Optional<StoredValue> data) {
        return new TransactionValue.Builder().cell(cell).maybeData(data).build();
    }

    public static KvsValue kvsValue(Cell cell, long startTimestamp, Optional<StoredValue> data) {
        return new KvsValue.Builder().cell(cell).startTimestamp(startTimestamp).maybeData(data).build();
    }

    @Value.Immutable
    public static abstract class TransactionValue extends NewValue {

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.transactionValue(cell(), maybeData());
        }

        static final class Builder extends ImmutableTransactionValue.Builder {}
    }

    @Value.Immutable
    public static abstract class CommittedValue extends NewValue {
        public abstract long commitTimestamp();

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.committedValue(cell(), commitTimestamp(), maybeData());
        }

        static final class Builder extends ImmutableCommittedValue.Builder {}
    }

    @Value.Immutable
    public static abstract class KvsValue extends NewValue {
        public abstract long startTimestamp();

        public CommittedValue toCommitted(long commitTimestamp) {
            checkState(commitTimestamp > startTimestamp(), "commitTimestamp > startTimestamp");
            return new CommittedValue.Builder()
                    .cell(cell())
                    .maybeData(maybeData())
                    .commitTimestamp(commitTimestamp)
                    .build();
        }

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.kvsValue(cell(), startTimestamp(), maybeData());
        }

        static final class Builder extends ImmutableKvsValue.Builder {}
    }
}
