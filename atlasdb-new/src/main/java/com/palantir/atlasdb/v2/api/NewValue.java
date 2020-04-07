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

package com.palantir.atlasdb.v2.api;

import java.util.Optional;

import org.immutables.value.Value;

import com.palantir.atlasdb.v2.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.NewIds.StoredValue;

public abstract class NewValue {
    public abstract <T> T accept(Visitor<T> visitor);

    public interface Visitor<T> {
        T kvsValue(Cell cell, long startTimestamp, StoredValue data);
        T committedValue(Cell cell, long commitTimestamp, StoredValue data);
        T transactionValue(Cell cell, Optional<StoredValue> maybeData);
    }

    public abstract Cell cell();
    public abstract StoredValue data();
    public abstract boolean isLive();

    public static TransactionValue transactionValue(Cell cell, Optional<StoredValue> data) {
        return new TransactionValue.Builder().cell(cell).maybeData(data).build();
    }

    @Value.Immutable
    public static abstract class TransactionValue extends NewValue {
        public abstract Optional<StoredValue> getMaybeData();

        @Override
        public final StoredValue data() {
            return getMaybeData().get();
        }

        @Override
        public final boolean isLive() {
            return getMaybeData().isPresent();
        }

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.transactionValue(cell(), getMaybeData());
        }

        static final class Builder extends ImmutableTransactionValue.Builder {}
    }

    @Value.Immutable
    public static abstract class CommittedValue extends NewValue {
        public abstract long commitTimestamp();

        @Override
        public final boolean isLive() {
            return true;
        }

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.committedValue(cell(), commitTimestamp(), data());
        }

        static final class Builder extends ImmutableCommittedValue.Builder {}
    }

    @Value.Immutable
    public static abstract class KvsValue extends NewValue {
        public abstract long startTimestamp();

        @Override
        public final boolean isLive() {
            return true;
        }

        @Override
        public final <T> T accept(Visitor<T> visitor) {
            return visitor.kvsValue(cell(), startTimestamp(), data());
        }

        static final class Builder extends ImmutableKvsValue.Builder {}
    }
}
