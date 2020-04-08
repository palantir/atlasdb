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

package com.palantir.atlasdb.v2.api.transaction.state;

import java.util.function.UnaryOperator;

import com.palantir.atlasdb.v2.api.NewIds;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public final class TransactionWrites {
    static final TransactionWrites EMPTY = new Builder().build();
    private final Map<NewIds.Table, TableWrites> writes;

    private TransactionWrites(Map<NewIds.Table, TableWrites> writes) {
        this.writes = writes;
    }

    public boolean isEmpty() {
        return writes.values().forAll(TableWrites::isEmpty);
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private Map<NewIds.Table, TableWrites> writes;

        public Builder() {
            writes = HashMap.empty();
        }

        public Builder(TransactionWrites transactionWrites) {
            writes = transactionWrites.writes;
        }

        public Builder clear() {
            writes = HashMap.empty();
            return this;
        }

        public Builder put(NewIds.Table table, TableWrites tableWrites) {
            writes = writes.put(table, tableWrites);
            return this;
        }

        public Builder mutateWrites(NewIds.Table table, UnaryOperator<TableWrites.Builder> mutator) {
            TableWrites.Builder tableWrites = writes.getOrElse(table, TableWrites.EMPTY).toBuilder();
            mutator.apply(tableWrites);
            writes = writes.put(table, tableWrites.build());
            return this;
        }

        public TransactionWrites build() {
            return new TransactionWrites(writes);
        }

        public TransactionWrites buildPartial() {
            return new TransactionWrites(writes);
        }

        public Builder mergeFrom(TransactionWrites other) {
            if (writes.isEmpty()) {
                writes = other.writes;
                return this;
            }
            for (Tuple2<NewIds.Table, TableWrites> value : other.writes) {
                writes = writes.put(value._1, value._2);
            }
            return this;
        }
    }
}
