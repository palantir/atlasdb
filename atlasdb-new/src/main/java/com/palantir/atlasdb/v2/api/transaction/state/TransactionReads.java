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

import java.util.Iterator;
import java.util.function.UnaryOperator;

import com.palantir.atlasdb.v2.api.api.NewIds;

import io.vavr.Tuple2;
import io.vavr.collection.LinkedHashMap;
import io.vavr.collection.Map;

public final class TransactionReads implements Iterable<TableReads> {
    static final TransactionReads EMPTY = new Builder().build();
    private final Map<NewIds.Table, TableReads> reads;

    private TransactionReads(Map<NewIds.Table, TableReads> reads) {
        this.reads = reads;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    @Override
    public Iterator<TableReads> iterator() {
        return reads.values().iterator();
    }

    public static final class Builder {
        private Map<NewIds.Table, TableReads> reads;

        public Builder() {
            reads = LinkedHashMap.empty();
        }

        public Builder(TransactionReads transactionWrites) {
            reads = transactionWrites.reads;
        }

        public Builder clear() {
            reads = LinkedHashMap.empty();
            return this;
        }

        public Builder put(NewIds.Table table, TableReads tableReads) {
            reads = reads.put(table, tableReads);
            return this;
        }

        public Builder mutateReads(NewIds.Table table, UnaryOperator<TableReads.Builder> mutator) {
            TableReads.Builder tableReads = reads.getOrElse(table, TableReads.EMPTY).toBuilder();
            mutator.apply(tableReads);
            reads = reads.put(table, tableReads.build());
            return this;
        }

        public TransactionReads build() {
            return new TransactionReads(reads);
        }

        public TransactionReads buildPartial() {
            return new TransactionReads(reads);
        }

        public Builder mergeFrom(TransactionReads other) {
            if (reads.isEmpty()) {
                reads = other.reads;
                return this;
            }
            for (Tuple2<NewIds.Table, TableReads> value : other.reads) {
                reads = reads.put(value._1, value._2);
            }
            return this;
        }
    }
}
