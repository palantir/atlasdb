/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.transaction;

import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import com.palantir.atlasdb.workload.store.DefaultTable;
import com.palantir.atlasdb.workload.store.Table;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeIteratorCreationTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeReadTransactionAction;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public final class InMemoryTransactionReplayer implements WitnessedTransactionActionVisitor<Void> {
    private final StructureHolder<Map<String, Table>> values = StructureHolder.create(HashMap::empty);

    @Override
    public Void visit(WitnessedReadTransactionAction _readTransactionAction) {
        return null;
    }

    @Override
    public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
        values.with(map -> {
            Tuple2<Table, ? extends Map<String, Table>> tuple =
                    map.computeIfAbsent(writeTransactionAction.table(), unused -> DefaultTable.empty());
            tuple._1()
                    .put(
                            writeTransactionAction.cell().key(),
                            writeTransactionAction.cell().column(),
                            writeTransactionAction.value());
            return tuple._2();
        });
        return null;
    }

    /**
     * Deletes are explicitly tracked to enable invariants that rely on verifying that deleted values stay deleted.
     */
    @Override
    public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
        TableAndWorkloadCell tableAndWorkloadCell =
                TableAndWorkloadCell.of(deleteTransactionAction.table(), deleteTransactionAction.cell());
        values.with(map -> {
            Tuple2<Table, ? extends Map<String, Table>> tuple =
                    map.computeIfAbsent(deleteTransactionAction.table(), unused -> DefaultTable.empty());
            tuple._1()
                    .delete(
                            deleteTransactionAction.cell().key(),
                            deleteTransactionAction.cell().column());
            return tuple._2();
        });
        return null;
    }

    @Override
    public Void visit(WitnessedRowsColumnRangeReadTransactionAction unused) {
        return null;
    }

    @Override
    public Void visit(WitnessedRowsColumnRangeIteratorCreationTransactionAction unused) {
        return null;
    }

    public Map<String, Table> getValues() {
        return values.getSnapshot();
    }
}
