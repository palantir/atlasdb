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

import com.palantir.atlasdb.workload.store.ColumnAndValue;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowColumnRangeReadTransactionAction;
import java.util.List;
import org.immutables.value.Value;

/**
 * This transaction action models a column range read in AtlasDB ("getRowsColumnRange" in the AtlasDB API).
 * <p>
 * In the interest of implementation simplicity, two significant simplifications are made. Firstly, the action is
 * presumed to act on a single row, though calls to getRowsColumnRange in practice can act on multiple rows in parallel.
 * Secondly, values read from getRowsColumnRange are accessed through a live iterator, while for purposes of gathering
 * a witness, we immediately drain the iterator and use the set of columns read as the witness to this action.
 */
@Value.Immutable
public interface RowColumnRangeReadTransactionAction extends TransactionAction {
    String table();

    int row();

    ColumnRangeSelection columnRangeSelection();

    @Override
    default <T> T accept(TransactionActionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    default WitnessedRowColumnRangeReadTransactionAction witness(List<ColumnAndValue> columnsAndValues) {
        return WitnessedRowColumnRangeReadTransactionAction.builder()
                .originalQuery(this)
                .columnsAndValues(columnsAndValues)
                .build();
    }

    static ImmutableRowColumnRangeReadTransactionAction.Builder builder() {
        return ImmutableRowColumnRangeReadTransactionAction.builder();
    }
}
