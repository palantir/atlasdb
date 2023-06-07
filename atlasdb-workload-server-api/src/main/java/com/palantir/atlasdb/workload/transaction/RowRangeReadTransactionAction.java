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

import java.util.Optional;
import java.util.SortedSet;
import org.immutables.value.Value;

/**
 * This transaction action models a row range scan in AtlasDB ("getRange" in the AtlasDB API).
 * <p>
 * In the interest of implementation simplicity, one significant simplification is made: values read from getRanges
 * are accessed in batches through a live iterator, while for purposes of gathering a witness, we immediately drain the
 * iterator and use the set of cells read as the witness to this action.
 */
@Value.Immutable
public interface RowRangeReadTransactionAction extends TransactionAction {
    String table();

    RangeSlice rowsToRead();

    SortedSet<Integer> columns();

    boolean reverse();


    @Override
    default <T> T accept(TransactionActionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
