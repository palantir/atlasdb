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

package com.palantir.atlasdb.workload.transaction.witnessed;

import com.palantir.atlasdb.workload.transaction.RowColumnRangeReadTransactionAction;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface WitnessedRowColumnRangeReadTransactionAction extends WitnessedTransactionAction {
    RowColumnRangeReadTransactionAction originalQuery();

    Map<Integer, Integer> columnsAndValues();

    @Override
    default <T> T accept(WitnessedTransactionActionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    static ImmutableWitnessedRowColumnRangeReadTransactionAction.Builder builder() {
        return ImmutableWitnessedRowColumnRangeReadTransactionAction.builder();
    }
}
