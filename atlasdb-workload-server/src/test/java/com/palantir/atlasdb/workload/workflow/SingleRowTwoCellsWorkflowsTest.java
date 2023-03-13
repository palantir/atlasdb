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

package com.palantir.atlasdb.workload.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import org.junit.Test;

public class SingleRowTwoCellsWorkflowsTest {
    private static final String TABLE_NAME = "coffee";

    @Test
    public void shouldWriteToFirstCellOnEvenIndices() {
        assertThat(SingleRowTwoCellsWorkflows.shouldWriteToFirstCell(0)).isTrue();
        assertThat(SingleRowTwoCellsWorkflows.shouldWriteToFirstCell(2)).isTrue();
        assertThat(SingleRowTwoCellsWorkflows.shouldWriteToFirstCell(24682468)).isTrue();
    }

    @Test
    public void shouldNotWriteToFirstCellOnOddIndices() {
        assertThat(SingleRowTwoCellsWorkflows.shouldWriteToFirstCell(1)).isFalse();
        assertThat(SingleRowTwoCellsWorkflows.shouldWriteToFirstCell(3)).isFalse();
        assertThat(SingleRowTwoCellsWorkflows.shouldWriteToFirstCell(35793579)).isFalse();
    }

    @Test
    public void createsCorrectTransactionActions() {
        assertThat(SingleRowTwoCellsWorkflows.createTransactionActions(0, TABLE_NAME))
                .containsExactly(
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL),
                        WriteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL, 0),
                        DeleteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL));
        assertThat(SingleRowTwoCellsWorkflows.createTransactionActions(1, TABLE_NAME))
                .containsExactly(
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL),
                        DeleteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        WriteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL, 1),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL));
    }

    @Test
    public void writesValueCorrespondingToTaskIndexInRelevantCell() {
        assertThat(SingleRowTwoCellsWorkflows.createTransactionActions(31415926, TABLE_NAME))
                .containsExactly(
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL),
                        WriteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL, 31415926),
                        DeleteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL));
        assertThat(SingleRowTwoCellsWorkflows.createTransactionActions(6021023, TABLE_NAME))
                .containsExactly(
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL),
                        DeleteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        WriteTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL, 6021023),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.FIRST_CELL),
                        ReadTransactionAction.of(TABLE_NAME, SingleRowTwoCellsWorkflows.SECOND_CELL));
    }
}
