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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import org.junit.Test;

public class MaybeWitnessedTransactionTest {

    private static final Long START_TIMESTAMP = 100L;
    private static final Long COMMIT_TIMESTAMP = 200L;

    @Test
    public void throwsWhenCommitTimestampIsNotPresentOnCreation() {
        assertThatLoggableExceptionThrownBy(() -> MaybeWitnessedTransaction.builder()
                        .startTimestamp(START_TIMESTAMP)
                        .build())
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining(
                        "Given how the transaction protocol works, a transaction for which we haven't retrieved the"
                                + " commit timestamp");
    }

    @Test
    public void doesNotThrowWhenCommitTimestampPresentOnCreation() {
        assertThatCode(() -> MaybeWitnessedTransaction.builder()
                        .startTimestamp(START_TIMESTAMP)
                        .commitTimestamp(COMMIT_TIMESTAMP)
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void toFullyWitnessedCopiesArgumentsCorrectly() {
        WitnessedWriteTransactionAction writeTransactionAction = mock(WitnessedWriteTransactionAction.class);
        WitnessedReadTransactionAction readTransactionAction = mock(WitnessedReadTransactionAction.class);
        List<WitnessedTransactionAction> actions = List.of(readTransactionAction, writeTransactionAction);
        FullyWitnessedTransaction fullyWitnessedTransaction = MaybeWitnessedTransaction.builder()
                .startTimestamp(START_TIMESTAMP)
                .commitTimestamp(COMMIT_TIMESTAMP)
                .actions(actions)
                .build()
                .toFullyWitnessed();
        assertThat(fullyWitnessedTransaction.startTimestamp()).isEqualTo(START_TIMESTAMP);
        assertThat(fullyWitnessedTransaction.commitTimestamp()).contains(COMMIT_TIMESTAMP);
        assertThat(fullyWitnessedTransaction.actions()).containsExactlyElementsOf(actions);
    }
}
