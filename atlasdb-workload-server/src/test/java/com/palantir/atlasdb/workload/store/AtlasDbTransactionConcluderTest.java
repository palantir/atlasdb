/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class AtlasDbTransactionConcluderTest {
    private static final long START_TS = 1L;
    private static final long COMMIT_TS = 5L;

    private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException("sad times");

    @Mock
    private TransactionService transactionService;

    private AtlasDbTransactionConcluder transactionConcluder;

    @BeforeEach
    public void setUp() {
        transactionConcluder = new AtlasDbTransactionConcluder(transactionService);
    }

    @MethodSource("concludedTransactionStatuses")
    @ParameterizedTest
    public void forceTransactionConclusionPassesThroughConcludedStatuses(TransactionStatus status) {
        when(transactionService.getV2(START_TS)).thenReturn(status);

        assertThat(transactionConcluder.forceTransactionConclusion(START_TS)).isEqualTo(status);
    }

    @MethodSource("unconcludedTransactionStatuses")
    @ParameterizedTest
    public void forceTransactionConclusionAbortsAndReturnsAbortedForUnconcludedStatuses(TransactionStatus status) {
        when(transactionService.getV2(START_TS)).thenReturn(status);

        assertThat(transactionConcluder.forceTransactionConclusion(START_TS)).isEqualTo(TransactionStatus.aborted());
        verify(transactionService).putUnlessExists(START_TS, TransactionConstants.FAILED_COMMIT_TS);
    }

    @MethodSource("unconcludedTransactionStatuses")
    @ParameterizedTest
    public void forceTransactionConclusionReturnsAlternativeConcurrentConclusionIfLosingDataRace(
            TransactionStatus status) {
        when(transactionService.getV2(START_TS)).thenReturn(status).thenReturn(TransactionStatus.committed(COMMIT_TS));
        doThrow(new KeyAlreadyExistsException("key already exists"))
                .when(transactionService)
                .putUnlessExists(START_TS, TransactionConstants.FAILED_COMMIT_TS);

        assertThat(transactionConcluder.forceTransactionConclusion(START_TS))
                .isEqualTo(TransactionStatus.committed(COMMIT_TS));
    }

    @Test
    public void forceTransactionConclusionReturnsAlternativeConcurrentConclusionIfLosingDataRaceTwice() {
        when(transactionService.getV2(START_TS))
                .thenReturn(TransactionStatus.unknown())
                .thenReturn(TransactionStatus.inProgress())
                .thenReturn(TransactionStatus.committed(COMMIT_TS));
        doThrow(new KeyAlreadyExistsException("key already exists"))
                .when(transactionService)
                .putUnlessExists(START_TS, TransactionConstants.FAILED_COMMIT_TS);

        assertThat(transactionConcluder.forceTransactionConclusion(START_TS))
                .isEqualTo(TransactionStatus.committed(COMMIT_TS));
    }

    @MethodSource("unconcludedTransactionStatuses")
    @ParameterizedTest
    public void forceTransactionConclusionRetriesOnExceptions(TransactionStatus status) {
        when(transactionService.getV2(START_TS)).thenReturn(status);
        doThrow(RUNTIME_EXCEPTION)
                .doThrow(RUNTIME_EXCEPTION)
                .doAnswer(invocation -> null)
                .when(transactionService)
                .putUnlessExists(START_TS, TransactionConstants.FAILED_COMMIT_TS);

        assertThat(transactionConcluder.forceTransactionConclusion(START_TS)).isEqualTo(TransactionStatus.aborted());
    }

    @MethodSource("unconcludedTransactionStatuses")
    @ParameterizedTest
    public void forceTransactionConclusionThrowsIfRetryLimitReached(TransactionStatus status) {
        when(transactionService.getV2(START_TS)).thenReturn(status);
        doThrow(RUNTIME_EXCEPTION)
                .when(transactionService)
                .putUnlessExists(START_TS, TransactionConstants.FAILED_COMMIT_TS);

        assertThatThrownBy(() -> transactionConcluder.forceTransactionConclusion(START_TS))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Failed to force transaction conclusion");
    }

    private static Stream<TransactionStatus> concludedTransactionStatuses() {
        return Stream.of(TransactionStatus.aborted(), TransactionStatus.committed(COMMIT_TS));
    }

    private static Stream<TransactionStatus> unconcludedTransactionStatuses() {
        return Stream.of(TransactionStatus.inProgress(), TransactionStatus.unknown());
    }
}
