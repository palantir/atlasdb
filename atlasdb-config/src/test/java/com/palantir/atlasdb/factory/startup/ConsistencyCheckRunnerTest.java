/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory.startup;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.factory.ImmutableTransactionManagerConsistencyResult;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResult;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResults;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.consistency.TransactionManagerConsistencyCheck;
import com.palantir.exception.NotInitializedException;
import org.junit.Test;

public class ConsistencyCheckRunnerTest {
    private static final RuntimeException EXCEPTION_1 = new RuntimeException("bad");
    private static final RuntimeException EXCEPTION_2 = new RuntimeException("worse");

    private static final TransactionManagerConsistencyCheck CONSISTENT_CHECK =
            getConsistencyCheckReturning(TransactionManagerConsistencyResults.CONSISTENT_RESULT);
    private static final TransactionManagerConsistencyCheck INDETERMINATE_CHECK =
            getConsistencyCheckReturning(ImmutableTransactionManagerConsistencyResult.builder()
                    .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.INDETERMINATE)
                    .reasonForInconsistency(EXCEPTION_1)
                    .build());
    private static final TransactionManagerConsistencyCheck INDETERMINATE_CHECK_2 =
            getConsistencyCheckReturning(ImmutableTransactionManagerConsistencyResult.builder()
                    .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.INDETERMINATE)
                    .reasonForInconsistency(EXCEPTION_2)
                    .build());
    private static final TransactionManagerConsistencyCheck TERMINAL_CHECK =
            getConsistencyCheckReturning(ImmutableTransactionManagerConsistencyResult.builder()
                    .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.TERMINAL)
                    .reasonForInconsistency(EXCEPTION_2)
                    .build());

    private final TransactionManager mockTxMgr = mock(TransactionManager.class);

    @Test
    public void zeroConsistencyChecksImpliesConsistency() {
        initializeRunnerWithChecks();
    }

    @Test
    public void oneConsistencyCheckReturningConsistentImpliesConsistency() {
        initializeRunnerWithChecks(CONSISTENT_CHECK);
    }

    @Test
    public void oneConsistencyCheckReturningIndeterminateImpliesNotReady() {
        assertThatThrownBy(() -> initializeRunnerWithChecks(INDETERMINATE_CHECK))
                .isInstanceOf(NotInitializedException.class);
    }

    @Test
    public void oneConsistencyCheckReturningTerminalThrowsAssertionError() {
        assertThatThrownBy(() -> initializeRunnerWithChecks(TERMINAL_CHECK))
                .isInstanceOf(AssertionError.class)
                .hasStackTraceContaining(EXCEPTION_2.getMessage());
    }

    @Test
    public void oneConsistentOneIndeterminateCheckImpliesIndeterminate() {
        assertThatThrownBy(() -> initializeRunnerWithChecks(CONSISTENT_CHECK, INDETERMINATE_CHECK))
                .isInstanceOf(NotInitializedException.class);
    }

    @Test
    public void oneConsistentOneTerminalCheckImpliesTerminal() {
        assertThatThrownBy(() -> initializeRunnerWithChecks(CONSISTENT_CHECK, TERMINAL_CHECK))
                .isInstanceOf(AssertionError.class)
                .hasStackTraceContaining(EXCEPTION_2.getMessage());
    }

    @Test
    public void oneIndeterminateOneTerminalCheckImpliesTerminal() {
        assertThatThrownBy(() -> initializeRunnerWithChecks(INDETERMINATE_CHECK, TERMINAL_CHECK))
                .isInstanceOf(AssertionError.class)
                .hasStackTraceContaining(EXCEPTION_2.getMessage());
    }

    @Test
    public void returnsResultWithHighestSeverity() {
        assertThatThrownBy(() -> initializeRunnerWithChecks(INDETERMINATE_CHECK, TERMINAL_CHECK, CONSISTENT_CHECK))
                .isInstanceOf(AssertionError.class)
                .hasStackTraceContaining(EXCEPTION_2.getMessage());
    }

    @Test
    public void multipleIndeterminateChecksDoNotImplyTerminal() {
        assertThatThrownBy(() -> initializeRunnerWithChecks(INDETERMINATE_CHECK, INDETERMINATE_CHECK_2))
                .isInstanceOf(NotInitializedException.class);
    }

    private static TransactionManagerConsistencyCheck getConsistencyCheckReturning(
            TransactionManagerConsistencyResult result) {
        return _unused -> result;
    }

    private void initializeRunnerWithChecks(TransactionManagerConsistencyCheck... checks) {
        new ConsistencyCheckRunner(ImmutableList.copyOf(checks)).init(mockTxMgr);
    }
}
