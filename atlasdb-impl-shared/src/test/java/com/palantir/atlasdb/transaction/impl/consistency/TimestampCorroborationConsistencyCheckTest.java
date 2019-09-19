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
package com.palantir.atlasdb.transaction.impl.consistency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.palantir.atlasdb.factory.ImmutableTransactionManagerConsistencyResult;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResult;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.function.ToLongFunction;
import org.junit.Test;

public class TimestampCorroborationConsistencyCheckTest {
    private static final RuntimeException EXCEPTION = new IllegalStateException("bad");
    private static final ToLongFunction<TransactionManager> EXCEPTION_THROWER = unused -> {
        throw EXCEPTION;
    };

    @Test
    public void returnsIndeterminateIfCannotGetConservativeBound() {
        TimestampCorroborationConsistencyCheck checkFailingToGetBound =
                ImmutableTimestampCorroborationConsistencyCheck.builder()
                        .conservativeBound(EXCEPTION_THROWER)
                        .freshTimestampSource(txMgr -> 7L)
                        .build();
        assertThat(checkFailingToGetBound.apply(mock(TransactionManager.class)))
                .isEqualTo(ImmutableTransactionManagerConsistencyResult.builder()
                        .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.INDETERMINATE)
                        .reasonForInconsistency(EXCEPTION)
                        .build());
    }

    @Test
    public void returnsIndeterminateIfCannotGetFreshTimestamp() {
        TimestampCorroborationConsistencyCheck checkFailingToGetFresh =
                ImmutableTimestampCorroborationConsistencyCheck.builder()
                        .conservativeBound(txMgr -> 10L)
                        .freshTimestampSource(EXCEPTION_THROWER)
                        .build();
        assertThat(checkFailingToGetFresh.apply(mock(TransactionManager.class)))
                .isEqualTo(ImmutableTransactionManagerConsistencyResult.builder()
                        .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.INDETERMINATE)
                        .reasonForInconsistency(EXCEPTION)
                        .build());
    }

    @Test
    public void returnsConsistentIfBoundIsBelowFreshTimestamp() {
        TimestampCorroborationConsistencyCheck check = createForTimestamps(10, 20);
        assertThat(check.apply(mock(TransactionManager.class)))
                .isEqualTo(ImmutableTransactionManagerConsistencyResult.builder()
                        .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.CONSISTENT)
                        .build());
    }

    @Test
    public void returnsTerminalIfBoundEqualsFreshTimestamp() {
        TimestampCorroborationConsistencyCheck check = createForTimestamps(42, 42);
        assertThat(check.apply(mock(TransactionManager.class))).satisfies(
                result -> {
                    assertThat(result.consistencyState()).isEqualTo(
                            TransactionManagerConsistencyResult.ConsistencyState.TERMINAL);
                    assertThat(result.reasonForInconsistency()).isPresent();
                });
    }

    @Test
    public void returnsTerminalIfBoundIsAboveFreshTimestamp() {
        TimestampCorroborationConsistencyCheck check = createForTimestamps(111, 42);
        assertThat(check.apply(mock(TransactionManager.class))).satisfies(
                result -> {
                    assertThat(result.consistencyState()).isEqualTo(
                            TransactionManagerConsistencyResult.ConsistencyState.TERMINAL);
                    assertThat(result.reasonForInconsistency()).isPresent();
                });
    }

    private TimestampCorroborationConsistencyCheck createForTimestamps(long conservativeBound, long freshTimestamp) {
        return ImmutableTimestampCorroborationConsistencyCheck.builder()
                .conservativeBound(unused -> conservativeBound)
                .freshTimestampSource(unused -> freshTimestamp)
                .build();
    }
}
