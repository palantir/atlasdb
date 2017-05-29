/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

public class BatchingPaxosLatestRoundVerifierTest {

    private BatchingSupplier<PaxosQuorumStatus> delegate = mock(BatchingSupplier.class);
    private Function<Long, BatchingSupplier<PaxosQuorumStatus>> delegateFactory = mock(Function.class);
    private BatchingPaxosLatestRoundVerifier verifier = new BatchingPaxosLatestRoundVerifier(delegateFactory);

    @Before
    public void before() {
        when(delegateFactory.apply(anyLong())).thenReturn(delegate);
    }

    @Test
    public void returns_supplied_value() {
        PaxosQuorumStatus expected = PaxosQuorumStatus.SOME_DISAGREED;

        when(delegate.get()).thenReturn(Futures.immediateFuture(expected));

        assertThat(verifier.isLatestRound(5L)).isEqualTo(expected);
    }

    @Test
    public void uses_correct_supplier_for_different_rounds() {
        PaxosQuorumStatus round1 = PaxosQuorumStatus.SOME_DISAGREED;
        PaxosQuorumStatus round2 = PaxosQuorumStatus.NO_QUORUM;

        when(delegateFactory.apply(1L)).thenReturn(supplierOf(round1));
        when(delegateFactory.apply(2L)).thenReturn(supplierOf(round2));

        assertThat(verifier.isLatestRound(1L)).isEqualTo(round1);
        assertThat(verifier.isLatestRound(2L)).isEqualTo(round2);
    }

    @Test
    public void propagates_exceptions_to_caller() {
        RuntimeException expected = new RuntimeException("foo");

        when(delegate.get()).thenThrow(expected);

        assertThatThrownBy(() -> verifier.isLatestRound(1L)).isEqualTo(expected);
    }

    private BatchingSupplier<PaxosQuorumStatus> supplierOf(PaxosQuorumStatus result) {
        return new BatchingSupplier<>(() -> result, MoreExecutors.newDirectExecutorService());
    }

}
