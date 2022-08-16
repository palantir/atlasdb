/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.client.MultiClientTimeLockUnlocker.UnlockConsumer;
import com.palantir.lock.v2.LockToken;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class MultiClientTimeLockUnlockerTest {
    private static final Namespace NAMESPACE_1 = Namespace.of("namespace");
    private static final Namespace NAMESPACE_2 = Namespace.of("Namensbereich");
    private static final Namespace NAMESPACE_3 = Namespace.of("名称空间");

    private static final LockToken TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_3 = LockToken.of(UUID.randomUUID());

    private static final ConjureLockTokenV2 CONJURE_TOKEN_1 = ConjureLockTokenV2.of(TOKEN_1.getRequestId());
    private static final ConjureLockTokenV2 CONJURE_TOKEN_2 = ConjureLockTokenV2.of(TOKEN_2.getRequestId());
    private static final ConjureLockTokenV2 CONJURE_TOKEN_3 = ConjureLockTokenV2.of(TOKEN_3.getRequestId());

    private final InternalMultiClientConjureTimelockService conjureTimelockService =
            mock(InternalMultiClientConjureTimelockService.class);
    private final MultiClientTimeLockUnlocker unlocker =
            new MultiClientTimeLockUnlocker(conjureTimelockService, OptionalInt.of(1024));

    @Test
    public void canUnlockOneUserRequest() {
        when(conjureTimelockService.unlock(
                        ImmutableMap.of(NAMESPACE_1, ConjureUnlockRequestV2.of(ImmutableSet.of(CONJURE_TOKEN_1)))))
                .thenReturn(ImmutableMap.of(NAMESPACE_1, ConjureUnlockResponseV2.of(ImmutableSet.of(CONJURE_TOKEN_1))))
                .thenReturn(ImmutableMap.of(NAMESPACE_1, ConjureUnlockResponseV2.of(ImmutableSet.of())));
        assertThat(unlocker.unlock(NAMESPACE_1, ImmutableSet.of(TOKEN_1))).containsExactly(TOKEN_1);
        assertThat(unlocker.unlock(NAMESPACE_1, ImmutableSet.of(TOKEN_1))).isEmpty();
    }

    @Test
    public void delegatesMultipleTokensCorrectly() {
        DisruptorFuture<Set<LockToken>> firstResultFuture = new DisruptorFuture<>("test");
        DisruptorFuture<Set<LockToken>> secondResultFuture = new DisruptorFuture<>("test2");
        DisruptorFuture<Set<LockToken>> thirdResultFuture = new DisruptorFuture<>("test3");
        when(conjureTimelockService.unlock(ImmutableMap.of(
                        NAMESPACE_1,
                        ConjureUnlockRequestV2.of(ImmutableSet.of(CONJURE_TOKEN_1, CONJURE_TOKEN_2)),
                        NAMESPACE_2,
                        ConjureUnlockRequestV2.of(ImmutableSet.of(CONJURE_TOKEN_2, CONJURE_TOKEN_3)))))
                .thenReturn(ImmutableMap.of(
                        NAMESPACE_1,
                        ConjureUnlockResponseV2.of(ImmutableSet.of(CONJURE_TOKEN_1, CONJURE_TOKEN_2)),
                        NAMESPACE_2,
                        ConjureUnlockResponseV2.of(ImmutableSet.of(CONJURE_TOKEN_3))));
        UnlockConsumer unlockConsumer = new UnlockConsumer(conjureTimelockService);
        unlockConsumer.accept(ImmutableList.of(
                BatchElement.of(ImmutableUnlockRequest.of(NAMESPACE_1, ImmutableSet.of(TOKEN_1)), firstResultFuture),
                BatchElement.of(ImmutableUnlockRequest.of(NAMESPACE_1, ImmutableSet.of(TOKEN_2)), secondResultFuture),
                BatchElement.of(
                        ImmutableUnlockRequest.of(NAMESPACE_2, ImmutableSet.of(TOKEN_2, TOKEN_3)), thirdResultFuture)));

        assertThat(Futures.getUnchecked(firstResultFuture)).containsExactly(TOKEN_1);
        assertThat(Futures.getUnchecked(secondResultFuture)).containsExactly(TOKEN_2);
        assertThat(Futures.getUnchecked(thirdResultFuture)).containsExactly(TOKEN_3);
    }

    @Test
    public void individualTokenIsOnlySuccessfullyUnlockedOnce() {
        DisruptorFuture<Set<LockToken>> firstResultFuture = new DisruptorFuture<>("test");
        DisruptorFuture<Set<LockToken>> secondResultFuture = new DisruptorFuture<>("test2");
        DisruptorFuture<Set<LockToken>> thirdResultFuture = new DisruptorFuture<>("test3");
        when(conjureTimelockService.unlock(ImmutableMap.of(
                        NAMESPACE_1,
                        ConjureUnlockRequestV2.of(ImmutableSet.of(CONJURE_TOKEN_1, CONJURE_TOKEN_2, CONJURE_TOKEN_3)))))
                .thenReturn(ImmutableMap.of(
                        NAMESPACE_1,
                        ConjureUnlockResponseV2.of(
                                ImmutableSet.of(CONJURE_TOKEN_1, CONJURE_TOKEN_2, CONJURE_TOKEN_3))));
        UnlockConsumer unlockConsumer = new UnlockConsumer(conjureTimelockService);
        unlockConsumer.accept(ImmutableList.of(
                BatchElement.of(
                        ImmutableUnlockRequest.of(NAMESPACE_1, ImmutableSet.of(TOKEN_1, TOKEN_2)), firstResultFuture),
                BatchElement.of(
                        ImmutableUnlockRequest.of(NAMESPACE_1, ImmutableSet.of(TOKEN_2, TOKEN_3)), secondResultFuture),
                BatchElement.of(
                        ImmutableUnlockRequest.of(NAMESPACE_1, ImmutableSet.of(TOKEN_1, TOKEN_3)), thirdResultFuture)));

        assertThat(Futures.getUnchecked(firstResultFuture)).containsExactly(TOKEN_1, TOKEN_2);
        assertThat(Futures.getUnchecked(secondResultFuture)).containsExactly(TOKEN_3);
        assertThat(Futures.getUnchecked(thirdResultFuture)).isEmpty();
    }

    @Test
    public void passesThroughFailureOnExceptions() {
        RuntimeException runtimeException = new RuntimeException("I am a RuntimeException, short and stout");
        when(conjureTimelockService.unlock(ImmutableMap.of(
                        NAMESPACE_1,
                        ConjureUnlockRequestV2.of(ImmutableSet.of(CONJURE_TOKEN_1, CONJURE_TOKEN_2, CONJURE_TOKEN_3)))))
                .thenThrow(runtimeException);
        UnlockConsumer unlockConsumer = new UnlockConsumer(conjureTimelockService);
        assertThatThrownBy(() -> unlockConsumer.accept(ImmutableList.of(
                        BatchElement.of(
                                ImmutableUnlockRequest.of(NAMESPACE_1, ImmutableSet.of(TOKEN_1, TOKEN_2)),
                                new DisruptorFuture<>("test")),
                        BatchElement.of(
                                ImmutableUnlockRequest.of(NAMESPACE_1, ImmutableSet.of(TOKEN_2, TOKEN_3)),
                                new DisruptorFuture<>("test2")))))
                .isEqualTo(runtimeException);
        // Ensuring the futures have failed is the responsibility of IndependentBatchingEventHandler, not the function.
    }
}
