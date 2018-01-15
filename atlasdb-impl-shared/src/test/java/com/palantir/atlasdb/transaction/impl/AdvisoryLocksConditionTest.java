/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutNonRetriableException;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.TimeDuration;

public class AdvisoryLocksConditionTest {

    private static final HeldLocksToken LOCK_TOKEN = getHeldLocksToken();
    private static final LockRefreshToken LOCK_REFRESH_TOKEN = LOCK_TOKEN.getLockRefreshToken();

    private LockService lockService;
    private TransactionLocksCondition transactionLocksCondition;
    private ExternalLocksCondition externalLocksCondition;

    @Before
    public void before() {
        lockService = mock(LockService.class);
        transactionLocksCondition = new TransactionLocksCondition(lockService, LOCK_TOKEN);
        externalLocksCondition = new ExternalLocksCondition(lockService, ImmutableSet.of(LOCK_TOKEN));
    }

    @Test
    public void transactionLocksCondition_cleanUpReleasesLock() {
        transactionLocksCondition.cleanup();
        verify(lockService).unlock(LOCK_REFRESH_TOKEN);
    }

    @Test
    public void transactionLocksCondition_conditionFails() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of());

        assertThatThrownBy(() -> transactionLocksCondition.throwIfConditionInvalid(0L))
                .isInstanceOf(TransactionLockTimeoutException.class)
                .hasMessageContaining("Provided transaction lock expired");
    }

    @Test
    public void transactionLocksCondition_conditionSucceeds() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of(LOCK_REFRESH_TOKEN));
        transactionLocksCondition.throwIfConditionInvalid(0L);
    }

    @Test
    public void transactionLocksCondition_getLocks() {
        assertThat(transactionLocksCondition.getLocks()).containsOnly(LOCK_TOKEN);
    }

    @Test
    public void externalLocksCondition_cleanUpDoesNotReleaseLock() {
        externalLocksCondition.cleanup();
        verifyZeroInteractions(lockService);
    }

    @Test
    public void externalLocksCondition_conditionFails() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of());

        assertThatThrownBy(() -> externalLocksCondition.throwIfConditionInvalid(0L))
                .isInstanceOf(TransactionLockTimeoutNonRetriableException.class)
                .hasMessageContaining("Provided external lock tokens expired. Retry is not possible");
    }

    @Test
    public void externalLocksCondition_conditionSucceeds() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of(LOCK_REFRESH_TOKEN));
        externalLocksCondition.throwIfConditionInvalid(0L);
    }

    @Test
    public void externalLocksCondition_getLocks() {
        assertThat(externalLocksCondition.getLocks()).containsOnly(LOCK_TOKEN);
    }



    // test AdvisoryLockConditionSuppliers, External/Combined/TransactionLockCondition

    private static HeldLocksToken getHeldLocksToken() {
        ImmutableSortedMap.Builder<LockDescriptor, LockMode> builder = ImmutableSortedMap.naturalOrder();
        builder.put(
                AtlasRowLockDescriptor.of(
                        TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                        TransactionConstants.getValueForTimestamp(0L)),
                LockMode.WRITE);
        long creationDateMs = System.currentTimeMillis();
        long expirationDateMs = creationDateMs - 1;
        TimeDuration lockTimeout = SimpleTimeDuration.of(0, TimeUnit.SECONDS);
        long versionId = 0L;
        return new HeldLocksToken(
                BigInteger.ZERO,
                LockClient.of("fake lock client"),
                creationDateMs,
                expirationDateMs,
                LockCollections.of(builder.build()),
                lockTimeout,
                versionId,
                "Dummy thread");
    }
}
