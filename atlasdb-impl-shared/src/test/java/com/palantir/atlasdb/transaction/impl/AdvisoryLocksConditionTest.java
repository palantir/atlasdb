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
package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutNonRetriableException;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.SortedLockCollection;
import com.palantir.lock.TimeDuration;
import java.math.BigInteger;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;

public class AdvisoryLocksConditionTest {

    private static final SortedLockCollection<LockDescriptor> LOCK_DESCRIPTORS =
            LockCollections.of(ImmutableSortedMap.<LockDescriptor, LockMode>naturalOrder()
                    .put(
                            AtlasRowLockDescriptor.of(
                                    TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                                    TransactionConstants.getValueForTimestamp(0L)),
                            LockMode.WRITE)
                    .build());
    private static final LockRequest LOCK_REQUEST =
            LockRequest.builder(LOCK_DESCRIPTORS).build();
    private static final Supplier<LockRequest> LOCK_REQUEST_SUPPLIER = () -> LOCK_REQUEST;

    private static final HeldLocksToken TRANSACTION_LOCK_TOKEN = getHeldLocksToken(BigInteger.ZERO);
    private static final LockRefreshToken TRANSACTION_LOCK_REFRESH_TOKEN = TRANSACTION_LOCK_TOKEN.getLockRefreshToken();
    private static final HeldLocksToken EXTERNAL_LOCK_TOKEN = getHeldLocksToken(BigInteger.ONE);
    private static final LockRefreshToken EXTERNAL_LOCK_REFRESH_TOKEN = EXTERNAL_LOCK_TOKEN.getLockRefreshToken();

    private LockService lockService;
    private TransactionLocksCondition transactionLocksCondition;
    private ExternalLocksCondition externalLocksCondition;
    private CombinedLocksCondition combinedLocksCondition;

    @Before
    public void before() {
        lockService = mock(LockService.class);
        transactionLocksCondition = new TransactionLocksCondition(lockService, TRANSACTION_LOCK_TOKEN);
        externalLocksCondition = new ExternalLocksCondition(lockService, ImmutableSet.of(EXTERNAL_LOCK_TOKEN));
        combinedLocksCondition =
                new CombinedLocksCondition(lockService, ImmutableSet.of(EXTERNAL_LOCK_TOKEN), TRANSACTION_LOCK_TOKEN);
    }

    @Test
    public void transactionLocksCondition_cleanUpReleasesLock() {
        transactionLocksCondition.cleanup();
        verify(lockService).unlock(TRANSACTION_LOCK_REFRESH_TOKEN);
    }

    @Test
    public void transactionLocksCondition_conditionFails() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(TRANSACTION_LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of());

        assertThatThrownBy(() -> transactionLocksCondition.throwIfConditionInvalid(0L))
                .isInstanceOf(TransactionLockTimeoutException.class)
                .hasMessageContaining("Provided transaction lock expired");
    }

    @Test
    public void transactionLocksCondition_conditionSucceeds() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(TRANSACTION_LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of(TRANSACTION_LOCK_REFRESH_TOKEN));
        transactionLocksCondition.throwIfConditionInvalid(0L);
    }

    @Test
    public void transactionLocksCondition_getLocks() {
        assertThat(transactionLocksCondition.getLocks()).containsOnly(TRANSACTION_LOCK_TOKEN);
    }

    @Test
    public void externalLocksCondition_cleanUpDoesNotReleaseLock() {
        externalLocksCondition.cleanup();
        verifyNoMoreInteractions(lockService);
    }

    @Test
    public void externalLocksCondition_conditionFails() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(EXTERNAL_LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of());

        assertThatThrownBy(() -> externalLocksCondition.throwIfConditionInvalid(0L))
                .isInstanceOf(TransactionLockTimeoutNonRetriableException.class)
                .hasMessageContaining("Provided external lock tokens expired. Retry is not possible");
    }

    @Test
    public void externalLocksCondition_conditionSucceeds() {
        when(lockService.refreshLockRefreshTokens(ImmutableSet.of(EXTERNAL_LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of(EXTERNAL_LOCK_REFRESH_TOKEN));
        externalLocksCondition.throwIfConditionInvalid(0L);
    }

    @Test
    public void externalLocksCondition_getLocks() {
        assertThat(externalLocksCondition.getLocks()).containsOnly(EXTERNAL_LOCK_TOKEN);
    }

    @Test
    public void combinedLocksCondition_checkExternalLocksFirst() {
        when(lockService.refreshLockRefreshTokens(Collections.singleton(EXTERNAL_LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of());
        when(lockService.refreshLockRefreshTokens(Collections.singleton(TRANSACTION_LOCK_REFRESH_TOKEN)))
                .thenReturn(ImmutableSet.of());
        assertThatThrownBy(() -> combinedLocksCondition.throwIfConditionInvalid(0L))
                .isInstanceOf(TransactionLockTimeoutNonRetriableException.class)
                .hasMessageContaining("Provided external lock tokens expired. Retry is not possible");
    }

    @Test
    public void combinedLocksCondition_getLocks() {
        assertThat(combinedLocksCondition.getLocks()).containsOnly(EXTERNAL_LOCK_TOKEN, TRANSACTION_LOCK_TOKEN);
    }

    @Test
    public void conditionSupplier_noLocks() {
        Supplier<AdvisoryLocksCondition> conditionSupplier =
                AdvisoryLockConditionSuppliers.get(lockService, ImmutableSet.of(), () -> null);
        assertThat(conditionSupplier.get()).isSameAs(AdvisoryLockConditionSuppliers.NO_LOCKS_CONDITION);
    }

    @Test
    public void conditionSupplier_externalLocks() {
        Supplier<AdvisoryLocksCondition> conditionSupplier =
                AdvisoryLockConditionSuppliers.get(lockService, ImmutableSet.of(EXTERNAL_LOCK_TOKEN), () -> null);
        assertThat(conditionSupplier.get().getLocks()).containsOnly(EXTERNAL_LOCK_TOKEN);
    }

    @Test
    public void conditionSupplier_transactionLockSuccess() throws InterruptedException {
        Supplier<AdvisoryLocksCondition> conditionSupplier =
                AdvisoryLockConditionSuppliers.get(lockService, ImmutableSet.of(), LOCK_REQUEST_SUPPLIER);
        when(lockService.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), LOCK_REQUEST))
                .thenReturn(TRANSACTION_LOCK_TOKEN);
        assertThat(conditionSupplier.get().getLocks()).containsOnly(TRANSACTION_LOCK_TOKEN);
    }

    @Test
    public void conditionSupplier_transactionLockFailure() throws InterruptedException {
        Supplier<AdvisoryLocksCondition> conditionSupplier =
                AdvisoryLockConditionSuppliers.get(lockService, ImmutableSet.of(), LOCK_REQUEST_SUPPLIER);
        when(lockService.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), LOCK_REQUEST))
                .thenReturn(null);
        assertThatThrownBy(conditionSupplier::get)
                .isInstanceOf(LockAcquisitionException.class)
                .hasMessageContaining("Failed to lock using the provided lock request");
    }

    @Test
    public void conditionSupplier_transactionLockFailureBeforeSuccess() throws InterruptedException {
        Supplier<AdvisoryLocksCondition> conditionSupplier =
                AdvisoryLockConditionSuppliers.get(lockService, ImmutableSet.of(), LOCK_REQUEST_SUPPLIER);
        when(lockService.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), LOCK_REQUEST))
                .thenReturn(null)
                .thenReturn(null)
                .thenReturn(TRANSACTION_LOCK_TOKEN);
        assertThat(conditionSupplier.get().getLocks()).containsOnly(TRANSACTION_LOCK_TOKEN);
    }

    @Test
    public void conditionSupplier_bothLocks() throws InterruptedException {
        Supplier<AdvisoryLocksCondition> conditionSupplier = AdvisoryLockConditionSuppliers.get(
                lockService, ImmutableSet.of(EXTERNAL_LOCK_TOKEN), LOCK_REQUEST_SUPPLIER);
        when(lockService.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), LOCK_REQUEST))
                .thenReturn(TRANSACTION_LOCK_TOKEN);
        assertThat(conditionSupplier.get().getLocks()).containsOnly(EXTERNAL_LOCK_TOKEN, TRANSACTION_LOCK_TOKEN);
    }

    private static HeldLocksToken getHeldLocksToken(BigInteger tokenId) {
        long creationDateMs = System.currentTimeMillis();
        long expirationDateMs = creationDateMs - 1;
        TimeDuration lockTimeout = SimpleTimeDuration.of(0, TimeUnit.SECONDS);
        long versionId = 0L;
        return new HeldLocksToken(
                tokenId,
                LockClient.of("fake lock client"),
                creationDateMs,
                expirationDateMs,
                LOCK_DESCRIPTORS,
                lockTimeout,
                versionId,
                "Dummy thread");
    }
}
