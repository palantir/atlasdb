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
package com.palantir.atlasdb.timelock;

import static com.palantir.atlasdb.timelock.AbstractAsyncTimelockServiceIntegrationTest.DEFAULT_SINGLE_SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampManagementService;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class PaxosTimeLockServerIntegrationTest {

    private static final SafeLogger log = SafeLoggerFactory.get(PaxosTimeLockServerIntegrationTest.class);

    private static final String CLIENT_1 = "test";
    private static final String CLIENT_2 = "test2";
    private static final String CLIENT_3 = "test3";
    private static final String LEARNER = "learner";
    private static final String ACCEPTOR = "acceptor";
    private static final ImmutableList<String> NAMESPACES =
            ImmutableList.of(CLIENT_1, CLIENT_2, CLIENT_3, LEARNER, ACCEPTOR);
    private static final String INVALID_CLIENT = "test2\b";

    private static final long ONE_MILLION = 1000000;
    private static final long TWO_MILLION = 2000000;
    private static final int FORTY_TWO = 42;

    private static final String LOCK_CLIENT_NAME = "remoteLock-client-name";
    private static final LockDescriptor LOCK_1 = StringLockDescriptor.of("lock1");
    private static final ImmutableSortedMap<LockDescriptor, LockMode> LOCK_MAP =
            ImmutableSortedMap.of(LOCK_1, LockMode.WRITE);

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private static final TemporaryConfigurationHolder TEMPORARY_CONFIG_HOLDER =
            new TemporaryConfigurationHolder(TEMPORARY_FOLDER, "paxosSingleServer.ftl", DEFAULT_SINGLE_SERVER);
    private static final TimeLockServerHolder TIMELOCK_SERVER_HOLDER =
            new TimeLockServerHolder(TEMPORARY_CONFIG_HOLDER::getTemporaryConfigFileLocation, DEFAULT_SINGLE_SERVER);
    private static final TestableTimelockServer TIMELOCK =
            new TestableTimelockServer("https://localhost", TIMELOCK_SERVER_HOLDER);

    private static NamespacedClients namespace1;
    private static NamespacedClients namespace2;

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(TEMPORARY_FOLDER)
            .around(TEMPORARY_CONFIG_HOLDER)
            .around(TIMELOCK_SERVER_HOLDER);

    @BeforeClass
    public static void waitForClusterToStabilize() {
        namespace1 = TIMELOCK.client(CLIENT_1);
        namespace2 = TIMELOCK.client(CLIENT_2);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        // Returns true only if this node is ready to serve timestamps and locks on all clients.
                        NAMESPACES.forEach(client -> TIMELOCK.client(client).getFreshTimestamp());
                        NAMESPACES.forEach(client ->
                                TIMELOCK.client(client).timelockService().currentTimeMillis());
                        NAMESPACES.forEach(client ->
                                TIMELOCK.client(client).legacyLockService().currentTimeMillis());
                        return TIMELOCK.pinger().ping(NAMESPACES).containsAll(NAMESPACES);
                    } catch (Throwable t) {
                        log.error("erreur!", t);
                        return false;
                    }
                });
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutLocks() throws InterruptedException {
        LockService lockService = namespace1.legacyLockService();

        LockRefreshToken token = lockService.lock(
                LOCK_CLIENT_NAME,
                com.palantir.lock.LockRequest.builder(LOCK_MAP).doNotBlock().build());

        assertThat(token).isNotNull();

        lockService.unlock(token);
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutSameLockInDifferentNamespaces() throws InterruptedException {
        LockService lockService1 = namespace1.legacyLockService();
        LockService lockService2 = namespace2.legacyLockService();

        LockRefreshToken token1 = lockService1.lock(
                LOCK_CLIENT_NAME,
                com.palantir.lock.LockRequest.builder(LOCK_MAP).doNotBlock().build());
        LockRefreshToken token2 = lockService2.lock(
                LOCK_CLIENT_NAME,
                com.palantir.lock.LockRequest.builder(LOCK_MAP).doNotBlock().build());

        assertThat(token1).isNotNull();
        assertThat(token2).isNotNull();

        lockService1.unlock(token1);
        lockService2.unlock(token2);
    }

    @Test
    public void lockServiceShouldNotAllowUsToRefreshLocksFromDifferentNamespaces() throws InterruptedException {
        LockService lockService1 = namespace1.legacyLockService();
        LockService lockService2 = namespace2.legacyLockService();

        com.palantir.lock.LockRequest request =
                com.palantir.lock.LockRequest.builder(LOCK_MAP).doNotBlock().build();

        LockRefreshToken token = lockService1.lock(LOCK_CLIENT_NAME, request);

        assertThat(token).isNotNull();
        assertThat(lockService1.refreshLockRefreshTokens(ImmutableList.of(token)))
                .isNotEmpty();
        assertThat(lockService2.refreshLockRefreshTokens(ImmutableList.of(token)))
                .isEmpty();

        lockService1.unlock(token);
    }

    @Test
    public void asyncLockServiceShouldAllowUsToTakeOutLocks() {
        LockToken token = namespace1.lock(newLockV2Request(LOCK_1)).getToken();
        assertThat(namespace1.unlock(token)).isTrue();
    }

    @Test
    public void asyncLockServiceShouldAllowUsToTakeOutSameLockInDifferentNamespaces() {
        LockToken token1 = namespace1.lock(newLockV2Request(LOCK_1)).getToken();
        LockToken token2 = namespace2.lock(newLockV2Request(LOCK_1)).getToken();

        namespace1.unlock(token1);
        namespace2.unlock(token2);
    }

    @Test
    public void asyncLockServiceShouldNotAllowUsToRefreshLocksFromDifferentNamespaces() {
        LockToken token = namespace1.lock(newLockV2Request(LOCK_1)).getToken();

        assertThat(namespace1.refreshLockLease(token)).isTrue();
        assertThat(namespace2.refreshLockLease(token)).isFalse();

        namespace1.unlock(token);
    }

    @Test
    public void timestampServiceShouldGiveUsIncrementalTimestamps() {
        long timestamp1 = namespace1.getFreshTimestamp();
        long timestamp2 = namespace1.getFreshTimestamp();

        assertThat(timestamp1).isLessThan(timestamp2);
    }

    @Test
    public void timestampServiceShouldRespectDistinctClientsWhenIssuingTimestamps() {
        long firstServiceFirstTimestamp = namespace1.getFreshTimestamp();
        long secondServiceFirstTimestamp = namespace2.getFreshTimestamp();

        getFortyTwoFreshTimestamps(namespace1.timelockService());

        long firstServiceSecondTimestamp = namespace1.getFreshTimestamp();
        long secondServiceSecondTimestamp = namespace2.getFreshTimestamp();

        assertThat(firstServiceSecondTimestamp - firstServiceFirstTimestamp).isGreaterThanOrEqualTo(FORTY_TWO);
        assertThat(secondServiceSecondTimestamp - secondServiceFirstTimestamp).isBetween(0L, (long) FORTY_TWO);
    }

    @Test
    public void timestampServiceRespectsTimestampManagementService() {
        long currentTimestampIncrementedByOneMillion = namespace1.getFreshTimestamp() + ONE_MILLION;
        namespace1.timestampManagementService().fastForwardTimestamp(currentTimestampIncrementedByOneMillion);
        assertThat(namespace1.getFreshTimestamp()).isGreaterThan(currentTimestampIncrementedByOneMillion);
    }

    @Test
    public void timestampManagementServiceRespectsTimestampService() {
        long currentTimestampIncrementedByOneMillion = namespace1.getFreshTimestamp() + ONE_MILLION;
        namespace1.timestampManagementService().fastForwardTimestamp(currentTimestampIncrementedByOneMillion);
        getFortyTwoFreshTimestamps(namespace1.timelockService());
        namespace1.timestampManagementService().fastForwardTimestamp(currentTimestampIncrementedByOneMillion + 1);
        assertThat(namespace1.getFreshTimestamp()).isGreaterThan(currentTimestampIncrementedByOneMillion + FORTY_TWO);
    }

    @Test
    public void lockServiceShouldDisallowGettingMinLockedInVersionId() {
        LockService lockService = namespace1.legacyLockService();

        // Catching any exception since this currently is an error deserialization exception
        // until we stop requiring http-remoting2 errors
        assertThatThrownBy(() -> lockService.getMinLockedInVersionId(CLIENT_1)).isInstanceOf(Exception.class);
    }

    private static void getFortyTwoFreshTimestamps(TimelockService timelockService) {
        for (int i = 0; i < FORTY_TWO; i++) {
            timelockService.getFreshTimestamp();
        }
    }

    @Test
    public void fastForwardRespectsDistinctClients() {
        TimestampManagementService anotherClientTimestampManagementService = namespace2.timestampManagementService();

        long currentTimestamp = namespace1.getFreshTimestamp();
        anotherClientTimestampManagementService.fastForwardTimestamp(currentTimestamp + ONE_MILLION);
        assertThat(namespace1.getFreshTimestamp()).isBetween(currentTimestamp + 1, currentTimestamp + ONE_MILLION - 1);
    }

    @Test
    public void fastForwardToThePastDoesNothing() {
        long currentTimestamp = namespace1.getFreshTimestamp();
        long currentTimestampIncrementedByOneMillion = currentTimestamp + ONE_MILLION;
        long currentTimestampIncrementedByTwoMillion = currentTimestamp + TWO_MILLION;

        namespace1.timestampManagementService().fastForwardTimestamp(currentTimestampIncrementedByTwoMillion);
        namespace1.timestampManagementService().fastForwardTimestamp(currentTimestampIncrementedByOneMillion);
        assertThat(namespace1.getFreshTimestamp()).isGreaterThan(currentTimestampIncrementedByTwoMillion);
    }

    @Test
    public void throwsOnQueryingTimestampWithInvalidClientName() {
        TimelockService invalidTimelockService = TIMELOCK.client(INVALID_CLIENT).timelockService();
        assertThatThrownBy(invalidTimelockService::getFreshTimestamp).hasMessageContaining("INVALID_ARGUMENT");
    }

    @Test
    public void supportsClientNamesMatchingPaxosRoles() {
        TIMELOCK.client(LEARNER).getFreshTimestamp();
        TIMELOCK.client(ACCEPTOR).getFreshTimestamp();
    }

    private static LockRequest newLockV2Request(LockDescriptor lock) {
        return LockRequest.of(ImmutableSet.of(lock), 10_000L);
    }
}
