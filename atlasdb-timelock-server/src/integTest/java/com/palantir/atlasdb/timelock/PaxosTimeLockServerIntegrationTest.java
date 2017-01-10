/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.SortedMap;

import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.timestamp.TimestampService;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class PaxosTimeLockServerIntegrationTest {
    private static final String NOT_FOUND_CODE = "404";

    private static final String CLIENT_1 = "test";
    private static final String CLIENT_2 = "test2";
    private static final String NONEXISTENT_CLIENT = "nonexistent";
    private static final String INVALID_CLIENT = "test2\b";

    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
    private static final String LOCK_CLIENT_NAME = "lock-client-name";
    private static final SortedMap<LockDescriptor, LockMode> LOCK_MAP = ImmutableSortedMap.of(
            StringLockDescriptor.of("lock1"), LockMode.WRITE);

    @ClassRule
    public static final DropwizardAppRule<TimeLockServerConfiguration> APP = new DropwizardAppRule<>(
            TimeLockServer.class,
            ResourceHelpers.resourceFilePath("paxosSingleServer.yml"));

    @AfterClass
    public static void tearDownClass() throws Exception {
        FileUtils.deleteDirectory(new File("var/test/data"));
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutLocks() throws InterruptedException {
        RemoteLockService lockService = getLockService(CLIENT_1);

        LockRefreshToken token = lockService.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token).isNotNull();

        lockService.unlock(token);
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutSameLockInDifferentNamespaces() throws InterruptedException {
        RemoteLockService lockService1 = getLockService(CLIENT_1);
        RemoteLockService lockService2 = getLockService(CLIENT_2);

        LockRefreshToken token1 = lockService1.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());
        LockRefreshToken token2 = lockService2.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token1).isNotNull();
        assertThat(token2).isNotNull();

        lockService1.unlock(token1);
        lockService2.unlock(token2);
    }

    @Test
    public void lockServiceShouldNotAllowUsToRefreshLocksFromDifferentNamespaces() throws InterruptedException {
        RemoteLockService lockService1 = getLockService(CLIENT_1);
        RemoteLockService lockService2 = getLockService(CLIENT_2);

        LockRefreshToken token1 = lockService1.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token1).isNotNull();
        assertThat(lockService1.refreshLockRefreshTokens(ImmutableList.of(token1))).isNotEmpty();
        assertThat(lockService2.refreshLockRefreshTokens(ImmutableList.of(token1))).isEmpty();

        lockService1.unlock(token1);
    }

    @Test
    public void timestampServiceShouldGiveUsIncrementalTimestamps() {
        TimestampService timestampService = getTimestampService(CLIENT_1);

        long timestamp1 = timestampService.getFreshTimestamp();
        long timestamp2 = timestampService.getFreshTimestamp();

        assertThat(timestamp1).isLessThan(timestamp2);
    }

    @Test
    public void timestampServiceShouldRespectDistinctClientsWhenIssuingTimestamps() {
        TimestampService timestampService1 = getTimestampService(CLIENT_1);
        TimestampService timestampService2 = getTimestampService(CLIENT_2);

        long firstServiceFirstTimestamp = timestampService1.getFreshTimestamp();
        long secondServiceFirstTimestamp = timestampService2.getFreshTimestamp();

        long firstServiceSecondTimestamp = timestampService1.getFreshTimestamp();
        long secondServiceSecondTimestamp = timestampService2.getFreshTimestamp();

        assertThat(firstServiceFirstTimestamp + 1).isEqualTo(firstServiceSecondTimestamp);
        assertThat(secondServiceFirstTimestamp + 1).isEqualTo(secondServiceSecondTimestamp);
    }

    @Test
    public void returnsNotFoundOnQueryingNonexistentClient() {
        RemoteLockService lockService = getLockService(NONEXISTENT_CLIENT);
        assertThatThrownBy(lockService::currentTimeMillis)
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    @Test
    public void returnsNotFoundOnQueryingClientWithInvalidName() {
        TimestampService timestampService = getTimestampService(INVALID_CLIENT);
        assertThatThrownBy(timestampService::getFreshTimestamp)
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    private static RemoteLockService getLockService(String client) {
        return AtlasDbHttpClients.createProxy(
                NO_SSL,
                String.format("http://localhost:%d/%s", APP.getLocalPort(), client),
                RemoteLockService.class);
    }

    private static TimestampService getTimestampService(String client) {
        return AtlasDbHttpClients.createProxy(
                NO_SSL,
                String.format("http://localhost:%d/%s", APP.getLocalPort(), client),
                TimestampService.class);
    }
}
