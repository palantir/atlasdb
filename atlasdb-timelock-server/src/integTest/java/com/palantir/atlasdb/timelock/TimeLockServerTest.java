/**
 * Copyright 2016 Palantir Technologies
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

import java.util.Collections;
import java.util.Set;
import java.util.SortedMap;

import javax.annotation.Nullable;
import javax.net.ssl.SSLSocketFactory;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.atomix.DistributedValues;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.timestamp.TimestampAdministrationService;
import com.palantir.timestamp.TimestampService;

import io.atomix.Atomix;
import io.atomix.AtomixClient;
import io.atomix.catalyst.transport.Address;
import io.atomix.variables.DistributedValue;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class TimeLockServerTest {
    private static final String NOT_FOUND_CODE = "404";
    private static final String INTERNAL_SERVER_ERROR_CODE = "500";
    private static final String SERVICE_NOT_AVAILABLE_CODE = "503";

    private static final String CLIENT_1 = "test";
    private static final String CLIENT_2 = "test2";
    private static final String NONEXISTENT_CLIENT = "nonexistent-client";
    private static final String INVALID_CLIENT = "invalid-client-with-symbol-$";

    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
    private static final String LOCK_CLIENT_NAME = "lock-client-name";
    private static final SortedMap<LockDescriptor, LockMode> LOCK_MAP = ImmutableSortedMap.of(
            StringLockDescriptor.of("lock1"), LockMode.WRITE);

    private static Atomix atomixClient;

    @ClassRule
    public static final DropwizardAppRule<TimeLockServerConfiguration> APP = new DropwizardAppRule<>(
            TimeLockServer.class,
            ResourceHelpers.resourceFilePath("singleTestServer.yml"));

    @BeforeClass
    public static void setupAtomixClient() {
        atomixClient = AtomixClient.builder()
                .build()
                .connect(new Address("localhost", 8700))
                .join();
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
    public void lockServiceShouldBeInvalidatedOnNewLeader() throws InterruptedException {
        RemoteLockService lockService = getLockService(CLIENT_1);

        LockRefreshToken token = lockService.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token).isNotNull();

        String serverLeaderId = getLeaderId();
        try {
            setLeaderId(null);
            assertThatThrownBy(lockService::currentTimeMillis).hasMessageContaining(SERVICE_NOT_AVAILABLE_CODE);

            setLeaderId(serverLeaderId);
            Set<LockRefreshToken> refreshedLocks = lockService.refreshLockRefreshTokens(Collections.singleton(token));
            assertThat(refreshedLocks).isEmpty();
        } finally {
            setLeaderId(serverLeaderId);
        }
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
    public void timestampServiceShouldThrowIfQueryingNonexistentClient() {
        TimestampService nonexistent = getTimestampService(NONEXISTENT_CLIENT);
        assertThatThrownBy(nonexistent::getFreshTimestamp)
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    @Test
    public void timestampServiceShouldThrowIfQueryingInvalidClient() {
        TimestampService nonexistent = getTimestampService(INVALID_CLIENT);
        assertThatThrownBy(nonexistent::getFreshTimestamp)
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    @Test
    public void timestampServiceShouldNotIssueTimestampsIfNotLeader() {
        String leader = getLeaderId();
        TimestampService timestampService = getTimestampService(CLIENT_1);
        try {
            setLeaderId(null);
            assertThatThrownBy(timestampService::getFreshTimestamp)
                    .hasMessageContaining(SERVICE_NOT_AVAILABLE_CODE);
        } finally {
            setLeaderId(leader);
        }
    }

    @Test
    public void timestampServiceShouldIssueTimestampsAgainAfterRegainingLeadership() {
        String leader = getLeaderId();
        TimestampService timestampService = getTimestampService(CLIENT_1);
        try {
            long ts1 = timestampService.getFreshTimestamp();

            setLeaderId(null);
            assertThatThrownBy(timestampService::getFreshTimestamp)
                    .hasMessageContaining(SERVICE_NOT_AVAILABLE_CODE);

            setLeaderId(leader);
            long ts2 = timestampService.getFreshTimestamp();

            assertThat(ts1).isLessThan(ts2);
        } finally {
            setLeaderId(leader);
        }
    }

    @Test
    public void timestampAdministrationServiceShouldThrowIfQueryingNonexistentClient() {
        TimestampAdministrationService nonexistent = getTimestampAdministrationService(NONEXISTENT_CLIENT);
        assertThatThrownBy(() -> nonexistent.fastForwardTimestamp(Long.MAX_VALUE))
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    @Test
    public void timestampAdministrationServiceShouldThrowIfQueryingInvalidClient() {
        TimestampAdministrationService invalid = getTimestampAdministrationService(NONEXISTENT_CLIENT);
        assertThatThrownBy(() -> invalid.fastForwardTimestamp(Long.MAX_VALUE))
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    @Test
    public void timestampAdministrationServiceInvalidationShouldBeNamespaced() {
        TimestampService service1 = getTimestampService(CLIENT_1);
        TimestampAdministrationService adminService1 = getTimestampAdministrationService(CLIENT_1);
        TimestampService service2 = getTimestampService(CLIENT_2);
        TimestampAdministrationService adminService2 = getTimestampAdministrationService(CLIENT_2);

        try {
            adminService1.invalidateTimestamps();
            assertThatThrownBy(service1::getFreshTimestamp).hasMessageContaining(INTERNAL_SERVER_ERROR_CODE);
            service2.getFreshTimestamp();
        } finally {
            adminService1.fastForwardTimestamp(0L);
        }
    }

    @Test
    public void timestampAdministrationServiceFastForwardShouldBeNamespaced() {
        TimestampService service1 = getTimestampService(CLIENT_1);
        TimestampService service2 = getTimestampService(CLIENT_2);
        TimestampAdministrationService adminService2 = getTimestampAdministrationService(CLIENT_2);

        long service2OldTimestamp = service2.getFreshTimestamp();
        try {
            adminService2.fastForwardTimestamp(Long.MAX_VALUE);
            long freshTimestamp = service1.getFreshTimestamp();
            assertThat(freshTimestamp).isGreaterThanOrEqualTo(0L);
        } finally {
            adminService2.invalidateTimestamps();
            adminService2.fastForwardTimestamp(service2OldTimestamp);
        }
    }

    @Test
    public void timestampAdministrationServiceStillUsableIfNotLeader() {
        String leader = getLeaderId();
        TimestampService service = getTimestampService(CLIENT_1);
        TimestampAdministrationService adminService = getTimestampAdministrationService(CLIENT_1);
        try {
            long oldTimestamp = service.getFreshTimestamp();
            long delta = 10000;

            setLeaderId(null);
            adminService.fastForwardTimestamp(oldTimestamp + 10000);

            setLeaderId(leader);
            long newTimestamp = service.getFreshTimestamp();
            assertThat(newTimestamp - oldTimestamp).isGreaterThan(delta);
        } finally {
            setLeaderId(leader);
        }
    }

    @Nullable
    private String getLeaderId() {
        DistributedValue<String> currentLeaderId = DistributedValues.getLeaderId(atomixClient);
        return Futures.getUnchecked(currentLeaderId.get());
    }

    private void setLeaderId(@Nullable String leaderId) {
        DistributedValue<String> currentLeaderId = DistributedValues.getLeaderId(atomixClient);
        Futures.getUnchecked(currentLeaderId.set(leaderId));
    }

    private static RemoteLockService getLockService(String client) {
        return createProxyForService(client, RemoteLockService.class);
    }

    private static TimestampService getTimestampService(String client) {
        return createProxyForService(client, TimestampService.class);
    }

    private static TimestampAdministrationService getTimestampAdministrationService(String client) {
        return createProxyForService(client, TimestampAdministrationService.class);
    }

    private static <T> T createProxyForService(String client, Class<T> clazz) {
        return AtlasDbHttpClients.createProxy(
                NO_SSL,
                String.format("http://localhost:%d/%s", APP.getLocalPort(), client),
                clazz);
    }
}
