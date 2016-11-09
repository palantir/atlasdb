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
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.IntStream;

import javax.annotation.Nullable;
import javax.net.ssl.SSLSocketFactory;

import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
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
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

import io.atomix.AtomixReplica;
import io.atomix.variables.DistributedValue;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class TimeLockServerTest {
    private static final String NAMESPACE_FORMAT = "http://localhost:%d/%s";
    private static final String INTERNAL_SERVER_ERROR_CODE = "500";
    private static final String SERVICE_NOT_AVAILABLE_CODE = "503";

    private static final String TEST_NAMESPACE_1 = "test";
    private static final String TEST_NAMESPACE_2 = "test2";
    private static final String NONEXISTENT_NAMESPACE = "not-a-service";

    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
    private static final SortedMap<LockDescriptor, LockMode> LOCK_MAP = ImmutableSortedMap.of(
            StringLockDescriptor.of("lock1"), LockMode.WRITE);

    @ClassRule
    public static final DropwizardAppRule<TimeLockServerConfiguration> APP = new DropwizardAppRule<>(
            TimeLockServer.class,
            ResourceHelpers.resourceFilePath("testServer.yml"));

    @Test
    public void lockServiceShouldBeInvalidatedOnNewLeader() throws InterruptedException {
        RemoteLockService lockService = AtlasDbHttpClients.createProxy(
                NO_SSL,
                getPathForNamespace(TEST_NAMESPACE_1),
                RemoteLockService.class);

        LockRefreshToken token = lockService.lock("test", LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token).isNotNull();

        String serverLeaderId = getLeaderId();
        setLeaderId(null);

        assertThatThrownBy(lockService::currentTimeMillis).hasMessageContaining(SERVICE_NOT_AVAILABLE_CODE);

        setLeaderId(serverLeaderId);
        Set<LockRefreshToken> refreshedLocks = lockService.refreshLockRefreshTokens(Collections.singleton(token));

        assertThat(refreshedLocks).isEmpty();
    }

    @Test
    public void timestampServiceShouldIssueTimestampRanges() {
        TimestampService timestampService = getTimestampService(TEST_NAMESPACE_1);

        int numTimestamps = 1000;
        TimestampRange range = timestampService.getFreshTimestamps(numTimestamps);
        assertThat(range.getLowerBound() + numTimestamps - 1).isEqualTo(range.getUpperBound());
    }

    @Test
    public void timestampServiceShouldRespectDistinctNamespacesWhenIssuingTimestamps() {
        TimestampService timestampService1 = getTimestampService(TEST_NAMESPACE_1);
        TimestampService timestampService2 = getTimestampService(TEST_NAMESPACE_2);

        long firstServiceFirstTimestamp = timestampService1.getFreshTimestamp();
        long secondServiceFirstTimestamp = timestampService2.getFreshTimestamp();
        long firstServiceSecondTimestamp = timestampService1.getFreshTimestamp();
        long secondServiceSecondTimestamp = timestampService2.getFreshTimestamp();

        assertThat(firstServiceFirstTimestamp + 1).isEqualTo(firstServiceSecondTimestamp);
        assertThat(secondServiceFirstTimestamp + 1).isEqualTo(secondServiceSecondTimestamp);
    }

    @Test
    public void timestampServiceShouldThrowIfQueryingNonexistentNamespace() {
        TimestampService nonexistent = getTimestampService(NONEXISTENT_NAMESPACE);
        assertThatThrownBy(nonexistent::getFreshTimestamp).hasMessageContaining(INTERNAL_SERVER_ERROR_CODE);
    }

    @Test
    public void timestampServiceShouldNotIssueTimestampsIfNotLeader() {
        String leader = getLeaderId();
        TimestampService timestampService = getTimestampService(TEST_NAMESPACE_1);
        try {
            setLeaderId(null);
            assertThatThrownBy(timestampService::getFreshTimestamp).hasMessageContaining(SERVICE_NOT_AVAILABLE_CODE);
        } finally {
            setLeaderId(leader);
        }
    }

    @Test
    public void timestampServiceShouldIssueTimestampsAgainAfterRegainingLeadership() {
        String leader = getLeaderId();
        TimestampService timestampService = getTimestampService(TEST_NAMESPACE_1);
        List<Long> timestampList = Lists.newArrayList();
        try {
            int numIterations = 10;
            for (int i = 0; i < numIterations; i++) {
                timestampList.add(timestampService.getFreshTimestamp());
                setLeaderId(null);
                assertThatThrownBy(timestampService::getFreshTimestamp)
                        .hasMessageContaining(SERVICE_NOT_AVAILABLE_CODE);
                setLeaderId(leader);
            }
        } finally {
            setLeaderId(leader);
        }
        assertIncreasing(timestampList);
    }

    private void assertIncreasing(List<Long> timestampList) {
        IntStream.range(1, timestampList.size())
                .forEach(index -> assertThat(timestampList.get(index - 1)).isLessThan(timestampList.get(index)));
    }

    @Nullable
    private String getLeaderId() {
        AtomixReplica localNode = APP.<TimeLockServer>getApplication().getLocalNode();
        DistributedValue<String> currentLeaderId = DistributedValues.getLeaderId(localNode);
        return Futures.getUnchecked(currentLeaderId.get());
    }

    private void setLeaderId(@Nullable String leaderId) {
        AtomixReplica localNode = APP.<TimeLockServer>getApplication().getLocalNode();
        DistributedValue<String> currentLeaderId = DistributedValues.getLeaderId(localNode);
        Futures.getUnchecked(currentLeaderId.set(leaderId));
    }

    private TimestampService getTimestampService(String namespace) {
        return AtlasDbHttpClients.createProxy(
                NO_SSL,
                getPathForNamespace(namespace),
                TimestampService.class);
    }

    private String getPathForNamespace(String namespace) {
        return String.format(NAMESPACE_FORMAT, APP.getLocalPort(), namespace);
    }
}
