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

import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;

import io.atomix.AtomixReplica;
import io.atomix.variables.DistributedValue;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class TimeLockServerTest {
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
                String.format("http://localhost:%d/test", APP.getLocalPort()),
                RemoteLockService.class);

        LockRefreshToken token = lockService.lock("test", LockRequest.builder(LOCK_MAP)
                        .doNotBlock()
                        .build());

        assertThat(token).isNotNull();

        String serverLeaderId = getLeaderId();
        setLeaderId(null);

        assertThatThrownBy(lockService::currentTimeMillis).hasMessageContaining("503");

        setLeaderId(serverLeaderId);
        Set<LockRefreshToken> refreshedLocks = lockService.refreshLockRefreshTokens(Collections.singleton(token));

        assertThat(refreshedLocks).isEmpty();
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
}
