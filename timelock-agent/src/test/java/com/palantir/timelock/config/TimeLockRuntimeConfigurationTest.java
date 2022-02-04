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
package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.tokens.auth.BearerToken;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("CheckReturnValue")
public class TimeLockRuntimeConfigurationTest {
    private static final String SERVER_A = "horses-for-courses:1234";
    private static final String SERVER_B = "paddock-and-chips:2345";
    private static final ClusterConfiguration CLUSTER_CONFIG = ImmutableDefaultClusterConfiguration.builder()
            .localServer(SERVER_A)
            .cluster(PartialServiceConfiguration.of(
                    ImmutableList.of(SERVER_A, SERVER_B, "the-mane-event:3456"), Optional.empty()))
            .addKnownNewServers(SERVER_B)
            .build();
    private static final BearerToken BEARER_TOKEN = BearerToken.valueOf("quit-horsing-around");

    @Test
    public void canCreateWithZeroClients() {
        ImmutableTimeLockRuntimeConfiguration.builder()
                .permittedBackupToken(BEARER_TOKEN)
                .clusterSnapshot(CLUSTER_CONFIG)
                .build();
    }

    @Test
    public void canSpecifyPositiveLockLoggerTimeout() {
        ImmutableTimeLockRuntimeConfiguration.builder()
                .permittedBackupToken(BEARER_TOKEN)
                .clusterSnapshot(CLUSTER_CONFIG)
                .slowLockLogTriggerMillis(1L)
                .build();
    }

    @Test
    public void throwOnNegativeLeaderPingResponseWait() {
        assertThatThrownBy(() -> ImmutableTimeLockRuntimeConfiguration.builder()
                        .permittedBackupToken(BEARER_TOKEN)
                        .clusterSnapshot(CLUSTER_CONFIG)
                        .slowLockLogTriggerMillis(-1L)
                        .build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void newNodeInExistingServiceRecognisedAsNew() {
        assertThat(ImmutableTimeLockRuntimeConfiguration.builder()
                        .permittedBackupToken(BEARER_TOKEN)
                        .clusterSnapshot(ImmutableDefaultClusterConfiguration.builder()
                                .localServer(SERVER_A)
                                .cluster(PartialServiceConfiguration.of(
                                        ImmutableList.of(SERVER_A, SERVER_B, "hoof-moved-my-cheese:4567"),
                                        Optional.empty()))
                                .addKnownNewServers(SERVER_A)
                                .build())
                        .build()
                        .clusterSnapshot()
                        .isNewServiceNode())
                .isTrue();
    }
}
