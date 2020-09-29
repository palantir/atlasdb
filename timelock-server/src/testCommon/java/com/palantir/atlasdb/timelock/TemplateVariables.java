/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
@Value.Style(attributeBuilderDetection = true)
public interface TemplateVariables {
    int PROXY_OFFSET = 100;

    @Nullable
    String getDataDirectory();
    @Nullable
    String getSqliteDataDirectory();
    List<Integer> getServerPorts();
    Integer getLocalServerPort();
    TimestampPaxos getClientPaxos();
    PaxosLeaderMode getLeaderMode();

    @Value.Default
    default Integer getLocalProxyPort() {
        return doProxyTransform(getLocalServerPort());
    }

    @Value.Derived
    default List<Integer> getServerProxyPorts() {
        return getServerPorts().stream().map(TemplateVariables::doProxyTransform).collect(Collectors.toList());
    }

    static int doProxyTransform(int port) {
        return port + PROXY_OFFSET;
    }

    @Value.Check
    default TemplateVariables putLocalServerPortAmongstAllServerPorts() {
        // It is guaranteed that the transform ensures that after this runs once, the local proxy port will be present
        // in the server proxy ports.
        if (getServerProxyPorts().contains(getLocalProxyPort())) {
            return this;
        }

        return ImmutableTemplateVariables.builder().from(this)
                .addServerPorts(getLocalServerPort())
                .build();
    }

    @Value.Immutable
    interface TimestampPaxos {
        boolean isUseBatchPaxosTimestamp();
        @Value.Default
        default boolean isBatchSingleLeader() {
            return false;
        }
    }

    static Iterable<TemplateVariables> generateThreeNodeTimelockCluster(
            int startingPort,
            UnaryOperator<ImmutableTemplateVariables.Builder> customizer) {
        List<Integer> allPorts = IntStream.range(startingPort, startingPort + 3).boxed().collect(Collectors.toList());
        return IntStream.range(startingPort, startingPort + 3)
                .boxed()
                .map(port -> ImmutableTemplateVariables.builder()
                        .serverPorts(allPorts)
                        .localServerPort(port))
                .map(customizer)
                .map(ImmutableTemplateVariables.Builder::build)
                .collect(Collectors.toList());
    }
}
