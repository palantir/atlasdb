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

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;

@Value.Immutable
@Value.Enclosing
@Value.Style(attributeBuilderDetection = true)
public interface TemplateVariables {
    @Nullable
    String getDataDirectory();
    List<Integer> getServerPorts();
    Integer getLocalServerPort();
    TimestampPaxos getClientPaxos();
    PaxosLeaderMode getLeaderMode();

    @Value.Check
    default TemplateVariables putLocalServerPortAmongstAllServerPorts() {
        if (getServerPorts().contains(getLocalServerPort())) {
            return this;
        }

        return ImmutableTemplateVariables.builder().from(this)
                .addServerPorts(getLocalServerPort())
                .build();
    }

    @Value.Immutable
    interface TimestampPaxos {
        boolean isUseBatchPaxos();
    }

    static Iterable<TemplateVariables> generateThreeNodeTimelockCluster(
            int startingPort,
            UnaryOperator<ImmutableTemplateVariables.Builder> customizer) {
        List<Integer> allPorts = IntStream.range(startingPort, startingPort + 3).boxed().collect(Collectors.toList());
        return IntStream.range(startingPort, startingPort + 3)
                .boxed()
                .map(port -> ImmutableTemplateVariables.builder()
                        .serverPorts(allPorts.stream().map(p -> p.equals(port) ? port : p * 2).collect(Collectors.toList()))
                        .localServerPort(port))
                .map(customizer)
                .map(ImmutableTemplateVariables.Builder::build)
                .collect(Collectors.toList());
    }
}
