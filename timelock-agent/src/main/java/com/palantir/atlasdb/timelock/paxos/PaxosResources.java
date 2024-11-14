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

package com.palantir.atlasdb.timelock.paxos;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.paxos.api.NamespaceLeadershipTakeoverServiceEndpoints;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public abstract class PaxosResources {

    public abstract NetworkClientFactories.Factory<ManagedTimestampService> timestampServiceFactory();

    abstract LocalPaxosComponents timestampPaxosComponents();

    abstract Map<PaxosUseCase, LocalPaxosComponents> leadershipBatchComponents();

    public abstract LeadershipContextFactory leadershipContextFactory();

    abstract List<Object> adhocResources();

    abstract List<UndertowService> adhocUndertowServices();

    public abstract TimeLockCorruptionComponents timeLockCorruptionComponents();

    @Value.Derived
    NamespaceTakeoverComponent namespaceTakeoverComponent() {
        return new NamespaceTakeoverComponent(leadershipComponents());
    }

    @Value.Derived
    public List<UndertowService> undertowServices() {
        return ImmutableList.<UndertowService>builder()
                .addAll(adhocUndertowServices())
                .add(NamespaceLeadershipTakeoverServiceEndpoints.of(
                        new NamespaceTakeoverService(namespaceTakeoverComponent())))
                .build();
    }

    @Value.Derived
    public List<Object> resourcesForRegistration() {
        return ImmutableList.builder()
                .addAll(adhocResources())
                .add(new NamespaceTakeoverResource(namespaceTakeoverComponent()))
                .build();
    }

    @Value.Derived
    public LeadershipComponents leadershipComponents() {
        return new LeadershipComponents(
                leadershipContextFactory(), leadershipContextFactory().healthCheckPingers());
    }
}
