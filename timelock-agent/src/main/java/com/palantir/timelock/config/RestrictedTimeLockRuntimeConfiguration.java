/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import java.util.Optional;

public class RestrictedTimeLockRuntimeConfiguration extends TimeLockRuntimeConfiguration {
    private final TimeLockRuntimeConfiguration runtime;

    public RestrictedTimeLockRuntimeConfiguration(TimeLockRuntimeConfiguration runtime) {
        this.runtime = runtime;
    }

    @Override
    public PaxosRuntimeConfiguration paxos() {
        return runtime.paxos();
    }

    @Override
    public ClusterConfiguration clusterSnapshot() {
        throw new UnsupportedOperationException("Cannot access live-reloaded cluster configuration!");
    }

    @Override
    public Integer maxNumberOfClients() {
        return runtime.maxNumberOfClients();
    }

    @Override
    public long slowLockLogTriggerMillis() {
        return runtime.slowLockLogTriggerMillis();
    }

    @Override
    public TimeLockAdjudicationConfiguration adjudication() {
        return runtime.adjudication();
    }

    @Override
    public Optional<TsBoundPersisterRuntimeConfiguration> timestampBoundPersistence() {
        return runtime.timestampBoundPersistence();
    }
}
