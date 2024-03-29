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
package com.palantir.timelock.paxos;

import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories;
import com.palantir.paxos.Client;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.function.Supplier;

public class PaxosTimestampCreator implements TimestampCreator {
    private final NetworkClientFactories.Factory<ManagedTimestampService> timestampServiceFactory;

    PaxosTimestampCreator(NetworkClientFactories.Factory<ManagedTimestampService> timestampServiceFactory) {
        this.timestampServiceFactory = timestampServiceFactory;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(Client client) {
        return () -> timestampServiceFactory.create(client);
    }

    @Override
    public void close() {
        // no op
    }
}
