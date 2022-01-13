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

package com.palantir.timelock.paxos;

import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class InMemoryTimeLockRule extends ExternalResource implements TimeLockServices {
    private final TemporaryFolder tempFolder = new TemporaryFolder();

    private final InMemoryTimelockServices services = new InMemoryTimelockServices(tempFolder);

    public InMemoryTimeLockRule() {}

    public InMemoryTimeLockRule(String client) {
        services.setClient(client);
    }

    @Rule
    public RuleChain chain = RuleChain.outerRule(tempFolder).around(services);

    public InMemoryTimelockServices get() {
        return services;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        Statement chainedStatement = chain.apply(base, description);
        return super.apply(chainedStatement, description);
    }

    @Override
    public TimestampService getTimestampService() {
        return services.getTimestampService();
    }

    @Override
    public LockService getLockService() {
        return services.getLockService();
    }

    @Override
    public AsyncTimelockResource getTimelockResource() {
        return services.getTimelockResource();
    }

    @Override
    public AsyncTimelockService getTimelockService() {
        return services.getTimelockService();
    }

    @Override
    public TimestampManagementService getTimestampManagementService() {
        return services.getTimestampManagementService();
    }

    public TimelockService getLegacyTimelockService() {
        return services.getLegacyTimelockService();
    }

    public ManagedTimestampService getManagedTimestampService() {
        return services.getManagedTimestampService();
    }

    public LockWatchManagerInternal getLockWatchManager() {
        return services.getLockWatchManager();
    }

    @Override
    public void close() {
        services.close();
    }
}
