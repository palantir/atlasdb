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

package com.palantir.atlasdb.containers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.rules.ExternalResource;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.qos.FakeQosClient;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.Throwables;

public class UninitializedCassandraResource extends ExternalResource {
    private final CassandraContainer containerInstance = CassandraContainer.throwawayContainer();
    private final Containers containers;
    private final KeyValueService kvs = CassandraKeyValueServiceImpl.create(
            MetricsManagers.createForTests(),
            containerInstance.getConfig(),
            CassandraKeyValueServiceRuntimeConfig::getDefault,
            CassandraContainer.LEADER_CONFIG,
            CassandraMutationTimestampProviders.legacyModeForTestsOnly(),
            true,
            FakeQosClient.INSTANCE);
    private AtomicBoolean initialized = new AtomicBoolean(false);

    public UninitializedCassandraResource(Class<?> classToSaveLogsFor) {
        containers = new Containers(classToSaveLogsFor).with(containerInstance);
    }

    public void initialize() {
        Preconditions.checkState(initialized.compareAndSet(false, true), "Cassandra was already initialized");
        try {
            containers.before();
        } catch (Throwable th) {
            throw Throwables.rewrapAndThrowUncheckedException(th);
        }
    }

    @Override
    public void after() {
        if (!initialized.get()) {
            return;
        }
        try {
            containers.getContainer(containerInstance.getServiceName()).kill();
        } catch (IOException | InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public KeyValueService getAsyncInitializeableKvs() {
        return kvs;
    }
}
