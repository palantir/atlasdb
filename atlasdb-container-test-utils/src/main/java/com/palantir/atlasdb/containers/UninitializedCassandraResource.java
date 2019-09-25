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
import java.net.Proxy;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Preconditions;

public class UninitializedCassandraResource extends ExternalResource {
    private final CassandraContainer containerInstance = CassandraContainer.throwawayContainer();
    private final Containers containers;

    private final Supplier<KeyValueService> kvs = Suppliers.memoize(this::createKvs);

    private AtomicBoolean initialized = new AtomicBoolean(false);

    private Proxy socksProxy;

    public UninitializedCassandraResource(Class<?> classToSaveLogsFor) {
        containers = new Containers(classToSaveLogsFor).with(containerInstance);
    }

    public void initialize() {
        Preconditions.checkState(initialized.compareAndSet(false, true), "Cassandra was already initialized");
        try {
            containers.getContainer(containerInstance.getServiceName()).up();
        } catch (Throwable th) {
            throw Throwables.rewrapAndThrowUncheckedException(th);
        }
    }

    @Override
    protected void before() throws Throwable {
        containers.before();
        try {
            socksProxy = containerInstance.getSocksProxy();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        containers.getContainer(containerInstance.getServiceName()).kill();
        containers.getDockerCompose().rm();
    }

    @Override
    public void after() {
        if (!initialized.get()) {
            return;
        }
        try {
            kvs.get().close();
            containers.getContainer(containerInstance.getServiceName()).kill();
        } catch (IOException | InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public KeyValueService getAsyncInitializeableKvs() {
        return kvs.get();
    }

    private KeyValueService createKvs() {
        Preconditions.checkNotNull(socksProxy, "There has to be a defined proxy");
        CassandraKeyValueServiceConfig config = containerInstance.getConfigWithProxy(socksProxy.address());

        return CassandraKeyValueServiceImpl.create(
                MetricsManagers.createForTests(),
                config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                CassandraMutationTimestampProviders.legacyModeForTestsOnly(),
                true);
    }
}
