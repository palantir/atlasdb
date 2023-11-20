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

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Preconditions;
import java.io.IOException;
import java.net.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class UninitializedCassandraResource implements BeforeAllCallback, AfterAllCallback {
    private final CassandraContainerV2 containerInstance = CassandraContainerV2.throwawayContainer();
    private final ContainersV2 containers;

    private KeyValueService kvs;

    private AtomicBoolean initialized = new AtomicBoolean(false);

    private Proxy socksProxy;

    public UninitializedCassandraResource(Class<?> classToSaveLogsFor) {
        containers = new ContainersV2(classToSaveLogsFor).with(containerInstance);
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
    public void beforeAll(ExtensionContext var1) throws IOException, InterruptedException {
        containers.beforeAll(var1);
        socksProxy = ContainersV2.getSocksProxy(containerInstance.getServiceName());
        containers.getContainer(containerInstance.getServiceName()).kill();
        containers.getDockerCompose().rm();
        kvs = createKvs();
    }

    @Override
    public void afterAll(ExtensionContext var1) {
        if (!initialized.get()) {
            return;
        }
        try {
            kvs.close();
            containers.getContainer(containerInstance.getServiceName()).kill();
        } catch (IOException | InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public KeyValueService getAsyncInitializeableKvs() {
        return kvs;
    }

    private KeyValueService createKvs() {
        Preconditions.checkNotNull(socksProxy, "There has to be a defined proxy");
        CassandraKeyValueServiceConfig config = containerInstance.getConfigWithProxy(socksProxy.address());

        return CassandraKeyValueServiceImpl.create(
                MetricsManagers.createForTests(),
                config,
                containerInstance.getRuntimeConfig(),
                CassandraMutationTimestampProviders.legacyModeForTestsOnly(),
                true);
    }
}
