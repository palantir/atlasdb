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

import org.junit.rules.ExternalResource;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;

public class CassandraResource extends ExternalResource {
    private final CassandraContainer containerInstance = new CassandraContainer();
    private final Containers containers;
    private CassandraKeyValueService kvs = null;

    public CassandraResource(Class<?> classToSaveLogsFor) {
        containers = new Containers(classToSaveLogsFor).with(containerInstance);
    }

    @Override
    public void before() throws Throwable {
        containers.before();
    }

    @Override
    public void after() {
        if (kvs != null) {
            kvs.close();
        }
    }

    public synchronized CassandraKeyValueService getDefaultKvs() {
        if (kvs == null) {
            kvs = CassandraKeyValueServiceImpl
                    .createForTesting(containerInstance.getConfig(), CassandraContainer.LEADER_CONFIG);
        }
        return kvs;
    }

    public CassandraKeyValueServiceConfig getConfig() {
        return containerInstance.getConfig();
    }
}
