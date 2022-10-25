/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigs;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.CassandraClientConfig;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleterFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.common.base.Throwables;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.thrift.TException;

@AutoService(NamespaceDeleterFactory.class)
public final class CassandraNamespaceDeleterFactory implements NamespaceDeleterFactory {
    @Override
    public String getType() {
        return CassandraKeyValueServiceConfig.TYPE;
    }

    @Override
    public NamespaceDeleter createNamespaceDeleter(
            KeyValueServiceConfig config, Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        CassandraKeyValueServiceConfigs configs =
                CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(config, runtimeConfig);

        Supplier<CassandraClient> cassandraClientSupplier = () ->
                createClient(configs.installConfig(), configs.runtimeConfig().get());
        return new CassandraNamespaceDeleter(configs.installConfig(), cassandraClientSupplier);
    }

    private static CassandraClient createClient(
            CassandraKeyValueServiceConfig config, CassandraKeyValueServiceRuntimeConfig runtimeConfig) {
        try {
            InetSocketAddress host = runtimeConfig.servers().accept(ThriftHostsExtractingVisitor.INSTANCE).stream()
                    .findFirst()
                    .orElseThrow();
            return CassandraClientFactory.getClientInternal(CassandraServer.of(host), CassandraClientConfig.of(config));
        } catch (TException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
