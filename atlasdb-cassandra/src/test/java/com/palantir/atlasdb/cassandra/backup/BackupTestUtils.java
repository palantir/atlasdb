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

package com.palantir.atlasdb.cassandra.backup;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.ImmutableCqlCapableConfig;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("DnsLookup")
public final class BackupTestUtils {
    public static final int TEST_THRIFT_PORT = 44;
    public static final int TEST_CQL_PORT = 45;

    private BackupTestUtils() {
        // utility
    }

    public static CassandraServersConfigs.CqlCapableConfig cqlCapableConfig(String... hosts) {
        Iterable<InetSocketAddress> thriftHosts = constructHosts(TEST_THRIFT_PORT, hosts);
        Iterable<InetSocketAddress> cqlHosts = constructHosts(TEST_CQL_PORT, hosts);
        return ImmutableCqlCapableConfig.builder()
                .cqlHosts(cqlHosts)
                .thriftHosts(thriftHosts)
                .build();
    }

    private static List<InetSocketAddress> constructHosts(int port, String[] hosts) {
        return Stream.of(hosts)
                .map(host -> InetSocketAddress.createUnresolved(host, port))
                .collect(Collectors.toList());
    }
}
