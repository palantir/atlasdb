/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CqlQuery;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.PoolingContainer;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;

public final class HostnamesByIpSupplier implements Supplier<Map<String, String>> {
    private static final SafeLogger log = SafeLoggerFactory.get(HostnamesByIpSupplier.class);

    private static final String SYSTEM_PALANTIR_KEYSPACE = "system_palantir";
    private static final String HOSTNAMES_BY_IP_TABLE = "hostnames_by_ip";
    private static final String HOSTNAME_COLUMN = "hostname";
    private static final String IP_COLUMN = "ip";

    private final Supplier<List<PoolingContainer<CassandraClient>>> hosts;

    public HostnamesByIpSupplier(Supplier<List<PoolingContainer<CassandraClient>>> hosts) {
        this.hosts = hosts;
    }

    @Override
    public Map<String, String> get() {
        Map<String, String> result = ImmutableMap.of();
        List<PoolingContainer<CassandraClient>> containers = hosts.get();
        Stopwatch timer = Stopwatch.createStarted();
        for (PoolingContainer<CassandraClient> container : containers) {
            try {
                result = container.runWithPooledResource(getHostnamesByIp());
            } catch (Exception | Error e) {
                log.warn(
                        "Could not get hostnames by IP from Cassandra",
                        SafeArg.of("poolSize", containers.size()),
                        SafeArg.of("mappings", result),
                        SafeArg.of("elapsed", timer.elapsed()),
                        e);
            }

            if (result != null && !result.isEmpty()) {
                log.info(
                        "Found hostnames by IP mapping for pool",
                        SafeArg.of("poolSize", containers.size()),
                        SafeArg.of("mappings", result),
                        SafeArg.of("elapsed", timer.elapsed()));
                return result;
            } else if (timer.elapsed(TimeUnit.SECONDS) > 60) {
                log.warn(
                        "Could not find hostnames by IP mapping for pool within timeout",
                        SafeArg.of("poolSize", containers.size()),
                        SafeArg.of("elapsed", timer.elapsed()));
                return ImmutableMap.of();
            }
        }

        log.warn(
                "Could not find hostnames by IP mapping for pool",
                SafeArg.of("poolSize", containers.size()),
                SafeArg.of("elapsed", timer.elapsed()));
        return ImmutableMap.of();
    }

    public FunctionCheckedException<CassandraClient, Map<String, String>, Exception> getHostnamesByIp() {
        return client -> {
            KsDef systemPalantir;
            try {
                systemPalantir = client.describe_keyspace(SYSTEM_PALANTIR_KEYSPACE);
            } catch (NotFoundException e) {
                log.debug("Did not find keyspace with hostnames by ip, moving on without them", e);
                return ImmutableMap.of();
            }
            if (isCfNotPresent(systemPalantir, HOSTNAMES_BY_IP_TABLE)) {
                log.debug("Did not find table with hostnames by ip, moving on without them");
                return ImmutableMap.of();
            }

            CqlQuery query = CqlQuery.builder()
                    .safeQueryFormat("SELECT * FROM \"%s\".\"%s\";")
                    .addArgs(
                            SafeArg.of("keyspace", SYSTEM_PALANTIR_KEYSPACE),
                            SafeArg.of("table", HOSTNAMES_BY_IP_TABLE))
                    .build();

            return client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.LOCAL_QUORUM).getRows().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            row -> getNamedColumnValue(row, IP_COLUMN),
                            row -> getNamedColumnValue(row, HOSTNAME_COLUMN)));
        };
    }

    private boolean isCfNotPresent(KsDef ksDef, String cfName) {
        return ksDef.getCf_defs().stream().noneMatch(cfDef -> cfDef.name.equals(cfName));
    }

    private static String getNamedColumnValue(CqlRow row, String columnName) {
        return Iterables.getOnlyElement(row.getColumns().stream()
                .filter(col -> PtBytes.toString(col.getName()).equals(columnName))
                .map(col -> PtBytes.toString(col.getValue()))
                .collect(Collectors.toList()));
    }
}
