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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.SafeArg;

public final class CassandraUtils {
    private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);
    private static final String SYSTEM_PALANTIR_KEYSPACE = "system_palantir";
    private static final String HOSTNAMES_BY_IP_TABLE = "hostnames_by_ip";
    private static final String HOSTNAME_COLUMN = "hostname";
    private static final String IP_COLUMN = "ip";

    private CassandraUtils() {}

    public static FunctionCheckedException<CassandraClient, Void, Exception> getValidatePartitioner(
            CassandraKeyValueServiceConfig config) {
        return client -> {
            CassandraVerifier.validatePartitioner(client.describe_partitioner(), config);
            return null;
        };
    }

    public static FunctionCheckedException<CassandraClient, List<TokenRange>, Exception> getDescribeRing(
            CassandraKeyValueServiceConfig config) {
        return client -> client.describe_ring(config.getKeyspaceOrThrow());
    }

    public static FunctionCheckedException<CassandraClient, Map<String, String>, Exception> getHostnameMappings() {
        return client -> {
            try {
                KsDef systemPalantir = client.describe_keyspace(SYSTEM_PALANTIR_KEYSPACE);
                if (systemPalantir.getCf_defs().stream().noneMatch(cfDef -> cfDef.name.equals(HOSTNAMES_BY_IP_TABLE))) {
                    return ImmutableMap.of();
                }
                CqlQuery query = CqlQuery.builder()
                        .safeQueryFormat("SELECT * FROM \"%s\".\"%s\";")
                        .addArgs(
                                SafeArg.of("keyspace", SYSTEM_PALANTIR_KEYSPACE),
                                SafeArg.of("table", HOSTNAMES_BY_IP_TABLE))
                        .build();

                return client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.ONE).getRows().stream()
                        .collect(ImmutableMap.toImmutableMap(
                                row -> getNamedColumnValue(row, IP_COLUMN),
                                row -> getNamedColumnValue(row, HOSTNAME_COLUMN)));
            } catch (NotFoundException e) {
                logger.debug("Did not find table with hostname mappings, moving on without them");
                return ImmutableMap.of();
            } catch (Exception e) {
                logger.warn("Could not get hostname mappings from Cassandra", e);
                return ImmutableMap.of();
            }
        };
    }

    public static AtlasDbDependencyException wrapInIceForDeleteOrRethrow(RetryLimitReachedException ex) {
        if (ex.suppressed(UnavailableException.class) || ex.suppressed(InsufficientConsistencyException.class)) {
            throw new InsufficientConsistencyException("Deleting requires all Cassandra nodes to be available.", ex);
        }
        throw ex;
    }

    private static String getNamedColumnValue(CqlRow row, String columnName) {
        return Iterables.getOnlyElement(
                row.getColumns().stream()
                        .filter(col -> PtBytes.toString(col.getName()).equals(columnName))
                        .map(col -> PtBytes.toString(col.getValue()))
                        .collect(Collectors.toList()));
    }
}
