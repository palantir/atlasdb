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

package com.palantir.atlasdb.cassandra.backup;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.containers.CassandraResource;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CqlClusterIntegrationTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private CqlCluster cqlCluster;
    private CassandraKeyValueServiceConfig config;

    // private SortedMap<Token, TokenRange> tokenRangesByEnd;
    private Cluster cluster;
    private Token token1;
    // private Token token2;
    // private Token token8;

    @Before
    public void setUp() {
        config = CASSANDRA.getConfig();
        cluster = CqlCluster.createCluster(config);
        cqlCluster = new CqlCluster(cluster, config);

        // tokenRangesByEnd = StreamEx.of(cluster.getMetadata().getTokenRanges())
        //         .mapToEntry(TokenRange::getEnd)
        //         .invert()
        //         .toSortedMap();
        token1 = getToken("10");
        // token2 = getToken("30");
        // token8 = getToken("f0");
    }

    @Test
    public void testTokenMapping() {
        InetSocketAddress host = ((CqlCapableConfig) config.servers())
                .cqlHosts().stream().findAny().orElseThrow();

        Map<InetSocketAddress, Set<TokenRange>> mapping = cqlCluster.getTokenRanges("_coordination");

        assertThat(mapping.get(host)).hasSize(1);
        assertThat(mapping.get(host).iterator().next())
                .isEqualTo(cluster.getMetadata().newTokenRange(token1, getToken("20")));
    }

    private Token getToken(String hex) {
        return getToken(DatatypeConverter.parseHexBinary(hex));
    }

    private Token getToken(byte[] tokenBytes) {
        return cluster.getMetadata().newToken(ByteBuffer.wrap(tokenBytes));
    }
}
