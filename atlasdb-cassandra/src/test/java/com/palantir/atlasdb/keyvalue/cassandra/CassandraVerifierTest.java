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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import java.net.InetSocketAddress;
import java.util.Arrays;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

public class CassandraVerifierTest {
    private static final String DC_1 = "dc1";
    private static final String DC_2 = "dc2";
    private static final String HOST_1 = "host1";
    private static final String HOST_2 = "host2";
    private static final String HOST_3 = "host3";
    private static final String HOST_4 = "host4";
    private static final String RACK_1 = "test_rack1";
    private static final String RACK_2 = "test_rack2";
    private static final String RACK_3 = "test_rack3";

    private CassandraClient client = mock(CassandraClient.class);
    private CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);

    @Before
    public void beforeEach() {
        CassandraVerifier.sanityCheckedDatacenters.invalidateAll();
    }

    @Test
    public void unsetTopologyAndHighRfThrows() throws TException {
        setTopology(defaultTopology(HOST_1), defaultTopology(HOST_2));
        when(config.replicationFactor()).thenReturn(3);

        assertThatThrownBy(() -> CassandraVerifier.sanityCheckDatacenters(client, config))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void unsetTopologyAndRfOneSucceeds() throws TException {
        setTopology(defaultTopology(HOST_1), defaultTopology(HOST_2));
        when(config.replicationFactor()).thenReturn(1);

        assertThat(CassandraVerifier.sanityCheckDatacenters(client, config))
                .containsExactly(CassandraConstants.DEFAULT_DC);
    }

    @Test
    public void nonDefaultDcAndHighRfSucceeds() throws TException {
        setTopology(createDetails(DC_1, RACK_1, HOST_1));
        when(config.replicationFactor()).thenReturn(3);

        assertThat(CassandraVerifier.sanityCheckDatacenters(client, config)).containsExactly(DC_1);
    }

    @Test
    public void oneDcOneRackAndMoreHostsThanRfThrows() throws TException {
        setTopology(
                createDetails(DC_1, RACK_1, HOST_1),
                createDetails(DC_1, RACK_1, HOST_2),
                createDetails(DC_1, RACK_1, HOST_3),
                createDetails(DC_1, RACK_1, HOST_4));
        when(config.replicationFactor()).thenReturn(3);

        assertThatThrownBy(() -> CassandraVerifier.sanityCheckDatacenters(client, config))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void moreDcsPresentThanInStrategyOptionsSucceeds() throws TException {
        setTopology(createDetails(DC_1, RACK_1, HOST_1));
        when(config.replicationFactor()).thenReturn(3);

        KsDef ksDef = CassandraVerifier.createKsDefForFresh(client, config);
        CassandraVerifier.sanityCheckedDatacenters.invalidateAll();
        CassandraVerifier.sanityCheckedDatacenters.cleanUp();
        setTopology(createDetails(DC_1, RACK_1, HOST_1), createDetails(DC_2, RACK_2, HOST_2));

        assertThatCode(() -> CassandraVerifier.checkAndSetReplicationFactor(client, ksDef, config))
                .as("strategy options should only contain info for DC_1 but should not throw despite detecting two DCs")
                .doesNotThrowAnyException();
    }

    @Test
    public void oneDcFewerRacksThanRfAndMoreHostsThanRfThrows() throws TException {
        setTopology(
                defaultDcDetails(RACK_1, HOST_1),
                defaultDcDetails(RACK_2, HOST_2),
                defaultDcDetails(RACK_1, HOST_3),
                defaultDcDetails(RACK_2, HOST_4));
        when(config.replicationFactor()).thenReturn(3);

        assertThatThrownBy(() -> CassandraVerifier.sanityCheckDatacenters(client, config))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void oneDcMoreRacksThanRfAndMoreHostsThanRfSucceeds() throws TException {
        setTopology(
                defaultDcDetails(RACK_1, HOST_1),
                defaultDcDetails(RACK_2, HOST_2),
                defaultDcDetails(RACK_1, HOST_3),
                defaultDcDetails(RACK_3, HOST_4));
        when(config.replicationFactor()).thenReturn(2);

        assertThat(CassandraVerifier.sanityCheckDatacenters(client, config))
                .containsExactly(CassandraConstants.DEFAULT_DC);
    }

    @Test
    public void oneDcFewerRacksThanRfAndFewerHostsThanRfSucceeds() throws TException {
        setTopology(
                defaultDcDetails(RACK_1, HOST_1),
                defaultDcDetails(RACK_2, HOST_2),
                defaultDcDetails(RACK_1, HOST_2),
                defaultDcDetails(RACK_2, HOST_1));
        when(config.replicationFactor()).thenReturn(3);

        assertThat(CassandraVerifier.sanityCheckDatacenters(client, config))
                .containsExactly(CassandraConstants.DEFAULT_DC);
    }

    @Test
    public void multipleDcSuceeds() throws TException {
        setTopology(
                createDetails(DC_1, RACK_1, HOST_1),
                createDetails(DC_1, RACK_1, HOST_2),
                createDetails(DC_1, RACK_1, HOST_3),
                createDetails(DC_2, RACK_1, HOST_4));
        when(config.replicationFactor()).thenReturn(3);

        assertThat(CassandraVerifier.sanityCheckDatacenters(client, config)).containsExactlyInAnyOrder(DC_1, DC_2);
    }

    @Test
    public void freshInstanceSetsStrategyOptions() throws TException {
        setTopology(
                createDetails(DC_1, RACK_1, HOST_1),
                createDetails(DC_1, RACK_1, HOST_2),
                createDetails(DC_1, RACK_1, HOST_3),
                createDetails(DC_2, RACK_1, HOST_4));
        when(config.replicationFactor()).thenReturn(3);

        KsDef ksDef = CassandraVerifier.createKsDefForFresh(client, config);
        assertThat(ksDef.strategy_options).isEqualTo(ImmutableMap.of(DC_1, "3", DC_2, "3"));
    }

    @Test
    public void simpleStrategyOneDcOneRfSucceedsAndUpdatesKsDef() throws TException {
        setTopology(createDetails(DC_1, RACK_1, HOST_1));
        when(config.replicationFactor()).thenReturn(1);
        KsDef ksDef = new KsDef("test", CassandraConstants.SIMPLE_STRATEGY, ImmutableList.of());
        ImmutableMap<String, String> strategyOptions =
                ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1");
        ksDef.setStrategy_options(strategyOptions);

        ksDef = CassandraVerifier.checkAndSetReplicationFactor(client, ksDef, config);
        assertThat(ksDef.strategy_class).isEqualTo(CassandraConstants.NETWORK_STRATEGY);
        assertThat(ksDef.strategy_options).isEqualTo(ImmutableMap.of(DC_1, "1"));
    }

    @Test
    public void simpleStrategyOneDcHighRfThrows() throws TException {
        KsDef ksDef = new KsDef("test", CassandraConstants.SIMPLE_STRATEGY, ImmutableList.of());
        ksDef.setStrategy_options(ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "3"));

        assertThatThrownBy(() -> CassandraVerifier.checkAndSetReplicationFactor(client, ksDef, config))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void simpleStrategyMultipleDcsThrows() throws TException {
        setTopology(
                createDetails(DC_1, RACK_1, HOST_1),
                createDetails(DC_1, RACK_1, HOST_2),
                createDetails(DC_1, RACK_1, HOST_3),
                createDetails(DC_2, RACK_1, HOST_4));
        when(config.replicationFactor()).thenReturn(1);
        KsDef ksDef = new KsDef("test", CassandraConstants.SIMPLE_STRATEGY, ImmutableList.of());
        ksDef.setStrategy_options(ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1"));

        assertThatThrownBy(() -> CassandraVerifier.checkAndSetReplicationFactor(client, ksDef, config))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void returnSameKsDefIfNodeTopologyChecksIgnored() throws TException {
        setTopology(createDetails(DC_1, RACK_1, HOST_1));
        when(config.replicationFactor()).thenReturn(7);
        when(config.ignoreNodeTopologyChecks()).thenReturn(true);
        KsDef ksDef = new KsDef("test", CassandraConstants.SIMPLE_STRATEGY, ImmutableList.of());
        ImmutableMap<String, String> strategyOptions =
                ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "1", DC_1, "7");
        ksDef.setStrategy_options(strategyOptions);

        ksDef = CassandraVerifier.checkAndSetReplicationFactor(client, ksDef, config);
        assertThat(ksDef.strategy_class).isEqualTo(CassandraConstants.SIMPLE_STRATEGY);
        assertThat(ksDef.strategy_options).isEqualTo(strategyOptions);
    }

    @Test
    public void networkStrategyMultipleDcsSucceeds() throws TException {
        setTopology(
                createDetails(DC_1, RACK_1, HOST_1),
                createDetails(DC_1, RACK_1, HOST_2),
                createDetails(DC_1, RACK_1, HOST_3),
                createDetails(DC_2, RACK_1, HOST_4));
        when(config.replicationFactor()).thenReturn(3);
        KsDef ksDef = new KsDef("test", CassandraConstants.NETWORK_STRATEGY, ImmutableList.of());
        ImmutableMap<String, String> strategyOptions =
                ImmutableMap.of(CassandraConstants.REPLICATION_FACTOR_OPTION, "3", DC_1, "3", DC_2, "3");
        ksDef.setStrategy_options(strategyOptions);

        ksDef = CassandraVerifier.checkAndSetReplicationFactor(client, ksDef, config);
        assertThat(ksDef.strategy_options).isEqualTo(strategyOptions);
    }

    @Test
    public void differentRfThanConfigThrows() {
        KsDef ksDef = new KsDef("test", CassandraConstants.SIMPLE_STRATEGY, ImmutableList.of());
        ksDef.setStrategy_options(ImmutableMap.of(DC_1, "1", DC_2, "2"));
        when(config.replicationFactor()).thenReturn(1);
        assertThatThrownBy(() -> CassandraVerifier.sanityCheckReplicationFactor(
                        ksDef, config, ImmutableSortedSet.of(DC_1, DC_2)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private void setTopology(EndpointDetails... details) throws TException {
        when(client.describe_ring(CassandraConstants.SIMPLE_RF_TEST_KEYSPACE))
                .thenReturn(ImmutableList.of(mockRangeWithDetails(details)));
        when(config.servers())
                .thenReturn(ImmutableDefaultConfig.builder()
                        .addThriftHosts(InetSocketAddress.createUnresolved(HOST_1, 8080))
                        .build());
    }

    private TokenRange mockRangeWithDetails(EndpointDetails... details) {
        TokenRange mockRange = new TokenRange();
        mockRange.setEndpoint_details(Arrays.asList(details));
        return mockRange;
    }

    private EndpointDetails defaultTopology(String host) {
        return createDetails(CassandraConstants.DEFAULT_DC, CassandraConstants.DEFAULT_RACK, host);
    }

    private EndpointDetails defaultDcDetails(String rack, String host) {
        return createDetails(CassandraConstants.DEFAULT_DC, rack, host);
    }

    private EndpointDetails createDetails(String dc, String rack, String host) {
        EndpointDetails details = new EndpointDetails(host, dc);
        return details.setRack(rack);
    }
}
