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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CassandraCliParserTest {
    private static final String CORRUPT_STRING = "sodu89sydihusd:KSDNLSA";

    private final CassandraCliParser parser = new CassandraCliParser(CassandraVersion.from("2.2.9"));

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateUnsupportedParser() {
        new CassandraCliParser(CassandraVersion.from("1.2.19"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateFutureParser() {
        new CassandraCliParser(CassandraVersion.from("4.0"));
    }

    @Test
    public void canParseThreeUpNodesFromNodetoolStatus() {
        String nodetoolStatus = "Datacenter: dc1\n"
                + "===============\n"
                + "Status=Up/Down\n"
                + "|/ State=Normal/Leaving/Joining/Moving\n"
                + "--  Address     Load       Tokens       Owns (effective)"
                + "  Host ID                               Rack\n"
                + "UN  172.30.0.4  1.74 MB    512          64.4%           "
                + "  45cc5892-a0d2-4e7c-9080-c77329c05e0b  rack1\n"
                + "UN  172.30.0.3  1.74 MB    512          66.0%           "
                + "  7f436bc3-5f5c-4a07-beed-f4bb96189cca  rack1\n"
                + "UN  172.30.0.2  1.87 MB    512          69.6%           "
                + "  58b310df-5ce2-4565-a479-0ed37e69b04f  rack1";

        assertThat(parser.parseNumberOfUpNodesFromNodetoolStatus(nodetoolStatus))
                .isEqualTo(3);
    }

    @Test
    public void canParseTwoUpAndOneDownNodesFromNodetoolStatus() {
        String nodetoolStatus = "Datacenter: dc1\n"
                + "===============\n"
                + "Status=Up/Down\n"
                + "|/ State=Normal/Leaving/Joining/Moving\n"
                + "--  Address     Load       Tokens       Owns (effective)"
                + "  Host ID                               Rack\n"
                + "DN  172.30.0.4  1.72 MB    512          64.4%           "
                + "  45cc5892-a0d2-4e7c-9080-c77329c05e0b  rack1\n"
                + "UN  172.30.0.3  1.72 MB    512          66.0%           "
                + "  7f436bc3-5f5c-4a07-beed-f4bb96189cca  rack1\n"
                + "UN  172.30.0.2  1.87 MB    512          69.6%           "
                + "  58b310df-5ce2-4565-a479-0ed37e69b04f  rack1";

        assertThat(parser.parseNumberOfUpNodesFromNodetoolStatus(nodetoolStatus))
                .isEqualTo(2);
    }

    @Test
    public void parsesCorruptResponseFromNodetoolStatusSilently() {
        assertThat(parser.parseNumberOfUpNodesFromNodetoolStatus(CORRUPT_STRING))
                .isEqualTo(0);
    }

    @Test
    public void parsesReplicationFactorOfSystemAuthKeyspace_2_2() {
        String output = "\n"
                + " keyspace_name      | durable_writes | strategy_class                              "
                + "| strategy_options\n"
                + "--------------------+----------------+---------------------------------------------"
                + "+----------------------------\n"
                + "             system |           True |  org.apache.cassandra.locator.LocalStrategy "
                + "|                         {}\n"
                + "        system_auth |           True | org.apache.cassandra.locator.SimpleStrategy "
                + "| {\"replication_factor\":\"4\"}\n"
                + " system_distributed |           True | org.apache.cassandra.locator.SimpleStrategy "
                + "| {\"replication_factor\":\"3\"}\n"
                + "\n"
                + "(3 rows)";
        assertThat(parser.parseSystemAuthReplicationFromCqlsh(output)).isEqualTo(4);
    }

    @Test
    @SuppressWarnings("checkstyle:LineLength")
    public void parsesReplicationFactorOfSystemAuthKeyspace_3_7() {
        CassandraCliParser parserThreeSeven = new CassandraCliParser(CassandraVersion.from("3.7"));

        String output = "\n"
            + " keyspace_name      | durable_writes | replication\n"
            + "--------------------+----------------+-------------------------------------------------------------------------------------\n"
            + "             system |           True |                             {'class':"
            + " 'org.apache.cassandra.locator.LocalStrategy'}\n"
            + "        system_auth |           True | {'class': 'org.apache.cassandra.locator.SimpleStrategy',"
            + " 'replication_factor': '4'}\n"
            + " system_distributed |           True | {'class': 'org.apache.cassandra.locator.SimpleStrategy',"
            + " 'replication_factor': '3'}\n"
            + "      system_schema |           True |                             {'class':"
            + " 'org.apache.cassandra.locator.LocalStrategy'}\n"
            + "      system_traces |           True | {'class': 'org.apache.cassandra.locator.SimpleStrategy',"
            + " 'replication_factor': '2'}\n"
            + "\n"
            + "(5 rows)";
        assertThat(parserThreeSeven.parseSystemAuthReplicationFromCqlsh(output)).isEqualTo(4);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parsingFailsWhenSystemAuthKeyspaceIsNotThere() {
        String output = "\n"
                + " keyspace_name      | durable_writes | strategy_class                              "
                + "| strategy_options\n"
                + "--------------------+----------------+---------------------------------------------"
                + "+----------------------------\n"
                + "             system |           True |  org.apache.cassandra.locator.LocalStrategy "
                + "|                         {}\n"
                + " system_distributed |           True | org.apache.cassandra.locator.SimpleStrategy "
                + "| {\"replication_factor\":\"3\"}\n"
                + "      system_traces |           True | org.apache.cassandra.locator.SimpleStrategy "
                + "| {\"replication_factor\":\"2\"}\n"
                + "\n"
                + "(3 rows)";
        parser.parseSystemAuthReplicationFromCqlsh(output);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parsingFailsWhenSystemAuthKeyspaceIsNotANumber() {
        String output = "\n"
                + " keyspace_name      | durable_writes | strategy_class                              "
                + "| strategy_options\n"
                + "--------------------+----------------+---------------------------------------------"
                + "+----------------------------\n"
                + "             system |           True |  org.apache.cassandra.locator.LocalStrategy "
                + "|                         {}\n"
                + "        system_auth |           True | org.apache.cassandra.locator.SimpleStrategy "
                + "| {\"replication_factor\":\"foo\"}\n"
                + "      system_traces |           True | org.apache.cassandra.locator.SimpleStrategy "
                + "| {\"replication_factor\":\"2\"}\n"
                + "\n"
                + "(3 rows)";
        parser.parseSystemAuthReplicationFromCqlsh(output);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parsingFailsWhenSystemAuthKeyspaceOutputCorrupt() {
        parser.parseSystemAuthReplicationFromCqlsh(CORRUPT_STRING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parsingFailsWhenSystemAuthKeyspaceReplicationStrategyIsNotSimple() {
        String output = "\n"
                + " keyspace_name      | durable_writes | strategy_class                                       "
                + "| strategy_options\n"
                + "--------------------+----------------+------------------------------------------------------"
                + "+----------------------------\n"
                + "             system |           True |  org.apache.cassandra.locator.LocalStrategy          "
                + "|                         {}\n"
                + "        system_auth |           True | org.apache.cassandra.locator.NetworkTopologyStrategy "
                + "| {\"dc1\":\"1\"}\n"
                + "      system_traces |           True | org.apache.cassandra.locator.SimpleStrategy          "
                + "| {\"replication_factor\":\"2\"}\n"
                + "\n"
                + "(3 rows)";
        parser.parseSystemAuthReplicationFromCqlsh(output);
    }
}
