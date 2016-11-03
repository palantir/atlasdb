/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.containers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class CassandraCliParserTest {
    public static final String CORRUPT_STRING = "sodu89sydihusd:KSDNLSA";

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

        assertThat(CassandraCliParser.parseNumberOfUpNodesFromNodetoolStatus(nodetoolStatus), is(3));
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

        assertThat(CassandraCliParser.parseNumberOfUpNodesFromNodetoolStatus(nodetoolStatus), is(2));
    }

    @Test
    public void parsesCorruptResponseFromNodetoolStatusSilently() {
        assertThat(CassandraCliParser.parseNumberOfUpNodesFromNodetoolStatus(CORRUPT_STRING), is(0));
    }

    @Test
    public void parsesReplicationFactorOfSystemAuthKeyspace() {
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
        assertThat(CassandraCliParser.parseSystemAuthReplicationFromCqlsh(output), is(4));
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
        CassandraCliParser.parseSystemAuthReplicationFromCqlsh(output);
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
        CassandraCliParser.parseSystemAuthReplicationFromCqlsh(output);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parsingFailsWhenSystemAuthKeyspaceOutputCorrupt() {
        CassandraCliParser.parseSystemAuthReplicationFromCqlsh(CORRUPT_STRING);
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
        CassandraCliParser.parseSystemAuthReplicationFromCqlsh(output);
    }
}
