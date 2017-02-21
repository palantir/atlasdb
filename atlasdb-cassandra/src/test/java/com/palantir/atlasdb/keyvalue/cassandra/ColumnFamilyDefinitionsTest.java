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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;

import org.apache.cassandra.thrift.CfDef;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class ColumnFamilyDefinitionsTest {
    ImmutableCassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(new InetSocketAddress("localhost", 666))
            .replicationFactor(1)
            .keyspace("atlasdb")
            .build();

    @Test
    public void compactionStrategiesShouldMatchWithOrWithoutPackageName() {
        CfDef standard = ColumnFamilyDefinitions.getCfDef(
                TableReference.fromString("test_table"),
                new byte[0],
                config);

        CfDef fullyQualified = standard.setCompaction_strategy("com.palantir.AwesomeCompactionStrategy");
        CfDef onlyClassName = standard.deepCopy().setCompaction_strategy("AwesomeCompactionStrategy");

        assertTrue(
                String.format("Compaction strategies %s and %s should match",
                        fullyQualified.compaction_strategy,
                        onlyClassName.compaction_strategy),
                ColumnFamilyDefinitions.isMatchingCf(fullyQualified, onlyClassName));
    }
}
