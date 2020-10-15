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

package com.palantir.atlasdb.timelock.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqliteNamespaceLoaderTest {
    private static final Client NAMESPACE_1 = Client.of("eins");
    private static final Client NAMESPACE_2 = Client.of("zwei");

    private static final String SEQUENCE_ID_1 = "seq1";
    private static final String SEQUENCE_ID_2 = "seq2";
    private static final PaxosValue PAXOS_VALUE = new PaxosValue(UUID.randomUUID().toString(), 1, PtBytes.toBytes("a"));

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private DataSource dataSource;
    private PersistentNamespaceLoader namespaceLoader;

    @Before
    public void setup() {
        dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        namespaceLoader = SqliteNamespaceLoader.create(dataSource);
    }

    @Test
    public void canReturnZeroNamespaces() {
        assertThat(namespaceLoader.getAllPersistedNamespaces()).isEmpty();
    }

    @Test
    public void returnsOneNamespaceEvenIfMultipleSequencesPresent() {
        initializeLog(NAMESPACE_1, SEQUENCE_ID_1);
        initializeLog(NAMESPACE_1, SEQUENCE_ID_2);
        assertThat(namespaceLoader.getAllPersistedNamespaces()).containsExactlyInAnyOrder(NAMESPACE_1);
    }

    @Test
    public void returnsMultipleNamespacesIfNeeded() {
        initializeLog(NAMESPACE_1, SEQUENCE_ID_1);
        initializeLog(NAMESPACE_1, SEQUENCE_ID_2);
        initializeLog(NAMESPACE_2, SEQUENCE_ID_1);
        initializeLog(NAMESPACE_2, SEQUENCE_ID_2);
        assertThat(namespaceLoader.getAllPersistedNamespaces()).containsExactlyInAnyOrder(NAMESPACE_1, NAMESPACE_2);
    }

    private void initializeLog(Client namespace, String sequenceId) {
        SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(namespace, sequenceId), dataSource)
                .writeRound(1, PAXOS_VALUE);
    }
}
