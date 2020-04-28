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

package com.palantir.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlitePaxosStateLogFactoryTest {
    private static final String NAMESPACE_1 = "namespace1";
    private static final String NAMESPACE_2 = "namespace2";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Supplier<Connection> connSupplier;
    private SqlitePaxosStateLogFactory<PaxosValue> factory;

    @Before
    public void setup() {
        connSupplier = SqliteConnections
                .createSqliteDatabase(tempFolder.getRoot().toPath().resolve("test.db").toString());
        factory = SqlitePaxosStateLogFactory.create(connSupplier);
    }

    @Test
    public void namespacesRegisteredWhenStateLogsAreCreated() {
        factory.createPaxosStateLog(NAMESPACE_1);
        factory.createPaxosStateLog(NAMESPACE_2);

        assertThat(factory.getAllRegisteredNamespaces()).containsExactlyInAnyOrder(NAMESPACE_1, NAMESPACE_2);
    }

    @Test
    public void getRegisteredNamespacesReturnsNothingIfNoNamespacesExistYet() {
        assertThat(factory.getAllRegisteredNamespaces()).containsExactly();
    }

    @Test
    public void canCreateSameLogTwice() {
        factory.createPaxosStateLog(NAMESPACE_1);
        factory.createPaxosStateLog(NAMESPACE_1);

        assertThat(factory.getAllRegisteredNamespaces()).containsExactly(NAMESPACE_1);
    }
}
