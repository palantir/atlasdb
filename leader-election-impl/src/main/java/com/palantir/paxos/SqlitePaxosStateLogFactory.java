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

import java.sql.Connection;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import com.palantir.common.persist.Persistable;

public class SqlitePaxosStateLogFactory {
    private final ReadWriteLock sharedLock = new ReentrantReadWriteLock();

    public <V extends Persistable & Versionable> PaxosStateLog<V> create(
            NamespaceAndUseCase namespaceAndUseCase,
            Supplier<Connection> connectionSupplier) {
        return SqlitePaxosStateLog.create(namespaceAndUseCase, connectionSupplier, sharedLock);
    }

    public SqlitePaxosStateLogMigrationState createMigrationState(
            NamespaceAndUseCase namespaceAndUseCase,
            Supplier<Connection> connectionSupplier) {
        return SqlitePaxosStateLogMigrationState.create(namespaceAndUseCase, connectionSupplier, sharedLock);
    }
}
