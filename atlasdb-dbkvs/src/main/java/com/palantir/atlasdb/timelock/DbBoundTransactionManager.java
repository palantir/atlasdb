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

package com.palantir.atlasdb.timelock;

import java.util.function.Function;
import java.util.function.Supplier;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;

public final class DbBoundTransactionManager {
    private final Supplier<PhysicalDbBoundStore> storeFactory;

    private DbBoundTransactionManager(Supplier<PhysicalDbBoundStore> storeFactory) {
        this.storeFactory = storeFactory;
    }

    public static DbBoundTransactionManager create(ConnectionManager connectionManager, String client) {
        Supplier<PhysicalDbBoundStore> storeFactory = getStoreFactory(connectionManager, client);
        return new DbBoundTransactionManager(storeFactory);
    }

    public <T> T runTaskTransactionScoped(Function<PhysicalDbBoundStore, T> transactionTask) {
        return transactionTask.apply(storeFactory.get());
    }

    private static Supplier<PhysicalDbBoundStore> getStoreFactory(ConnectionManager connectionManager, String client) {
        Supplier<PhysicalDbBoundStore> storeFactory;
        switch (connectionManager.getDbType()) {
            case ORACLE:
                throw new UnsupportedOperationException("Not implemented yet");
            case POSTGRESQL:
                storeFactory = () -> PostgresPhysicalDbBoundStore.create(connectionManager, client);
                break;
            case H2_MEMORY:
                throw new UnsupportedOperationException("H2 is not supported for the db bound manager");
            default:
                throw new SafeIllegalStateException("Unexpected db type",
                        SafeArg.of("dbType", connectionManager.getDbType()));
        }
        return storeFactory;
    }
}
