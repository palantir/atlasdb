/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.timestamp.server.config;

import java.io.Closeable;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.timestamp.TimestampService;

public class AtlasDbServerState implements Closeable {
    final KeyValueService keyValueService;
    final Supplier<TimestampService> timestampSupplier;
    final SerializableTransactionManager transactionManager;

    public AtlasDbServerState(KeyValueService keyValueService,
                              Supplier<TimestampService> timestampSupplier,
                              SerializableTransactionManager transactionManager) {
        this.keyValueService = keyValueService;
        this.timestampSupplier = timestampSupplier;
        this.transactionManager = transactionManager;
    }

    public KeyValueService getKeyValueService() {
        return keyValueService;
    }

    public Supplier<TimestampService> getTimestampSupplier() {
        return timestampSupplier;
    }

    public SerializableTransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Override
    public void close() {
        transactionManager.getCleaner().close();
        keyValueService.close();
    }
}