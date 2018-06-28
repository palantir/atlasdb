/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.services;

import javax.inject.Named;
import javax.inject.Singleton;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampService;

import dagger.Component;

@Singleton
@Component(modules = { ServicesConfigModule.class, KeyValueServiceModule.class, RawKeyValueServiceModule.class,
        LockAndTimestampModule.class, MetricsModule.class, SweeperModule.class, TransactionManagerModule.class })
public abstract class AtlasDbServices implements AutoCloseable {

    public abstract AtlasDbConfig getAtlasDbConfig();

    public abstract AtlasDbRuntimeConfig getAtlasDbRuntimeConfig();

    public abstract TimelockService getTimelockService();

    public abstract TimestampService getTimestampService();

    public abstract LockService getLockService();

    @Named("kvs")
    public abstract KeyValueService getKeyValueService();

    public abstract SerializableTransactionManager getTransactionManager();

    public abstract SweepTaskRunner getSweepTaskRunner();

    public abstract TransactionService getTransactionService();

    @Override
    public void close() {
        getTransactionManager().close();
    }
}
