/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockService;
import com.palantir.atlasdb.persistentlock.NoOpPersistentLockService;
import com.palantir.atlasdb.persistentlock.PersistentLockService;
import com.palantir.atlasdb.sweep.CellsSweeper;
import com.palantir.atlasdb.sweep.PersistentLockManager;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;

import dagger.Module;
import dagger.Provides;

@Module
public class SweeperModule {

    @Provides
    public PersistentLockManager providePersistentLockManager(@Named("kvs") KeyValueService kvs,
                                                              ServicesConfig config) {
        PersistentLockService persistentLockService = kvs.supportsCheckAndSet() ?
            KvsBackedPersistentLockService.create(kvs, config.atlasDbConfig().initializeAsync()) :
            new NoOpPersistentLockService();
        return new PersistentLockManager(
                persistentLockService,
                AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS);
    }

    @Provides
    @Singleton
    public SweepTaskRunner provideSweepTaskRunner(SerializableTransactionManager txm,
                                                  @Named("kvs") KeyValueService kvs,
                                                  TransactionService transactionService,
                                                  SweepStrategyManager sweepStrategyManager,
                                                  Follower follower,
                                                  PersistentLockManager persistentLockManager,
                                                  ServicesConfig config) {
        return new SweepTaskRunner(
                kvs,
                txm::getUnreadableTimestamp,
                txm::getImmutableTimestamp,
                transactionService,
                sweepStrategyManager,
                new CellsSweeper(
                        txm,
                        kvs,
                        persistentLockManager,
                        ImmutableList.of(follower)));
    }

}
