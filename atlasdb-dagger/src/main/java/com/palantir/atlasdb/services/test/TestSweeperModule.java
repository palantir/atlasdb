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
package com.palantir.atlasdb.services.test;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.DeletionLock;
import com.palantir.atlasdb.sweep.CellsSweeper;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.SweepTaskRunnerImpl;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;

import dagger.Module;
import dagger.Provides;

@Module
public class TestSweeperModule {

    private final Optional<Supplier<Long>> unreadableTs;
    private final Optional<Supplier<Long>> immutableTs;

    public static TestSweeperModule create(Supplier<Long> ts) {
        return new TestSweeperModule(Optional.of(ts), Optional.of(ts));
    }

    public static TestSweeperModule create(Supplier<Long> unreadableTs, Supplier<Long> immutableTs) {
        return new TestSweeperModule(Optional.of(unreadableTs), Optional.of(immutableTs));
    }

    public TestSweeperModule() {
        this.unreadableTs = Optional.absent();
        this.immutableTs = Optional.absent();
    }

    private TestSweeperModule(Optional<Supplier<Long>> unreadableTs, Optional<Supplier<Long>> immutableTs) {
        this.unreadableTs = unreadableTs;
        this.immutableTs = immutableTs;
    }

    @Provides
    @Singleton
    public SweepTaskRunner provideSweepTaskRunner(SerializableTransactionManager txm,
                                                  @Named("kvs") KeyValueService kvs,
                                                  TransactionService transactionService,
                                                  SweepStrategyManager sweepStrategyManager,
                                                  Follower follower) {
        Supplier<Long> unreadable = unreadableTs.isPresent() ? unreadableTs.get() : txm::getUnreadableTimestamp;
        Supplier<Long> immutable = immutableTs.isPresent() ? immutableTs.get() : txm::getImmutableTimestamp;
        DeletionLock deletionLock = DeletionLock.create(kvs);
        return new SweepTaskRunnerImpl(
                kvs,
                deletionLock,
                unreadable,
                immutable,
                transactionService,
                sweepStrategyManager,
                new CellsSweeper(txm, kvs, ImmutableList.of(follower)));
    }

}
