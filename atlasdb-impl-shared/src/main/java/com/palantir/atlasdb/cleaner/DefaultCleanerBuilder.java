/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.time.Clock;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampService;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class DefaultCleanerBuilder {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultCleanerBuilder.class);

    private final KeyValueService keyValueService;
    private final TimelockService timelockService;
    private final List<Follower> followerList;
    private final TransactionService transactionService;
    private final MetricsManager metricsManager;

    private long transactionReadTimeout = AtlasDbConstants.DEFAULT_TRANSACTION_READ_TIMEOUT;
    private long punchIntervalMillis = AtlasDbConstants.DEFAULT_PUNCH_INTERVAL_MILLIS;
    private boolean backgroundScrubAggressively = AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_AGGRESSIVELY;
    private int backgroundScrubThreads = AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_THREADS;
    private int backgroundScrubReadThreads = AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_READ_THREADS;
    private long backgroundScrubFrequencyMillis = AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_FREQUENCY_MILLIS;
    private int backgroundScrubBatchSize = AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_BATCH_SIZE;
    private boolean initalizeAsync = AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC;

    public DefaultCleanerBuilder(
            KeyValueService keyValueService,
            LockService lockService,
            TimestampService timestampService,
            LockClient lockClient,
            List<? extends Follower> followerList,
            TransactionService transactionService,
            MetricsManager metricsManager) {
        this(
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                followerList,
                transactionService,
                metricsManager);
    }

    public DefaultCleanerBuilder(
            KeyValueService keyValueService,
            TimelockService timelockService,
            List<? extends Follower> followerList,
            TransactionService transactionService,
            MetricsManager metricsManager) {
        this.keyValueService = keyValueService;
        this.timelockService = timelockService;
        this.followerList = ImmutableList.copyOf(followerList);
        this.transactionService = transactionService;
        this.metricsManager = metricsManager;
    }

    public DefaultCleanerBuilder setTransactionReadTimeout(long transactionReadTimeout) {
        this.transactionReadTimeout = transactionReadTimeout;
        return this;
    }

    public DefaultCleanerBuilder setPunchIntervalMillis(long punchIntervalMillis) {
        this.punchIntervalMillis = punchIntervalMillis;
        return this;
    }

    public DefaultCleanerBuilder setBackgroundScrubAggressively(boolean backgroundScrubAggressively) {
        this.backgroundScrubAggressively = backgroundScrubAggressively;
        return this;
    }

    public DefaultCleanerBuilder setBackgroundScrubThreads(int backgroundScrubThreads) {
        this.backgroundScrubThreads = backgroundScrubThreads;
        return this;
    }

    public DefaultCleanerBuilder setBackgroundScrubReadThreads(int backgroundScrubReadThreads) {
        this.backgroundScrubReadThreads = backgroundScrubReadThreads;
        return this;
    }

    public DefaultCleanerBuilder setBackgroundScrubFrequencyMillis(long backgroundScrubFrequencyMillis) {
        this.backgroundScrubFrequencyMillis = backgroundScrubFrequencyMillis;
        return this;
    }

    public DefaultCleanerBuilder setBackgroundScrubBatchSize(int backgroundScrubBatchSize) {
        this.backgroundScrubBatchSize = backgroundScrubBatchSize;
        return this;
    }

    public DefaultCleanerBuilder setInitializeAsync(boolean initializeAsync) {
        this.initalizeAsync = initializeAsync;
        return this;
    }

    private Puncher buildPuncher(LongSupplier timestampSeedSource) {
        PuncherStore keyValuePuncherStore = KeyValueServicePuncherStore.create(keyValueService, initalizeAsync);
        PuncherStore cachingPuncherStore = CachingPuncherStore.create(keyValuePuncherStore, punchIntervalMillis * 3);
        Clock clock = GlobalClock.create(timelockService);
        SimplePuncher simplePuncher =
                SimplePuncher.create(cachingPuncherStore, clock, Suppliers.ofInstance(transactionReadTimeout));
        return AsyncPuncher.create(simplePuncher, punchIntervalMillis, timestampSeedSource);
    }

    private Scrubber buildScrubber(
            Supplier<Long> unreadableTimestampSupplier, Supplier<Long> immutableTimestampSupplier) {
        ScrubberStore scrubberStore = KeyValueServiceScrubberStore.create(keyValueService, initalizeAsync);
        return Scrubber.create(
                keyValueService,
                scrubberStore,
                Suppliers.ofInstance(backgroundScrubFrequencyMillis),
                Suppliers.ofInstance(true),
                unreadableTimestampSupplier,
                immutableTimestampSupplier,
                transactionService,
                backgroundScrubAggressively,
                Suppliers.ofInstance(backgroundScrubBatchSize),
                backgroundScrubThreads,
                backgroundScrubReadThreads,
                followerList,
                metricsManager);
    }

    public Cleaner buildCleaner() {
        Puncher puncher = buildPuncher(timelockService::getFreshTimestamp);
        Supplier<Long> immutableTs = ImmutableTimestampSupplier.createMemoizedWithExpiration(timelockService);
        Scrubber scrubber = buildScrubber(puncher.getTimestampSupplier(), immutableTs);
        return new SimpleCleaner(scrubber, puncher, Suppliers.ofInstance(transactionReadTimeout));
    }
}
