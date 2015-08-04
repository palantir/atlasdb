/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.cleaner;

import java.util.List;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.time.Clock;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class DefaultCleanerBuilder {
    private static final int PUNCH_INTERVAL_MILLIS = 60000;
    private static final long DEFAULT_TRANSACTION_READ_TIMEOUT_MILLIS = 24 * 60 * 60 * 1000;
    private static final boolean DEFAULT_AGGRESSIVE_SCRUB = false;
    private static final int DEFAULT_SCRUB_BATCH_SIZE = 2000;
    private static final int DEFAULT_SCRUB_THREADS = 8;

    private static final Supplier<Long> readTimeoutSupplier = Suppliers.ofInstance(DEFAULT_TRANSACTION_READ_TIMEOUT_MILLIS);
    private static final Supplier<Integer> batchSizeSupplier = Suppliers.ofInstance(DEFAULT_SCRUB_BATCH_SIZE);

    final KeyValueService keyValueService;
    final RemoteLockService lockService;
    final TimestampService timestampService;
    final LockClient lockClient;
    final List<Follower> followerList;
    final TransactionService transactionService;

    public DefaultCleanerBuilder(KeyValueService keyValueService,
                                 RemoteLockService lockService,
                                 TimestampService timestampService,
                                 LockClient lockClient,
                                 List<? extends Follower> followerList,
                                 TransactionService transactionService) {
        this.keyValueService = keyValueService;
        this.lockService = lockService;
        this.timestampService = timestampService;
        this.lockClient = lockClient;
        this.followerList = ImmutableList.copyOf(followerList);
        this.transactionService = transactionService;
    }

    private Puncher buildPuncher() {
        KeyValueServicePuncherStore keyValuePuncherStore = KeyValueServicePuncherStore.create(keyValueService);
        PuncherStore cachingPuncherStore = CachingPuncherStore.create(
                keyValuePuncherStore,
                PUNCH_INTERVAL_MILLIS * 3);
        Clock clock = GlobalClock.create(lockService);
        SimplePuncher simplePuncher = SimplePuncher.create(
                cachingPuncherStore,
                clock,
                readTimeoutSupplier);
        return AsyncPuncher.create(simplePuncher, PUNCH_INTERVAL_MILLIS);
    }

    private Scrubber buildScrubber(Supplier<Long> unreadableTimestampSupplier,
                          Supplier<Long> immutableTimestampSupplier) {
        ScrubberStore scrubberStore = KeyValueServiceScrubberStore.create(keyValueService);
        Supplier<Long> backgroundScrubFrequencyMillisSupplier = Suppliers.ofInstance(AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_FREQUENCY_MILLIS);
        return Scrubber.create(
                keyValueService,
                scrubberStore,
                backgroundScrubFrequencyMillisSupplier,
                unreadableTimestampSupplier,
                immutableTimestampSupplier,
                transactionService,
                DEFAULT_AGGRESSIVE_SCRUB,
                batchSizeSupplier,
                DEFAULT_SCRUB_THREADS,
                followerList);
    }

    public Cleaner buildCleaner() {
        Puncher puncher = buildPuncher();
        Supplier<Long> immutableTs = ImmutableTimestampSupplier.createMemoizedWithExpiration(lockService, timestampService, lockClient);
        Scrubber scrubber = buildScrubber(puncher.getTimestampSupplier(), immutableTs);
        return new SimpleCleaner(scrubber, puncher, readTimeoutSupplier);
    }
}
