/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.config;

import java.util.Optional;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonDeserialize(as = ImmutableAtlasDbConfig.class)
@JsonSerialize(as = ImmutableAtlasDbConfig.class)
@Value.Immutable
public abstract class AtlasDbConfig {

    public abstract KeyValueServiceConfig keyValueService();

    public abstract Optional<LeaderConfig> leader();

    public abstract Optional<TimeLockClientConfig> timelock();

    public abstract Optional<ServerListConfig> lock();

    public abstract Optional<ServerListConfig> timestamp();

    /**
     * The transaction read timeout is the maximum amount of
     * time a read only transaction can safely run. Read only
     * transactions that run any longer may fail if they attempt
     * to perform additional reads.
     * <p>
     * The benefit of making this smaller is making overwritten
     * data 'unreadable' more quickly. This allows the background
     * sweeper to delete overwritten data sooner.
     */
    @Value.Default
    public long getTransactionReadTimeoutMillis() {
        return AtlasDbConstants.DEFAULT_TRANSACTION_READ_TIMEOUT;
    }

    /**
     * The punch interval is how frequently a row mapping the
     * current wall clock time to the maximum timestamp is
     * recorded.
     * <p>
     * These records allow wall clock times (used by the
     * transaction read timeout) to be translated to timestamps.
     */
    @Value.Default
    public long getPunchIntervalMillis() {
        return AtlasDbConstants.DEFAULT_PUNCH_INTERVAL_MILLIS;
    }

    /**
     * Scrubbing is the process of removing overwritten or deleted
     * cells from the underlying key value store after a hard-delete
     * transaction has committed (as opposed to shadowing such data,
     * which still leaves the data available to transactions that
     * started before the overwrite or deletion).
     * <p>
     * Scrubbing non-aggressively will cause scrubbing to be delayed
     * until the transaction read timeout passes ensuring that no
     * (well behaved, shorter than read timeout) transactions will
     * attempt to read scrubbed data. (Note: Badly behaved transactions
     * that do so will abort with an exception).
     * <p>
     * Scrubbing aggressively will cause the deletion to occur
     * immediately, which will cause any active transactions that
     * attempt to read the deleted cell to abort and fail with an
     * exception.
     */
    @Value.Default
    public boolean backgroundScrubAggressively() {
        return AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_AGGRESSIVELY;
    }

    /**
     * The number of background threads to use to perform scrubbing.
     */
    @Value.Default
    public int getBackgroundScrubThreads() {
        return AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_THREADS;
    }

    /**
     * The number of background threads to use to read from the scrub queue.
     */
    @Value.Default
    public int getBackgroundScrubReadThreads() {
        return AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_READ_THREADS;
    }

    /**
     * The frequency with which the background sweeper runs to clean up
     * cells that have been non-aggressively scrubbed.
     */
    @Value.Default
    public long getBackgroundScrubFrequencyMillis() {
        return AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_FREQUENCY_MILLIS;
    }

    /**
     * The number of cells to scrub per batch by the background scrubber.
     */
    @Value.Default
    public int getBackgroundScrubBatchSize() {
        return AtlasDbConstants.DEFAULT_BACKGROUND_SCRUB_BATCH_SIZE;
    }

    /**
     * If true, a background thread will periodically delete cells that
     * have been overwritten or deleted. This differs from scrubbing
     * because it is an untargeted cleaning process that scans all data
     * looking for cells to delete.
     */
    @Value.Default
    public boolean enableSweep() {
        return AtlasDbConstants.DEFAULT_ENABLE_SWEEP;
    }

    /**
     * The number of milliseconds to wait between each batch of cells
     * processed by the background sweeper.
     */
    @Value.Default
    public long getSweepPauseMillis() {
        return AtlasDbConstants.DEFAULT_SWEEP_PAUSE_MILLIS;
    }

    /**
     * The number of milliseconds to wait between retries when the background sweeper can't delete data, due to the
     * persistent lock being taken.
     */
    @Value.Default
    public long getSweepPersistentLockWaitMillis() {
        return AtlasDbConstants.DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS;
    }

    /**
     * The target number of (cell, timestamp) pairs to examine in a single run of the background sweeper.
     */
    // TODO(gbonik): make this Default after we delete the deprecated options. For now, we need to be able to detect
    // whether the field is present in the configuration file.
    @Nullable
    public abstract Integer getSweepReadLimit();

    /**
     * The target number of candidate (cell, timestamp) pairs to load per batch while sweeping.
     */
    // TODO(gbonik): make this Default after we delete the deprecated options. For now, we need to be able to detect
    // whether the field is present in the configuration file.
    @Nullable
    public abstract Integer getSweepCandidateBatchHint();

    /**
     * The target number of (cell, timestamp) pairs to delete at once while sweeping.
     */
    // TODO(gbonik): make this Default after we delete the deprecated options. For now, we need to be able to detect
    // whether the field is present in the configuration file.
    @Nullable
    public abstract Integer getSweepDeleteBatchHint();

    /**
     * @deprecated Use {@link #getSweepReadLimit()}, {@link #getSweepCandidateBatchHint()}
     * and {@link #getSweepDeleteBatchHint()} instead.
     */
    @Deprecated
    @Nullable
    public abstract Integer getSweepBatchSize();

    /**
     * @deprecated Use {@link #getSweepReadLimit()}, {@link #getSweepCandidateBatchHint()}
     * and {@link #getSweepDeleteBatchHint()} instead.
     */
    @Deprecated
    @Nullable
    public abstract Integer getSweepCellBatchSize();

    /**
     * The time threshold for ProfilingKeyValueService to log a KVS operation for being slow.
     */
    @Value.Default
    public long getKvsSlowLogThresholdMillis() {
        return 1000;
    }

    /**
     * The default lock expiration time for requests to the lock service.
     */
    @Value.Default
    public int getDefaultLockTimeoutSeconds() {
        return AtlasDbConstants.DEFAULT_LOCK_TIMEOUT_SECONDS;
    }

    @Value.Check
    protected final void check() {
        if (leader().isPresent()) {
            Preconditions.checkState(areTimeAndLockConfigsAbsent(),
                    "If the leader block is present, then the lock and timestamp server blocks must both be absent.");
            Preconditions.checkState(!timelock().isPresent(),
                    "If the leader block is present, then the timelock block must be absent.");
        }

        if (timelock().isPresent()) {
            Preconditions.checkState(areTimeAndLockConfigsAbsent(),
                    "If the timelock block is present, then the lock and timestamp blocks must both be absent.");
        }

        Preconditions.checkState(lock().isPresent() == timestamp().isPresent(),
                "Lock and timestamp server blocks must either both be present or both be absent.");
        if (getSweepBatchSize() != null || getSweepCellBatchSize() != null) {
            Preconditions.checkState(
                    getSweepReadLimit() == null
                            && getSweepCandidateBatchHint() == null
                            && getSweepDeleteBatchHint() == null,
                    "Your configuration mixes both the old and the new parameters"
                            + " for setting sweep batch sizes. Please use 'sweepMaxCellTsPairsToExamine',"
                            + " 'sweepCandidateBatchSize' and 'sweepDeleteBatchSize' instead of the deprecated"
                            + " 'sweepBatchSize' and 'sweepCellBatchSize'.");
        }
    }

    private boolean areTimeAndLockConfigsAbsent() {
        return !lock().isPresent() && !timestamp().isPresent();
    }

    @JsonIgnore
    public AtlasDbConfig toOfflineConfig() {
        return ImmutableAtlasDbConfig.builder()
                .from(this)
                .leader(Optional.empty())
                .lock(Optional.empty())
                .timestamp(Optional.empty())
                .build();
    }
}
