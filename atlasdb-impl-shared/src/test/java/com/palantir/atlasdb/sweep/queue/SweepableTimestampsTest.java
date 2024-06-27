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
package com.palantir.atlasdb.sweep.queue;

import static com.palantir.atlasdb.sweep.queue.ShardAndStrategy.conservative;
import static com.palantir.atlasdb.sweep.queue.ShardAndStrategy.thorough;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.maxTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.minTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.sweep.Sweeper;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SweepableTimestampsTest extends AbstractSweepQueueTest {
    private ShardProgress progress;
    private SweepableTimestamps sweepableTimestamps;

    @BeforeEach
    @Override
    public void setup() {
        super.setup();
        progress = new ShardProgress(spiedKvs);
        sweepableTimestamps = new SweepableTimestamps(spiedKvs, partitioner);
        shardCons = writeToDefaultCellCommitted(sweepableTimestamps, TS, TABLE_CONS);
        shardThor = writeToDefaultCellCommitted(sweepableTimestamps, TS2, TABLE_THOR);
    }

    @Test
    public void canReadNextTimestampWhenSweepTsIsLarge() {
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void cannotReadForWrongSweepStrategy() {
        assertThat(readThorough(shardCons)).isEmpty();
        assertThat(readConservative(shardThor)).isEmpty();
    }

    @Test
    public void cannotReadForWrongShard() {
        assertThat(readConservative(shardCons + 1)).isEmpty();
        assertThat(readThorough(shardThor + 1)).isEmpty();
    }

    @Test
    public void canReadForAbortedTransactionMultipleTimes() {
        long timestamp = 2 * TS_FINE_GRANULARITY + 1L;
        writeToDefaultCellAborted(sweepableTimestamps, timestamp, TABLE_CONS);
        assertThat(readConservative(CONS_SHARD)).contains(tsPartitionFine(timestamp));
        assertThat(readConservative(CONS_SHARD)).contains(tsPartitionFine(timestamp));
    }

    @Test
    public void canReadForUncommittedTransactionMultipleTimes() {
        long timestamp = 3 * TS_FINE_GRANULARITY + 1L;
        writeToDefaultCellUncommitted(sweepableTimestamps, timestamp, TABLE_CONS);
        assertThat(readConservative(CONS_SHARD)).contains(tsPartitionFine(timestamp));
        assertThat(readConservative(CONS_SHARD)).contains(tsPartitionFine(timestamp));
    }

    @Test
    public void canReadNextTsForTombstone() {
        long timestamp = 10L;
        putTombstoneToDefaultCommitted(sweepableTimestamps, timestamp, TABLE_CONS);
        assertThat(readConservative(CONS_SHARD)).contains(tsPartitionFine(timestamp));
    }

    @Test
    public void noNextTimestampWhenImmutableTsInSmallerPartitionForEitherSweepStrategy() {
        immutableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isLessThan(TS_FINE_PARTITION);
        assertThat(readConservative(shardCons)).isEmpty();

        assertThat(tsPartitionFine(immutableTs)).isLessThan(TS2_FINE_PARTITION);
        assertThat(readThorough(shardThor)).isEmpty();
    }

    @Test
    public void noNextTimestampWhenUnreadableTsInSmallerPartitionForConservativeOnly() {
        unreadableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(TS_FINE_PARTITION);
        assertThat(readConservative(shardCons)).isEmpty();

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(TS2_FINE_PARTITION);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void noNextTimestampWhenSweepTimestampInSamePartitionAndLower() {
        immutableTs = minTsForFinePartition(TS_FINE_PARTITION);

        assertThat(tsPartitionFine(getSweepTsCons())).isEqualTo(TS_FINE_PARTITION);
        assertThat(getSweepTsCons()).isLessThan(TS);
        assertThat(readConservative(shardCons)).isEmpty();
    }

    @Test
    public void canReadNextTimestampWhenSweepTimestampInSamePartitionAndGreater() {
        immutableTs = maxTsForFinePartition(TS_FINE_PARTITION);

        assertThat(tsPartitionFine(getSweepTsCons())).isEqualTo(TS_FINE_PARTITION);
        assertThat(getSweepTsCons()).isGreaterThan(TS);
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForConservative() {
        progress.updateLastSweptTimestamp(conservative(shardCons), TS - 1);
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void canReadNextTimestampIfProgressedBeyondButInSamePartitionForConservative() {
        progress.updateLastSweptTimestamp(conservative(shardCons), maxTsForFinePartition(TS_FINE_PARTITION) - 1);
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void noNextTimestampIfProgressedToEndOfPartitionForConservative() {
        progress.updateLastSweptTimestamp(conservative(shardCons), maxTsForFinePartition(TS_FINE_PARTITION));
        assertThat(readConservative(shardCons)).isEmpty();
    }

    @Test
    public void canReadNextWhenOtherShardsAndStrategiesProgressToEndOfPartitionForConservative() {
        progress.updateLastSweptTimestamp(thorough(shardCons), maxTsForFinePartition(TS_FINE_PARTITION));
        progress.updateLastSweptTimestamp(conservative(shardThor), maxTsForFinePartition(TS_FINE_PARTITION));
        progress.updateLastSweptTimestamp(thorough(shardThor), maxTsForFinePartition(TS_FINE_PARTITION));
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardThor), TS2 - 1);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void canReadNextTimestampIfProgressedBeyondButInSamePartitionForForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardThor), maxTsForFinePartition(TS2_FINE_PARTITION) - 1);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void noNextTimestampIfProgressedToEndOfPartitionForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardThor), maxTsForFinePartition(TS2_FINE_PARTITION));
        assertThat(readThorough(shardThor)).isEmpty();
    }

    @Test
    public void canReadNextWhenOtherShardsAndStrategiesProgressToEndOfPartitionForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardCons), maxTsForFinePartition(TS2_FINE_PARTITION));
        progress.updateLastSweptTimestamp(conservative(shardThor), maxTsForFinePartition(TS2_FINE_PARTITION));
        progress.updateLastSweptTimestamp(conservative(shardCons), maxTsForFinePartition(TS2_FINE_PARTITION));
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void getCorrectNextTimestampWhenMultipleCandidates() {
        for (long timestamp = 1000L; tsPartitionFine(timestamp) < 10L; timestamp += TS_FINE_GRANULARITY / 5) {
            writeToDefaultCellCommitted(sweepableTimestamps, timestamp, TABLE_CONS);
        }
        assertThat(readConservative(shardCons)).contains(tsPartitionFine(1000L));

        progress.updateLastSweptTimestamp(conservative(shardCons), 2L * TS_FINE_GRANULARITY);
        assertThat(readConservative(shardCons)).contains(tsPartitionFine(2L * TS_FINE_GRANULARITY + 1000L));

        setSweepTimestampAndGet(4 * TS_FINE_GRANULARITY);
        assertThat(readConservative(shardCons)).contains(tsPartitionFine(2L * TS_FINE_GRANULARITY + 1000L));
    }

    @Test
    public void canReadNonSweepable() {
        writeToDefaultCellCommitted(sweepableTimestamps, 100L, TABLE_NOTH);
        assertThat(readNonSweepable()).hasValue(0L);
    }

    @Test
    public void noClashWithNonSweepable() {
        writeToDefaultCellCommitted(sweepableTimestamps, 250L, TABLE_CONS);
        writeToDefaultCellUncommitted(sweepableTimestamps, 50_250L, TABLE_THOR);
        writeToDefaultCellAborted(sweepableTimestamps, 100_250L, TABLE_NOTH);

        assertThat(readConservative(shardCons)).hasValue(0L);
        assertThat(readNonSweepable()).hasValue(2L);
        assertThat(readThorough(shardThor)).hasValue(1L);
    }

    @Test
    public void nonSweepableRespectsProgress() {
        writeToDefaultCellUncommitted(sweepableTimestamps, 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 50_250L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 150_250L, TABLE_NOTH);

        setSweepTimestampAndGet(100_000L);

        assertThat(readNonSweepable()).hasValue(0L);
        assertThat(readNonSweepable()).hasValue(0L);
        progress.updateLastSweptTimestamp(ShardAndStrategy.nonSweepable(), 49_999L);
        assertThat(readNonSweepable()).hasValue(1L);
    }

    @Test
    public void readMultiplePartitionsFollowsRequestLimit() {
        writeToDefaultCellUncommitted(sweepableTimestamps, 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 2 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 3 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 4 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 6 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);

        setSweepTimestampAndGet(Long.MAX_VALUE);
        assertThat(readMultipleNonSweepable(1000)).containsExactly(0L, 2L, 3L, 4L, 6L);
        assertThat(readMultipleNonSweepable(3)).containsExactly(0L, 2L, 3L);
        assertThat(readMultipleNonSweepable(2)).containsExactly(0L, 2L);
        assertThat(readMultipleNonSweepable(1)).containsExactly(0L);
    }

    @Test
    public void readMultiplePartitionsDoesNotSelectPartitionsAfterTheSweepTimestamp() {
        writeToDefaultCellUncommitted(sweepableTimestamps, 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 2 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 3 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 4 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 6 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);

        setSweepTimestampAndGet(3 * TS_FINE_GRANULARITY - 1);
        assertThat(readMultipleNonSweepable(1000)).containsExactly(0L, 2L);

        setSweepTimestampAndGet(3 * TS_FINE_GRANULARITY);
        assertThat(readMultipleNonSweepable(1000))
                .as("partition 3 should not be considered even if the sweep timestamp is at its head, since"
                        + " only values strictly before the sweep timestamp are eligible for sweeping")
                .containsExactly(0L, 2L);

        setSweepTimestampAndGet(3 * TS_FINE_GRANULARITY + 1);
        assertThat(readMultipleNonSweepable(1000))
                .as("partition 3 should be considered once the sweep timestamp has passed its first timestamp")
                .containsExactly(0L, 2L, 3L);
    }

    @Test
    public void readMultiplePartitionsDoesNotSelectPartitionsThatHaveBeenProgressedPast() {
        writeToDefaultCellUncommitted(sweepableTimestamps, 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 2 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 3 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 4 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);
        writeToDefaultCellUncommitted(sweepableTimestamps, 6 * TS_FINE_GRANULARITY + 1L, TABLE_NOTH);

        setSweepTimestampAndGet(Long.MAX_VALUE);

        progress.updateLastSweptTimestamp(ShardAndStrategy.nonSweepable(), TS_FINE_GRANULARITY - 1);
        assertThat(readMultipleNonSweepable(1000)).containsExactly(2L, 3L, 4L, 6L);

        progress.updateLastSweptTimestamp(ShardAndStrategy.nonSweepable(), 3 * TS_FINE_GRANULARITY);
        assertThat(readMultipleNonSweepable(1000)).containsExactly(3L, 4L, 6L);

        progress.updateLastSweptTimestamp(ShardAndStrategy.nonSweepable(), 7 * TS_FINE_GRANULARITY - 2);
        assertThat(readMultipleNonSweepable(1000))
                .as("with a possible value at the end of partition 6, it must still be considered")
                .containsExactly(6L);

        progress.updateLastSweptTimestamp(ShardAndStrategy.nonSweepable(), 7 * TS_FINE_GRANULARITY - 1);
        assertThat(readMultipleNonSweepable(1000))
                .as("as partition 6 has been fully completed, it no longer need be considered")
                .isEmpty();
    }

    // TODO (jkong): Parameterize the above tests, and add support for Thorough and Conservative.
    private Optional<Long> readConservative(int shardNumber) {
        return sweepableTimestamps.nextTimestampPartition(
                conservative(shardNumber),
                progress.getLastSweptTimestamp(ShardAndStrategy.conservative(shardNumber)),
                Sweeper.CONSERVATIVE.getSweepTimestamp(timestampsSupplier));
    }

    private Optional<Long> readThorough(int shardNumber) {
        return sweepableTimestamps.nextTimestampPartition(
                thorough(shardNumber),
                progress.getLastSweptTimestamp(ShardAndStrategy.thorough(shardNumber)),
                Sweeper.THOROUGH.getSweepTimestamp(timestampsSupplier));
    }

    private Optional<Long> readNonSweepable() {
        return sweepableTimestamps.nextTimestampPartition(
                ShardAndStrategy.nonSweepable(),
                progress.getLastSweptTimestamp(ShardAndStrategy.nonSweepable()),
                Sweeper.NO_OP.getSweepTimestamp(timestampsSupplier));
    }

    private List<Long> readMultipleNonSweepable(int limit) {
        return sweepableTimestamps.nextTimestampPartitions(
                ShardAndStrategy.nonSweepable(),
                progress.getLastSweptTimestamp(ShardAndStrategy.nonSweepable()),
                Sweeper.NO_OP.getSweepTimestamp(timestampsSupplier),
                limit);
    }

    private long setSweepTimestampAndGet(long timestamp) {
        immutableTs = timestamp;
        return Sweeper.CONSERVATIVE.getSweepTimestamp(timestampsSupplier);
    }
}
