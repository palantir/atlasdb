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
package com.palantir.atlasdb.sweep.metrics;

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.SlidingWindowMeanGauge;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.time.Clock;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.util.AggregatingVersionedMetric;
import com.palantir.util.AggregatingVersionedSupplier;
import com.palantir.util.CachedComposedSupplier;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:FinalClass") // non-final for mocking
public class TargetedSweepMetrics {
    private static final Logger log = LoggerFactory.getLogger(TargetedSweepMetrics.class);
    private static final long ONE_WEEK = TimeUnit.DAYS.toMillis(7L);
    private final Map<SweeperStrategy, MetricsForStrategy> metricsForStrategyMap;

    private TargetedSweepMetrics(
            MetricsManager metricsManager,
            Function<Long, Long> tsToMillis,
            Clock clock,
            MetricsConfiguration metricsConfiguration) {
        metricsForStrategyMap = KeyedStream.of(metricsConfiguration.trackedSweeperStrategies())
                .map(TargetedSweepMetrics::getTagForStrategy)
                .map(strategyTag -> new MetricsForStrategy(
                        metricsManager,
                        strategyTag,
                        tsToMillis,
                        clock,
                        metricsConfiguration.millisBetweenRecomputingMetrics()))
                .collectToMap();
    }

    public static TargetedSweepMetrics create(
            MetricsManager metricsManager,
            TimelockService timelock,
            KeyValueService kvs,
            MetricsConfiguration metricsConfiguration) {
        return createWithClock(metricsManager, kvs, timelock::currentTimeMillis, metricsConfiguration);
    }

    public static TargetedSweepMetrics createWithClock(
            MetricsManager metricsManager,
            KeyValueService kvs,
            Clock clock,
            MetricsConfiguration metricsConfiguration) {
        return new TargetedSweepMetrics(
                metricsManager,
                ts -> getMillisForTimestampBoundedAtOneWeek(kvs, ts, clock),
                clock,
                metricsConfiguration);
    }

    private static long getMillisForTimestampBoundedAtOneWeek(KeyValueService kvs, long ts, Clock clock) {
        return KeyValueServicePuncherStore.getMillisForTimestampIfNotPunchedBefore(
                kvs, ts, clock.getTimeMillis() - ONE_WEEK);
    }

    public void updateEnqueuedWrites(ShardAndStrategy shardStrategy, long writes) {
        updateMetricsIfPresent(shardStrategy, metrics -> metrics.updateEnqueuedWrites(writes));
    }

    public void updateEntriesRead(ShardAndStrategy shardStrategy, long reads) {
        updateMetricsIfPresent(shardStrategy, metrics -> metrics.updateEntriesRead(reads));
    }

    public void updateNumberOfTombstones(ShardAndStrategy shardStrategy, long tombstones) {
        updateMetricsIfPresent(shardStrategy, metrics -> metrics.updateNumberOfTombstones(tombstones));
    }

    public void updateAbortedWritesDeleted(ShardAndStrategy shardStrategy, long deletes) {
        updateMetricsIfPresent(shardStrategy, metrics -> metrics.updateAbortedWritesDeleted(deletes));
    }

    public void updateSweepTimestamp(ShardAndStrategy shardStrategy, long value) {
        updateMetricsIfPresent(shardStrategy, metrics -> metrics.updateSweepTimestamp(value));
    }

    public void updateProgressForShard(ShardAndStrategy shardStrategy, long lastSweptTs) {
        updateMetricsIfPresent(
                shardStrategy, metrics -> metrics.updateProgressForShard(shardStrategy.shard(), lastSweptTs));
    }

    public void registerOccurrenceOf(ShardAndStrategy shardStrategy, SweepOutcome outcome) {
        updateMetricsIfPresent(shardStrategy, metrics -> metrics.registerOccurrenceOf(outcome));
    }

    public void registerOccurrenceOf(SweeperStrategy strategy, SweepOutcome outcome) {
        updateMetricsIfPresent(strategy, metrics -> metrics.registerOccurrenceOf(outcome));
    }

    public void registerEntriesReadInBatch(ShardAndStrategy shardStrategy, long batchSize) {
        updateMetricsIfPresent(shardStrategy, metrics -> metrics.registerEntriesReadInBatch(batchSize));
    }

    public void updateSweepDelayMetric(SweeperStrategy strategy, long delay) {
        updateMetricsIfPresent(strategy, metrics -> metrics.updateSweepDelayMetric(delay));
    }

    private void updateMetricsIfPresent(ShardAndStrategy shardStrategy, Consumer<MetricsForStrategy> update) {
        updateMetricsIfPresent(shardStrategy.strategy(), update);
    }

    private void updateMetricsIfPresent(SweeperStrategy strategy, Consumer<MetricsForStrategy> update) {
        Optional.ofNullable(getMetrics(strategy)).ifPresent(update);
    }

    private MetricsForStrategy getMetrics(SweeperStrategy strategy) {
        return metricsForStrategyMap.get(strategy);
    }

    private static String getTagForStrategy(SweeperStrategy strategy) {
        switch (strategy) {
            case CONSERVATIVE:
                return AtlasDbMetricNames.TAG_CONSERVATIVE;
            case THOROUGH:
                return AtlasDbMetricNames.TAG_THOROUGH;
            default:
                throw new SafeIllegalStateException("Unexpected sweeper strategy", SafeArg.of("strategy", strategy));
        }
    }

    private static final class MetricsForStrategy {
        private final Map<String, String> tag;
        private final MetricsManager manager;
        private final AccumulatingValueMetric enqueuedWrites;
        private final AccumulatingValueMetric entriesRead;
        private final AccumulatingValueMetric tombstonesPut;
        private final AccumulatingValueMetric abortedWritesDeleted;
        private final CurrentValueMetric<Long> sweepTimestamp;
        private final AggregatingVersionedMetric<Long> lastSweptTs;
        private final Gauge<Long> millisSinceLastSwept;
        private final SweepOutcomeMetrics outcomeMetrics;
        private final SlidingWindowMeanGauge batchSizeMean;
        private final CurrentValueMetric<Long> sweepDelayMetric;

        private MetricsForStrategy(
                MetricsManager manager,
                String strategy,
                Function<Long, Long> tsToMillis,
                Clock wallClock,
                long recomputeMillis) {
            tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
            this.manager = manager;
            enqueuedWrites = new AccumulatingValueMetric();
            entriesRead = new AccumulatingValueMetric();
            tombstonesPut = new AccumulatingValueMetric();
            abortedWritesDeleted = new AccumulatingValueMetric();
            sweepTimestamp = new CurrentValueMetric<>();
            lastSweptTs = createLastSweptTsMetric(recomputeMillis);
            millisSinceLastSwept = createMillisSinceLastSweptMetric(tsToMillis, wallClock, recomputeMillis);
            batchSizeMean = new SlidingWindowMeanGauge();
            sweepDelayMetric = new CurrentValueMetric<>();

            TargetedSweepMetricPublicationFilter filter = createMetricPublicationFilter();
            registerProgressMetrics(strategy, filter);
            outcomeMetrics = SweepOutcomeMetrics.registerTargeted(manager, tag, filter);
        }

        private void registerProgressMetrics(String strategy, TargetedSweepMetricPublicationFilter filter) {
            // This is kind of against the point of metrics-filter, but is needed for our filtering
            AtlasDbMetricNames.TARGETED_SWEEP_PROGRESS_METRIC_NAMES.stream()
                    .map(operationName -> MetricName.builder()
                            .safeName("targetedSweepProgress." + operationName)
                            .putSafeTags("strategy", strategy)
                            .build())
                    .forEach(metricName -> manager.addMetricFilter(metricName, filter));

            TargetedSweepProgressMetrics progressMetrics = TargetedSweepProgressMetrics.of(manager.getTaggedRegistry());
            progressMetrics.enqueuedWrites().strategy(strategy).build(enqueuedWrites);
            progressMetrics.entriesRead().strategy(strategy).build(entriesRead);
            progressMetrics.tombstonesPut().strategy(strategy).build(tombstonesPut);
            progressMetrics.abortedWritesDeleted().strategy(strategy).build(abortedWritesDeleted);
            progressMetrics.sweepTimestamp().strategy(strategy).build(sweepTimestamp);
            progressMetrics.lastSweptTimestamp().strategy(strategy).build(lastSweptTs);
            progressMetrics.millisSinceLastSweptTs().strategy(strategy).build(millisSinceLastSwept);
            progressMetrics.batchSizeMean().strategy(strategy).build(batchSizeMean);
            progressMetrics.sweepDelay().strategy(strategy).build(sweepDelayMetric);
        }

        private TargetedSweepMetricPublicationFilter createMetricPublicationFilter() {
            return new TargetedSweepMetricPublicationFilter(ImmutableDecisionMetrics.builder()
                    .enqueuedWrites(enqueuedWrites::getValue)
                    .entriesRead(entriesRead::getValue)
                    .millisSinceLastSweptTs(() ->
                            Optional.ofNullable(millisSinceLastSwept.getValue()).orElse(0L))
                    .build());
        }

        private AggregatingVersionedMetric<Long> createLastSweptTsMetric(long millis) {
            AggregatingVersionedSupplier<Long> lastSweptTimestamp = AggregatingVersionedSupplier.min(millis);
            return new AggregatingVersionedMetric<>(lastSweptTimestamp);
        }

        private Gauge<Long> createMillisSinceLastSweptMetric(
                Function<Long, Long> tsToMillis, Clock wallClock, long millis) {
            Supplier<Long> millisSinceLastSweptTs = new CachedComposedSupplier<>(
                    sweptTs -> estimateMillisSinceTs(sweptTs, wallClock, tsToMillis),
                    lastSweptTs::getVersionedValue,
                    millis,
                    wallClock);

            return millisSinceLastSweptTs::get;
        }

        private Long estimateMillisSinceTs(Long sweptTs, Clock clock, Function<Long, Long> tsToMillis) {
            if (sweptTs == null) {
                return null;
            }
            long timeBeforeRecomputing = System.currentTimeMillis();
            long result = clock.getTimeMillis() - tsToMillis.apply(sweptTs);

            long timeTaken = System.currentTimeMillis() - timeBeforeRecomputing;
            if (timeTaken > TimeUnit.SECONDS.toMillis(10)) {
                log.warn("Recomputing the millisSinceLastSwept metric took {} ms.", SafeArg.of("timeTaken", timeTaken));
            } else if (timeTaken > TimeUnit.SECONDS.toMillis(1)) {
                log.info("Recomputing the millisSinceLastSwept metric took {} ms.", SafeArg.of("timeTaken", timeTaken));
            }
            return result;
        }

        private void updateEnqueuedWrites(long writes) {
            enqueuedWrites.accumulateValue(writes);
        }

        private void updateEntriesRead(long writes) {
            entriesRead.accumulateValue(writes);
        }

        private void updateNumberOfTombstones(long tombstones) {
            tombstonesPut.accumulateValue(tombstones);
        }

        private void updateAbortedWritesDeleted(long deletes) {
            abortedWritesDeleted.accumulateValue(deletes);
        }

        private void updateSweepTimestamp(long value) {
            sweepTimestamp.setValue(value);
        }

        private void updateProgressForShard(int shard, long sweptTs) {
            lastSweptTs.update(shard, sweptTs);
        }

        private void registerOccurrenceOf(SweepOutcome outcome) {
            outcomeMetrics.registerOccurrenceOf(outcome);
        }

        private void registerEntriesReadInBatch(long batchSize) {
            batchSizeMean.update(batchSize);
        }

        private void updateSweepDelayMetric(long delay) {
            sweepDelayMetric.setValue(delay);
        }
    }

    @JsonSerialize(as = ImmutableMetricsConfiguration.class)
    @JsonDeserialize(as = ImmutableMetricsConfiguration.class)
    @Value.Immutable
    @SuppressWarnings("ClassInitializationDeadlock")
    public interface MetricsConfiguration {
        @Deprecated
        MetricsConfiguration DEFAULT = ImmutableMetricsConfiguration.builder().build();

        /**
         * Which sweeper strategies metrics should be tracked for (defaults to all known sweeper strategies).
         * Legitimate use cases for overriding this include products where there is no or extremely minimal usage
         * of tables with a given sweep strategy (e.g. AtlasDB-Proxy in contexts where the stream store is not used).
         */
        @Value.Default
        default Set<SweeperStrategy> trackedSweeperStrategies() {
            return ImmutableSet.copyOf(SweeperStrategy.values());
        }

        /**
         * Milliseconds to pause between recomputing computationally intensive sweep metrics (that may require
         * database reads or remote calls).
         */
        @Value.Default
        default long millisBetweenRecomputingMetrics() {
            return SweepQueueUtils.REFRESH_TIME;
        }

        @Value.Check
        default void check() {
            Preconditions.checkState(
                    millisBetweenRecomputingMetrics() >= 0,
                    "Cannot specify negative interval between metric computations!");
        }

        static ImmutableMetricsConfiguration.Builder builder() {
            return ImmutableMetricsConfiguration.builder();
        }
    }
}
