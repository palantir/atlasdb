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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.MillisAndMaybeTimestamp;
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
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.util.AggregatingVersionedMetric;
import com.palantir.util.AggregatingVersionedSupplier;
import com.palantir.util.CachedComposedSupplier;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.immutables.value.Value;

@SuppressWarnings("checkstyle:FinalClass") // non-final for mocking
public class TargetedSweepMetrics {
    private static final SafeLogger log = SafeLoggerFactory.get(TargetedSweepMetrics.class);
    private final Map<SweeperStrategy, MetricsForStrategy> metricsForStrategyMap;

    private TargetedSweepMetrics(
            MetricsManager metricsManager,
            BiFunction<Long, MillisAndMaybeTimestamp, MillisAndMaybeTimestamp> tsToMillis,
            Clock clock,
            MetricsConfiguration metricsConfiguration,
            int shards) {
        metricsForStrategyMap = KeyedStream.of(metricsConfiguration.trackedSweeperStrategies())
                .map(TargetedSweepMetrics::getTagForStrategy)
                .map(strategyTag -> new MetricsForStrategy(
                        metricsManager, strategyTag, tsToMillis, clock, metricsConfiguration, shards))
                .collectToMap();
    }

    public static TargetedSweepMetrics create(
            MetricsManager metricsManager,
            TimelockService timelock,
            KeyValueService kvs,
            MetricsConfiguration metricsConfiguration,
            int shards) {
        return createWithClock(metricsManager, kvs, timelock::currentTimeMillis, metricsConfiguration, shards);
    }

    @VisibleForTesting
    static TargetedSweepMetrics createWithClock(
            MetricsManager metricsManager,
            KeyValueService kvs,
            Clock clock,
            MetricsConfiguration metricsConfiguration,
            int shards) {
        return new TargetedSweepMetrics(
                metricsManager,
                (ts, previous) -> getMillisForTimestamp(kvs, ts, clock, previous),
                clock,
                metricsConfiguration,
                shards);
    }

    private static MillisAndMaybeTimestamp getMillisForTimestamp(
            KeyValueService kvs, long ts, Clock clock, MillisAndMaybeTimestamp previous) {
        return KeyValueServicePuncherStore.getMillisForTimestampWithinBounds(kvs, ts, previous, clock.getTimeMillis())
                .orElse(null);
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
        private final AggregatingVersionedMetric<TimestampAndShard> lastSweptTsWithShard;
        private final Map<String, Gauge<Long>> millisSinceLastSwept;
        private final SweepOutcomeMetrics outcomeMetrics;
        private final SlidingWindowMeanGauge batchSizeMean;
        private final CurrentValueMetric<Long> sweepDelayMetric;
        private final Map<Integer, MillisAndMaybeTimestamp> lastMillisAndTsPerShard = new ConcurrentHashMap<>();

        private MetricsForStrategy(
                MetricsManager manager,
                String strategy,
                BiFunction<Long, MillisAndMaybeTimestamp, MillisAndMaybeTimestamp> tsToMillis,
                Clock wallClock,
                MetricsConfiguration metricsConfig,
                int shards) {
            tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
            this.manager = manager;
            enqueuedWrites = new AccumulatingValueMetric();
            entriesRead = new AccumulatingValueMetric();
            tombstonesPut = new AccumulatingValueMetric();
            abortedWritesDeleted = new AccumulatingValueMetric();
            sweepTimestamp = new CurrentValueMetric<>();
            lastSweptTsWithShard = createLastSweptTsMetric(metricsConfig.millisBetweenRecomputingMetrics());
            millisSinceLastSwept = createMillisSinceLastSweptMetric(tsToMillis, wallClock, metricsConfig, shards);

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
                            .putSafeTags("libraryName", "atlasdb")
                            .putSafeTags("libraryVersion", "unknown")
                            .build())
                    .forEach(metricName -> manager.addMetricFilter(metricName, filter));

            millisSinceLastSwept
                    .keySet()
                    .forEach(shard -> manager.addMetricFilter(
                            getProgressMetricNameBuilder(strategy)
                                    .putSafeTags("shard", shard)
                                    .putSafeTags("libraryName", "atlasdb")
                                    .putSafeTags("libraryVersion", "unknown")
                                    .build(),
                            filter));

            TargetedSweepProgressMetrics progressMetrics = TargetedSweepProgressMetrics.of(manager.getTaggedRegistry());
            progressMetrics.enqueuedWrites().strategy(strategy).build(enqueuedWrites);
            progressMetrics.entriesRead().strategy(strategy).build(entriesRead);
            progressMetrics.tombstonesPut().strategy(strategy).build(tombstonesPut);
            progressMetrics.abortedWritesDeleted().strategy(strategy).build(abortedWritesDeleted);
            progressMetrics.sweepTimestamp().strategy(strategy).build(sweepTimestamp);
            progressMetrics.lastSweptTimestamp().strategy(strategy).build(() -> extractTimestamp(lastSweptTsWithShard));
            millisSinceLastSwept.forEach((shard, gauge) -> progressMetrics
                    .millisSinceLastSweptTs()
                    .strategy(strategy)
                    .shard(shard)
                    .build(gauge));
            progressMetrics.batchSizeMean().strategy(strategy).build(batchSizeMean);
            progressMetrics.sweepDelay().strategy(strategy).build(sweepDelayMetric);
        }

        private static MetricName.Builder getProgressMetricNameBuilder(String strategy) {
            return MetricName.builder()
                    .safeName("targetedSweepProgress." + AtlasDbMetricNames.LAG_MILLIS)
                    .putSafeTags("strategy", strategy);
        }

        private TargetedSweepMetricPublicationFilter createMetricPublicationFilter() {
            return new TargetedSweepMetricPublicationFilter(ImmutableDecisionMetrics.builder()
                    .enqueuedWrites(enqueuedWrites::getValue)
                    .entriesRead(entriesRead::getValue)
                    .millisSinceLastSweptTs(() -> millisSinceLastSwept.values().stream()
                            .map(Gauge::getValue)
                            .filter(Objects::nonNull)
                            .max(Long::compareTo)
                            .orElse(0L))
                    .build());
        }

        private AggregatingVersionedMetric<TimestampAndShard> createLastSweptTsMetric(long millis) {
            AggregatingVersionedSupplier<TimestampAndShard> lastSweptTimestamp =
                    AggregatingVersionedSupplier.min(millis);
            return new AggregatingVersionedMetric<>(lastSweptTimestamp);
        }

        private Long extractTimestamp(Gauge<TimestampAndShard> gauge) {
            TimestampAndShard result = gauge.getValue();
            return Optional.ofNullable(result).map(TimestampAndShard::timestamp).orElse(null);
        }

        private Map<String, Gauge<Long>> createMillisSinceLastSweptMetric(
                BiFunction<Long, MillisAndMaybeTimestamp, MillisAndMaybeTimestamp> tsToMillis,
                Clock wallClock,
                MetricsConfiguration metricsConfiguration,
                int shards) {
            if (metricsConfiguration.trackSweepLagPerShard().stream()
                    .map(TargetedSweepMetrics::getTagForStrategy)
                    .anyMatch(strategy -> strategy.equals(tag.get(AtlasDbMetricNames.TAG_STRATEGY)))) {
                return KeyedStream.of(IntStream.range(0, shards).boxed())
                        .<Gauge<Long>>map(shard -> () -> estimateMillisSinceTs(
                                lastSweptTsWithShard.getLastValueForKey(shard), wallClock, tsToMillis))
                        .mapKeys(shard -> Integer.toString(shard))
                        .collectToMap();
            } else {
                Supplier<Long> millisSinceLastSweptTs = new CachedComposedSupplier<>(
                        shardAndTs -> estimateMillisSinceTs(shardAndTs, wallClock, tsToMillis),
                        lastSweptTsWithShard::getVersionedValue,
                        metricsConfiguration.millisBetweenRecomputingMetrics(),
                        wallClock);
                return ImmutableMap.of(AtlasDbMetricNames.TAG_CUMULATIVE, millisSinceLastSweptTs::get);
            }
        }

        private Long estimateMillisSinceTs(
                TimestampAndShard shardAndTs,
                Clock clock,
                BiFunction<Long, MillisAndMaybeTimestamp, MillisAndMaybeTimestamp> tsToMillis) {
            if (shardAndTs == null) {
                log.info(
                        "Encountered null value for shard and last swept timestamp when recomputing sweep lag metric "
                                + "for strategy {}. This is only expected before the first iteration of sweep "
                                + "completes.",
                        SafeArg.of("sweepStrategy", tag.get(AtlasDbMetricNames.TAG_STRATEGY)));
                return null;
            }
            long timeBeforeRecomputing = System.currentTimeMillis();
            MillisAndMaybeTimestamp lastKnown = lastMillisAndTsPerShard.get(shardAndTs.shard());
            MillisAndMaybeTimestamp millisAndMaybeTs = tsToMillis.apply(shardAndTs.timestamp(), lastKnown);
            if (millisAndMaybeTs == null) {
                log.warn(
                        "Could not calculate the sweep lag metric for strategy {}, shard {}, last swept timestamp {}."
                                + " Last known punch entry for this shard was {}.",
                        SafeArg.of("sweepStrategy", tag.get(AtlasDbMetricNames.TAG_STRATEGY)),
                        SafeArg.of("shard", shardAndTs.shard()),
                        SafeArg.of("timestamp", shardAndTs.timestamp()),
                        SafeArg.of("lastKnownPunchEntry", lastKnown));
                return null;
            }
            lastMillisAndTsPerShard.put(shardAndTs.shard(), millisAndMaybeTs);
            long result = clock.getTimeMillis() - millisAndMaybeTs.millis();

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
            lastSweptTsWithShard.update(shard, ImmutableTimestampAndShard.of(sweptTs, shard));
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
        /**
         * Which sweeper strategies metrics should be tracked for (defaults to all known sweeper strategies). Legitimate
         * use cases for overriding this include products where there is no or extremely minimal usage of tables with a
         * given sweep strategy (e.g. AtlasDB-Proxy in contexts where the stream store is not used).
         */
        @Value.Default
        default Set<SweeperStrategy> trackedSweeperStrategies() {
            return ImmutableSet.copyOf(SweeperStrategy.values());
        }

        /**
         * Milliseconds to pause between recomputing computationally intensive sweep metrics (that may require database
         * reads or remote calls).
         */
        @Value.Default
        default long millisBetweenRecomputingMetrics() {
            return SweepQueueUtils.REFRESH_TIME;
        }

        /**
         * Report {@link AtlasDbMetricNames#LAG_MILLIS} per shard instead of cumulative for the specified strategies.
         * This should generally be used if targeted sweep falls behind and we subsequently increase the number of
         * shards, as that can result in an imbalance that makes the standard metric.
         */
        @Value.Default
        default Set<SweeperStrategy> trackSweepLagPerShard() {
            return ImmutableSet.of();
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

    @Value.Immutable
    public interface TimestampAndShard extends Comparable<TimestampAndShard> {
        Comparator<TimestampAndShard> COMPARATOR =
                Comparator.comparing(TimestampAndShard::timestamp).thenComparing(TimestampAndShard::shard);

        @Value.Parameter
        long timestamp();

        @Value.Parameter
        int shard();

        @Override
        default int compareTo(TimestampAndShard other) {
            return COMPARATOR.compare(this, other);
        }
    }
}
