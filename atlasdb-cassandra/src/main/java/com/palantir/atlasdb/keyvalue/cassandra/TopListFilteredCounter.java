/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.Counter;
import com.google.common.collect.Sets;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import one.util.streamex.EntryStream;

final class TopListFilteredCounter<T> {
    private static final SafeLogger log = SafeLoggerFactory.get(TopListFilteredCounter.class);

    private final int maxSize;
    private final TaggedMetricRegistry registry;
    private final Function<T, MetricName> tagToMetricName;
    private final Comparator<Entry<T, Long>> entryComparator;

    private final Map<T, Counter> counters = new ConcurrentHashMap<>();
    private final Set<T> reportedTags = ConcurrentHashMap.newKeySet();

    private TopListFilteredCounter(
            int maxSize,
            TaggedMetricRegistry registry,
            Function<T, MetricName> tagToMetricName,
            Comparator<Entry<T, Long>> entryComparator) {
        this.maxSize = maxSize;
        this.registry = registry;
        this.tagToMetricName = tagToMetricName;
        this.entryComparator = entryComparator;
    }

    static <T> TopListFilteredCounter<T> create(
            int maxSize,
            Duration initialDelay,
            Duration resetInterval,
            Function<T, MetricName> tagToMetricName,
            Comparator<T> tagComparator,
            TaggedMetricRegistry registry,
            ScheduledExecutorService executor) {
        // the tag comparator is used to provide a stable ordering of tags when they have the same count
        Comparator<Entry<T, Long>> entryComparator =
                Entry.<T, Long>comparingByValue(Comparator.reverseOrder()).thenComparing(Entry::getKey, tagComparator);

        TopListFilteredCounter<T> counter =
                new TopListFilteredCounter<>(maxSize, registry, tagToMetricName, entryComparator);
        executor.scheduleAtFixedRate(
                counter::resilientUpdateMetricsReporting,
                initialDelay.toNanos(),
                resetInterval.toNanos(),
                TimeUnit.NANOSECONDS);

        return counter;
    }

    void inc(T tag, long count) {
        Counter counter = counters.computeIfAbsent(tag, unused -> new Counter());
        counter.inc(count);
    }

    private void resilientUpdateMetricsReporting() {
        try {
            updateMetricsReporting();
        } catch (RuntimeException e) {
            log.error("Failed to update top listed counter metric", e);
        }
    }

    private void updateMetricsReporting() {
        Set<T> nextBatchOfTagsToReport = computeTopListTags();

        deregisterTagsForMetricsProduction(Sets.difference(reportedTags, nextBatchOfTagsToReport));
        registerTagsForMetricsProduction(Sets.difference(nextBatchOfTagsToReport, reportedTags));

        reportedTags.clear();
        reportedTags.addAll(nextBatchOfTagsToReport);
    }

    private void registerTagsForMetricsProduction(Set<T> tags) {
        tags.forEach(this::registerTag);
    }

    private void deregisterTagsForMetricsProduction(Set<T> tags) {
        tags.forEach(this::deregisterTag);
    }

    private void registerTag(T tag) {
        MetricName metricName = tagToMetricName.apply(tag);
        Counter counter = counters.get(tag);
        // registers the counter only if no other counter is currently tracked for this metric name
        registry.counter(metricName, () -> counter);
    }

    private void deregisterTag(T tag) {
        MetricName metricName = tagToMetricName.apply(tag);
        registry.remove(metricName);
    }

    private Set<T> computeTopListTags() {
        Map<T, Long> counterSnapshots =
                EntryStream.of(counters).mapValues(Counter::getCount).toMap();

        return counterSnapshots.entrySet().stream()
                .sorted(entryComparator)
                .limit(maxSize)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }
}
