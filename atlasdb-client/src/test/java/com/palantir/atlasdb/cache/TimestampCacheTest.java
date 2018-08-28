/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cache;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.SortedMap;

import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class TimestampCacheTest {
    private static final String TEST_CACHE_NAME = MetricRegistry.name(TimestampCacheTest.class, "test");

    private final MetricRegistry metrics = new MetricRegistry();

    @Test
    public void cacheExposesMetrics() throws Exception {
        Cache<Long, Long> cache = TimestampCache.createCache(AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
        AtlasDbMetrics.registerCache(metrics, cache, TEST_CACHE_NAME);

        TimestampCache timestampCache = new TimestampCache(cache);

        SortedMap<String, Gauge> gauges = metrics.getGauges(startsWith(TimestampCache.class.getName()));
        assertThat(gauges.keySet(), hasItems(cacheMetricName("hit.count"), cacheMetricName("miss.ratio")));

        assertThat(timestampCache.getCommitTimestampIfPresent(1L), is(nullValue()));

        timestampCache.putAlreadyCommittedTransaction(1L, 2L);

        assertThat(timestampCache.getCommitTimestampIfPresent(1L), is(2L));
        assertThat(timestampCache.getCommitTimestampIfPresent(1L), is(2L));
        assertThat(timestampCache.getCommitTimestampIfPresent(1L), is(2L));

        timestampCache.clear();

        assertThat(timestampCache.getCommitTimestampIfPresent(1L), is(nullValue()));

        assertThat(gauges.get(cacheMetricName("hit.count")).getValue(), equalTo(3L));
        assertThat(gauges.get(cacheMetricName("hit.ratio")).getValue(), equalTo(0.6d));
        assertThat(gauges.get(cacheMetricName("miss.count")).getValue(), equalTo(2L));
        assertThat(gauges.get(cacheMetricName("miss.ratio")).getValue(), equalTo(0.4d));
        assertThat(gauges.get(cacheMetricName("request.count")).getValue(), equalTo(5L));
    }

    private static String cacheMetricName(String name) {
        return TEST_CACHE_NAME + ".cache." + name;
    }

    private MetricFilter startsWith(String prefix) {
        return (name, metric) -> name.startsWith(prefix);
    }

}
