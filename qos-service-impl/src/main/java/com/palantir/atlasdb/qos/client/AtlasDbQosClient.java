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
package com.palantir.atlasdb.qos.client;

import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QosMetrics;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiter;

public class AtlasDbQosClient implements QosClient {

    private static final Logger log = LoggerFactory.getLogger(AtlasDbQosClient.class);

    private final QosRateLimiter rateLimiter;
    private final QosMetrics metrics;

    public AtlasDbQosClient(QosRateLimiter rateLimiter, QosMetrics metrics) {
        this.metrics = metrics;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public <T, E extends Exception> T executeRead(
            Supplier<Integer> estimatedWeigher,
            ReadQuery<T, E> query,
            Function<T, Integer> weigher) throws E {
        int estimatedWeight = getWeight(estimatedWeigher, 1);
        rateLimiter.consumeWithBackoff(estimatedWeight);

        // TODO(nziebart): decide what to do if we encounter a timeout exception
        T result = query.execute();

        int actualWeight = getWeight(() -> weigher.apply(result), estimatedWeight);
        metrics.updateReadCount();
        metrics.updateBytesRead(actualWeight);
        rateLimiter.recordAdjustment(actualWeight - estimatedWeight);

        return result;
    }

    @Override
    public <T, E extends Exception> void executeWrite(Supplier<Integer> weigher, WriteQuery<E> query) throws E {
        int weight = getWeight(weigher, 1);
        rateLimiter.consumeWithBackoff(weight);

        // TODO(nziebart): decide what to do if we encounter a timeout exception
        query.execute();

        metrics.updateWriteCount();
        metrics.updateBytesWritten(weight);
    }

    // TODO(nziebart): error handling in the weight calculation should be responsibility of the caller
    private <T> Integer getWeight(Supplier<Integer> weigher, int fallback) {
        try {
            return weigher.get();
        } catch (Exception e) {
            log.warn("Exception while calculating response weight", e);
            return fallback;
        }
    }

}
