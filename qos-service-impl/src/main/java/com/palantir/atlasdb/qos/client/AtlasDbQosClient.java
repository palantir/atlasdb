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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QosService;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiter;

public class AtlasDbQosClient implements QosClient {
    private final QosService qosService;
    private final String clientName;
    private final QosRateLimiter rateLimiter;

    private volatile long credits;

    public AtlasDbQosClient(QosService qosService,
            ScheduledExecutorService limitRefresher,
            String clientName,
            QosRateLimiter rateLimiter) {
        this.qosService = qosService;
        this.clientName = clientName;
        this.rateLimiter = rateLimiter;
        limitRefresher.scheduleAtFixedRate(() -> {
            try {
                credits = qosService.getLimit(clientName);
            } catch (Exception e) {
                // do nothing
            }
        }, 0L, 60L, TimeUnit.SECONDS);
    }

    // The KVS layer should call this before every read/write operation
    // Currently all operations are treated equally; each uses up a unit of credits
    @Override
    public void checkLimit() {
        // always return immediately - i.e. no backoff
        // TODO if soft-limited, pause
        // if hard-limited, throw exception
        if (credits > 0) {
            credits--;
        } else {
            // TODO This should be a ThrottleException?
            throw new RuntimeException("Rate limit exceeded");
        }
    }
}
