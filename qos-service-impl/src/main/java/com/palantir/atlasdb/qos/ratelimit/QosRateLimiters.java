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

package com.palantir.atlasdb.qos.ratelimit;

import java.util.function.Supplier;

import org.immutables.value.Value;

import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.common.concurrent.PTExecutors;

@Value.Immutable
public interface QosRateLimiters {

    static QosRateLimiters create(Supplier<Long> maxBackoffSleepTimeMillis, Supplier<Long> readLimitSupplier,
            Supplier<Long> writeLimitSupplier) {
        QosRateLimiter readLimiter = QosRateLimiter.create(maxBackoffSleepTimeMillis,
                readLimitSupplier,
                "read",
                PTExecutors.newSingleThreadScheduledExecutor(),
                QosClientLimitsConfig.BYTES_READ_PER_SECOND_PER_CLIENT);

        QosRateLimiter writeLimiter = QosRateLimiter.create(maxBackoffSleepTimeMillis,
                writeLimitSupplier,
                "write",
                PTExecutors.newSingleThreadScheduledExecutor(),
                QosClientLimitsConfig.BYTES_WRITTEN_PER_SECOND_PER_CLIENT);

        return ImmutableQosRateLimiters.builder()
                .read(readLimiter)
                .write(writeLimiter)
                .build();
    }

    QosRateLimiter read();

    QosRateLimiter write();

}
