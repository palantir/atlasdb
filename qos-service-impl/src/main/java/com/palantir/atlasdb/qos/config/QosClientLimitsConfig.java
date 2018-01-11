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

package com.palantir.atlasdb.qos.config;

import java.io.Serializable;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonDeserialize(as = ImmutableQosClientLimitsConfig.class)
@JsonSerialize(as = ImmutableQosClientLimitsConfig.class)
public abstract class QosClientLimitsConfig implements Serializable {
    // These numbers come from the metrics released and are scaled by a factor of 2,
    // we should probably scale up more or set to no limit?
    public static final long BYTES_READ_PER_SECOND_PER_CLIENT = 50_000_000L; // 50 MB/s
    public static final long BYTES_WRITTEN_PER_SECOND_PER_CLIENT = 10_000_000L; // 10 MB/s

    @Value.Default
    public QosLimitsConfig limits() {
        return ImmutableQosLimitsConfig.builder()
                .readBytesPerSecond(BYTES_READ_PER_SECOND_PER_CLIENT)
                .writeBytesPerSecond(BYTES_WRITTEN_PER_SECOND_PER_CLIENT)
                .build();
    }

    @Value.Default
    public QosPriority clientPriority() {
        return QosPriority.HIGH;
    }
}
