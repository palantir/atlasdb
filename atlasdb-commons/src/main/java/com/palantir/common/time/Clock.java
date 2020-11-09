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
package com.palantir.common.time;

import java.time.Instant;

public interface Clock {
    /**
     * @return The time in milliseconds. This is conventionally interpreted as the number of
     *         milliseconds since 1970-01-01T00:00Z excluding leap seconds not included in the
     *         present UTC day, which means it is not monotonic even if the underlying clock is
     *         perfectly accurate and never adjusted! Clock geekery aside, this should just return
     *         System.currentTimeMillis().
     */
    long getTimeMillis();

    default Instant instant() {
        return Instant.ofEpochMilli(getTimeMillis());
    }
}
