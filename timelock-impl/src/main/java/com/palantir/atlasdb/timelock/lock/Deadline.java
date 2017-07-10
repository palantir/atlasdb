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

package com.palantir.atlasdb.timelock.lock;

import org.immutables.value.Value;

import com.palantir.common.time.Clock;

@Value.Immutable
public interface Deadline {

    @Value.Parameter
    long getTimeMillis();

    default long getMillisRemaining(Clock clock) {
        return getTimeMillis() - clock.getTimeMillis();
    }

    static Deadline at(long timeMillis) {
        return ImmutableDeadline.of(timeMillis);
    }

    static Deadline fromTimeoutMillis(long timeoutMillis, Clock clock) {
        return at(clock.getTimeMillis() + timeoutMillis);
    }

    static Deadline expired() {
        return at(Long.MIN_VALUE);
    }

}
