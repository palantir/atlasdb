/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.timestamp;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class LastReturnedTimestamp {
    private long timestamp;

    public LastReturnedTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public synchronized void increaseToAtLeast(long newTimestamp) {
        long current = this.timestamp;
        if (newTimestamp > current) {
            this.timestamp = newTimestamp;
        }
    }

    public synchronized long get() {
        return this.timestamp;
    }
}
